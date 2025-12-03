import struct
import time
from multiprocessing import shared_memory
from typing import Optional

from .utils.process import ProcessUtils


class RingBuffer:
    """
    Shared-memory ring buffer for fixed-size records.
    Layout: header (write_pos, generation, last_ts) followed by data region.
    """

    _HEADER = struct.Struct("<QQQ")  # write_pos, generation, last_ts

    def __init__(self, name: str, data_size: int, create: bool = False):
        if data_size <= 0:
            raise ValueError("data_size must be > 0")
        # align to page to reduce fragmentation
        if data_size % 4096 != 0:
            data_size = (data_size + 4095) & ~4095
        self.name = name
        self.data_size = data_size
        self.total_size = self._HEADER.size + data_size
        try:
            self.shm = shared_memory.SharedMemory(name=name, create=create, size=self.total_size)
            self.created = create
        except FileExistsError:
            self.shm = shared_memory.SharedMemory(name=name, create=False)
            self.created = False
        self.buf = self.shm.buf
        self.data = self.buf[self._HEADER.size:]
        if self.created:
            self._HEADER.pack_into(self.buf, 0, 0, 0, 0)

    def header(self) -> tuple[int, int, int]:
        return self._HEADER.unpack_from(self.buf, 0)

    def update_header(self, write_pos: int, generation: int, last_ts: int) -> None:
        self._HEADER.pack_into(self.buf, 0, write_pos, generation, last_ts)

    def close(self, unlink: bool = False) -> None:
        try:
            self.data.release()
        except Exception:
            pass
        try:
            self.buf.release()
        except Exception:
            pass
        self.shm.close()
        if unlink:
            try:
                self.shm.unlink()
            except FileNotFoundError:
                pass


class RingWriter:
    """Single-writer for live-only feeds backed by a shared-memory ring."""

    def __init__(self, platform, feed_name: str):
        self.platform = platform
        self.feed_name = feed_name
        self.feed_config = platform.lifecycle(feed_name)
        self.record_format = platform.get_record_format(feed_name)
        self.my_pid = ProcessUtils.get_current_pid()

        self._S = struct.Struct(self.record_format["fmt"])
        self._rec_size = self._S.size
        value_fields = [f.get("name") for f in self.record_format["fields"] if f.get("name") != "_"]
        ts_name = self.record_format.get("ts_name")
        try:
            self._ts_idx = value_fields.index(ts_name) if ts_name else None
        except ValueError:
            self._ts_idx = None

        self.ring_bytes = int(self.feed_config.get("chunk_size_bytes", 8 * 1024 * 1024))
        if self.ring_bytes < self._rec_size:
            raise ValueError(f"ring size {self.ring_bytes} too small for record size {self._rec_size}")

        ring_name = f"{self.feed_name}-ring"
        self.ring = RingBuffer(ring_name, data_size=self.ring_bytes, create=True)

    def _write_bytes(self, payload: bytes, last_ts: int) -> int:
        if len(payload) != self._rec_size:
            raise ValueError(f"record length {len(payload)} != expected {self._rec_size}")
        hdr = self.ring.header()
        write_pos, generation = hdr[0], hdr[1]
        if write_pos + self._rec_size > self.ring_bytes:
            write_pos = 0
            generation += 1
        self.ring.data[write_pos:write_pos + self._rec_size] = payload
        write_pos += self._rec_size
        self.ring.update_header(write_pos, generation, last_ts)
        return write_pos

    def write(self, timestamp: int, record_data: bytes, create_index: bool = False) -> int:
        """Byte-oriented write; create_index is ignored for rings."""
        _ = create_index
        return self._write_bytes(record_data, last_ts=timestamp)

    def write_values(self, *vals) -> int:
        """Pack values into the ring; overwrites oldest data when full."""
        last_ts = vals[self._ts_idx] if self._ts_idx is not None else 0
        buf = bytearray(self._rec_size)
        self._S.pack_into(buf, 0, *vals)
        return self._write_bytes(bytes(buf), last_ts=last_ts)

    def write_batch_bytes(self, data: bytes) -> int:
        """Write a blob of packed records (len must be multiple of record_size) with one header update."""
        rec_sz = self._rec_size
        n = len(data)
        if n == 0 or n % rec_sz != 0:
            raise ValueError("batch length must be a multiple of record_size")
        write_pos, generation, _ = self.ring.header()
        # If batch won't fit, wrap and overwrite oldest
        if write_pos + n > self.ring_bytes:
            write_pos = 0
            generation += 1
            if n > self.ring_bytes:
                raise ValueError("batch larger than ring capacity")
        end = write_pos + n
        self.ring.data[write_pos:end] = data
        # last ts is unknown here; keep previous
        self.ring.update_header(end, generation, 0)
        return end

    def resize(self, new_size_bytes: int):
        """Recreate the ring with a new size."""
        if new_size_bytes <= 0:
            raise ValueError("new ring size must be >0")
        new_size_bytes = int(new_size_bytes)
        old = self.ring
        try:
            old.close(unlink=True)
        except Exception:
            pass
        self.feed_config["chunk_size_bytes"] = new_size_bytes
        try:
            self.platform.registry.update_metadata(self.feed_name, chunk_size_bytes=new_size_bytes)
        except Exception:
            pass
        self.ring_bytes = new_size_bytes
        self.ring = RingBuffer(f"{self.feed_name}-ring", data_size=new_size_bytes, create=True)

    def close(self):
        self.ring.close()


class RingReader:
    """Reader that tails a shared-memory ring without chunk/index files."""

    def __init__(self, platform, feed_name: str):
        self.platform = platform
        self.feed_name = feed_name
        self.record_format = platform.get_record_format(feed_name)
        self._S = struct.Struct(self.record_format["fmt"])
        self._rec_size = self._S.size
        self.ring_bytes = int(platform.lifecycle(feed_name).get("chunk_size_bytes", 8 * 1024 * 1024))
        ring_name = f"{self.feed_name}-ring"
        self.ring = RingBuffer(ring_name, data_size=self.ring_bytes, create=False)
        self.read_pos = self.ring.header()[0]
        self.read_gen = self.ring.header()[1]

    def stream_live(self):
        """Yield records as they appear; skips data if the reader falls behind."""
        data = self.ring.data
        rec_sz = self._rec_size
        ring_sz = self.ring_bytes
        read_pos = self.read_pos
        read_gen = self.read_gen

        while True:
            write_pos, write_gen, _ = self.ring.header()

            # Detect overwrite: writer wrapped past our position.
            if write_gen > read_gen + 1 or (write_gen > read_gen and write_pos >= read_pos):
                read_gen = write_gen
                read_pos = write_pos

            if read_gen == write_gen:
                if read_pos < write_pos:
                    record = self._S.unpack_from(data, read_pos)
                    read_pos += rec_sz
                    if read_pos >= ring_sz:
                        read_pos = 0
                        read_gen += 1
                    yield record
                    continue
            else:
                # consume tail then wrap to start
                if read_pos + rec_sz <= ring_sz:
                    record = self._S.unpack_from(data, read_pos)
                    read_pos += rec_sz
                    if read_pos >= ring_sz:
                        read_pos = 0
                        read_gen = write_gen
                    yield record
                    continue

            time.sleep(0.0005)

    def stream_latest_records(self, playback: bool = False):
        """Compatibility shim to mirror chunked Reader API."""
        _ = playback  # playback is not supported on rings
        yield from self.stream_live()

    def stream_latest_records_raw(self):
        """Yield raw memoryviews from the ring."""
        data = self.ring.data
        rec_sz = self._rec_size
        ring_sz = self.ring_bytes
        read_pos = self.read_pos
        read_gen = self.read_gen

        while True:
            write_pos, write_gen, _ = self.ring.header()
            if write_gen > read_gen + 1 or (write_gen > read_gen and write_pos >= read_pos):
                read_gen = write_gen
                read_pos = write_pos

            if read_gen == write_gen:
                if read_pos < write_pos:
                    mv = memoryview(data)[read_pos:read_pos+rec_sz]
                    read_pos += rec_sz
                    if read_pos >= ring_sz:
                        read_pos = 0
                        read_gen += 1
                    yield mv
                    continue
            else:
                if read_pos + rec_sz <= ring_sz:
                    mv = memoryview(data)[read_pos:read_pos+rec_sz]
                    read_pos += rec_sz
                    if read_pos >= ring_sz:
                        read_pos = 0
                        read_gen = write_gen
                    yield mv
                    continue
            time.sleep(0.0005)

    def read_batch(self, count: int):
        """Yield batches of packed records."""
        batch = []
        for rec in self.stream_latest_records(playback=False):
            batch.append(rec)
            if len(batch) >= count:
                yield batch
                batch = []

    def read_batch_raw(self, count: int):
        """Yield batches of raw memoryviews."""
        batch = []
        for rec in self.stream_latest_records_raw():
            batch.append(rec)
            if len(batch) >= count:
                yield batch
                batch = []

    def stream_time_range(self, start_time: int, end_time: Optional[int] = None):
        """Rings are live-only; time windows are unsupported."""
        raise NotImplementedError("Ring feeds are live-only; time windows unavailable.")

    def get_latest_record(self) -> tuple:
        """Return the most recent record currently written."""
        write_pos, write_gen, _ = self.ring.header()
        if write_pos == 0 and write_gen == 0:
            raise RuntimeError("No records written yet.")
        if write_pos == 0:
            pos = self.ring_bytes - self._rec_size
        else:
            pos = max(0, write_pos - self._rec_size)
        return self._S.unpack_from(self.ring.data, pos)

    def close(self):
        self.ring.close()
