import fcntl
import os
import time
import struct
import logging
from pathlib import Path
import numpy as np

from typing import Union

from ..chunk import Chunk
from ..blob_sidecar import BlobRef, BlobSidecarWriters, ref_values
from ..ring import RingBuffer, _yield_cpu, ring_buffer_shm_names
from ...metadata.feed_registry import FeedRegistry, ON_DISK, UINT64_MAX
from ...metadata.feed_metadata import load_feed_metadata
from ...metadata.feed_schema import load_record_schema_for_feed
from ...metadata.segments import SegmentStore

log = logging.getLogger("dw.writer")


def _noop_note_write(*_args, **_kwargs) -> None:
    return None


def _noop_note_batch(*_args, **_kwargs) -> None:
    return None


def _load_durable_frontier(feed_dir, feed_name: str) -> tuple[int, int]:
    reg_path = feed_dir / f"{feed_name}.reg"
    if not reg_path.exists():
        return 0, 0
    try:
        registry = FeedRegistry(reg_path, mode="r")
    except (OSError, ValueError):
        # A brand-new persistent feed can briefly expose an empty/truncated
        # registry file while metadata is still being initialized.
        return 0, 0
    try:
        total_records = 0
        latest_ts = 0
        latest_chunk_id = registry.get_latest_chunk_idx() or 0
        for chunk_id in range(1, int(latest_chunk_id) + 1):
            meta = registry.get_chunk_metadata(chunk_id)
            try:
                total_records += int(meta.num_records)
                qmax = int(meta.get_qmax(0))
                qmin = int(meta.get_qmin(0))
                if qmax and qmin != UINT64_MAX and qmax >= qmin:
                    latest_ts = qmax
            finally:
                meta.release()
        return total_records, latest_ts
    finally:
        registry.close()


def _persistent_ring_owner_healthy(base_path: Path) -> bool:
    try:
        from ...metadata.global_registry import GlobalRegistry

        registry = GlobalRegistry(base_path)
        try:
            owner = registry.get_persistent_ring_owner()
            return bool(owner.get("healthy") and not owner.get("stale"))
        finally:
            registry.close()
    except Exception:
        return False


def _blob_index_record(writer, timestamp: int, sidecar_name: str, ref: BlobRef) -> dict:
    sidecars = writer._blob_writers.sidecars
    sidecar = sidecars[sidecar_name]
    fields = sidecar.ref_fields
    record = {name: 0 for name in writer._value_fields}
    record[writer._value_fields[0]] = int(timestamp)
    (
        record[fields.chunk_id],
        record[fields.offset],
        record[fields.size],
        record[fields.codec],
        record[fields.schema_id],
        record[fields.flags],
        record[fields.crc],
    ) = ref_values(ref)
    return record


def _write_blob(writer, timestamp: int, payload, *, sidecar: str, codec: str | None, schema_id: int, flags: int) -> BlobRef:
    if writer._blob_writers is None:
        writer._blob_writers = BlobSidecarWriters(writer.base_path, writer.feed_name)
    sidecar_meta = writer._blob_writers.sidecars[sidecar]
    selected_codec = sidecar_meta.codec if codec is None else str(codec)
    ref = writer._blob_writers[sidecar].write(payload, codec=selected_codec, schema_id=schema_id, flags=flags)
    writer.write_dict(_blob_index_record(writer, timestamp, sidecar, ref))
    return ref


class RingWriter:
    """Single-writer for ring-backed feeds."""

    def __init__(self, base_path, feed_name: str):
        self.base_path = Path(base_path)
        self.feed_name = feed_name
        self.feed_metadata = load_feed_metadata(self.base_path, feed_name)
        if self.feed_metadata is None:
            raise KeyError(feed_name)
        self.record_format = load_record_schema_for_feed(self.base_path, feed_name)
        self._persist = bool(self.feed_metadata.persist)
        self._segment_tracking = bool(self.feed_metadata.segment_tracking)
        self._prefault_ring = bool(self.feed_metadata.prefault_ring)
        self._writer_lock_fd = None
        self._closed = False
        self._blob_writers = None

        self._S = struct.Struct(self.record_format.fmt)
        self._u64 = struct.Struct("<Q")
        self._rec_size = self._S.size
        value_fields = [f.name for f in self.record_format.fields if f.name != "_"]
        self._value_fields = tuple(value_fields)
        self._ts_idx = 0 if value_fields else None

        ring_bytes = int(self.feed_metadata.ring_size_bytes or self.feed_metadata.chunk_size_bytes)
        if ring_bytes < self._rec_size:
            raise ValueError(f"ring size {ring_bytes} too small for record size {self._rec_size}")

        lock_path = self.base_path / "data" / feed_name / f"{feed_name}.writer.lock"
        self._writer_lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(self._writer_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self._writer_lock_fd)
            self._writer_lock_fd = None
            raise RuntimeError(f"ring writer locked by another writer for feed '{feed_name}'")

        shm_names = ring_buffer_shm_names(self.base_path, self.feed_name)
        self.ring = RingBuffer(
            self.feed_name,
            data_size=ring_bytes,
            create=True,
            shm_name=shm_names[0],
        )
        self._ring_write_pos = self.ring._write_pos
        self._ring_start_pos = self.ring._start_pos
        self._ring_generation = self.ring._generation
        self._ring_last_ts = self.ring._last_ts
        self._ring_record_count = self.ring._record_count
        self._ring_durable_record_count = self.ring._durable_record_count
        self._ring_data = self.ring.data
        if self._prefault_ring:
            self._prefault_ring_pages()
        self._ring_capacity = self.ring.data_size // self._rec_size
        self.segment_store = SegmentStore(self.base_path / "data" / feed_name, feed_name) if self._segment_tracking else None
        self._bootstrap_persisted_ring()
        if self._persist:
            self._repair_persisted_chunks_for_recovery()
            self._bootstrap_persisted_ring()
        if self.segment_store is not None:
            last_ts = int(self._ring_last_ts[0])
            self.segment_store.recover_open_segment(last_ts if last_ts > 0 else None)
            self._segment_note_write = self.segment_store.note_write_one
            self._segment_note_batch = self.segment_store.note_batch
        else:
            self._segment_note_write = _noop_note_write
            self._segment_note_batch = _noop_note_batch

    @property
    def closed(self) -> bool:
        return bool(self._closed)

    def __enter__(self) -> "RingWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError(f"ring writer is closed for feed '{self.feed_name}'")

    def _prefault_ring_pages(self) -> None:
        try:
            mv = self._ring_data
            step = max(4096, os.sysconf("SC_PAGE_SIZE"))
            acc = 0
            ring_size = self.ring.data_size
            for i in range(0, ring_size, step):
                acc ^= mv[i]
            if ring_size > 0:
                acc ^= mv[ring_size - 1]
            self._prefault_checksum = acc
        except Exception:
            pass

    def _bootstrap_persisted_ring(self) -> None:
        if not self._persist:
            return
        durable_count, durable_last_ts = _load_durable_frontier(self.base_path / "data" / self.feed_name, self.feed_name)
        write_pos, start_pos, generation, last_ts, record_count, ring_durable_count, ring_durable_last_ts, overrun_count, lost_records = self.ring.header()
        if record_count == 0 and ring_durable_count == 0 and durable_count > 0:
            self.ring.update_live_header(0, 0, 0, durable_last_ts, durable_count)
            self.ring.update_durable_header(
                durable_record_count=durable_count,
                durable_last_ts=durable_last_ts,
                overrun_count=overrun_count,
                lost_records=lost_records,
            )
            return
        if durable_count > ring_durable_count:
            self.ring.update_durable_header(
                durable_record_count=durable_count,
                durable_last_ts=max(ring_durable_last_ts, durable_last_ts),
                overrun_count=overrun_count,
                lost_records=lost_records,
            )
            if record_count < durable_count:
                self.ring.update_live_header(
                    write_pos,
                    start_pos,
                    generation,
                    max(last_ts, durable_last_ts),
                    durable_count,
                )

    def _repair_persisted_chunks_for_recovery(self) -> None:
        reg_path = self.base_path / "data" / self.feed_name / f"{self.feed_name}.reg"
        if not reg_path.exists():
            return
        try:
            from ...ops import repair

            registry = FeedRegistry(reg_path, mode="w")
        except Exception:
            return
        try:
            latest_idx = registry.get_latest_chunk_idx() or 0
            for chunk_id in range(1, int(latest_idx) + 1):
                meta = registry.get_chunk_metadata(chunk_id)
                try:
                    repair.validate_and_repair_chunk(
                        chunk_id=chunk_id,
                        meta=meta,
                        feed_name=self.feed_name,
                        feed_dir=self.base_path / "data" / self.feed_name,
                        record_format=self.record_format,
                    )
                finally:
                    meta.release()
        finally:
            registry.close()

    def _wait_for_persisted_space(self, needed_records: int) -> None:
        while (
            self._persist
            and int(self._ring_record_count[0]) - int(self._ring_durable_record_count[0]) + needed_records > self._ring_capacity
        ):
            _yield_cpu()

    def _write_bytes(self, payload: bytes, last_ts: int) -> int:
        self._ensure_open()
        if len(payload) != self._rec_size:
            raise ValueError(f"record length {len(payload)} != expected {self._rec_size}")
        while self._persist and int(self._ring_record_count[0]) - int(self._ring_durable_record_count[0]) >= self._ring_capacity:
            _yield_cpu()
        ring_size = self.ring.data_size
        write_pos = int(self._ring_write_pos[0])
        generation = int(self._ring_generation[0])
        start_pos = int(self._ring_start_pos[0])
        record_count = int(self._ring_record_count[0])
        slice_end = write_pos + self._rec_size
        next_write_pos = 0 if slice_end == ring_size else slice_end
        if record_count >= self._ring_capacity:
            start_pos = next_write_pos
        self._ring_data[write_pos:slice_end] = payload
        self.ring.update_live_header(next_write_pos, start_pos, generation, int(last_ts), record_count + 1)
        self._segment_note_write(last_ts)
        return next_write_pos

    def write(self, timestamp: int, record_data: bytes) -> int:
        self._ensure_open()
        while self._persist and int(self._ring_record_count[0]) - int(self._ring_durable_record_count[0]) >= self._ring_capacity:
            _yield_cpu()
        ring_size = self.ring.data_size
        write_pos = int(self._ring_write_pos[0])
        generation = int(self._ring_generation[0])
        start_pos = int(self._ring_start_pos[0])
        record_count = int(self._ring_record_count[0])
        slice_end = write_pos + self._rec_size
        next_write_pos = 0 if slice_end == ring_size else slice_end
        if record_count >= self._ring_capacity:
            start_pos = next_write_pos
        self._ring_data[write_pos:slice_end] = record_data
        self.ring.update_live_header(next_write_pos, start_pos, generation, int(timestamp), record_count + 1)
        self._segment_note_write(timestamp)
        return next_write_pos

    write_fast = write

    def write_values(self, *vals) -> int:
        self._ensure_open()
        last_ts = vals[self._ts_idx] if self._ts_idx is not None else 0
        while self._persist and int(self._ring_record_count[0]) - int(self._ring_durable_record_count[0]) >= self._ring_capacity:
            _yield_cpu()
        ring_size = self.ring.data_size
        write_pos = int(self._ring_write_pos[0])
        generation = int(self._ring_generation[0])
        start_pos = int(self._ring_start_pos[0])
        record_count = int(self._ring_record_count[0])
        next_write_pos = write_pos + self._rec_size
        if next_write_pos == ring_size:
            next_write_pos = 0
        if record_count >= self._ring_capacity:
            start_pos = next_write_pos
        self._S.pack_into(self._ring_data, write_pos, *vals)
        last_ts_i = int(last_ts)
        self.ring.update_live_header(next_write_pos, start_pos, generation, last_ts_i, record_count + 1)
        self._segment_note_write(last_ts_i)
        return next_write_pos

    def write_tuple(self, record: tuple) -> int:
        return self.write_values(*record)

    def write_dict(self, record: dict) -> int:
        vals = tuple(record[name] for name in self._value_fields)
        return self.write_values(*vals)

    def write_blob(
        self,
        timestamp: int,
        payload,
        *,
        sidecar: str,
        codec: str | None = None,
        schema_id: int = 0,
        flags: int = 0,
    ) -> BlobRef:
        """Append a sidecar payload, then publish its fixed-width index row."""
        return _write_blob(
            self,
            timestamp,
            payload,
            sidecar=sidecar,
            codec=codec,
            schema_id=schema_id,
            flags=flags,
        )

    def write_batch_bytes(self, data: bytes) -> int:
        self._ensure_open()
        rec_sz = self._rec_size
        n = len(data)
        if n == 0 or n % rec_sz != 0:
            raise ValueError("batch length must be a positive multiple of record_size")
        ring_size = self.ring.data_size
        if n > ring_size:
            raise ValueError("batch larger than ring capacity")

        batch_records = n // rec_sz
        while (
            self._persist
            and int(self._ring_record_count[0]) - int(self._ring_durable_record_count[0]) + batch_records > self._ring_capacity
        ):
            _yield_cpu()

        write_pos = int(self._ring_write_pos[0])
        generation = int(self._ring_generation[0])
        start_pos = int(self._ring_start_pos[0])
        record_count = int(self._ring_record_count[0])
        last_ts_prev = int(self._ring_last_ts[0])

        new_record_count = record_count + batch_records
        old_earliest = max(0, record_count - self._ring_capacity)
        new_earliest = max(0, new_record_count - self._ring_capacity)
        advanced = new_earliest - old_earliest
        if advanced > 0:
            start_pos = (start_pos + (advanced * rec_sz)) % ring_size

        end = write_pos + n
        self._ring_data[write_pos:end] = data
        if end > ring_size:
            generation += 1

        unpack_from = self._u64.unpack_from
        first_ts = unpack_from(data, 0)[0]
        last_ts = unpack_from(data, n - rec_sz)[0]
        end_pos = end % ring_size
        self.ring.update_live_header(end_pos, start_pos, generation, last_ts if last_ts else last_ts_prev, new_record_count)
        self._segment_note_batch(first_ts, last_ts, batch_records)
        return end_pos

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._persist:
            if int(self._ring_durable_record_count[0]) < int(self._ring_record_count[0]):
                self.ring.request_drain()
                if _persistent_ring_owner_healthy(self.base_path):
                    deadline = time.monotonic() + 30.0
                    while time.monotonic() < deadline:
                        if int(self._ring_durable_record_count[0]) >= int(self._ring_record_count[0]):
                            break
                        _yield_cpu()
        if self.segment_store is not None:
            self.segment_store.close_open_segment("writer_close")
        if self._blob_writers is not None:
            self._blob_writers.close()
            self._blob_writers = None
        try:
            self.ring.close(unlink=False)
        except Exception:
            pass
        if self._writer_lock_fd is not None:
            try:
                fcntl.flock(self._writer_lock_fd, fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                os.close(self._writer_lock_fd)
            except Exception:
                pass
            self._writer_lock_fd = None

    def mark_segment_boundary(self, reason: str = "disconnect") -> bool:
        if self.segment_store is None:
            return False
        return self.segment_store.close_open_segment(reason)


class ChunkWriter:
    __slots__ = (
        "base_path", "feed_name", "feed_metadata", "record_format", "my_pid",
        "data_dir", "registry",
        "current_chunk", "current_chunk_metadata", "current_chunk_id",
        "_S", "_rec_size", "_clock_level", "_key_min_set", "_u64", "_qmins", "_qmaxs",
        "_value_fields", "_blob_writers",
        "segment_store", "_segment_note_write", "_segment_note_batch",
    )

    def __init__(self, base_path, feed_name: str, *, segment_tracking: bool | None = None):
        self.base_path = Path(base_path)
        self.feed_name = feed_name
        self.feed_metadata = load_feed_metadata(self.base_path, feed_name)
        if self.feed_metadata is None:
            raise KeyError(feed_name)
        self.record_format = load_record_schema_for_feed(self.base_path, feed_name)
        self.my_pid = os.getpid()

        self.data_dir = self.base_path / "data" / feed_name
        self.registry = None
        self.current_chunk = None
        self.current_chunk_metadata = None
        self.segment_store = None
        self._key_min_set = None
        self._blob_writers = None

        try:
            self.registry = FeedRegistry(self.data_dir / f"{feed_name}.reg", mode="w")
            self.current_chunk_id = self.registry.get_latest_chunk_idx() or 0

            if self.current_chunk_id > 0:
                self._validate_chunk()

            self._S = struct.Struct(self.record_format.fmt)
            self._u64 = struct.Struct("<Q")
            self._rec_size = self._S.size
            self._clock_level = self.feed_metadata.clock_level
            self._value_fields = tuple(f.name for f in self.record_format.fields if f.name != "_")
            if bool(self.feed_metadata.segment_tracking if segment_tracking is None else segment_tracking):
                self.segment_store = SegmentStore(self.data_dir, self.feed_name)
                self._segment_note_write = self.segment_store.note_write_one
                self._segment_note_batch = self.segment_store.note_batch
                if self.segment_store.has_open_segment():
                    self.segment_store.recover_open_segment(self._latest_level1_timestamp())
            else:
                self.segment_store = None
                self._segment_note_write = _noop_note_write
                self._segment_note_batch = _noop_note_batch

            self._create_new_chunk()
        except Exception:
            try:
                self._close_current_chunk_file()
            except Exception:
                pass
            if self.current_chunk_metadata is not None:
                try:
                    self.current_chunk_metadata.release()
                except Exception:
                    pass
                self.current_chunk_metadata = None
            if self.registry is not None:
                try:
                    self.registry.close()
                except Exception:
                    pass
                self.registry = None
            if self.segment_store is not None:
                try:
                    self.segment_store.close_open_segment("writer_init_failed")
                except Exception:
                    pass
                self.segment_store = None
            raise

    @property
    def chunk_record_capacity(self) -> int:
        return int(self.feed_metadata.chunk_size_bytes) // self._rec_size

    def __enter__(self) -> "ChunkWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def _latest_level1_timestamp(self):
        latest_idx = self.registry.get_latest_chunk_idx()
        if latest_idx is None:
            return None

        idx = int(latest_idx)
        while idx > 0:
            meta = self.registry.get_chunk_metadata(idx)
            try:
                qmin = meta.get_qmin(0)
                qmax = meta.get_qmax(0)
                if qmax != 0 and qmin != UINT64_MAX and qmin <= qmax:
                    return int(qmax)
            finally:
                meta.release()
            idx -= 1
        return None

    def _create_new_chunk(self):
        self.current_chunk_id += 1

        new_start_time = None
        if self.current_chunk_metadata is not None:
            self._close_current_chunk_file()
            self.current_chunk_metadata.status = ON_DISK
            self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
            new_start_time = self.current_chunk_metadata.end_time
            self.current_chunk_metadata.release()
            self.current_chunk_metadata = None

        self._key_min_set = [False for _ in range(self._clock_level)]

        chunk_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
        self.current_chunk = Chunk.create_file(
            path=str(chunk_path),
            size=self.feed_metadata.chunk_size_bytes,
        )
        self.registry.register_chunk(
            time.time_ns() // 1_000,
            self.current_chunk_id,
            self.feed_metadata.chunk_size_bytes,
            status=ON_DISK,
            clock_level=self._clock_level,
        )

        self.current_chunk_metadata = self.registry.get_chunk_metadata(self.current_chunk_id)
        if new_start_time is not None:
            self.current_chunk_metadata.start_time = new_start_time
        self.current_chunk_metadata.clock_level = self._clock_level
        self._qmins = self.current_chunk_metadata._qmins
        self._qmaxs = self.current_chunk_metadata._qmaxs

    def _close_current_chunk_file(self) -> None:
        if self.current_chunk is not None:
            self.current_chunk.close_file()

    def _discard_current_chunk(self) -> None:
        if self.current_chunk is None or self.current_chunk_metadata is None:
            return
        chunk_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
        try:
            self.current_chunk.buffer.release()
        except Exception:
            pass
        mm = self.current_chunk.closeables[0]
        fd = self.current_chunk.closeables[1]
        try:
            mm.close()
        except Exception:
            pass
        try:
            os.close(fd)
        except Exception:
            pass
        self.current_chunk_metadata.release()
        self.current_chunk = None
        self.current_chunk_metadata = None
        try:
            os.unlink(chunk_path)
        except FileNotFoundError:
            pass
        self.registry.pop_latest_chunk()
        self.current_chunk_id -= 1

    def _finalize_current_chunk(self, *, discard_empty: bool = False) -> None:
        if self.current_chunk is None or self.current_chunk_metadata is None:
            return
        if discard_empty and int(self.current_chunk_metadata.num_records) == 0:
            self._discard_current_chunk()
            return
        self._close_current_chunk_file()
        self.current_chunk_metadata.status = ON_DISK
        self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
        self.current_chunk_metadata.release()
        self.current_chunk = None
        self.current_chunk_metadata = None

    def _validate_chunk(self):
        from ...ops import repair

        meta = self.registry.get_chunk_metadata(self.current_chunk_id)
        if meta is None:
            return

        try:
            repair.validate_and_repair_chunk(
                chunk_id=self.current_chunk_id,
                meta=meta,
                feed_name=self.feed_name,
                feed_dir=self.data_dir,
                record_format=self.record_format,
            )
        finally:
            meta.release()

    def write(self, timestamp: int, record_data: Union[bytes, np.ndarray]) -> int:
        if self.current_chunk is None:
            return 0

        if isinstance(record_data, np.ndarray):
            record_data = record_data.tobytes()
        view = memoryview(record_data).cast("B")
        record_size = view.nbytes
        if record_size != self._rec_size:
            raise ValueError(f"record length {record_size} != expected {self._rec_size}")

        meta = self.current_chunk_metadata
        position = meta._wp[0]
        if record_size > meta._size[0] - position:
            self._create_new_chunk()
            meta = self.current_chunk_metadata
            position = meta._wp[0]

        u64 = self._u64
        self.current_chunk.buffer[position:position + record_size] = view
        ts0 = int(timestamp)

        key_min_set = self._key_min_set
        key_count = self._clock_level
        qmins = self._qmins
        qmaxs = self._qmaxs
        self._segment_note_write(ts0)
        if not key_min_set[0]:
            key_min_set[0] = True
            qmins[0] = ts0
        qmaxs[0] = ts0
        if key_count >= 2:
            ts1 = u64.unpack_from(record_data, 8)[0]
            if not key_min_set[1]:
                key_min_set[1] = True
                qmins[1] = ts1
            qmaxs[1] = ts1
        if key_count >= 3:
            ts2 = u64.unpack_from(record_data, 16)[0]
            if not key_min_set[2]:
                key_min_set[2] = True
                qmins[2] = ts2
            qmaxs[2] = ts2
        end = position + record_size
        meta._wp[0] = end
        meta._nr[0] += 1
        return end

    write_fast = write

    def write_values(self, *vals) -> int:
        if self.current_chunk is None:
            return 0
        if self.current_chunk_metadata.write_pos + self._rec_size > self.current_chunk_metadata.size:
            self._create_new_chunk()
        pos = self.current_chunk_metadata.write_pos
        self._S.pack_into(self.current_chunk.buffer, pos, *vals)
        key_min_set = self._key_min_set
        key_count = self._clock_level
        qmins = self._qmins
        qmaxs = self._qmaxs
        ts0 = vals[0]
        self._segment_note_write(ts0)
        if not key_min_set[0]:
            key_min_set[0] = True
            qmins[0] = ts0
        qmaxs[0] = ts0
        if key_count >= 2:
            ts1 = vals[1]
            if not key_min_set[1]:
                key_min_set[1] = True
                qmins[1] = ts1
            qmaxs[1] = ts1
        if key_count >= 3:
            ts2 = vals[2]
            if not key_min_set[2]:
                key_min_set[2] = True
                qmins[2] = ts2
            qmaxs[2] = ts2
        self.current_chunk_metadata.write_pos = pos + self._rec_size
        self.current_chunk_metadata.num_records += 1
        return self.current_chunk_metadata.write_pos

    def write_tuple(self, record: tuple) -> int:
        return self.write_values(*record)

    def write_dict(self, record: dict) -> int:
        vals = tuple(record[name] for name in self._value_fields)
        return self.write_values(*vals)

    def write_blob(
        self,
        timestamp: int,
        payload,
        *,
        sidecar: str,
        codec: str | None = None,
        schema_id: int = 0,
        flags: int = 0,
    ) -> BlobRef:
        """Append a sidecar payload, then publish its fixed-width index row."""
        return _write_blob(
            self,
            timestamp,
            payload,
            sidecar=sidecar,
            codec=codec,
            schema_id=schema_id,
            flags=flags,
        )

    def write_batch_bytes(self, data) -> int:
        if self.current_chunk is None:
            return 0

        view = data if isinstance(data, memoryview) else memoryview(data)
        data_len = len(view)
        if data_len == 0 or data_len % self._rec_size != 0:
            raise ValueError("batch length must be a positive multiple of record_size")
        if self.current_chunk_metadata.write_pos + data_len > self.current_chunk_metadata.size:
            self._create_new_chunk()
        start = self.current_chunk_metadata.write_pos
        end = start + data_len
        rec_sz = self._rec_size
        self.current_chunk.buffer[start:end] = view

        key_min_set = self._key_min_set
        key_count = self._clock_level
        qmins = self._qmins
        qmaxs = self._qmaxs
        u64 = self._u64
        ts_first = u64.unpack_from(view, 0)[0]
        ts_last = u64.unpack_from(view, data_len - rec_sz)[0]
        self._segment_note_batch(ts_first, ts_last, data_len // rec_sz)
        if not key_min_set[0]:
            key_min_set[0] = True
            qmins[0] = ts_first
        qmaxs[0] = ts_last
        if key_count >= 2:
            ts_first = u64.unpack_from(view, 8)[0]
            ts_last = u64.unpack_from(view, data_len - rec_sz + 8)[0]
            if not key_min_set[1]:
                key_min_set[1] = True
                qmins[1] = ts_first
            qmaxs[1] = ts_last
        if key_count >= 3:
            ts_first = u64.unpack_from(view, 16)[0]
            ts_last = u64.unpack_from(view, data_len - rec_sz + 16)[0]
            if not key_min_set[2]:
                key_min_set[2] = True
                qmins[2] = ts_first
            qmaxs[2] = ts_last
        self.current_chunk_metadata.write_pos = end
        self.current_chunk_metadata.num_records += data_len // rec_sz
        return self.current_chunk_metadata.write_pos

    def commit_chunk_bytes(self, data) -> int:
        view = data if isinstance(data, memoryview) else memoryview(data)
        data_len = len(view)
        if data_len == 0 or data_len % self._rec_size != 0:
            raise ValueError("chunk length must be a positive multiple of record_size")
        if data_len > int(self.feed_metadata.chunk_size_bytes):
            raise ValueError("chunk payload exceeds configured chunk size")
        self.begin_chunk_commit()
        written = self.write_batch_bytes(view)
        self.finish_chunk_commit()
        return written

    def begin_chunk_commit(self) -> None:
        if self.current_chunk is None:
            self._create_new_chunk()
        elif int(self.current_chunk_metadata.num_records) != 0:
            self._create_new_chunk()

    def finish_chunk_commit(self) -> None:
        self._finalize_current_chunk(discard_empty=True)

    def close(self):
        try:
            if self.current_chunk is not None:
                self._finalize_current_chunk(discard_empty=True)
        finally:
            try:
                if self.registry is not None:
                    self.registry.close()
            finally:
                if self._blob_writers is not None:
                    self._blob_writers.close()
                    self._blob_writers = None
                if self.segment_store is not None:
                    self.segment_store.close_open_segment("writer_close")

    def mark_segment_boundary(self, reason: str = "disconnect") -> bool:
        if self.current_chunk is None or self.segment_store is None:
            return False
        return self.segment_store.close_open_segment(reason)
