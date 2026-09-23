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
from ...ops import repair
from ...metadata.feed_registry import FeedRegistry, ON_DISK, IN_MEMORY, UINT64_MAX
from ...metadata.feed_metadata import load_feed_metadata
from ...metadata.feed_schema import load_record_schema_for_feed

log = logging.getLogger("dw.writer")


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


def _blob_index_record(writer, timestamp: int, sidecar_name: str, ref: BlobRef, index_values=None) -> dict:
    sidecars = writer._blob_writers.sidecars
    sidecar = sidecars[sidecar_name]
    fields = sidecar.ref_fields
    record = {name: 0 for name in writer._value_fields}
    if index_values:
        reserved = {writer._value_fields[0], *(getattr(fields, key) for key in ("chunk_id", "offset", "size", "codec", "schema_id", "flags", "crc"))}
        for key, value in index_values.items():
            if key not in record or key in reserved:
                raise ValueError(f"invalid custom blob index field: {key!r}")
            record[key] = value
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


def _write_blob(writer, timestamp: int, payload, *, sidecar: str, codec: str | None, schema_id: int, flags: int, index_values=None) -> BlobRef:
    writer._ensure_open()
    if writer._blob_writers is None:
        writer._blob_writers = BlobSidecarWriters(writer.base_path, writer.feed_name)
    sidecar_meta = writer._blob_writers.sidecars[sidecar]
    selected_codec = sidecar_meta.codec if codec is None else str(codec)
    storage = writer._blob_writers[sidecar]
    try:
        ref = storage.write(payload, codec=selected_codec, schema_id=schema_id, flags=flags)
        writer.write_dict(_blob_index_record(writer, timestamp, sidecar, ref, index_values))
        storage.publish()
    except BaseException:
        storage.abort()
        raise
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
        try:
            self.ring = RingBuffer(
                self.feed_name,
                data_size=ring_bytes,
                create=True,
                shm_name=shm_names[0],
            )
        except BaseException:
            os.close(self._writer_lock_fd)
            self._writer_lock_fd = None
            raise
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
        self.ring.initialize_epoch()
        self.ring.recover_write(self._rec_size)
        # Persistence recovery belongs to the persister, which owns the registry.
        # The producer only imports the durable timestamp on a fresh RAM epoch.
        if self._persist and self.ring.created:
            reg_path = self.base_path / "data" / self.feed_name / f"{self.feed_name}.reg"
            if reg_path.exists():
                registry = FeedRegistry(reg_path, mode="r")
                try:
                    self.ring.update_durable_header(durable_last_ts=registry.latest_timestamp())
                finally:
                    registry.close()
        try:
            self._blob_writers = BlobSidecarWriters(self.base_path, self.feed_name, parent=self)
        except BaseException:
            self.ring.close(unlink=False)
            os.close(self._writer_lock_fd)
            self._writer_lock_fd = None
            self._closed = True
            raise

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

    def _wait_for_persisted_space(self, needed_records: int) -> None:
        while (
            self._persist
            and int(self._ring_record_count[0]) - int(self._ring_durable_record_count[0]) + needed_records > self._ring_capacity
        ):
            self._ensure_open()
            self.ring.request_drain()
            _yield_cpu()

    def _write_bytes(self, payload: bytes, last_ts: int) -> int:
        self._ensure_open()
        if len(payload) != self._rec_size:
            raise ValueError(f"record length {len(payload)} != expected {self._rec_size}")
        last_ts = int(last_ts)
        if not 0 <= last_ts < 1 << 64:
            raise ValueError("timestamp must fit uint64")
        self._wait_for_persisted_space(1)
        record_count = int(self._ring_record_count[0])
        ring_size = self.ring.data_size
        write_pos = (record_count * self._rec_size) % ring_size
        next_write_pos = (write_pos + self._rec_size) % ring_size
        start_pos = (max(0, record_count + 1 - self._ring_capacity) * self._rec_size) % ring_size
        self.ring.begin_write(record_count, 1, self._ring_capacity)
        try:
            self._ring_data[write_pos:write_pos + self._rec_size] = payload
            self.ring.update_live_header(next_write_pos, start_pos, int(self._ring_generation[0]), last_ts, record_count + 1)
        finally:
            self.ring.end_write()
        return next_write_pos

    def write(self, timestamp: int, record_data: bytes) -> int:
        return self._write_bytes(record_data, timestamp)

    write_fast = write

    def write_values(self, *vals) -> int:
        # Validate the whole row before touching a slot that may still be read.
        payload = self._S.pack(*vals)
        return self._write_bytes(payload, vals[self._ts_idx] if self._ts_idx is not None else 0)

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
        index_values: dict | None = None,
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
            index_values=index_values,
        )

    def write_batch_bytes(self, data: bytes) -> int:
        self._ensure_open()
        rec_sz = self._rec_size
        data = bytes(data)
        n = len(data)
        if n == 0 or n % rec_sz != 0:
            raise ValueError("batch length must be a positive multiple of record_size")
        ring_size = self.ring.data_size
        if n > ring_size:
            raise ValueError("batch larger than ring capacity")
        batch_records = n // rec_sz
        self._wait_for_persisted_space(batch_records)
        record_count = int(self._ring_record_count[0])
        write_pos = (record_count * rec_sz) % ring_size
        new_count = record_count + batch_records
        start_pos = (max(0, new_count - self._ring_capacity) * rec_sz) % ring_size
        last_ts = self._u64.unpack_from(data, n - rec_sz)[0]
        end_pos = (write_pos + n) % ring_size
        generation = int(self._ring_generation[0]) + int(write_pos + n > ring_size)
        self.ring.begin_write(record_count, batch_records, self._ring_capacity)
        try:
            self._ring_data[write_pos:write_pos + n] = data
            self.ring.update_live_header(end_pos, start_pos, generation, last_ts, new_count)
        finally:
            self.ring.end_write()
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


class ChunkWriter:
    __slots__ = (
        "base_path", "feed_name", "feed_metadata", "record_format", "my_pid",
        "data_dir", "registry",
        "current_chunk", "current_chunk_metadata", "current_chunk_id",
        "_S", "_rec_size", "_clock_level", "_key_min_set", "_u64", "_qmins", "_qmaxs",
        "_value_fields", "_blob_writers", "_commit_mode",
    )

    def __init__(self, base_path, feed_name: str, *, commit_mode: bool = False):
        self._commit_mode = commit_mode
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
            raise

    def _ensure_open(self) -> None:
        if self.registry is None or self.registry.fd is None:
            raise RuntimeError(f"chunk writer is closed for feed '{self.feed_name}'")

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
            self.current_chunk_metadata.status = IN_MEMORY if self._commit_mode else ON_DISK
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
            status=IN_MEMORY if self._commit_mode else ON_DISK,
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
        self.current_chunk_metadata.status = IN_MEMORY if self._commit_mode else ON_DISK
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
        index_values: dict | None = None,
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
            index_values=index_values,
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

    def publish_chunk_commit(self, chunk_id: int) -> None:
        meta = self.registry.get_chunk_metadata(chunk_id)
        try:
            meta.status = ON_DISK
        finally:
            meta.release()
        self.registry.flush()
        os.fsync(self.registry.fd)

    def abort_chunk_commit(self) -> None:
        self._discard_current_chunk()

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
