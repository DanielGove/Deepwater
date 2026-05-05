import fcntl
import hashlib
import os
import struct
import time
from multiprocessing import resource_tracker, shared_memory
from typing import Iterator, Optional

from ..metadata.feed_registry import FeedRegistry, UINT64_MAX
from ..metadata.segments import SegmentStore

_DRAIN_REQUEST_FLAG = 1 << 63
_COUNT_MASK = _DRAIN_REQUEST_FLAG - 1


def _yield_cpu() -> None:
    try:
        os.sched_yield()
    except AttributeError:
        time.sleep(0)


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


def ring_buffer_shm_names(base_path, feed_name: str) -> tuple[str, ...]:
    base_key = os.fspath(base_path)
    digest = hashlib.blake2b(base_key.encode("utf-8", "surrogatepass"), digest_size=8).hexdigest()
    primary = f"dw_{digest}_{feed_name}"
    return (primary, feed_name, f"{feed_name}-ring")


class RingBuffer:
    """
    Shared-memory ring buffer for fixed-size records.

    Layout:
      write_pos, start_pos, generation, last_ts, record_count,
      durable_record_count, durable_last_ts, overrun_count, lost_records,
      followed by the data region.
    """

    _OFF_WRITE_POS = 0
    _OFF_START_POS = 8
    _OFF_GENERATION = 16
    _OFF_LAST_TS = 24
    _OFF_RECORD_COUNT = 32
    _OFF_DURABLE_RECORD_COUNT = 40
    _OFF_DURABLE_LAST_TS = 48
    _OFF_OVERRUN_COUNT = 56
    _OFF_LOST_RECORDS = 64
    HEADER_SIZE = 72

    def __init__(
        self,
        name: str,
        data_size: int,
        create: bool = False,
        *,
        shm_name: str | None = None,
        fallback_names: tuple[str, ...] = (),
    ):
        if data_size <= 0:
            raise ValueError("data_size must be > 0")
        if data_size % 4096 != 0:
            data_size = (data_size + 4095) & ~4095
        self.name = name
        self.shm_name = shm_name or name
        self.data_size = data_size
        self.total_size = self.HEADER_SIZE + data_size
        self.created = False
        if create:
            try:
                self.shm = shared_memory.SharedMemory(name=self.shm_name, create=True, size=self.total_size)
                self.created = True
            except FileExistsError:
                self.shm = shared_memory.SharedMemory(name=self.shm_name, create=False)
        else:
            last_error = None
            for candidate in (self.shm_name, *fallback_names):
                try:
                    self.shm = shared_memory.SharedMemory(name=candidate, create=False)
                    self.shm_name = candidate
                    break
                except FileNotFoundError as exc:
                    last_error = exc
            else:
                raise last_error or FileNotFoundError(self.shm_name)
        actual_size = getattr(self.shm, "size", len(self.shm.buf))
        if actual_size != self.total_size:
            try:
                self.shm.close()
            finally:
                raise RuntimeError(
                    f"ring shared memory size mismatch for '{self.name}': "
                    f"existing {actual_size} bytes != expected {self.total_size} bytes "
                    f"(shm_name={self.shm_name})"
                )
        try:
            resource_name = getattr(
                self.shm,
                "_name",
                self.shm_name if str(self.shm_name).startswith("/") else f"/{self.shm_name}",
            )
            resource_tracker.unregister(resource_name, "shared_memory")
        except Exception:
            pass
        self.buf = self.shm.buf
        if self.created:
            self.buf[:self.HEADER_SIZE] = b"\x00" * self.HEADER_SIZE
        self._write_pos = self.buf[self._OFF_WRITE_POS:self._OFF_START_POS].cast("Q")
        self._start_pos = self.buf[self._OFF_START_POS:self._OFF_GENERATION].cast("Q")
        self._generation = self.buf[self._OFF_GENERATION:self._OFF_LAST_TS].cast("Q")
        self._last_ts = self.buf[self._OFF_LAST_TS:self._OFF_RECORD_COUNT].cast("Q")
        self._record_count = self.buf[self._OFF_RECORD_COUNT:self._OFF_DURABLE_RECORD_COUNT].cast("Q")
        self._durable_record_count = self.buf[self._OFF_DURABLE_RECORD_COUNT:self._OFF_DURABLE_LAST_TS].cast("Q")
        self._durable_last_ts = self.buf[self._OFF_DURABLE_LAST_TS:self._OFF_OVERRUN_COUNT].cast("Q")
        self._overrun_count = self.buf[self._OFF_OVERRUN_COUNT:self._OFF_LOST_RECORDS].cast("Q")
        self._lost_records = self.buf[self._OFF_LOST_RECORDS:self.HEADER_SIZE].cast("Q")
        self.data = self.buf[self.HEADER_SIZE:]

    def _header_snapshot(self) -> tuple[int, int, int, int, int, int, int, int, int]:
        """
        Return a stable header snapshot.

        Live writers update the shared-memory header field-by-field, so a single
        read can observe a torn mix of old/new values. Readers depend on these
        values to map sequence numbers into byte offsets; a torn snapshot can
        point at the wrong slot and decode garbage.

        We avoid that by requiring two consecutive identical snapshots before
        returning. This is cheap relative to a record decode and dramatically
        more reliable under concurrent writer activity.
        """
        prev = None
        for _ in range(64):
            cur = (
                self._write_pos[0],
                self._start_pos[0],
                self._generation[0],
                self._last_ts[0],
                self._record_count[0],
                self._durable_record_count[0],
                self._durable_last_ts[0],
                self._overrun_count[0] & _COUNT_MASK,
                self._lost_records[0],
            )
            if cur == prev:
                return cur
            prev = cur
        return prev  # type: ignore[return-value]

    def header(self) -> tuple[int, int, int, int, int, int, int, int, int]:
        return self._header_snapshot()

    def live_header(self) -> tuple[int, int, int, int, int]:
        write_pos, start_pos, generation, last_ts, record_count, *_ = self._header_snapshot()
        return (write_pos, start_pos, generation, last_ts, record_count)

    def durable_header(self) -> tuple[int, int, int, int]:
        _, _, _, _, _, durable_record_count, durable_last_ts, overrun_count, lost_records = self._header_snapshot()
        return (durable_record_count, durable_last_ts, overrun_count, lost_records)

    def update_live_header(
        self,
        write_pos: int,
        start_pos: int,
        generation: int,
        last_ts: int,
        record_count: int,
    ) -> None:
        self._write_pos[0] = int(write_pos)
        self._start_pos[0] = int(start_pos)
        self._generation[0] = int(generation)
        self._last_ts[0] = int(last_ts)
        self._record_count[0] = int(record_count)

    def update_durable_header(
        self,
        durable_record_count: Optional[int] = None,
        durable_last_ts: Optional[int] = None,
        overrun_count: Optional[int] = None,
        lost_records: Optional[int] = None,
    ) -> None:
        if durable_record_count is not None:
            self._durable_record_count[0] = int(durable_record_count)
        if durable_last_ts is not None:
            self._durable_last_ts[0] = int(durable_last_ts)
        if overrun_count is not None:
            flag = self._overrun_count[0] & _DRAIN_REQUEST_FLAG
            self._overrun_count[0] = flag | (int(overrun_count) & _COUNT_MASK)
        if lost_records is not None:
            self._lost_records[0] = int(lost_records)

    def drain_requested(self) -> bool:
        return bool(self._overrun_count[0] & _DRAIN_REQUEST_FLAG)

    def request_drain(self) -> None:
        self._overrun_count[0] = int(self._overrun_count[0]) | _DRAIN_REQUEST_FLAG

    def clear_drain_request(self) -> None:
        self._overrun_count[0] = int(self._overrun_count[0]) & _COUNT_MASK

    def close(self, unlink: bool = False) -> None:
        try:
            self.data.release()
        except Exception:
            pass
        for view in (
            self._write_pos,
            self._start_pos,
            self._generation,
            self._last_ts,
            self._record_count,
            self._durable_record_count,
            self._durable_last_ts,
            self._overrun_count,
            self._lost_records,
        ):
            try:
                view.release()
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
    """Single-writer for ring-backed feeds."""

    def __init__(self, platform, feed_name: str):
        self.platform = platform
        self.feed_name = feed_name
        self.feed_config = platform.lifecycle(feed_name)
        self.record_format = platform.get_record_format(feed_name)
        self._persist = bool(self.feed_config.get("persist", False))
        self._segment_tracking = bool(self.feed_config.get("segment_tracking", True))
        self._prefault_ring = bool(self.feed_config.get("prefault_ring", True))
        self._writer_lock_fd = None
        self._closed = False

        self._S = struct.Struct(self.record_format["fmt"])
        self._u64 = struct.Struct("<Q")
        self._rec_size = self._S.size
        value_fields = [f.get("name") for f in self.record_format["fields"] if f.get("name") != "_"]
        self._value_fields = tuple(value_fields)
        ts_name = self.record_format.get("ts_name")
        try:
            self._ts_idx = value_fields.index(ts_name) if ts_name else (0 if value_fields else None)
        except ValueError:
            self._ts_idx = 0 if value_fields else None

        self.ring_bytes = int(
            self.record_format.get("ring_size_bytes")
            or self.feed_config.get("ring_size_bytes")
            or self.feed_config.get("chunk_size_bytes", 8 * 1024 * 1024)
        )
        if self.ring_bytes < self._rec_size:
            raise ValueError(f"ring size {self.ring_bytes} too small for record size {self._rec_size}")

        lock_path = platform.base_path / "data" / feed_name / f"{feed_name}.writer.lock"
        self._writer_lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(self._writer_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self._writer_lock_fd)
            self._writer_lock_fd = None
            raise RuntimeError(f"ring writer locked by another writer for feed '{feed_name}'")

        shm_names = ring_buffer_shm_names(platform.base_path, self.feed_name)
        self.ring = RingBuffer(
            self.feed_name,
            data_size=self.ring_bytes,
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
        self._ring_capacity = self.ring_bytes // self._rec_size
        self._usable_bytes = self._ring_capacity * self._rec_size
        self.segment_store = SegmentStore(platform.base_path / "data" / feed_name, feed_name) if self._segment_tracking else None
        self._bootstrap_persisted_ring()
        if self._persist:
            self._repair_persisted_chunks_for_recovery()
        if self.segment_store is not None:
            last_ts = int(self._ring_last_ts[0])
            self.segment_store.recover_open_segment(last_ts if last_ts > 0 else None)
            self._segment_note_write = self.segment_store.note_write_one
            self._segment_note_batch = self.segment_store.note_batch
        else:
            self._segment_note_write = lambda *_a, **_k: None
            self._segment_note_batch = lambda *_a, **_k: None

    @property
    def closed(self) -> bool:
        return bool(self._closed)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError(f"ring writer is closed for feed '{self.feed_name}'")

    def _prefault_ring_pages(self) -> None:
        try:
            mv = self._ring_data
            step = max(4096, os.sysconf("SC_PAGE_SIZE"))
            acc = 0
            for i in range(0, self.ring_bytes, step):
                acc ^= mv[i]
            if self.ring_bytes > 0:
                acc ^= mv[self.ring_bytes - 1]
            self._prefault_checksum = acc
        except Exception:
            pass

    def _bootstrap_persisted_ring(self) -> None:
        if not self._persist:
            return
        durable_count, durable_last_ts = _load_durable_frontier(self.platform.feed_dir(self.feed_name), self.feed_name)
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
        reg_path = self.platform.feed_dir(self.feed_name) / f"{self.feed_name}.reg"
        if not reg_path.exists():
            return
        try:
            from ..ops import repair

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
                        feed_dir=self.platform.feed_dir(self.feed_name),
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

    def _prepare_single_write(self):
        while self._persist and self._ring_record_count - self._ring_durable_record_count >= self._ring_capacity:
            _yield_cpu() # No Overruns, but wait for space to be persisted if needed
        write_pos = int(self._ring_write_pos[0])
        start_pos = int(self._ring_start_pos[0])
        generation = int(self._ring_generation[0])
        record_count = int(self._ring_record_count[0])
        if write_pos + self._rec_size > self._usable_bytes:
            write_pos = 0
            generation += 1
        slice_end = write_pos + self._rec_size
        next_write_pos = 0 if slice_end >= self._usable_bytes else slice_end
        if record_count >= self._ring_capacity:
            start_pos = next_write_pos
        new_record_count = record_count + 1
        return write_pos, next_write_pos, start_pos, generation, new_record_count

    def _write_bytes(self, payload: bytes, last_ts: int) -> int:
        self._ensure_open()
        if len(payload) != self._rec_size:
            raise ValueError(f"record length {len(payload)} != expected {self._rec_size}")
        while self._persist and int(self._ring_record_count[0]) - int(self._ring_durable_record_count[0]) >= self._ring_capacity:
            _yield_cpu() # No Overruns, but wait for space to be persisted if needed
        write_pos = int(self._ring_write_pos[0])
        generation = int(self._ring_generation[0])
        start_pos = int(self._ring_start_pos[0])
        record_count = int(self._ring_record_count[0])
        if write_pos + self._rec_size > self._usable_bytes:
            write_pos = 0
            generation += 1
        slice_end = write_pos + self._rec_size
        next_write_pos = 0 if slice_end >= self._usable_bytes else slice_end
        if record_count >= self._ring_capacity:
            start_pos = next_write_pos
        self._ring_data[write_pos:slice_end] = payload
        self.ring.update_live_header(next_write_pos, start_pos, generation, int(last_ts), record_count + 1)
        self._segment_note_write(last_ts)
        return next_write_pos

    def write(self, timestamp: int, record_data: bytes, create_index: bool = False) -> int:
        self._ensure_open()
        _ = create_index
        while self._persist and int(self._ring_record_count[0]) - int(self._ring_durable_record_count[0]) >= self._ring_capacity:
            _yield_cpu()
        write_pos = int(self._ring_write_pos[0])
        generation = int(self._ring_generation[0])
        start_pos = int(self._ring_start_pos[0])
        record_count = int(self._ring_record_count[0])
        if write_pos + self._rec_size > self._usable_bytes:
            write_pos = 0
            generation += 1
        slice_end = write_pos + self._rec_size
        next_write_pos = 0 if slice_end >= self._usable_bytes else slice_end
        if record_count >= self._ring_capacity:
            start_pos = next_write_pos
        self._ring_data[write_pos:slice_end] = record_data
        self.ring.update_live_header(next_write_pos, start_pos, generation, int(timestamp), record_count + 1)
        self._segment_note_write(timestamp)
        return next_write_pos

    write_fast = write

    def write_values(self, *vals, create_index: bool = False) -> int:
        self._ensure_open()
        _ = create_index
        last_ts = vals[self._ts_idx] if self._ts_idx is not None else 0
        while self._persist and int(self._ring_record_count[0]) - int(self._ring_durable_record_count[0]) >= self._ring_capacity:
            _yield_cpu() # No Overruns, but wait for space to be persisted if needed
        write_pos = int(self._ring_write_pos[0])
        generation = int(self._ring_generation[0])
        start_pos = int(self._ring_start_pos[0])
        record_count = int(self._ring_record_count[0])
        if write_pos + self._rec_size > self._usable_bytes:
            write_pos = 0
            generation += 1
        next_write_pos = write_pos + self._rec_size
        if next_write_pos >= self._usable_bytes:
            next_write_pos = 0
        if record_count >= self._ring_capacity:
            start_pos = next_write_pos
        self._S.pack_into(self._ring_data, write_pos, *vals)
        last_ts_i = int(last_ts)
        self.ring.update_live_header(next_write_pos, start_pos, generation, last_ts_i, record_count + 1)
        self._segment_note_write(last_ts_i)
        return next_write_pos

    def write_tuple(self, record: tuple, create_index: bool = False) -> int:
        return self.write_values(*record, create_index=create_index)

    def write_dict(self, record: dict, create_index: bool = False) -> int:
        vals = tuple(record[name] for name in self._value_fields)
        return self.write_values(*vals, create_index=create_index)

    def write_batch_bytes(self, data: bytes) -> int:
        self._ensure_open()
        rec_sz = self._rec_size
        n = len(data)
        if n == 0 or n % rec_sz != 0:
            raise ValueError("batch length must be a positive multiple of record_size")
        if n > self._usable_bytes:
            raise ValueError("batch larger than ring capacity")

        batch_records = n // rec_sz
        while (
            self._persist
            and int(self._ring_record_count[0]) - int(self._ring_durable_record_count[0]) + batch_records > self._ring_capacity
        ):
            _yield_cpu() # No Overruns, but wait for space to be persisted if needed

        write_pos = int(self._ring_write_pos[0])
        generation = int(self._ring_generation[0])
        start_pos = int(self._ring_start_pos[0])
        record_count = int(self._ring_record_count[0])
        last_ts_prev = int(self._ring_last_ts[0])
        if write_pos + n > self._usable_bytes:
            write_pos = 0
            generation += 1

        new_record_count = record_count + batch_records
        if record_count >= self._ring_capacity:
            start_pos = write_pos
        elif new_record_count > self._ring_capacity:
            overwritten = new_record_count - self._ring_capacity
            start_pos = (start_pos + (overwritten * rec_sz)) % self._usable_bytes

        end = write_pos + n
        self._ring_data[write_pos:end] = data

        unpack_from = self._u64.unpack_from
        first_ts = unpack_from(data, 0)[0]
        last_ts = unpack_from(data, n - rec_sz)[0]
        end_pos = end if end < self._usable_bytes else 0
        if record_count >= self._ring_capacity:
            start_pos = end_pos
        self.ring.update_live_header(end_pos, start_pos, generation, last_ts if last_ts else last_ts_prev, new_record_count)
        self._segment_note_batch(first_ts, last_ts, batch_records)
        return end_pos

    def resize(self, new_size_bytes: int):
        self._ensure_open()
        if new_size_bytes <= 0:
            raise ValueError("new ring size must be > 0")
        try:
            self.ring.close(unlink=True)
        except Exception:
            pass
        self.ring_bytes = int(new_size_bytes)
        self._ring_capacity = self.ring_bytes // self._rec_size
        self._usable_bytes = self._ring_capacity * self._rec_size
        shm_names = ring_buffer_shm_names(self.platform.base_path, self.feed_name)
        self.ring = RingBuffer(
            self.feed_name,
            data_size=self.ring_bytes,
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
        self._bootstrap_persisted_ring()

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._persist:
            if int(self._ring_durable_record_count[0]) < int(self._ring_record_count[0]):
                self.ring.request_drain()
                try:
                    self.platform._ensure_persistent_ring_persister()
                except Exception:
                    pass
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                if int(self._ring_durable_record_count[0]) >= int(self._ring_record_count[0]):
                    break
                _yield_cpu()
        if self.segment_store is not None:
            self.segment_store.close_open_segment("writer_close")
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
        try:
            if self.platform._writers.get(self.feed_name) is self:
                self.platform._writers.pop(self.feed_name, None)
            if self._persist and not any(
                not bool(getattr(writer, "closed", False))
                and bool((self.platform.lifecycle(name) or {}).get("persist", True))
                for name, writer in self.platform._writers.items()
            ):
                self.platform._stop_persistent_ring_persister()
        except Exception:
            pass

    def mark_segment_boundary(self, reason: str = "disconnect") -> bool:
        if self.segment_store is None:
            return False
        return self.segment_store.close_open_segment(reason)


class RingReader:
    """Reader that tails a shared-memory ring."""

    def __init__(self, platform, feed_name: str):
        self.platform = platform
        self.feed_name = feed_name
        self.record_format = platform.get_record_format(feed_name)
        self._S = struct.Struct(self.record_format["fmt"])
        self._u64 = struct.Struct("<Q")
        self._rec_size = self._S.size
        lifecycle = platform.lifecycle(feed_name)
        self.ring_bytes = int(
            self.record_format.get("ring_size_bytes")
            or lifecycle.get("ring_size_bytes")
            or lifecycle.get("chunk_size_bytes", 8 * 1024 * 1024)
        )
        self._ring_capacity = self.ring_bytes // self._rec_size
        self._usable_bytes = self._ring_capacity * self._rec_size
        shm_names = ring_buffer_shm_names(platform.base_path, self.feed_name)
        self.ring = RingBuffer(
            self.feed_name,
            data_size=self.ring_bytes,
            create=False,
            shm_name=shm_names[0],
            fallback_names=shm_names[1:],
        )
        self._field_names = tuple(
            f["name"]
            for f in self.record_format.get("fields", [])
            if not f.get("type", "").startswith("_") or f["name"] != "_"
        )
        self._primary_ts_off = self.record_format.get("ts_offset", 0)
        self.read_pos = 0
        self.records_read = 0
        self._read_head_seq: Optional[int] = None

        state = self.state()
        self.read_pos = state["start_pos"]
        self.records_read = state["earliest_live_seq"]

    @property
    def format(self) -> str:
        return self.record_format["fmt"]

    @property
    def field_names(self) -> tuple:
        return self._field_names

    def state(self) -> dict:
        write_pos, start_pos, generation, last_ts, record_count, durable_record_count, durable_last_ts, overrun_count, lost_records = self.ring.header()
        ring_capacity = self._ring_capacity
        earliest_live_seq = max(0, record_count - ring_capacity)
        return {
            "write_pos": write_pos,
            "start_pos": start_pos,
            "generation": generation,
            "last_ts": last_ts,
            "record_count": record_count,
            "durable_record_count": durable_record_count,
            "durable_last_ts": durable_last_ts,
            "overrun_count": overrun_count,
            "lost_records": lost_records,
            "earliest_live_seq": earliest_live_seq,
            "ring_capacity": ring_capacity,
        }

    def _seq_to_pos(self, state: dict, seq: int) -> int:
        delta = seq - state["earliest_live_seq"]
        return (state["start_pos"] + (delta * self._rec_size)) % self._usable_bytes

    def _set_cursor_from_seq(self, seq: int) -> int:
        state = self.state()
        seq = max(int(seq), state["earliest_live_seq"])
        seq = min(seq, state["record_count"])
        self.read_pos = self._seq_to_pos(state, seq) if seq < state["record_count"] else state["write_pos"]
        self.records_read = seq
        return seq

    def _record_matches(self, pos: int, ts_off: int, start_time: Optional[int], end_time: Optional[int]) -> bool:
        if start_time is None and end_time is None:
            return True
        ts = self._u64.unpack_from(self.ring.data, pos + ts_off)[0]
        if start_time is not None and ts < start_time:
            return False
        if end_time is not None and ts >= end_time:
            return False
        return True

    def _pack_raw(self, records: list[tuple]) -> memoryview:
        if not records:
            return memoryview(b"")
        raw = bytearray(len(records) * self._rec_size)
        for i, rec in enumerate(records):
            self._S.pack_into(raw, i * self._rec_size, *rec)
        return memoryview(raw)

    def _format_records(self, records: list[tuple], format: str):
        if format == "tuple":
            return records
        if format == "dict":
            return [{self._field_names[i]: rec[i] for i in range(len(self._field_names))} for rec in records]
        if format == "raw":
            return self._pack_raw(records)
        if format == "numpy":
            raise NotImplementedError("Ring feeds don't support numpy format (use 'tuple', 'dict', or 'raw')")
        raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")

    def read_seq_range(
        self,
        start_seq: int,
        end_seq: int,
        *,
        format: str = "tuple",
        ts_off: int = 0,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        state = self.state()
        start_seq = max(int(start_seq), state["earliest_live_seq"])
        end_seq = min(int(end_seq), state["record_count"])
        if end_seq <= start_seq:
            return [] if format != "raw" else memoryview(b"")

        data = self.ring.data
        rec_sz = self._rec_size
        records: list[tuple] = []
        pos = self._seq_to_pos(state, start_seq)
        for _seq in range(start_seq, end_seq):
            if pos + rec_sz > self._usable_bytes:
                pos = 0
            if self._record_matches(pos, ts_off, start_time, end_time):
                records.append(self._S.unpack_from(data, pos))
            pos += rec_sz
        return self._format_records(records, format)

    def stream_from_seq(
        self,
        start_seq: int,
        *,
        format: str = "tuple",
        ts_off: int = 0,
        start_time: Optional[int] = None,
    ):
        self._set_cursor_from_seq(start_seq)
        data = self.ring.data
        rec_sz = self._rec_size
        read_pos = self.read_pos
        records_read = self.records_read
        header = self.ring.header
        ring_bytes = self._usable_bytes
        ring_capacity = self._ring_capacity
        unpack = self._S.unpack_from
        unpack_u64 = self._u64.unpack_from
        field_names = self._field_names

        while True:
            write_pos, start_pos, _generation, _last_ts, record_count, _durable_count, _durable_last_ts, _overrun_count, _lost_records = header()
            earliest_live_seq = max(0, record_count - ring_capacity)
            if records_read < earliest_live_seq:
                records_read = earliest_live_seq
                read_pos = start_pos

            if records_read >= record_count:
                _yield_cpu()
                continue

            if read_pos >= ring_bytes:
                read_pos = 0

            record = unpack(data, read_pos)
            ts = unpack_u64(data, read_pos + ts_off)[0]
            read_pos += rec_sz
            records_read += 1

            self.read_pos = read_pos
            self.records_read = records_read

            if start_time is not None and ts < start_time:
                continue

            if format == "tuple":
                yield record
            elif format == "dict":
                yield {field_names[i]: record[i] for i in range(len(field_names))}
            elif format == "raw":
                raw = bytearray(self._rec_size)
                self._S.pack_into(raw, 0, *record)
                yield memoryview(raw)
            elif format == "numpy":
                raise NotImplementedError("Ring feeds don't support numpy format (use 'tuple', 'dict', or 'raw')")
            else:
                raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")

    def stream(self, start: Optional[int] = None, format: str = "tuple"):
        if start is not None:
            raise ValueError("Ring feeds don't support historical start times (start must be None)")
        state = self.state()
        return self.stream_from_seq(state["record_count"], format=format, ts_off=self._primary_ts_off)

    def range(self, start: int, end: int, format: str = "tuple"):
        state = self.state()
        return self.read_seq_range(
            state["earliest_live_seq"],
            state["record_count"],
            format=format,
            ts_off=self._primary_ts_off,
            start_time=start,
            end_time=end,
        )

    def _latest_end_us(self, ts_off: int) -> int:
        state = self.state()
        if state["record_count"] == 0:
            return 0
        pos = self._seq_to_pos(state, state["record_count"] - 1)
        return int(self._u64.unpack_from(self.ring.data, pos + ts_off)[0])

    def latest(self, seconds: float = 60.0, format: str = "tuple"):
        state = self.state()
        if state["record_count"] == 0:
            return [] if format != "raw" else memoryview(b"")
        end_us = self._latest_end_us(self._primary_ts_off)
        if end_us == 0:
            return [] if format != "raw" else memoryview(b"")
        start_us = end_us - int(seconds * 1_000_000)
        return self.range(start_us, end_us + 1, format=format)

    def read_available(self, max_records: Optional[int] = None, format: str = "tuple"):
        state = self.state()
        if self._read_head_seq is None:
            self._read_head_seq = state["record_count"]
            return [] if format != "raw" else memoryview(b"")
        end_seq = state["record_count"]
        if max_records is not None:
            end_seq = min(end_seq, self._read_head_seq + int(max_records))
        records = self.read_seq_range(self._read_head_seq, end_seq, format=format, ts_off=self._primary_ts_off)
        self._read_head_seq = end_seq
        return records

    def stream_live(self):
        yield from self.stream_from_seq(self.records_read, format="tuple", ts_off=self._primary_ts_off)

    def stream_latest_records(self, playback: bool = False):
        _ = playback
        yield from self.stream_live()

    def stream_latest_records_raw(self):
        yield from self.stream_from_seq(self.records_read, format="raw", ts_off=self._primary_ts_off)

    def read_batch(self, count: int):
        batch = []
        for rec in self.stream_latest_records(playback=False):
            batch.append(rec)
            if len(batch) >= count:
                yield batch
                batch = []

    def read_batch_raw(self, count: int):
        batch = []
        for rec in self.stream_latest_records_raw():
            batch.append(rec)
            if len(batch) >= count:
                yield batch
                batch = []

    def stream_time_range(self, start_time: int, end_time: Optional[int] = None):
        raise NotImplementedError("Ring feeds are live-only; time windows unavailable.")

    def get_latest_record(self) -> tuple:
        state = self.state()
        if state["record_count"] == 0:
            raise RuntimeError("No records written yet.")
        pos = self._seq_to_pos(state, state["record_count"] - 1)
        return self._S.unpack_from(self.ring.data, pos)

    def close(self):
        try:
            self.ring.close(unlink=False)
        except Exception:
            pass
