import hashlib
import math
import os
import time
import uuid
from pathlib import Path
from multiprocessing import resource_tracker, shared_memory
from typing import Optional

from .ring_shadow import atomic_load_u64, atomic_store_u64

_DRAIN_REQUEST_FLAG = 1 << 63
_COUNT_MASK = _DRAIN_REQUEST_FLAG - 1
PAGE_SIZE = int(os.sysconf("SC_PAGE_SIZE"))
_HEADER_BYTES = 80
RING_HEADER_SIZE = ((_HEADER_BYTES + PAGE_SIZE - 1) // PAGE_SIZE) * PAGE_SIZE


def _yield_cpu() -> None:
    try:
        os.sched_yield()
    except AttributeError:
        time.sleep(0)


def ring_buffer_shm_names(base_path, feed_name: str) -> tuple[str, ...]:
    base_key = os.fspath(Path(base_path).resolve())
    digest = hashlib.blake2b(base_key.encode("utf-8", "surrogatepass"), digest_size=8).hexdigest()
    primary = f"dw_{digest}_{feed_name}"
    return (primary, feed_name, f"{feed_name}-ring")


def _ring_data_shm_name(control_shm_name: str) -> str:
    return f"{control_shm_name}-data"


def normalize_ring_data_size(requested_bytes: int, record_size: int) -> int:
    if requested_bytes <= 0:
        raise ValueError("ring data size must be > 0")
    if record_size <= 0:
        raise ValueError("record size must be > 0")
    unit = math.lcm(PAGE_SIZE, int(record_size))
    return ((int(requested_bytes) + unit - 1) // unit) * unit


class RingBuffer:
    """
    Shared-memory ring buffer for fixed-size records.

    Control layout:
      write_pos, start_pos, generation, last_ts, record_count,
      durable_record_count, durable_last_ts, overrun_count, lost_records,
      data_size.

    Data lives in a sibling shared-memory object and is exposed as a doubled
    virtual mapping so wrapped ranges are contiguous.
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
    _OFF_DATA_SIZE = 72
    HEADER_SIZE = RING_HEADER_SIZE

    @classmethod
    def unlink_for_feed(cls, base_path, feed_name: str) -> None:
        """Remove every shared-memory object owned by a feed ring."""
        from .ring_shadow import unlink_shm

        from ..metadata.sidecar import load_sidecars
        from .blob_ring import BlobRing

        for sidecar in load_sidecars(base_path, feed_name):
            BlobRing.unlink(base_path, feed_name, sidecar.name)

        for shm_name in ring_buffer_shm_names(base_path, feed_name):
            try:
                shm = shared_memory.SharedMemory(name=shm_name, create=False)
            except FileNotFoundError:
                pass
            else:
                try:
                    shm.unlink()
                except FileNotFoundError:
                    pass
                finally:
                    shm.close()
            try:
                unlink_shm(_ring_data_shm_name(shm_name))
            except FileNotFoundError:
                pass

    @classmethod
    def open(cls, name: str, data_size: int | None = None, *, shm_name: str | None = None):
        shm_name = shm_name or name
        if data_size is None:
            shm = shared_memory.SharedMemory(name=shm_name, create=False)
            try:
                view = shm.buf[cls._OFF_DATA_SIZE:cls._OFF_DATA_SIZE + 8].cast("Q")
                try:
                    data_size = view[0]
                finally:
                    view.release()
            finally:
                shm.close()
        return cls(name, int(data_size), create=False, shm_name=shm_name)

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
        if data_size % PAGE_SIZE != 0:
            raise ValueError(f"data_size must be a multiple of page size {PAGE_SIZE}")
        self.name = name
        self.shm_name = shm_name or name
        self.data_shm_name = _ring_data_shm_name(self.shm_name)
        self.data_size = data_size
        self.total_size = self.HEADER_SIZE + data_size
        self.created = False
        self._resource_name = None
        self._resource_tracker_unregistered = False
        if create:
            try:
                self.shm = shared_memory.SharedMemory(name=self.shm_name, create=True, size=self.HEADER_SIZE)
                self.created = True
            except FileExistsError:
                self.shm = shared_memory.SharedMemory(name=self.shm_name, create=False)
            from .ring_shadow import ensure_shm

            try:
                ensure_shm(_ring_data_shm_name(self.shm_name), data_size, self.created)
            except BaseException:
                if self.created:
                    self.shm.unlink()
                else:
                    resource_tracker.unregister(self.shm._name, "shared_memory")
                self.shm.close()
                raise
        else:
            last_error = None
            for candidate in (self.shm_name, *fallback_names):
                shm = None
                try:
                    shm = shared_memory.SharedMemory(name=candidate, create=False)
                    self.shm = shm
                    self.shm_name = candidate
                    self.data_shm_name = _ring_data_shm_name(candidate)
                    break
                except FileNotFoundError as exc:
                    last_error = exc
                    if shm is not None:
                        try:
                            shm.close()
                        except Exception:
                            pass
            else:
                raise last_error or FileNotFoundError(self.shm_name)
        actual_size = getattr(self.shm, "size", len(self.shm.buf))
        if actual_size != self.HEADER_SIZE:
            try:
                self.shm.close()
            finally:
                raise RuntimeError(
                    f"ring control shared memory size mismatch for '{self.name}': "
                    f"existing {actual_size} bytes != expected {self.HEADER_SIZE} bytes "
                    f"(shm_name={self.shm_name})"
                )
        try:
            self._resource_name = getattr(
                self.shm,
                "_name",
                self.shm_name if str(self.shm_name).startswith("/") else f"/{self.shm_name}",
            )
            resource_tracker.unregister(self._resource_name, "shared_memory")
            self._resource_tracker_unregistered = True
        except Exception:
            pass
        self.buf = self.shm.buf
        if self.created:
            self.buf[:self.HEADER_SIZE] = b"\x00" * self.HEADER_SIZE
            self.buf[self._OFF_DATA_SIZE:self._OFF_DATA_SIZE + 8] = int(data_size).to_bytes(8, "little")
            self.buf[80:96] = uuid.uuid4().bytes
        self._write_pos = self.buf[self._OFF_WRITE_POS:self._OFF_START_POS].cast("Q")
        self._start_pos = self.buf[self._OFF_START_POS:self._OFF_GENERATION].cast("Q")
        self._generation = self.buf[self._OFF_GENERATION:self._OFF_LAST_TS].cast("Q")
        self._last_ts = self.buf[self._OFF_LAST_TS:self._OFF_RECORD_COUNT].cast("Q")
        self._record_count = self.buf[self._OFF_RECORD_COUNT:self._OFF_DURABLE_RECORD_COUNT].cast("Q")
        self._durable_record_count = self.buf[self._OFF_DURABLE_RECORD_COUNT:self._OFF_DURABLE_LAST_TS].cast("Q")
        self._durable_last_ts = self.buf[self._OFF_DURABLE_LAST_TS:self._OFF_OVERRUN_COUNT].cast("Q")
        self._overrun_count = self.buf[self._OFF_OVERRUN_COUNT:self._OFF_LOST_RECORDS].cast("Q")
        self._lost_records = self.buf[self._OFF_LOST_RECORDS:self._OFF_LOST_RECORDS + 8].cast("Q")
        if int.from_bytes(self.buf[self._OFF_DATA_SIZE:self._OFF_DATA_SIZE + 8], "little") != self.data_size:
            try:
                self.shm.close()
            finally:
                raise RuntimeError(
                    f"ring data size mismatch for '{self.name}': "
                    f"header {int.from_bytes(self.buf[self._OFF_DATA_SIZE:self._OFF_DATA_SIZE + 8], 'little')} bytes "
                    f"!= expected {self.data_size} bytes"
                )
        from .ring_shadow import map_shadow

        self._shadow_map = map_shadow(self.data_shm_name, 0, self.data_size)
        self.data = self._shadow_map.view

    @property
    def epoch(self) -> str:
        value = bytes(self.buf[80:96])
        if value == bytes(16):
            raise RuntimeError("legacy ring has no epoch; open its sole writer with the new engine first")
        return str(uuid.UUID(bytes=value))

    @property
    def epoch_tag(self) -> int:
        return int.from_bytes(hashlib.blake2b(uuid.UUID(self.epoch).bytes, digest_size=8).digest(), "little")

    def initialize_epoch(self) -> None:
        """Upgrade the reserved header area while holding the feed writer lock."""
        if bytes(self.buf[80:96]) == bytes(16):
            self.buf[80:96] = uuid.uuid4().bytes

    @property
    def write_sequence(self) -> int:
        return atomic_load_u64(self.buf, 96)

    @property
    def retained_record_floor(self) -> int:
        return atomic_load_u64(self.buf, 104)

    def begin_write(self, record_count: int, batch_records: int, capacity: int) -> None:
        sequence = self.write_sequence
        if sequence & 1:
            raise RuntimeError("interrupted ring write; reopen the sole writer")
        atomic_store_u64(self.buf, 96, sequence + 1)
        atomic_store_u64(self.buf, 104, max(self.retained_record_floor, record_count + batch_records - capacity))

    def end_write(self) -> None:
        sequence = self.write_sequence
        if not sequence & 1:
            raise RuntimeError("no ring write in progress")
        atomic_store_u64(self.buf, 96, sequence + 1)

    def recover_write(self, record_size: int) -> None:
        """Reconstruct cursors after a producer interruption, under its lock."""
        if self.write_sequence & 1:
            count = int(self._record_count[0])
            capacity = self.data_size // record_size
            write_pos = (count * record_size) % self.data_size
            start_pos = (max(0, count - capacity) * record_size) % self.data_size
            last_ts = 0 if count <= self.retained_record_floor else int.from_bytes(
                self.data[((count - 1) * record_size) % self.data_size:((count - 1) * record_size) % self.data_size + 8], "little")
            self.update_live_header(write_pos, start_pos, int(self._generation[0]), last_ts, count)
            self.end_write()

    @property
    def write_pos(self):
        return self._write_pos

    @property
    def start_pos(self):
        return self._start_pos

    @property
    def last_ts(self):
        return self._last_ts

    @property
    def record_count(self):
        return self._record_count

    @property
    def durable_record_count(self):
        return self._durable_record_count

    @property
    def durable_last_ts(self):
        return self._durable_last_ts

    @property
    def lost_records(self):
        return self._lost_records

    def overrun_count(self) -> int:
        return int(self._overrun_count[0] & _COUNT_MASK)

    def set_overrun_count(self, value: int) -> None:
        flag = self._overrun_count[0] & _DRAIN_REQUEST_FLAG
        self._overrun_count[0] = flag | (int(value) & _COUNT_MASK)

    def _header_snapshot(self) -> tuple[int, int, int, int, int, int, int, int, int]:
        """Best-effort diagnostic counters from consecutive matching reads.

        Producer and persister counters are independent. This is not a record
        snapshot: readers must validate write_sequence around owned data copies.
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
        atomic_store_u64(self.buf, self._OFF_RECORD_COUNT, int(record_count))

    def update_durable_header(
        self,
        durable_record_count: Optional[int] = None,
        durable_last_ts: Optional[int] = None,
        overrun_count: Optional[int] = None,
        lost_records: Optional[int] = None,
    ) -> None:
        if durable_last_ts is not None:
            self._durable_last_ts[0] = int(durable_last_ts)
        if overrun_count is not None:
            flag = self._overrun_count[0] & _DRAIN_REQUEST_FLAG
            self._overrun_count[0] = flag | (int(overrun_count) & _COUNT_MASK)
        if lost_records is not None:
            self._lost_records[0] = int(lost_records)
        if durable_record_count is not None:
            atomic_store_u64(self.buf, self._OFF_DURABLE_RECORD_COUNT, int(durable_record_count))

    def drain_requested(self) -> bool:
        return bool(self._overrun_count[0] & _DRAIN_REQUEST_FLAG)

    def request_drain(self) -> None:
        self._overrun_count[0] = int(self._overrun_count[0]) | _DRAIN_REQUEST_FLAG

    def clear_drain_request(self) -> None:
        self._overrun_count[0] = int(self._overrun_count[0]) & _COUNT_MASK

    def close(self, unlink: bool = False) -> None:
        if unlink:
            try:
                if self._resource_tracker_unregistered:
                    from .ring_shadow import unlink_shm

                    unlink_shm(self.shm_name)
                else:
                    self.shm.unlink()
            except FileNotFoundError:
                pass
            try:
                from .ring_shadow import unlink_shm

                unlink_shm(self.data_shm_name)
            except FileNotFoundError:
                pass
        shadow_closed = False
        try:
            self._shadow_map.close()
            shadow_closed = True
        except (AttributeError, BufferError):
            pass
        if shadow_closed:
            self.data = None
            self._shadow_map = None
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
        try:
            self.shm.close()
        except BufferError:
            pass
