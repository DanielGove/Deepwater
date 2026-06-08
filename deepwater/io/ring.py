import hashlib
import os
import time
from pathlib import Path
from multiprocessing import resource_tracker, shared_memory
from typing import Optional

_DRAIN_REQUEST_FLAG = 1 << 63
_COUNT_MASK = _DRAIN_REQUEST_FLAG - 1


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

    @classmethod
    def open(cls, name: str, data_size: int | None = None, *, shm_name: str | None = None):
        shm_name = shm_name or name
        if data_size is None:
            shm = shared_memory.SharedMemory(name=shm_name, create=False)
            try:
                data_size = int(getattr(shm, "size", len(shm.buf))) - cls.HEADER_SIZE
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
        if unlink:
            try:
                self.shm.unlink()
            except FileNotFoundError:
                pass
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
        try:
            self.shm.close()
        except BufferError:
            pass

