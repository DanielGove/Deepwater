import os
import struct
import mmap
import fcntl
from typing import Iterator, Optional
from multiprocessing import shared_memory

HEADER_STRUCT = struct.Struct("<QQ48x")  # Number of indices, capacity to 64 bits
HEADER_SIZE = HEADER_STRUCT.size

INDEX_STRUCT = struct.Struct("<QQQQ32x")  # ts0, ts1, ts2, offset, pad to 64B
INDEX_SIZE = INDEX_STRUCT.size

# Precomputed offsets for direct memory access (little-endian)
TS0_OFFSET = 0
TS1_OFFSET = 8
TS2_OFFSET = 16
OFFSET_OFFSET = 24

class IndexRecord:
    """
    Zero-copy, mutable view over a single 64-byte index record.
    Stores up to 3 timestamps (ts0/ts1/ts2) plus chunk offset.
    """
    __slots__ = ("_mv", "_ts0", "_ts1", "_ts2", "_offset")

    def __init__(self, chunk_record: memoryview):
        self._mv = chunk_record
        self._ts0 = chunk_record[0:8].cast("Q")
        self._ts1 = chunk_record[8:16].cast("Q")
        self._ts2 = chunk_record[16:24].cast("Q")
        self._offset = chunk_record[24:32].cast("Q")

    @property
    def ts0(self) -> int:
        return self._ts0[0]
    @ts0.setter
    def ts0(self, v: int) -> None:
        self._ts0[0] = v

    @property
    def ts1(self) -> int:
        return self._ts1[0]
    @ts1.setter
    def ts1(self, v: int) -> None:
        self._ts1[0] = v

    @property
    def ts2(self) -> int:
        return self._ts2[0]
    @ts2.setter
    def ts2(self, v: int) -> None:
        self._ts2[0] = v

    @property
    def offset(self) -> int:
        return self._offset[0]
    @offset.setter
    def offset(self, v: int) -> None:
        self._offset[0] = v
    def as_tuple(self) -> tuple[int, int, int, int]:
        return (self.ts0, self.ts1, self.ts2, self.offset)

    def release(self) -> None:
        self._ts0.release()
        self._ts1.release()
        self._ts2.release()
        self._offset.release()
        self._mv.release()

    def __repr__(self) -> str:
        return f"IndexRecord(ts0={self.ts0}, ts1={self.ts1}, ts2={self.ts2}, offset={self.offset})"

class ChunkIndex:
    __slots__ = ("_mv", "closeables", "read_only", "is_shm", "_count", "_capacity")

    def __init__(self, mv, closeables, read_only, is_shm):
        self._mv = mv
        self.closeables = closeables
        self.read_only = read_only
        self.is_shm = is_shm
        self._count = self._mv[0:8].cast("Q")
        self._capacity = self._mv[8:16].cast("Q")

    @property
    def count(self) -> int:
        return self._count[0]

    @property
    def capacity(self) -> int:
        return self._capacity[0]

    @classmethod
    def create_shm(cls, name: str, capacity: int = 2047):
        size = HEADER_SIZE + capacity * INDEX_SIZE
        shm = shared_memory.SharedMemory(name=name, create=True, size=size)
        HEADER_STRUCT.pack_into(shm.buf, 0, 0, capacity)
        mv = memoryview(shm.buf)
        return cls(mv=mv, closeables=[shm], read_only=False, is_shm=True)

    @classmethod
    def open_shm(cls, name: str):
        shm = shared_memory.SharedMemory(name=name, create=False)
        return cls(mv=memoryview(shm.buf), closeables=[shm], read_only=True, is_shm=True)
    
    def close_shm(self):
        self._capacity.release()
        self._count.release()
        self._mv.release()
        shm = self.closeables[0]
        if not self.read_only:
            shm.unlink()
        shm.close()

    @classmethod
    def create_file(cls, path: str, capacity: int = 2047):
        size = HEADER_SIZE + capacity * INDEX_SIZE
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
        os.ftruncate(fd, size)
        mm = mmap.mmap(fd, length=size, access=mmap.ACCESS_WRITE)
        HEADER_STRUCT.pack_into(mm, 0, 0, capacity)
        return cls(memoryview(mm), [mm, fd], read_only=False, is_shm=False)
    
    @classmethod
    def open_file(cls, path: str):
        fd = os.open(path, os.O_RDONLY)
        mm = mmap.mmap(fd, length=0, access=mmap.ACCESS_READ)
        return cls(memoryview(mm), [mm, fd], read_only=True, is_shm=False)

    def close_file(self):
        self._capacity.release()
        self._count.release()
        self._mv.release()
        mm = self.closeables[0]
        fd = self.closeables[1]
        if not self.read_only:
            mm.flush()
            os.fsync(fd)
        mm.close()
        os.close(fd)

    def create_index(self, timestamps, chunk_offset: int) -> int:
        """
        Append an index entry storing up to three clocks (ts0, ts1, ts2) plus chunk offset.
        `timestamps` may be an int (ts0 only) or iterable of up to 3 ints.
        """
        if self.read_only:
            raise PermissionError("read-only index")
        # Normalize timestamps to 3-tuple
        if isinstance(timestamps, int):
            ts0, ts1, ts2 = timestamps, 0, 0
        else:
            ts_list = list(timestamps)
            ts0 = ts_list[0] if len(ts_list) > 0 else 0
            ts1 = ts_list[1] if len(ts_list) > 1 else 0
            ts2 = ts_list[2] if len(ts_list) > 2 else 0
        idx = self.count
        if idx >= self.capacity:
            raise IndexError("Chunk index at capacity")
        entry_offset = HEADER_SIZE + (idx * INDEX_SIZE)
        INDEX_STRUCT.pack_into(self._mv, entry_offset, ts0, ts1, ts2, chunk_offset)
        self._count[0] = idx + 1
        return idx + 1

    def _entry(self, index: int) -> tuple[int, int, int, int]:
        entry_offset = HEADER_SIZE + (index * INDEX_SIZE)
        ts0 = int.from_bytes(self._mv[entry_offset + TS0_OFFSET:entry_offset + TS0_OFFSET + 8], "little")
        ts1 = int.from_bytes(self._mv[entry_offset + TS1_OFFSET:entry_offset + TS1_OFFSET + 8], "little")
        ts2 = int.from_bytes(self._mv[entry_offset + TS2_OFFSET:entry_offset + TS2_OFFSET + 8], "little")
        off = int.from_bytes(self._mv[entry_offset + OFFSET_OFFSET:entry_offset + OFFSET_OFFSET + 8], "little")
        return ts0, ts1, ts2, off

    def get_index(self, index: int) -> IndexRecord:
        if index < 0 or index >= self.count:
            raise IndexError("index out of range")
        offset = HEADER_SIZE + (index * INDEX_SIZE)
        return IndexRecord(self._mv[offset:offset + INDEX_SIZE])

    def get_latest_index(self) -> Optional[IndexRecord]:
        if self.count == 0:
            return None
        return self.get_index(self.count - 1)

    def _binary_search_start_time(self, target_time: int, ts_idx: int = 0) -> int:
        left, right = 0, self.count
        while left < right:
            mid = (left + right) >> 1
            if self._entry(mid)[ts_idx] < target_time:
                left = mid + 1
            else:
                right = mid
        return left

    def _binary_search_end_time(self, target_time: int, ts_idx: int = 0) -> int:
        left, right = 0, self.count
        while left < right:
            mid = (left + right) >> 1
            if self._entry(mid)[ts_idx] <= target_time:
                left = mid + 1
            else:
                right = mid
        return left

    def get_latest_at_or_before(self, time_t: int, ts_idx: int = 0) -> Optional[IndexRecord]:
        """Linear scan backwards for simplicity (index sizes are small). ts_idx selects ts0/ts1/ts2."""
        for idx in range(self.count - 1, -1, -1):
            ts0, ts1, ts2, _ = self._entry(idx)
            ts = (ts0, ts1, ts2)[ts_idx]
            if ts <= time_t:
                return self.get_index(idx)
        return None

    def get_indices_before(self, time_t: int, ts_idx: int = 0) -> Iterator[int]:
        end_idx = self._binary_search_start_time(time_t, ts_idx)
        return range(end_idx)

    def get_indices_after(self, time_t: int, ts_idx: int = 0) -> Iterator[int]:
        start_idx = self._binary_search_end_time(time_t, ts_idx)
        return range(start_idx, self.count)

    def get_indicies_in_range(self, start_time: int, end_time: int, ts_idx: int = 0) -> Iterator[int]:
        """Return indices where selected timestamp is within [start_time, end_time]."""
        start_idx = 0
        end_idx = self.count

        left, right = 0, self.count
        while left < right:
            mid = (left + right) >> 1
            if self._entry(mid)[ts_idx] < start_time:
                left = mid + 1
            else:
                right = mid
        start_idx = left

        left, right = start_idx, self.count
        while left < right:
            mid = (left + right) >> 1
            if self._entry(mid)[ts_idx] <= end_time:
                left = mid + 1
            else:
                right = mid
        end_idx = left

        return range(start_idx, end_idx)
