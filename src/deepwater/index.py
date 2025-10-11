import os
import struct
import mmap
import fcntl
from typing import Iterator, Optional
from multiprocessing import shared_memory

HEADER_STRUCT = struct.Struct("<QQ48x") # Number of indices, capacity to 64 bits
HEADER_SIZE = HEADER_STRUCT.size

INDEX_STRUCT = struct.Struct("<QQQ40x")  # timestamp, offset, size, 40x pad for later = 64Bytes
INDEX_SIZE = INDEX_STRUCT.size

# Precomputed offsets for direct memory access
TIMESTAMP_OFFSET = 0
OFFSET_OFFSET = 8
SIZE_OFFSET = 16

class IndexRecord:
    """
    Zero-copy, mutable view over a single 64-byte index record.
    Reads and writes go directly to the underlying memory.
    """
    __slots__ = ("_mv", "_timestamp", "_offset", "_size")

    def __init__(self, chunk_record: memoryview):
        self._mv = chunk_record
        self._timestamp  = self._mv[TIMESTAMP_OFFSET:OFFSET_OFFSET].cast("Q")
        self._offset    = chunk_record[OFFSET_OFFSET:SIZE_OFFSET].cast("Q")
        self._size     = chunk_record[SIZE_OFFSET:SIZE_OFFSET+8].cast("Q")

    @property
    def timestamp(self) -> int:
        return self._timestamp[0]
    @timestamp.setter
    def timestamp(self, v: int) -> None:
        self._timestamp[0] = v

    @property
    def offset(self) -> int:
        return self._offset[0]
    @offset.setter
    def offset(self, v: int) -> None:
        self._offset[0] = v

    @property
    def size(self) -> int:
        return self._size[0]
    @size.setter
    def size(self, v: int) -> None:
        self._size[0] = v

    def as_tuple(self) -> tuple[int,int,int]:
        return (self.timestamp, self.offset, self.size)

    def release(self) -> None:
        self._timestamp.release()
        self._offset.release()
        self._size.release()
        self._mv.release()

    def __repr__(self) -> str:
        return (f"IndexRecord(start={self.timestamp}, offset={self.offset}, size={self.size})")

class ChunkIndex:
    __slots__ = ("_mv", "closeables", "read_only", "is_shm", "_index_count", "_capacity")

    def __init__(self, mv, closeables, read_only, is_shm):
        self._mv = mv
        self.closeables = closeables
        self.read_only = read_only
        self.is_shm = is_shm
        self._index_count = self._mv[0:8].cast("Q")[0]
        self._capacity = self._mv[8:16].cast("Q")[0]

    @classmethod
    def create_shm(cls, name:str, capacity:int=2047):
        shm = shared_memory.SharedMemory(name=name, create=True, size=HEADER_SIZE+capacity*INDEX_SIZE)
        HEADER_STRUCT.pack_into(shm.buf, 0, 0, capacity)
        return cls(mv=shm.buf, closeables=[shm], read_only=False, is_shm=True)

    @classmethod
    def open_shm(cls, name:str):
        shm = shared_memory.SharedMemory(name=name, create=False)
        return cls(mv=shm.buf, closeables=[shm], read_only=True, is_shm=True)
    
    def close_shm(self):
        self._mv.release()
        self.closeables[0].close()
        if not self.read_only:
            self.closeables[0].unlink()

    @classmethod
    def create_file(cls, path:str, capacity:int=2047):
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
        os.ftruncate(fd, HEADER_SIZE+capacity*INDEX_SIZE)
        mm = mmap.mmap(fd, length=capacity, access=mmap.ACCESS_WRITE)
        HEADER_STRUCT.pack_into(mm, 0, 0, capacity)
        return cls(memoryview(mm), [mm, fd], read_only=False, is_shm=False)
    
    @classmethod
    def open_file(cls, path:str):
        fd = os.open(path, os.O_RDONLY)
        mm = mmap.mmap(fd, length=0, access=mmap.ACCESS_READ)
        return cls(memoryview(mm), [fd,mm], read_only=True, is_shm=False)

    def close_file(self):
        self._mv.release()
        mm = self.closeables[0]
        fd = self.closeables[1]
        if not self.read_only:
            mm.flush()
            os.ftruncate(fd, HEADER_SIZE + (self._index_count * INDEX_SIZE))
            os.fsync(fd)
        mm.close()
        os.close(fd)

    def create_index(self, timestamp: int, offset: int = None) -> int:
        offset = HEADER_SIZE + (self._index_count * INDEX_SIZE)
        self._mv[offset + TIMESTAMP_OFFSET:offset + OFFSET_OFFSET] = timestamp.to_bytes(8, 'little')
        self._mv[offset + OFFSET_OFFSET:offset + OFFSET_OFFSET + 8] = offset.to_bytes(8, 'little')
        self._index_count += 1
        return self._index_count

    def _timestamp(self, index: int) -> memoryview:
        offset = HEADER_SIZE + (index * INDEX_SIZE) + TIMESTAMP_OFFSET
        return self._mv[offset:offset + 8]

    def get_index(self, index: int) -> memoryview:
        offset = HEADER_SIZE + (index * INDEX_SIZE)
        return IndexRecord(self._mv[offset:offset + INDEX_SIZE])

    def _binary_search_start_time(self, target_time: int) -> int:
        left, right = 0, self._index_count
        while left < right:
            mid = (left + right) >> 1
            if self._timestamp(mid) < target_time:
                left = mid + 1
            else:
                right = mid
        return left

    def _binary_search_end_time(self, target_time: int) -> int:
        left, right = 0, self._index_count
        while left < right:
            mid = (left + right) >> 1
            if self._timestamp(mid) <= target_time:
                left = mid + 1
            else:
                right = mid
        return left

    def get_indices_before(self, time_t: int) -> Iterator[int]:
        end_idx = self._binary_search_start_time(time_t)
        return range(end_idx)

    def get_indices_after(self, time_t: int) -> Iterator[int]:
        start_idx = self._binary_search_end_time(time_t)
        return range(start_idx, self._index_count)

    def get_indicies_in_range(self, start_time: int, end_time: int) -> Iterator[int]:
        start_idx = 0
        end_idx = self._index_count

        left, right = 0, self._index_count
        while left < right:
            mid = (left + right) >> 1
            if self._timestamp(mid) < start_time:
                left = mid + 1
            else:
                right = mid
        start_idx = left

        left, right = start_idx, self._index_count
        while left < right:
            mid = (left + right) >> 1
            if self._timestamp(mid) <= end_time:
                left = mid + 1
            else:
                right = mid
        end_idx = left

        return range(start_idx, end_idx)
    
    def get_latest_index(self) -> Optional[IndexRecord]:
        if self._index_count == 0:
            return None
        return self.get_index(self._index_count-1)