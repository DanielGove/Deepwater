import os
import struct
import mmap
import fcntl
from typing import Iterator, Optional
from multiprocessing import shared_memory

HEADER_STRUCT = struct.Struct("<Q56x") # The number of indices to 64 bits
HEADER_SIZE = HEADER_STRUCT.size

INDEX_STRUCT = struct.Struct("<QQQ40x")  # timestamp, offset, size, 40x pad for later = 64Bytes
INDEX_SIZE = INDEX_STRUCT.size

# Precomputed offsets for direct memory access
TIMESTAMP_OFFSET = 0
OFFSET_WITHIN_THE_INDEX_STRUCT_OF_THE_VALUE_THAT_REPRESENTS_THE_OFFSET_OF_THE_RECORD_WITHIN_THE_CHUNK = 8
SIZE_OFFSET = 16

class IndexRecord:
    """
    Zero-copy, mutable view over a single 64-byte index record.
    Reads and writes go directly to the underlying memory.
    """
    __slots__ = ("_mv", "_timestamp", "_offset", "_size")

    def __init__(self, chunk_record: memoryview):
        self._mv = chunk_record
        self._timestamp  = self._mv[TIMESTAMP_OFFSET:OFFSET_WITHIN_THE_INDEX_STRUCT_OF_THE_VALUE_THAT_REPRESENTS_THE_OFFSET_OF_THE_RECORD_WITHIN_THE_CHUNK].cast("Q")
        self._offset    = chunk_record[OFFSET_WITHIN_THE_INDEX_STRUCT_OF_THE_VALUE_THAT_REPRESENTS_THE_OFFSET_OF_THE_RECORD_WITHIN_THE_CHUNK:SIZE_OFFSET].cast("Q")
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
    __slots__ = ("file_path", "max_indices", "is_shm", "file", "fd", "mmap", "_mv", "_index_count")

    def __init__(self, name: str = None, max_indices: int = 2047, create: bool = False, directory: str = None):
        self.file_path = directory/name
        self.max_indices = max_indices
        size = HEADER_SIZE + (max_indices * INDEX_SIZE)

        if self.file_path:
            already_exists = os.path.exists(self.file_path)
            if create and not already_exists:
                already_exists = os.path.exists(self.file_path)
                with open(self.file_path, 'wb') as f:
                    f.write(b'\x00' * size)

            self.file = open(self.file_path, 'r+b')
            self.mmap = mmap.mmap(self.file.fileno(), size)
            self._mv = memoryview(self.mmap)
            self.is_shm = False
        else:
            if create:
                self.shm = shared_memory.SharedMemory(name=name, create=True, size=size)
            else:
                self.shm = shared_memory.SharedMemory(name=name)

            self._mv = self.shm.buf
            self.is_shm = True

        self._index_count = self._mv[0:8].cast("Q")

    def close(self):
        self._index_count.release()
        self._mv.release()
        if self.is_shm:
            self.shm.close()
            self.shm.unlink()
        else:
            self.mmap.flush()
            self.mmap.close()
            self.file.close()

    def create_index(self, timestamp: int, offset: int = None, size: int = None) -> int:
        offset = HEADER_SIZE + (self._index_count[0] * INDEX_SIZE)
        self._mv[offset + TIMESTAMP_OFFSET:offset + OFFSET_WITHIN_THE_INDEX_STRUCT_OF_THE_VALUE_THAT_REPRESENTS_THE_OFFSET_OF_THE_RECORD_WITHIN_THE_CHUNK] = timestamp.to_bytes(8, 'little')
        self._mv[offset + OFFSET_WITHIN_THE_INDEX_STRUCT_OF_THE_VALUE_THAT_REPRESENTS_THE_OFFSET_OF_THE_RECORD_WITHIN_THE_CHUNK:offset + SIZE_OFFSET] = offset.to_bytes(8, 'little')
        self._mv[offset + SIZE_OFFSET:offset + SIZE_OFFSET + 8] = size.to_bytes(8, 'little')
        self._index_count[0] += 1
        return self._index_count[0]

    def _timestamp(self, index: int) -> memoryview:
        offset = HEADER_SIZE + (index * INDEX_SIZE) + TIMESTAMP_OFFSET
        return self._mv[offset:offset + 8]

    def get_index(self, index: int) -> memoryview:
        offset = HEADER_SIZE + (index * INDEX_SIZE)
        return IndexRecord(self._mv[offset:offset + INDEX_SIZE])

    def _binary_search_start_time(self, target_time: int) -> int:
        left, right = 0, self._index_count[0]
        while left < right:
            mid = (left + right) >> 1
            if self._timestamp(mid) < target_time:
                left = mid + 1
            else:
                right = mid
        return left

    def _binary_search_end_time(self, target_time: int) -> int:
        left, right = 0, self._index_count[0]
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
        return range(start_idx, self._index_count[0])

    def get_indicies_in_range(self, start_time: int, end_time: int) -> Iterator[int]:
        start_idx = 0
        end_idx = self._index_count[0]

        left, right = 0, self._index_count[0]
        while left < right:
            mid = (left + right) >> 1
            if self._timestamp(mid) < start_time:
                left = mid + 1
            else:
                right = mid
        start_idx = left

        left, right = start_idx, self._index_count[0]
        while left < right:
            mid = (left + right) >> 1
            if self._timestamp(mid) <= end_time:
                left = mid + 1
            else:
                right = mid
        end_idx = left

        return range(start_idx, end_idx)
    
    def get_latest_index(self) -> Optional[IndexRecord]:
        if self._index_count[0] == 0:
            return None
        return self.get_index(self._index_count[0]-1)