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

class ChunkIndex:
    #__slots__ = ("path", "max_chunks", "fd", "mm", "is_writer", "_mv", "_chunk_count")

    def __init__(self, name: str = None, max_indexes: int = 2047, create: bool = False, file_path: str = None):
        self.path = file_path
        self.max_chunks = max_indexes
        size = HEADER_SIZE + (max_indexes * INDEX_SIZE)

        if file_path:
            already_exists = os.path.exists(file_path)
            if create and not already_exists:
                already_exists = os.path.exists(file_path)
                with open(file_path, 'wb') as f:
                    f.write(b'\x00' * size)

            self.file = open(file_path, 'r+b')
            self.mmap = mmap.mmap(self.file.fileno(), size)
            self.buffer = memoryview(self.mmap)
            self.is_shm = False
        else:
            if create:
                self.shm = shared_memory.SharedMemory(name=name, create=True, size=size)
            else:
                self.shm = shared_memory.SharedMemory(name=name)

            self.buffer = memoryview(self.shm.buf)
            self.is_shm = True

        self._chunk_count = self._mv[0:8].cast("Q")

    # def close(self):
    #     self._mv.release()
    #     self._chunk_count.release()
    #     self.mm.flush()
    #     self.mm.close()
    #     fcntl.flock(self.fd, fcntl.LOCK_UN)
    #     os.close(self.fd)

    # def _get_defaults(self) -> tuple[int, int, int]:
    #     def_size = int.from_bytes(self._mv[8:16], 'little')
    #     def_chunk_id = int.from_bytes(self._mv[16:24], 'little')
    #     def_status = self._mv[24]
    #     return def_size, def_chunk_id, def_status

    # def register_chunk(self, start_time: int, chunk_id: int = None, size: int = None, status: int = None) -> int:
    #     if not self.is_writer:
    #         raise PermissionError("Read-only mode")
    #     self._chunk_count[0] += 1
    #     if self._chunk_count[0] >= self.max_chunks:
    #         if self.max_chunks >= 1048576:  # 1M chunks = 64MB, reasonable limit
    #             raise IndexError(f"Registry at maximum size: {self.max_chunks}")
    #         new_max = min(1 + self.max_chunks * 2, 1048576)  # Double it, cap at 1M
    #         self._resize_file(new_max)
        
    #     offset = HEADER_SIZE + ((self._chunk_count[0]-1)<< 6)
    #     self._mv[offset + CHUNK_START_OFFSET:offset + CHUNK_END_OFFSET] = start_time.to_bytes(8, 'little')
    #     self._mv[offset + CHUNK_END_OFFSET:offset + CHUNK_WRITE_POS_OFFSET] = b"\x00\x00\x00\x00\x00\x00\x00\x00"            # End Time, write position,
    #     self._mv[offset + CHUNK_WRITE_POS_OFFSET:offset + CHUNK_NUM_RECORDS_OFFSET] = b"\x00\x00\x00\x00\x00\x00\x00\x00"    # and number of records
    #     self._mv[offset + CHUNK_NUM_RECORDS_OFFSET:offset + CHUNK_LAST_UPDATE_OFFSET] = b"\x00\x00\x00\x00\x00\x00\x00\x00"           # all initialize to 0
    #     self._mv[offset + CHUNK_LAST_UPDATE_OFFSET:offset + CHUNK_ID_OFFSET] = b"\x00\x00\x00\x00\x00\x00\x00\x00"
    #     self._mv[offset + CHUNK_ID_OFFSET:offset + CHUNK_SIZE_OFFSET] = chunk_id.to_bytes(8, 'little')
    #     self._mv[offset + CHUNK_SIZE_OFFSET:offset + CHUNK_STATUS_OFFSET] = size.to_bytes(8, 'little')
    #     self._mv[offset + CHUNK_STATUS_OFFSET] = status
        
    #     self.mm.flush()
    #     return self._chunk_count[0]
    
    # def _resize_file(self, new_max_chunks: int):
    #     """Resize file and remap for writer. Readers will handle this separately."""
    #     if not self.is_writer:
    #         raise PermissionError("Read-only mode")
        
    #     new_total_size = HEADER_SIZE + (new_max_chunks << 6)
        
    #     # Extend the file
    #     os.posix_fallocate(self.fd, 0, new_total_size)
        
    #     # Close and remap to new size
    #     self.mm.close()
    #     self.mm = mmap.mmap(self.fd, new_total_size, mmap.MAP_SHARED,
    #                         mmap.PROT_WRITE | mmap.PROT_READ)
    #     self._mv = memoryview(self.mm)
        
    #     # Update our max_chunks
    #     self.max_chunks = new_max_chunks

    # def _chunk_start_time(self, index: int) -> int:
    #     offset = HEADER_SIZE + ((index-1) << 6) + CHUNK_START_OFFSET
    #     return int.from_bytes(self._mv[offset:offset+8], 'little')

    # def _chunk_end_time(self, index: int) -> int:
    #     offset = HEADER_SIZE + ((index-1) << 6) + CHUNK_END_OFFSET
    #     return int.from_bytes(self._mv[offset:offset+8], 'little')

    # def get_chunk_metadata(self, index: int) -> memoryview:
    #     offset = HEADER_SIZE + ((index-1) << 6)
    #     return ChunkMeta(self._mv[offset:offset + CHUNK_SIZE])

    # def _binary_search_start_time(self, target_time: int) -> int:
    #     left, right = 0, self._chunk_count[0]
    #     while left < right:
    #         mid = (left + right) >> 1
    #         if self._chunk_start_time(mid) < target_time:
    #             left = mid + 1
    #         else:
    #             right = mid
    #     return left

    # def _binary_search_end_time(self, target_time: int) -> int:
    #     left, right = 0, self._chunk_count[0]
    #     while left < right:
    #         mid = (left + right) >> 1
    #         if self._chunk_end_time(mid) <= target_time:
    #             left = mid + 1
    #         else:
    #             right = mid
    #     return left

    # def get_chunks_before(self, time_t: int) -> Iterator[int]:
    #     end_idx = self._binary_search_start_time(time_t)
    #     return range(end_idx)

    # def get_chunks_after(self, time_t: int) -> Iterator[int]:
    #     start_idx = self._binary_search_end_time(time_t)
    #     return range(start_idx, self._chunk_count[0])

    # def get_chunks_in_range(self, start_time: int, end_time: int) -> Iterator[int]:
    #     start_idx = 0
    #     end_idx = self._chunk_count[0]

    #     left, right = 0, self._chunk_count[0]
    #     while left < right:
    #         mid = (left + right) >> 1
    #         if self._chunk_end_time(mid) < start_time:
    #             left = mid + 1
    #         else:
    #             right = mid
    #     start_idx = left

    #     left, right = start_idx, self._chunk_count[0]
    #     while left < right:
    #         mid = (left + right) >> 1
    #         if self._chunk_start_time(mid) <= end_time:
    #             left = mid + 1
    #         else:
    #             right = mid
    #     end_idx = left

    #     return range(start_idx, end_idx)

    # def get_latest_chunk_idx(self) -> Optional[int]:
    #     return self._chunk_count[0] - 1 if self._chunk_count[0] > 0 else None
    
    # def get_latest_chunk(self) -> Optional[ChunkMeta]:
    #     if self._chunk_count[0] == 0:
    #         return None
    #     return self.get_chunk_metadata(self._chunk_count[0])

    # def iter_chunks_meta(self) -> Iterator[ChunkMeta]:
    #     for i in range(self._chunk_count[0]):
    #         yield self.get_chunk_metadata(i)

    # def find_chunk_by_id(self, chunk_id: int) -> Optional[int]:
    #     for i in range(self._chunk_count[0]):
    #         offset = HEADER_SIZE + (i << 6) + CHUNK_ID_OFFSET
    #         val = int.from_bytes(self._mv[offset:offset+8], 'little')
    #         if val == chunk_id:
    #             return i
    #     return None