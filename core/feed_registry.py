import os
import struct
import mmap
import fcntl
from typing import Iterator, Optional

# Only keep struct for initial setup - eliminate from hot paths
CHUNK_STRUCT = struct.Struct("<QQQQB39x")  # start, end, chunk_id, size, status + padding = 64 bytes
HEADER_STRUCT = struct.Struct("<QQQB51x")  # count, default_size, default_chunk_id, default_status + padding = 64 bytes

# No helper functions - direct inline operations for maximum speed
CHUNK_SIZE = 64  # Power of 2 for bit shifting
HEADER_SIZE = 64

# Status constants
STATUS_UNKNOWN = 0
STATUS_IN_MEMORY = 1
STATUS_ON_DISK = 2
STATUS_EXPIRED = 3

# Precomputed offsets for direct memory access
CHUNK_START_OFFSET = 0
CHUNK_END_OFFSET = 8
CHUNK_ID_OFFSET = 16
CHUNK_SIZE_OFFSET = 24
CHUNK_STATUS_OFFSET = 32

class ChunkMeta:
    __slots__ = ("start_time", "end_time", "chunk_id", "size", "status")

    def __init__(self, start_time: int, end_time: int, chunk_id: int, size: int, status: int):
        self.start_time = start_time
        self.end_time = end_time
        self.chunk_id = chunk_id
        self.size = size
        self.status = status

class FeedRegistry:
    __slots__ = ("path", "max_chunks", "fd", "mm", "is_writer", "_mv")

    def __init__(self, path: str, max_chunks: int = 65535, mode: str = "r",
                 default_size: int = 0, default_chunk_id: int = 0, default_status: int = STATUS_UNKNOWN):
        self.path = path
        self.max_chunks = max_chunks
        self.is_writer = (mode == "w")

        total_size = HEADER_SIZE + (max_chunks << 6)

        flags = os.O_RDWR | os.O_CREAT
        self.fd = os.open(path, flags)

        if self.is_writer:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("Feed registry locked by another writer")

            os.posix_fallocate(self.fd, 0, total_size)

            if os.fstat(self.fd).st_size == total_size:
                header_data = HEADER_STRUCT.pack(0, default_size, default_chunk_id, default_status)
                os.pwrite(self.fd, header_data, 0)

        self.mm = mmap.mmap(self.fd, total_size, mmap.MAP_SHARED,
                            mmap.PROT_WRITE | mmap.PROT_READ)
        self._mv = memoryview(self.mm)

    def close(self):
        self.mm.close()
        os.close(self.fd)

    @property
    def chunk_count(self) -> int:
        return int.from_bytes(self._mv[0:8], 'little')

    def _get_defaults(self) -> tuple[int, int, int]:
        def_size = int.from_bytes(self._mv[8:16], 'little')
        def_chunk_id = int.from_bytes(self._mv[16:24], 'little')
        def_status = self._mv[24]
        return def_size, def_chunk_id, def_status

    def register_chunk(self, start_time: int, end_time: int, chunk_id: int = None,
                        size: int = None, status: int = None) -> int:
        if not self.is_writer:
            raise PermissionError("Read-only mode")

        count = self.chunk_count
        if count >= self.max_chunks:
            if self.max_chunks >= 1048576:  # 1M chunks = 64MB, reasonable limit
                raise IndexError(f"Registry at maximum size: {self.max_chunks}")
            new_max = min(self.max_chunks * 2, 1048576)  # Double it, cap at 1M
            self._resize_file(new_max)

        if chunk_id is None or size is None or status is None:
            def_size = int.from_bytes(self._mv[8:16], 'little')
            def_chunk_id = int.from_bytes(self._mv[16:24], 'little')
            def_status = self._mv[24]
            chunk_id = chunk_id or def_chunk_id
            size = size or def_size
            status = status or def_status

        offset = HEADER_SIZE + (count << 6)
        self._mv[offset + CHUNK_START_OFFSET:offset + CHUNK_END_OFFSET] = start_time.to_bytes(8, 'little')
        self._mv[offset + CHUNK_END_OFFSET:offset + CHUNK_ID_OFFSET] = end_time.to_bytes(8, 'little')
        self._mv[offset + CHUNK_ID_OFFSET:offset + CHUNK_SIZE_OFFSET] = chunk_id.to_bytes(8, 'little')
        self._mv[offset + CHUNK_SIZE_OFFSET:offset + CHUNK_STATUS_OFFSET] = size.to_bytes(8, 'little')
        self._mv[offset + CHUNK_STATUS_OFFSET] = status

        new_count = count + 1
        self._mv[0:8] = new_count.to_bytes(8, 'little')

        return count
    
    def _resize_file(self, new_max_chunks: int):
        """Resize file and remap for writer. Readers will handle this separately."""
        if not self.is_writer:
            raise PermissionError("Read-only mode")
        
        old_total_size = HEADER_SIZE + (self.max_chunks << 6)
        new_total_size = HEADER_SIZE + (new_max_chunks << 6)
        
        # Extend the file
        os.posix_fallocate(self.fd, 0, new_total_size)
        
        # Close and remap to new size
        self.mm.close()
        self.mm = mmap.mmap(self.fd, new_total_size, mmap.MAP_SHARED,
                            mmap.PROT_WRITE | mmap.PROT_READ)
        self._mv = memoryview(self.mm)
        
        # Update our max_chunks
        self.max_chunks = new_max_chunks

    def _chunk_start_time(self, index: int) -> int:
        offset = HEADER_SIZE + (index << 6) + CHUNK_START_OFFSET
        return int.from_bytes(self._mv[offset:offset+8], 'little')

    def _chunk_end_time(self, index: int) -> int:
        offset = HEADER_SIZE + (index << 6) + CHUNK_END_OFFSET
        return int.from_bytes(self._mv[offset:offset+8], 'little')

    def get_chunk_raw(self, index: int) -> memoryview:
        offset = HEADER_SIZE + (index << 6)
        return self._mv[offset:offset + CHUNK_SIZE]

    def get_chunk(self, index: int) -> ChunkMeta:
        offset = HEADER_SIZE + (index << 6)
        start = int.from_bytes(self._mv[offset + CHUNK_START_OFFSET:offset + CHUNK_END_OFFSET], 'little')
        end = int.from_bytes(self._mv[offset + CHUNK_END_OFFSET:offset + CHUNK_ID_OFFSET], 'little')
        chunk_id = int.from_bytes(self._mv[offset + CHUNK_ID_OFFSET:offset + CHUNK_SIZE_OFFSET], 'little')
        size = int.from_bytes(self._mv[offset + CHUNK_SIZE_OFFSET:offset + CHUNK_STATUS_OFFSET], 'little')
        status = self._mv[offset + CHUNK_STATUS_OFFSET]
        return ChunkMeta(start, end, chunk_id, size, status)

    def _binary_search_start_time(self, target_time: int) -> int:
        left, right = 0, self.chunk_count
        while left < right:
            mid = (left + right) >> 1
            if self._chunk_start_time(mid) < target_time:
                left = mid + 1
            else:
                right = mid
        return left

    def _binary_search_end_time(self, target_time: int) -> int:
        left, right = 0, self.chunk_count
        while left < right:
            mid = (left + right) >> 1
            if self._chunk_end_time(mid) <= target_time:
                left = mid + 1
            else:
                right = mid
        return left

    def get_chunks_before(self, time_t: int) -> Iterator[int]:
        end_idx = self._binary_search_start_time(time_t)
        return range(end_idx)

    def get_chunks_after(self, time_t: int) -> Iterator[int]:
        start_idx = self._binary_search_end_time(time_t)
        return range(start_idx, self.chunk_count)

    def get_chunks_in_range(self, start_time: int, end_time: int) -> Iterator[int]:
        start_idx = 0
        end_idx = self.chunk_count

        left, right = 0, self.chunk_count
        while left < right:
            mid = (left + right) >> 1
            if self._chunk_end_time(mid) < start_time:
                left = mid + 1
            else:
                right = mid
        start_idx = left

        left, right = start_idx, self.chunk_count
        while left < right:
            mid = (left + right) >> 1
            if self._chunk_start_time(mid) <= end_time:
                left = mid + 1
            else:
                right = mid
        end_idx = left

        return range(start_idx, end_idx)

    def get_latest_chunk_idx(self) -> Optional[int]:
        count = self.chunk_count
        return count - 1 if count > 0 else None

    def iter_chunk_indices(self) -> Iterator[int]:
        return range(self.chunk_count)

    def iter_chunks_meta(self) -> Iterator[ChunkMeta]:
        count = self.chunk_count
        for i in range(count):
            yield self.get_chunk(i)

    def update_chunk_status(self, index: int, status: int):
        if not self.is_writer:
            raise PermissionError("Read-only mode")
        offset = HEADER_SIZE + (index << 6) + CHUNK_STATUS_OFFSET
        self._mv[offset] = status

    def find_chunk_by_id(self, chunk_id: int) -> Optional[int]:
        count = self.chunk_count
        for i in range(count):
            offset = HEADER_SIZE + (i << 6) + CHUNK_ID_OFFSET
            val = int.from_bytes(self._mv[offset:offset+8], 'little')
            if val == chunk_id:
                return i
        return None
