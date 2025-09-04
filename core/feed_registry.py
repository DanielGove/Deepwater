import os
import struct
import mmap
import fcntl
from typing import Iterator, Optional

# Only keep struct for initial setup - eliminate from hot paths
CHUNK_STRUCT = struct.Struct("<QQQQQQQB7x")  # start, end, write_pos, num_records, last_update, chunk_id, size, status + padding = 64 bytes
HEADER_STRUCT = struct.Struct("<QQB59x")  # chunk_count, default_size, default_status + padding = 64 bytes

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
CHUNK_WRITE_POS_OFFSET = 16
CHUNK_NUM_RECORDS_OFFSET = 24
CHUNK_LAST_UPDATE_OFFSET = 32
CHUNK_ID_OFFSET = 40
CHUNK_SIZE_OFFSET = 48
CHUNK_STATUS_OFFSET = 56

class ChunkMeta:
    """
    Zero-copy, mutable view over a single 64-byte chunk record.
    Reads and writes go directly to the underlying mmapped memory.
    """
    __slots__ = ("_mv", "_start", "_end", "_wp", "_nr", "_lu", "_id", "_size", "_status")

    def __init__(self, chunk_record: memoryview):
        self._mv = chunk_record
        self._start  = chunk_record[CHUNK_START_OFFSET:CHUNK_END_OFFSET].cast("Q")
        self._end    = chunk_record[CHUNK_END_OFFSET:CHUNK_WRITE_POS_OFFSET].cast("Q")
        self._wp     = chunk_record[CHUNK_WRITE_POS_OFFSET:CHUNK_NUM_RECORDS_OFFSET].cast("Q")
        self._nr     = chunk_record[CHUNK_NUM_RECORDS_OFFSET: CHUNK_LAST_UPDATE_OFFSET].cast("Q")
        self._lu     = chunk_record[CHUNK_LAST_UPDATE_OFFSET: CHUNK_ID_OFFSET].cast("Q")
        self._id     = chunk_record[CHUNK_ID_OFFSET:CHUNK_SIZE_OFFSET].cast("Q")
        self._size   = chunk_record[CHUNK_SIZE_OFFSET:CHUNK_STATUS_OFFSET].cast("Q")
        self._status = chunk_record[CHUNK_STATUS_OFFSET:CHUNK_STATUS_OFFSET+1].cast("B")

    @property
    def start_time(self) -> int:
        return self._start[0]
    @start_time.setter
    def start_time(self, v: int) -> None:
        self._start[0] = v

    @property
    def end_time(self) -> int:
        return self._end[0]
    @end_time.setter
    def end_time(self, v: int) -> None:
        self._end[0] = v

    @property
    def write_pos(self) -> int:
        return self._wp[0]
    @write_pos.setter
    def write_pos(self, v: int) -> None:
        self._wp[0] = v

    @property
    def num_records(self) -> int:
        return self._nr[0]
    @num_records.setter
    def num_records(self, v: int) -> None:
        self._nr[0] = v

    @property
    def last_update(self) -> int:
        return self._lu[0]
    @last_update.setter
    def last_update(self, v: int) -> None:
        self._lu[0] = v

    @property
    def chunk_id(self) -> int:
        return self._id[0]
    @chunk_id.setter
    def chunk_id(self, v: int) -> None:
        raise Exception("Cannot overwrite chunk id in registry")

    @property
    def size(self) -> int:
        return self._size[0]
    @size.setter
    def size(self, v: int) -> None:
        self._size[0] = v

    @property
    def status(self) -> int:
        return self._status[0]
    @status.setter
    def status(self, v: int) -> None:
        # clamp to 0..255 to avoid ValueError from casted B view
        self._status[0] = v & 0xFF

    # Optional helpers
    def as_tuple(self) -> tuple[int,int,int,int,int]:
        return (self.start_time, self.end_time, self.chunk_id, self.size, self.status)

    def release(self) -> None:
        # Release views (important before closing the mmap on some Python builds)
        self._start.release()
        self._end.release()
        self._wp.release()
        self._nr.release()
        self._lu.release()
        self._id.release()
        self._size.release()
        self._status.release()
        self._mv.release()

    def __repr__(self) -> str:
        return (f"ChunkMeta(start={self.start_time}, end={self.end_time}, "
                f"id={self.chunk_id}, size={self.size}, status={self.status})")


class FeedRegistry:
    __slots__ = ("path", "max_chunks", "fd", "mm", "is_writer", "_mv", "_chunk_count")

    def __init__(self, path: str, max_chunks: int = 65535, mode: str = "r",
                 default_size: int = 0, default_status: int = STATUS_UNKNOWN):
        self.path = path
        self.max_chunks = max_chunks
        self.is_writer = mode == "w"

        desired_size = HEADER_SIZE + (max_chunks << 6)
        already_exists = os.path.exists(path)

        self.fd = os.open(path, os.O_RDWR | os.O_CREAT)
        current_size = os.fstat(self.fd).st_size

        if self.is_writer:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("Feed registry locked by another writer")

            if current_size < desired_size:
                os.posix_fallocate(self.fd, 0, desired_size)
                current_size = desired_size

            if (not already_exists) or current_size == 0:
                header_data = HEADER_STRUCT.pack(0, default_size, default_status)
                os.pwrite(self.fd, header_data, 0)
                current_size = desired_size

        self.mm = mmap.mmap(self.fd, current_size, mmap.MAP_SHARED,
                            mmap.PROT_WRITE | mmap.PROT_READ)
        self._mv = memoryview(self.mm)
        self._chunk_count = self._mv[0:8].cast("Q")

    def close(self):
        self._mv.release()
        self._chunk_count.release()
        self.mm.flush()
        self.mm.close()
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)

    def _get_defaults(self) -> tuple[int, int, int]:
        def_size = int.from_bytes(self._mv[8:16], 'little')
        def_chunk_id = int.from_bytes(self._mv[16:24], 'little')
        def_status = self._mv[24]
        return def_size, def_chunk_id, def_status

    def register_chunk(self, start_time: int, chunk_id: int = None, size: int = None, status: int = None) -> int:
        if not self.is_writer:
            raise PermissionError("Read-only mode")
        self._chunk_count[0] += 1
        if self._chunk_count[0] >= self.max_chunks:
            if self.max_chunks >= 1048576:  # 1M chunks = 64MB, reasonable limit
                raise IndexError(f"Registry at maximum size: {self.max_chunks}")
            new_max = min(1 + self.max_chunks * 2, 1048576)  # Double it, cap at 1M
            self._resize_file(new_max)
        
        offset = HEADER_SIZE + ((self._chunk_count[0]-1)<< 6)
        self._mv[offset + CHUNK_START_OFFSET:offset + CHUNK_END_OFFSET] = start_time.to_bytes(8, 'little')
        self._mv[offset + CHUNK_END_OFFSET:offset + CHUNK_WRITE_POS_OFFSET] = b"\x00\x00\x00\x00\x00\x00\x00\x00"            # End Time, write position,
        self._mv[offset + CHUNK_WRITE_POS_OFFSET:offset + CHUNK_NUM_RECORDS_OFFSET] = b"\x00\x00\x00\x00\x00\x00\x00\x00"    # and number of records
        self._mv[offset + CHUNK_NUM_RECORDS_OFFSET:offset + CHUNK_LAST_UPDATE_OFFSET] = b"\x00\x00\x00\x00\x00\x00\x00\x00"           # all initialize to 0
        self._mv[offset + CHUNK_LAST_UPDATE_OFFSET:offset + CHUNK_ID_OFFSET] = b"\x00\x00\x00\x00\x00\x00\x00\x00"
        self._mv[offset + CHUNK_ID_OFFSET:offset + CHUNK_SIZE_OFFSET] = chunk_id.to_bytes(8, 'little')
        self._mv[offset + CHUNK_SIZE_OFFSET:offset + CHUNK_STATUS_OFFSET] = size.to_bytes(8, 'little')
        self._mv[offset + CHUNK_STATUS_OFFSET] = status
        
        self.mm.flush()
        return self._chunk_count[0]
    
    def _resize_file(self, new_max_chunks: int):
        """Resize file and remap for writer. Readers will handle this separately."""
        if not self.is_writer:
            raise PermissionError("Read-only mode")
        
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
        offset = HEADER_SIZE + ((index-1) << 6) + CHUNK_START_OFFSET
        return int.from_bytes(self._mv[offset:offset+8], 'little')

    def _chunk_end_time(self, index: int) -> int:
        offset = HEADER_SIZE + ((index-1) << 6) + CHUNK_END_OFFSET
        return int.from_bytes(self._mv[offset:offset+8], 'little')

    def get_chunk_metadata(self, index: int) -> memoryview:
        offset = HEADER_SIZE + ((index-1) << 6)
        return ChunkMeta(self._mv[offset:offset + CHUNK_SIZE])

    def _binary_search_start_time(self, target_time: int) -> int:
        left, right = 0, self._chunk_count[0]
        while left < right:
            mid = (left + right) >> 1
            if self._chunk_start_time(mid) < target_time:
                left = mid + 1
            else:
                right = mid
        return left

    def _binary_search_end_time(self, target_time: int) -> int:
        left, right = 0, self._chunk_count[0]
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
        return range(start_idx, self._chunk_count[0])

    def get_chunks_in_range(self, start_time: int, end_time: int) -> Iterator[int]:
        start_idx = 0
        end_idx = self._chunk_count[0]

        left, right = 0, self._chunk_count[0]
        while left < right:
            mid = (left + right) >> 1
            if self._chunk_end_time(mid) < start_time:
                left = mid + 1
            else:
                right = mid
        start_idx = left

        left, right = start_idx, self._chunk_count[0]
        while left < right:
            mid = (left + right) >> 1
            if self._chunk_start_time(mid) <= end_time:
                left = mid + 1
            else:
                right = mid
        end_idx = left

        return range(start_idx, end_idx)

    def get_latest_chunk_idx(self) -> Optional[int]:
        return self._chunk_count[0] - 1 if self._chunk_count[0] > 0 else None
    
    def get_latest_chunk(self) -> Optional[ChunkMeta]:
        if self._chunk_count[0] == 0:
            return None
        return self.get_chunk_metadata(self._chunk_count[0])

    def iter_chunks_meta(self) -> Iterator[ChunkMeta]:
        for i in range(self._chunk_count[0]):
            yield self.get_chunk_metadata(i)

    def find_chunk_by_id(self, chunk_id: int) -> Optional[int]:
        for i in range(self._chunk_count[0]):
            offset = HEADER_SIZE + (i << 6) + CHUNK_ID_OFFSET
            val = int.from_bytes(self._mv[offset:offset+8], 'little')
            if val == chunk_id:
                return i
        return None