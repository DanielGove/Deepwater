import struct
import mmap
import os
from pathlib import Path
from typing import Optional, Tuple, Iterator
from multiprocessing import shared_memory


class IndexEntry:
    """16-byte index entry: timestamp -> record location"""
    SIZE = 16
    FORMAT = '<QIHBx'  # timestamp, offset, size, type, reserved
    
    def __init__(self, timestamp: int = 0, offset: int = 0, size: int = 0, record_type: int = 0):
        self.timestamp = timestamp
        self.offset = offset  # Offset within chunk
        self.size = size      # Record size in bytes
        self.record_type = record_type
    
    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, self.timestamp, self.offset, self.size, self.record_type)
    
    @classmethod
    def unpack(cls, data: bytes) -> 'IndexEntry':
        timestamp, offset, size, record_type = struct.unpack(cls.FORMAT, data)
        return cls(timestamp, offset, size, record_type)


class ChunkIndex:
    """Unified chunk index for SHM and disk storage"""
    
    HEADER_SIZE = 32  # [entry_count: 8][chunk_start_time: 8][chunk_end_time: 8][reserved: 8]
    
    def __init__(self, feed_name: str, chunk_id: int, storage_type: str = 'shm', 
                 base_path: Optional[Path] = None, create: bool = False):
        self.feed_name = feed_name.ljust(32, '\0')[:32]  # Ensure 32 bytes
        self.chunk_id = chunk_id
        self.storage_type = storage_type
        self.base_path = base_path or Path('.')
        
        self.max_entries = 131072  # 128K entries max
        self.index_size = self.HEADER_SIZE + (self.max_entries * IndexEntry.SIZE)
        
        if storage_type == 'shm':
            self._init_shm(create)
        else:
            self._init_disk(create)
    
    def _init_shm(self, create: bool):
        """Initialize shared memory index"""
        shm_name = f"idx-{self.feed_name.strip('\0')}-{self.chunk_id}"
        
        try:
            if create:
                self.shm = shared_memory.SharedMemory(name=shm_name, create=True, size=self.index_size)
                self._init_header()
            else:
                self.shm = shared_memory.SharedMemory(name=shm_name)
        except FileExistsError:
            self.shm = shared_memory.SharedMemory(name=shm_name)
        
        self.buffer = memoryview(self.shm.buf)
        self._map_views()
    
    def _init_disk(self, create: bool):
        """Initialize disk-based index"""
        index_dir = self.base_path / 'indexes'
        index_dir.mkdir(exist_ok=True)
        
        self.index_file = index_dir / f"{self.feed_name.strip('\0')}_chunk_{self.chunk_id:06d}.idx"
        
        if create or not self.index_file.exists():
            with open(self.index_file, 'wb') as f:
                f.write(b'\x00' * self.index_size)
            self._init_header()
        
        self.fd = os.open(self.index_file, os.O_RDWR)
        self.mmap = mmap.mmap(self.fd, self.index_size)
        self.buffer = memoryview(self.mmap)
        self._map_views()
    
    def _map_views(self):
        """Map buffer views for header and entries"""
        self.header_view = self.buffer[:self.HEADER_SIZE]
        self.entries_view = self.buffer[self.HEADER_SIZE:]
    
    def _init_header(self):
        """Initialize header with zeros"""
        if hasattr(self, 'buffer'):
            self.buffer[:self.HEADER_SIZE] = b'\x00' * self.HEADER_SIZE
    
    @property
    def entry_count(self) -> int:
        return struct.unpack('<Q', self.header_view[:8])[0]
    
    @entry_count.setter
    def entry_count(self, value: int):
        struct.pack_into('<Q', self.header_view, 0, value)
    
    @property
    def chunk_start_time(self) -> int:
        return struct.unpack('<Q', self.header_view[8:16])[0]
    
    @chunk_start_time.setter
    def chunk_start_time(self, value: int):
        struct.pack_into('<Q', self.header_view, 8, value)
    
    @property
    def chunk_end_time(self) -> int:
        return struct.unpack('<Q', self.header_view[16:24])[0]
    
    @chunk_end_time.setter
    def chunk_end_time(self, value: int):
        struct.pack_into('<Q', self.header_view, 16, value)
    
    def add_entry(self, timestamp: int, offset: int, size: int, record_type: int = 0):
        """Add index entry (must maintain timestamp order)"""
        count = self.entry_count
        
        if count >= self.max_entries:
            raise ValueError(f"Chunk index full: {count} entries")
        
        # Update time bounds
        if count == 0:
            self.chunk_start_time = timestamp
        self.chunk_end_time = timestamp
        
        # Create and write entry
        entry = IndexEntry(timestamp, offset, size, record_type)
        entry_offset = count * IndexEntry.SIZE
        self.entries_view[entry_offset:entry_offset + IndexEntry.SIZE] = entry.pack()
        
        self.entry_count = count + 1
        self._sync()
    
    def find_ge(self, timestamp: int) -> Optional[Tuple[int, IndexEntry]]:
        """Binary search: find first entry >= timestamp"""
        count = self.entry_count
        if count == 0:
            return None
        
        left, right = 0, count - 1
        result = None
        
        while left <= right:
            mid = (left + right) // 2
            entry = self._get_entry(mid)
            
            if entry.timestamp >= timestamp:
                result = (mid, entry)
                right = mid - 1
            else:
                left = mid + 1
        
        return result
    
    def find_le(self, timestamp: int) -> Optional[Tuple[int, IndexEntry]]:
        """Binary search: find last entry <= timestamp"""
        count = self.entry_count
        if count == 0:
            return None
        
        left, right = 0, count - 1
        result = None
        
        while left <= right:
            mid = (left + right) // 2
            entry = self._get_entry(mid)
            
            if entry.timestamp <= timestamp:
                result = (mid, entry)
                left = mid + 1
            else:
                right = mid - 1
        
        return result
    
    def scan_range(self, start_time: int, end_time: int) -> Iterator[IndexEntry]:
        """Iterate entries in [start_time, end_time]"""
        start_result = self.find_ge(start_time)
        if not start_result:
            return
        
        start_idx = start_result[0]
        count = self.entry_count
        
        for i in range(start_idx, count):
            entry = self._get_entry(i)
            if entry.timestamp > end_time:
                break
            yield entry
    
    def _get_entry(self, index: int) -> IndexEntry:
        """Get entry by index"""
        offset = index * IndexEntry.SIZE
        entry_data = bytes(self.entries_view[offset:offset + IndexEntry.SIZE])
        return IndexEntry.unpack(entry_data)
    
    def _sync(self):
        """Force write to storage"""
        if self.storage_type == 'shm':
            pass  # SHM is always in sync
        else:
            self.mmap.flush()
    
    def persist_to_disk(self, disk_path: Path):
        """Copy SHM index to disk (for chunk expiration)"""
        if self.storage_type != 'shm':
            return
        
        disk_index = ChunkIndex(
            self.feed_name, self.chunk_id, 'disk', disk_path, create=True
        )
        
        # Copy entire buffer
        disk_index.buffer[:len(self.buffer)] = self.buffer
        disk_index._sync()
        disk_index.close()
    
    def close(self):
        """Clean shutdown"""
        if self.storage_type == 'shm':
            try:
                self.shm.close()
            except:
                pass
        else:
            try:
                self.mmap.close()
                os.close(self.fd)
            except:
                pass
    
    def unlink(self):
        """Remove shared memory (SHM only)"""
        if self.storage_type == 'shm':
            try:
                self.shm.unlink()
            except:
                pass