import struct
import mmap
import os
from pathlib import Path
from typing import List, Optional, Tuple, Iterator
from multiprocessing import shared_memory
import bisect


class ChunkEntry:
    """32-byte chunk registry entry"""
    SIZE = 32
    FORMAT = '<QQQBBHxxxx'  # start_time, end_time, location_hash, storage_type, ref_count, chunk_id, reserved
    
    def __init__(self, start_time: int = 0, end_time: int = 0, location_hash: int = 0,
                 storage_type: int = 0, chunk_id: int = 0):
        self.start_time = start_time
        self.end_time = end_time  
        self.location_hash = location_hash  # For disk: file path hash, for SHM: segment name hash
        self.storage_type = storage_type    # 0=disk, 1=SHM
        self.ref_count = 0                  # Active readers (SHM only)
        self.chunk_id = chunk_id
    
    def pack(self) -> bytes:
        return struct.pack(self.FORMAT, self.start_time, self.end_time, 
                          self.location_hash, self.storage_type, self.ref_count, self.chunk_id)
    
    @classmethod  
    def unpack(cls, data: bytes) -> 'ChunkEntry':
        start_time, end_time, location_hash, storage_type, ref_count, chunk_id = struct.unpack(cls.FORMAT, data)
        entry = cls(start_time, end_time, location_hash, storage_type, chunk_id)
        entry.ref_count = ref_count
        return entry
    
    def overlaps(self, start_time: int, end_time: int) -> bool:
        """Check if chunk overlaps with time range"""
        return not (self.end_time < start_time or self.start_time > end_time)


class FeedRegistry:
    """Time-sorted chunk registry for a single feed"""
    
    HEADER_SIZE = 64  # [entry_count: 8][feed_name: 32][first_time: 8][last_time: 8][reserved: 8]
    
    def __init__(self, feed_name: str, storage_type: str = 'disk', 
                 base_path: Optional[Path] = None, create: bool = False):
        self.feed_name = feed_name.ljust(32, '\0')[:32]
        self.storage_type = storage_type
        self.base_path = base_path or Path('.')
        
        self.max_entries = 100000 if storage_type == 'shm' else 1000000
        self.registry_size = self.HEADER_SIZE + (self.max_entries * ChunkEntry.SIZE)
        
        if storage_type == 'shm':
            self._init_shm(create)
        else:
            self._init_disk(create)
    
    def _init_shm(self, create: bool):
        """Initialize shared memory registry"""
        shm_name = f"feed_reg_{self.feed_name.strip('\0')}"
        
        try:
            if create:
                self.shm = shared_memory.SharedMemory(name=shm_name, create=True, size=self.registry_size)
                self._init_header()
            else:
                self.shm = shared_memory.SharedMemory(name=shm_name)
        except FileExistsError:
            self.shm = shared_memory.SharedMemory(name=shm_name)
        
        self.buffer = memoryview(self.shm.buf)
        self._map_views()
    
    def _init_disk(self, create: bool):
        """Initialize disk-based registry"""
        registry_dir = self.base_path / 'registries'
        registry_dir.mkdir(exist_ok=True)
        
        self.registry_file = registry_dir / f"{self.feed_name.strip('\0')}_registry.bin"
        
        if create or not self.registry_file.exists():
            with open(self.registry_file, 'wb') as f:
                f.write(b'\x00' * self.registry_size)
            self._init_header()
        
        self.fd = os.open(self.registry_file, os.O_RDWR)
        self.mmap = mmap.mmap(self.fd, 0)  # Map entire file
        self.buffer = memoryview(self.mmap)
        self._map_views()
    
    def _map_views(self):
        """Map buffer views for header and entries"""
        self.header_view = self.buffer[:self.HEADER_SIZE]
        self.entries_view = self.buffer[self.HEADER_SIZE:]
    
    def _init_header(self):
        """Initialize header"""
        if hasattr(self, 'buffer'):
            # entry_count=0, feed_name, first_time=0, last_time=0, reserved=0
            struct.pack_into('<Q32sQQQ', self.header_view, 0, 0, self.feed_name.encode(), 0, 0, 0)
    
    @property
    def entry_count(self) -> int:
        return struct.unpack('<Q', self.header_view[:8])[0]
    
    @entry_count.setter  
    def entry_count(self, value: int):
        struct.pack_into('<Q', self.header_view, 0, value)
    
    @property
    def first_time(self) -> int:
        return struct.unpack('<Q', self.header_view[40:48])[0]
    
    @first_time.setter
    def first_time(self, value: int):
        struct.pack_into('<Q', self.header_view, 40, value)
    
    @property  
    def last_time(self) -> int:
        return struct.unpack('<Q', self.header_view[48:56])[0]
    
    @last_time.setter
    def last_time(self, value: int):
        struct.pack_into('<Q', self.header_view, 48, value)
    
    def add_chunk(self, chunk_entry: ChunkEntry):
        """Add chunk entry maintaining time-sorted order"""
        count = self.entry_count
        
        if count >= self.max_entries:
            raise ValueError(f"Registry full: {count} entries")
        
        # Binary search for insertion point
        insert_pos = self._find_insert_position(chunk_entry.start_time)
        
        # Shift existing entries if needed
        if insert_pos < count:
            move_size = (count - insert_pos) * ChunkEntry.SIZE
            src_offset = insert_pos * ChunkEntry.SIZE
            dst_offset = (insert_pos + 1) * ChunkEntry.SIZE
            
            # Use memmove equivalent
            temp_data = bytes(self.entries_view[src_offset:src_offset + move_size])
            self.entries_view[dst_offset:dst_offset + move_size] = temp_data
        
        # Insert new entry
        entry_offset = insert_pos * ChunkEntry.SIZE
        self.entries_view[entry_offset:entry_offset + ChunkEntry.SIZE] = chunk_entry.pack()
        
        # Update counts and time bounds
        self.entry_count = count + 1
        if count == 0:
            self.first_time = chunk_entry.start_time
            self.last_time = chunk_entry.end_time
        else:
            self.first_time = min(self.first_time, chunk_entry.start_time)
            self.last_time = max(self.last_time, chunk_entry.end_time)
        
        self._sync()
    
    def _find_insert_position(self, start_time: int) -> int:
        """Binary search for insertion position to maintain sort order"""
        count = self.entry_count
        left, right = 0, count
        
        while left < right:
            mid = (left + right) // 2
            entry = self._get_entry(mid)
            
            if entry.start_time < start_time:
                left = mid + 1
            else:
                right = mid
        
        return left
    
    def find_overlapping_chunks(self, start_time: int, end_time: int) -> List[ChunkEntry]:
        """Find all chunks that overlap with time range"""
        result = []
        count = self.entry_count
        
        # Binary search for first potentially overlapping chunk
        start_pos = self._find_first_overlap(start_time, end_time)
        if start_pos is None:
            return result
        
        # Scan forward collecting overlapping chunks
        for i in range(start_pos, count):
            entry = self._get_entry(i)
            
            # If chunk starts after our end time, we're done
            if entry.start_time > end_time:
                break
                
            if entry.overlaps(start_time, end_time):
                result.append(entry)
        
        return result
    
    def _find_first_overlap(self, start_time: int, end_time: int) -> Optional[int]:
        """Find index of first chunk that might overlap"""
        count = self.entry_count
        if count == 0:
            return None
        
        # Binary search for first chunk where chunk.end_time >= start_time
        left, right = 0, count - 1
        result = None
        
        while left <= right:
            mid = (left + right) // 2
            entry = self._get_entry(mid)
            
            if entry.end_time >= start_time:
                result = mid
                right = mid - 1
            else:
                left = mid + 1
        
        return result
    
    def increment_ref_count(self, chunk_id: int) -> bool:
        """Increment ref count for SHM chunk (returns False if not found)"""
        if self.storage_type != 'shm':
            return True  # Disk chunks don't need ref counting
        
        count = self.entry_count
        for i in range(count):
            entry = self._get_entry(i)
            if entry.chunk_id == chunk_id:
                entry.ref_count += 1
                self._set_entry(i, entry)
                return True
        return False
    
    def decrement_ref_count(self, chunk_id: int) -> int:
        """Decrement ref count, return new count"""
        if self.storage_type != 'shm':
            return 0
        
        count = self.entry_count
        for i in range(count):
            entry = self._get_entry(i)
            if entry.chunk_id == chunk_id:
                entry.ref_count = max(0, entry.ref_count - 1)
                self._set_entry(i, entry)
                return entry.ref_count
        return 0
    
    def _get_entry(self, index: int) -> ChunkEntry:
        """Get entry by index"""
        offset = index * ChunkEntry.SIZE
        entry_data = bytes(self.entries_view[offset:offset + ChunkEntry.SIZE])
        return ChunkEntry.unpack(entry_data)
    
    def _set_entry(self, index: int, entry: ChunkEntry):
        """Set entry by index"""
        offset = index * ChunkEntry.SIZE
        self.entries_view[offset:offset + ChunkEntry.SIZE] = entry.pack()
        self._sync()
    
    def _sync(self):
        """Force write to storage"""
        if self.storage_type == 'shm':
            pass  # SHM is always in sync
        else:
            self.mmap.flush()
    
    def merge_with_disk_registry(self, disk_registry: 'FeedRegistry') -> List[ChunkEntry]:
        """Merge SHM registry with disk registry for unified view"""
        if self.storage_type != 'shm':
            raise ValueError("merge_with_disk_registry only valid for SHM registries")
        
        shm_chunks = [self._get_entry(i) for i in range(self.entry_count)]
        disk_chunks = [disk_registry._get_entry(i) for i in range(disk_registry.entry_count)]
        
        # Merge and sort by start_time
        all_chunks = shm_chunks + disk_chunks
        all_chunks.sort(key=lambda x: x.start_time)
        
        return all_chunks
    
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