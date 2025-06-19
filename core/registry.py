import os
import mmap
import struct
import threading
from pathlib import Path
from typing import Optional, Tuple
import numpy as np
from multiprocessing import shared_memory
from core.chunk import ChunkHeader, Chunk
from core.index import IndexEntry, FeedIndex

class BinaryRegistry:
    """High-performance binary registry - NO JSON in hot paths"""

    # Entry format: [feed_hash: 8][chunk_id: 8][location_hash: 8][is_shm: 1][reserved: 7] = 32 bytes
    ENTRY_SIZE = 32
    HEADER_SIZE = 16  # [entry_count: 8][reserved: 8]

    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.registry_file = base_path / "registry.bin"
        self.location_map_file = base_path / "locations.bin"  # For string storage
        self.lock = threading.Lock()

        # Create base directory
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Initialize if needed
        self._ensure_registry()

        # Memory mapped access
        self._mmap_registry()

    def _ensure_registry(self):
        """Ensure registry files exist"""
        if not self.registry_file.exists():
            # Create empty binary registry
            with open(self.registry_file, 'wb') as f:
                f.write(struct.pack('<Q', 0))  # entry_count = 0
                f.write(b'\x00' * 8)  # reserved

        if not self.location_map_file.exists():
            # Create empty location map
            with open(self.location_map_file, 'wb') as f:
                f.write(struct.pack('<Q', 0))  # string_count = 0
                f.write(b'\x00' * 8)  # reserved

    def _mmap_registry(self):
        """Memory map registry for fast access"""
        self.registry_fd = os.open(self.registry_file, os.O_RDWR)
        stat = os.fstat(self.registry_fd)

        if stat.st_size < 4096:  # Ensure minimum size
            os.ftruncate(self.registry_fd, 4096)

        self.registry_mmap = mmap.mmap(self.registry_fd, 0)
        self.registry_view = memoryview(self.registry_mmap)

    def _hash_string(self, s: str) -> int:
        """Generate 64-bit hash for string"""
        return hash(s) & 0x7FFFFFFFFFFFFFFF

    def find_latest_chunk(self, feed_name: str) -> Optional[Tuple[int, str, bool]]:
        """Binary search for feed's highest chunk_id"""
        with self.lock:
            feed_hash = self._hash_string(feed_name)
            entry_count = struct.unpack('<Q', self.registry_view[:8])[0]

            latest_chunk_id = -1
            latest_entry = None

            # Linear scan for now (could optimize with binary search)
            for i in range(entry_count):
                offset = self.HEADER_SIZE + i * self.ENTRY_SIZE
                entry_data = self.registry_view[offset:offset + self.ENTRY_SIZE]

                entry_feed_hash, chunk_id, location_hash, is_shm_byte = struct.unpack('<QQQ?', entry_data[:25])

                if entry_feed_hash == feed_hash and chunk_id > latest_chunk_id:
                    latest_chunk_id = chunk_id
                    location = self._resolve_location_hash(location_hash)
                    latest_entry = (chunk_id, location, bool(is_shm_byte))

            return latest_entry

    def register_chunk(self, feed_name: str, chunk_id: int, location: str, is_shm: bool):
        """Add chunk entry to registry"""
        with self.lock:
            feed_hash = self._hash_string(feed_name)
            location_hash = self._store_location(location)

            # Read current entry count
            entry_count = struct.unpack('<Q', self.registry_view[:8])[0]

            # Ensure we have space
            required_size = self.HEADER_SIZE + (entry_count + 1) * self.ENTRY_SIZE
            if len(self.registry_view) < required_size:
                self._expand_registry(required_size * 2)

            # Add new entry
            offset = self.HEADER_SIZE + entry_count * self.ENTRY_SIZE
            entry_data = struct.pack('<QQQ?7x', feed_hash, chunk_id, location_hash, is_shm)
            self.registry_view[offset:offset + self.ENTRY_SIZE] = entry_data

            # Update entry count
            struct.pack_into('<Q', self.registry_view, 0, entry_count + 1)

            # Force sync
            self.registry_mmap.flush()

    def _store_location(self, location: str) -> int:
        """Store location string and return hash"""
        location_hash = self._hash_string(location)
        # For now, just return hash. In production, would store full string mapping
        return location_hash

    def _resolve_location_hash(self, location_hash: int) -> str:
        """Resolve location hash back to string"""
        # Simplified: in production would maintain hash->string mapping
        # For now, reconstruct based on known patterns
        return f"location_{location_hash}"

    def _expand_registry(self, new_size: int):
        """Expand registry file and remap"""
        self.registry_mmap.close()
        os.ftruncate(self.registry_fd, new_size)
        self.registry_mmap = mmap.mmap(self.registry_fd, 0)
        self.registry_view = memoryview(self.registry_mmap)

    def close(self):
        """Clean shutdown"""
        if hasattr(self, 'registry_mmap'):
            self.registry_mmap.close()
        if hasattr(self, 'registry_fd'):
            os.close(self.registry_fd)