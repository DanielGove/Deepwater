import struct
from pathlib import Path
from multiprocessing import shared_memory
from typing import Optional
import numpy as np


class IndexEntry:
    """Binary index entry - 32 bytes"""
    SIZE = 32

    def __init__(self):
        self.timestamp = 0
        self.chunk_id = 0
        self.position = 0
        self.record_type = 0  # 0=auto, 1=snapshot, 2=disconnect, etc
        self.reserved = 0

    def pack(self) -> bytes:
        """Pack to binary format"""
        return struct.pack('<QQQQQ',
            self.timestamp, self.chunk_id, self.position,
            self.record_type, self.reserved)

    @classmethod
    def unpack(cls, data: bytes):
        """Unpack from binary format"""
        entry = cls()
        (entry.timestamp, entry.chunk_id, entry.position,
         entry.record_type, entry.reserved) = struct.unpack('<QQQQQ', data)
        return entry

class FeedIndex:
    """High-performance memory-mapped index for a single feed"""

    def __init__(self, feed_name: str, base_path: Path, create: bool = False):
        self.feed_name = feed_name
        self.base_path = base_path
        self.index_dir = base_path / "indexes"
        self.index_dir.mkdir(exist_ok=True)

        # Index file path
        self.index_file = self.index_dir / f"{feed_name}.idx"

        # Shared memory for live index
        self.shm_name = f"hft_idx_{feed_name}"
        self.max_entries = 100000  # Max entries in memory
        self.shm_size = 64 + self.max_entries * IndexEntry.SIZE  # Header + entries

        # Initialize
        self._init_index(create)

    def _init_index(self, create: bool):
        """Initialize shared memory index"""
        try:
            if create:
                # Create new shared memory
                self.shm = shared_memory.SharedMemory(
                    name=self.shm_name,
                    create=True,
                    size=self.shm_size
                )
                # Initialize header: entry_count(8) + last_sync_time(8) + reserved(48)
                header = np.frombuffer(self.shm.buf, dtype=np.int64, count=8)
                header[0] = 0  # entry_count
                header[1] = 0  # last_sync_time
            else:
                # Attach to existing
                self.shm = shared_memory.SharedMemory(name=self.shm_name)

        except FileExistsError:
            # Already exists, attach
            self.shm = shared_memory.SharedMemory(name=self.shm_name)

        # Memory views
        self.header = np.frombuffer(self.shm.buf, dtype=np.int64, count=8)
        self.entries_buffer = memoryview(self.shm.buf[64:])

        # Load from disk if exists
        if not create and self.index_file.exists():
            self._load_from_disk()

    def add_entry(self, timestamp: int, chunk_id: int, position: int, record_type: int = 0):
        """Add index entry to shared memory"""
        entry_count = int(self.header[0])

        if entry_count >= self.max_entries:
            # Need to persist and compact
            self._persist_and_compact()
            entry_count = int(self.header[0])

        # Create entry
        entry = IndexEntry()
        entry.timestamp = timestamp
        entry.chunk_id = chunk_id
        entry.position = position
        entry.record_type = record_type

        # Write to buffer
        offset = entry_count * IndexEntry.SIZE
        entry_data = entry.pack()
        self.entries_buffer[offset:offset + IndexEntry.SIZE] = entry_data

        # Update count
        self.header[0] = entry_count + 1

    def find_before(self, timestamp: int, record_type: Optional[int] = None) -> Optional[IndexEntry]:
        """Binary search for entry at or before timestamp"""
        entry_count = int(self.header[0])

        if entry_count == 0:
            return None

        # Binary search in memory
        left, right = 0, entry_count - 1
        result = None

        while left <= right:
            mid = (left + right) // 2
            offset = mid * IndexEntry.SIZE
            entry_data = bytes(self.entries_buffer[offset:offset + IndexEntry.SIZE])
            entry = IndexEntry.unpack(entry_data)

            if entry.timestamp <= timestamp:
                if record_type is None or entry.record_type == record_type:
                    result = entry
                left = mid + 1
            else:
                right = mid - 1

        return result

    def _persist_and_compact(self):
        """Persist current entries and keep recent ones"""
        entry_count = int(self.header[0])

        # Read all entries
        entries = []
        for i in range(entry_count):
            offset = i * IndexEntry.SIZE
            entry_data = bytes(self.entries_buffer[offset:offset + IndexEntry.SIZE])
            entry = IndexEntry.unpack(entry_data)
            entries.append(entry)

        # Sort all entries
        entries.sort(key=lambda e: e.timestamp)

        # Write to disk file
        with open(self.index_file, 'ab') as f:  # Append mode
            for entry in entries:
                f.write(entry.pack())

        # Keep only recent entries in memory (last 10k)
        keep_count = min(10000, entry_count)
        recent_entries = entries[-keep_count:] if keep_count > 0 else []

        # Clear memory and write recent entries back
        self.header[0] = len(recent_entries)

        for i, entry in enumerate(recent_entries):
            offset = i * IndexEntry.SIZE
            entry_data = entry.pack()
            self.entries_buffer[offset:offset + IndexEntry.SIZE] = entry_data

    def _load_from_disk(self):
        """Load recent entries from disk file"""
        if not self.index_file.exists():
            return

        # Read last N entries from file
        file_size = self.index_file.stat().st_size
        max_read = min(10000 * IndexEntry.SIZE, file_size)

        with open(self.index_file, 'rb') as f:
            f.seek(-max_read, 2)  # Seek from end
            data = f.read()

        # Parse entries
        entry_count = len(data) // IndexEntry.SIZE
        self.header[0] = entry_count

        # Copy to shared memory
        if entry_count > 0:
            self.entries_buffer[:len(data)] = data

    def close(self):
        """Clean shutdown"""
        self._persist_and_compact()

        # Clean up buffer references
        self.header = None
        self.entries_buffer = None

        try:
            self.shm.close()
        except Exception:
            pass

    def unlink(self):
        """Remove shared memory"""
        try:
            self.shm.unlink()
        except:
            pass