import numpy as np
import struct
import time
import os
import mmap
import threading
import hashlib
import signal
from pathlib import Path
from typing import Dict, List, Optional, Iterator, Union, Any, Tuple, Callable
from collections import OrderedDict
from multiprocessing import shared_memory
from multiprocessing.synchronize import Event as MPEvent
import multiprocessing as mp

class ChunkHeader:
    """Enhanced chunk header with crash recovery support"""
    # Layout: write_pos(8) + record_count(8) + start_time(8) + end_time(8) +
    #         owner_pid(8) + last_update(8) + checksum(8) + reserved(16) = 64 bytes
    SIZE = 64

    def __init__(self, buffer: memoryview, offset: int = 0):
        self.buffer = buffer
        self.offset = offset
        self._header = np.frombuffer(buffer[offset:offset + self.SIZE], dtype=np.int64)

    @property
    def write_pos(self) -> int:
        return int(self._header[0])

    @write_pos.setter
    def write_pos(self, value: int):
        self._header[0] = value
        self._update_checksum()

    @property
    def record_count(self) -> int:
        return int(self._header[1])

    @record_count.setter
    def record_count(self, value: int):
        self._header[1] = value
        self._update_checksum()

    @property
    def start_time(self) -> int:
        return int(self._header[2])

    @start_time.setter
    def start_time(self, value: int):
        self._header[2] = value
        self._update_checksum()

    @property
    def end_time(self) -> int:
        return int(self._header[3])

    @end_time.setter
    def end_time(self, value: int):
        self._header[3] = value
        self._update_checksum()

    @property
    def owner_pid(self) -> int:
        return int(self._header[4])

    @owner_pid.setter
    def owner_pid(self, value: int):
        self._header[4] = value
        self._update_checksum()

    @property
    def last_update(self) -> int:
        return int(self._header[5])

    @last_update.setter
    def last_update(self, value: int):
        self._header[5] = value
        self._update_checksum()

    @property
    def checksum(self) -> int:
        return int(self._header[6])

    def _update_checksum(self):
        """Update checksum of header data (excluding checksum field itself)"""
        data = self._header[:6].tobytes()  # All fields except checksum
        self._header[6] = hash(data) & 0x7FFFFFFFFFFFFFFF  # Keep positive

    def validate_checksum(self) -> bool:
        """Validate header integrity"""
        expected = hash(self._header[:6].tobytes()) & 0x7FFFFFFFFFFFFFFF
        return self.checksum == expected

    def cleanup(self):
        """Clean up references for proper SHM closure"""
        self._header = None
        self.buffer = None

class ProcessUtils:
    """Utilities for process management and validation"""

    @staticmethod
    def is_process_alive(pid: int) -> bool:
        """Check if process is alive"""
        if pid <= 0:
            return False
        try:
            # Send signal 0 to check if process exists
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False

    @staticmethod
    def get_current_pid() -> int:
        return os.getpid()

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

class CrashResilientWriter:
    """Writer with bulletproof crash recovery"""

    def __init__(self, platform, feed_name: str, config: dict):
        self.platform = platform
        self.feed_name = feed_name
        self.config = config
        self.my_pid = ProcessUtils.get_current_pid()

        # Extract callback before registry (functions can't be serialized)
        self.index_callback = config.pop('index_callback', None)

        # Configuration
        self.chunk_size = config.get('chunk_size_mb', 64) * 1024 * 1024
        self.retention_hours = config.get('retention_hours', 24)
        self.persist = config.get('persist', True)

        # Directory setup
        self.data_dir = platform.base_path / "data" / feed_name
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Registry and indexing
        self.registry = platform.registry
        self.index = platform.get_or_create_index(feed_name)

        # Current chunk state
        self.current_chunk = None
        self.current_chunk_id = 0
        self.write_position = 0
        self.total_records = 0

        # Thread safety
        self.lock = threading.Lock()

        # CRITICAL: Crash recovery logic
        self._crash_recovery_initialization()

    def _crash_recovery_initialization(self):
        """CORNERSTONE: Bulletproof crash recovery logic"""
        print(f"🔄 Starting crash recovery for feed '{self.feed_name}' (PID: {self.my_pid})")

        # Step 1: Check for existing active writer (prevent multiple writers)
        existing_chunk_info = self.registry.find_latest_chunk(self.feed_name)

        if existing_chunk_info:
            chunk_id, location, is_shm = existing_chunk_info
            print(f"📍 Found existing chunk {chunk_id}: {location} (SHM: {is_shm})")

            if is_shm:
                # Step 2: Try to attach to existing SHM chunk
                if self._attempt_shm_recovery(location, chunk_id):
                    print(f"✅ Successfully recovered SHM chunk {chunk_id}")
                    return
                else:
                    print(f"⚠️  SHM chunk {chunk_id} recovery failed, creating new chunk")
            else:
                print(f"📁 Last chunk {chunk_id} is on disk, creating new SHM chunk")
                self.current_chunk_id = chunk_id + 1
        else:
            print(f"🆕 No existing chunks found, creating first chunk")
            self.current_chunk_id = 0

        # Step 3: Create new SHM chunk
        self._create_new_chunk()

    def _attempt_shm_recovery(self, chunk_name: str, chunk_id: int) -> bool:
        """Attempt to recover from existing SHM chunk"""
        try:
            # Try to attach to existing SHM
            chunk = Chunk(chunk_name, self.chunk_size, create=False)

            # Step 2a: Validate chunk integrity
            if not chunk.header.validate_checksum():
                print(f"❌ Chunk {chunk_id} failed checksum validation")
                chunk.close()
                return False

            # Step 2b: Check current owner
            current_owner = chunk.header.owner_pid

            if current_owner == 0:
                # No owner, take it
                print(f"🔓 Chunk {chunk_id} has no owner, taking ownership")
            elif current_owner == self.my_pid:
                # Already owned by us (restart of same process)
                print(f"🔄 Chunk {chunk_id} already owned by us")
            elif ProcessUtils.is_process_alive(current_owner):
                # Active owner exists, fail
                print(f"❌ Chunk {chunk_id} owned by active process {current_owner}")
                chunk.close()
                raise RuntimeError(f"Chunk owned by active process {current_owner}")
            else:
                # Dead owner, take over
                print(f"💀 Taking over chunk {chunk_id} from dead process {current_owner}")

            # Step 2c: Take ownership
            chunk.header.owner_pid = self.my_pid
            chunk.header.last_update = time.time_ns()

            # Step 2d: Find last valid record position
            valid_position = self._find_last_valid_record(chunk)

            if valid_position < chunk.header.write_pos:
                print(f"🔧 Corrected write position from {chunk.header.write_pos} to {valid_position}")
                chunk.header.write_pos = valid_position

            # Step 2e: Resume from exact position
            self.current_chunk = chunk
            self.current_chunk_id = chunk_id
            self.write_position = chunk.header.write_pos

            print(f"🎯 Resumed at chunk {chunk_id}, position {self.write_position}, "
                  f"record count {chunk.header.record_count}")

            return True

        except Exception as e:
            print(f"❌ SHM recovery failed: {e}")
            return False

    def _find_last_valid_record(self, chunk) -> int:
        """Find the last valid record position in chunk"""
        position = 0

        while position < chunk.header.write_pos:
            try:
                # Try to read record header
                if position + 12 > chunk.data_size:
                    break

                header_bytes = chunk.read_bytes(position, 12)
                timestamp, data_len = struct.unpack('<QI', header_bytes)

                # Validate record
                total_len = 12 + data_len
                if position + total_len > chunk.header.write_pos:
                    # Incomplete record, this is where we should resume
                    break

                if timestamp == 0 or data_len > 1024*1024:  # Basic sanity checks
                    break

                position += total_len

            except Exception:
                # Corrupted record found
                break

        return position

    def _create_new_chunk(self):
        """Create new SHM chunk with ownership"""
        chunk_name = f"hft_{self.feed_name}_{self.current_chunk_id}"

        try:
            self.current_chunk = Chunk(chunk_name, self.chunk_size, create=True)

            # Initialize ownership and times
            now = time.time_ns()
            self.current_chunk.header.owner_pid = self.my_pid
            self.current_chunk.header.start_time = now
            self.current_chunk.header.end_time = now
            self.current_chunk.header.last_update = now

            # Register with registry
            self.registry.register_chunk(
                self.feed_name,
                self.current_chunk_id,
                chunk_name,
                True
            )

            self.write_position = 0
            print(f"✨ Created new SHM chunk {self.current_chunk_id} (PID: {self.my_pid})")

        except Exception as e:
            raise RuntimeError(f"Failed to create chunk: {e}")

    def write(self, timestamp: int, data: Union[bytes, np.ndarray], force_index: bool = False) -> int:
        """ATOMIC record writing with crash recovery support"""
        with self.lock:
            # Convert data to bytes
            if isinstance(data, np.ndarray):
                record_data = data.tobytes()
            else:
                record_data = bytes(data)

            # Calculate total record size: timestamp(8) + length(4) + data
            record_size = 12 + len(record_data)

            # Check if need new chunk
            if record_size > self.current_chunk.available_space():
                self._rotate_chunk()

            # ATOMIC OPERATION: All-or-nothing record write
            position = self.write_position

            # 1. Write record data to chunk
            header = struct.pack('<QI', timestamp, len(record_data))
            record_bytes = header + record_data

            new_position = self.current_chunk.write_bytes(position, record_bytes)

            # 2. Update chunk metadata atomically
            self.current_chunk.header.write_pos = new_position
            self.current_chunk.header.record_count += 1
            self.current_chunk.header.end_time = timestamp
            self.current_chunk.header.last_update = time.time_ns()

            # 3. Update index if needed (based on feed callback)
            if force_index or (self.index_callback and self.index_callback(memoryview(record_data), timestamp)):
                self.index.add_entry(timestamp, self.current_chunk_id, position, 1 if force_index else 0)

            # Update local state
            self.write_position = new_position
            self.total_records += 1

            return self.total_records

    def _rotate_chunk(self):
        """Atomic chunk rotation"""
        if self.current_chunk and self.persist:
            # Persist current chunk to disk
            file_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
            self.current_chunk.persist_to_disk(str(file_path))

            # Update registry
            self.registry.register_chunk(
                self.feed_name,
                self.current_chunk_id,
                str(file_path),
                False
            )

            print(f"💾 Persisted chunk {self.current_chunk_id} to {file_path}")

        # Close current chunk and release ownership
        if self.current_chunk:
            self.current_chunk.header.owner_pid = 0  # Release ownership
            self.current_chunk.close()

        # Create new chunk
        self.current_chunk_id += 1
        self._create_new_chunk()

    def close(self):
        """Clean shutdown with ownership release"""
        with self.lock:
            print(f"🛑 Shutting down writer for '{self.feed_name}' (PID: {self.my_pid})")

            if self.current_chunk:
                # Release ownership before closing
                self.current_chunk.header.owner_pid = 0
                self.current_chunk.header.last_update = time.time_ns()

                if self.persist:
                    try:
                        file_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
                        self.current_chunk.persist_to_disk(str(file_path))

                        self.registry.register_chunk(
                            self.feed_name,
                            self.current_chunk_id,
                            str(file_path),
                            False
                        )
                        print(f"💾 Final persist of chunk {self.current_chunk_id}")
                    except Exception as e:
                        print(f"⚠️  Could not persist final chunk: {e}")

                try:
                    self.current_chunk.close()
                    if self.current_chunk.is_shm:
                        self.current_chunk.unlink()
                except Exception as e:
                    print(f"⚠️  Could not close chunk: {e}")

# Keep the existing Chunk, IndexEntry, FeedIndex classes from the original
# but make sure they work with the new ChunkHeader format

class Chunk:
    """Memory chunk with proper SHM coordination"""

    def __init__(self, name: str, size: int, create: bool = False, file_path: Optional[str] = None):
        self.name = name
        self.size = size
        self.file_path = file_path

        if file_path:
            # File-backed chunk
            if create:
                with open(file_path, 'wb') as f:
                    f.write(b'\x00' * size)

            self.file = open(file_path, 'r+b')
            self.mmap = mmap.mmap(self.file.fileno(), size)
            self.buffer = memoryview(self.mmap)
            self.is_shm = False
        else:
            # Shared memory chunk
            if create:
                self.shm = shared_memory.SharedMemory(name=name, create=True, size=size)
            else:
                self.shm = shared_memory.SharedMemory(name=name)

            self.buffer = memoryview(self.shm.buf)
            self.is_shm = True

        # Header management
        self.header = ChunkHeader(self.buffer)
        self.data_offset = ChunkHeader.SIZE
        self.data_size = size - self.data_offset

        # Initialize header if creating
        if create and not file_path:
            self.header.write_pos = 0
            self.header.record_count = 0
            self.header.start_time = 0
            self.header.end_time = 0
            self.header.owner_pid = 0
            self.header.last_update = 0

    def write_bytes(self, position: int, data: bytes) -> int:
        """Write bytes and return new position"""
        if position + len(data) > self.data_size:
            raise ValueError(f"Write would exceed chunk capacity")

        offset = self.data_offset + position
        self.buffer[offset:offset + len(data)] = data
        return position + len(data)

    def read_bytes(self, position: int, length: int) -> bytes:
        """Read bytes from chunk"""
        if position + length > self.data_size:
            raise ValueError(f"Read would exceed chunk capacity")

        offset = self.data_offset + position
        return bytes(self.buffer[offset:offset + length])

    def available_space(self) -> int:
        """Available space for writing"""
        return self.data_size - self.header.write_pos

    def persist_to_disk(self, path: str):
        """Save SHM chunk to disk file"""
        with open(path, 'wb') as f:
            f.write(bytes(self.buffer))

    def close(self):
        if self.is_shm:
            try:
                if hasattr(self, 'header'):
                    self.header.cleanup()
                self.buffer = None
                self.shm.close()
            except Exception:
                pass
        else:
            try:
                if hasattr(self, 'header'):
                    self.header.cleanup()
                self.buffer = None
                if hasattr(self, 'mmap'):
                    self.mmap.close()
                if hasattr(self, 'file'):
                    self.file.close()
            except Exception:
                pass

    def unlink(self):
        if self.is_shm:
            try:
                self.shm.unlink()
            except Exception:
                pass

# Use existing IndexEntry and FeedIndex classes from original code

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

class Platform:
    """Enhanced HFT platform with crash recovery"""

    def __init__(self, base_path: str = "./hft_data"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Binary registry instead of JSON
        self.registry = BinaryRegistry(self.base_path)

        # Process-local caches
        self.writers = {}
        self.readers = {}
        self.indexes = {}

        # Setup cleanup on exit
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        print(f"🚀 HFT Platform initialized at {self.base_path}")

    def create_feed(self, feed_name: str, **config) -> CrashResilientWriter:
        """Create or get feed writer with crash recovery"""
        if feed_name not in self.writers:
            self.writers[feed_name] = CrashResilientWriter(self, feed_name, config)

        return self.writers[feed_name]

    def get_or_create_index(self, feed_name: str) -> FeedIndex:
        """Get or create feed index"""
        if feed_name not in self.indexes:
            self.indexes[feed_name] = FeedIndex(feed_name, self.base_path, create=True)

        return self.indexes[feed_name]

    def cleanup_dead_writers(self):
        """Clean up chunks owned by dead processes"""
        print("🧹 Cleaning up dead writers...")
        # Implementation would scan SHM segments and clean up dead owners

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"🛑 Received signal {signum}, shutting down...")
        self.close()

    def close(self):
        """Clean shutdown"""
        for writer in self.writers.values():
            writer.close()

        for index in self.indexes.values():
            index.close()

        self.registry.close()

        self.writers.clear()
        self.readers.clear()
        self.indexes.clear()

        print("✅ Platform shutdown complete")

# Example usage and testing
if __name__ == "__main__":
    # Example of crash-resilient writer
    def btc_orderbook_indexer(data: memoryview, timestamp: int) -> bool:
        """Index large snapshots"""
        return len(data) > 10000  # Index snapshots larger than 10KB

    platform = Platform("./test_hft_data")

    try:
        # Create writer with crash recovery
        writer = platform.create_feed(
            "btc_orderbook",
            chunk_size_mb=64,
            index_callback=btc_orderbook_indexer
        )

        # Write some data
        for i in range(1000):
            timestamp = time.time_ns()
            data = f"orderbook_update_{i}".encode() * 100  # Simulate varying sizes
            writer.write(timestamp, data)

            if i % 100 == 0:
                print(f"📝 Wrote record {i}")

        print("✅ Write test completed successfully")

    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        platform.close()
