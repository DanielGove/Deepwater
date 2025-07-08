import threading
import time
import struct
from typing import Union, Optional, Tuple
from pathlib import Path
import numpy as np
from multiprocessing import shared_memory
from core.chunk import Chunk
from core.index import ChunkIndex
from core.feed_registry import FeedRegistry
from utils.process import ProcessUtils

class Writer:
    """Writer with bulletproof crash recovery"""

    def __init__(self, platform, feed_name: str, config: dict):
        self.platform = platform
        self.feed_name = feed_name
        self.config = config
        self.my_pid = ProcessUtils.get_current_pid()

        # Try to resume existing feed
        feed_exists = platform.registry.feed_exists(feed_name)
        if not feed_exists:
            print(f"🆕 Creating new feed '{feed_name}' (PID: {self.my_pid})")
            self.platform.registry.register_feed(feed_name)

        # Extract callback before registry (functions can't be serialized)
        self.index_callback = config.pop('index_callback', None)

        # Configuration
        self.chunk_size = config.get('chunk_size_mb', 64) * 1024 * 1024
        self.retention_hours = config.get('retention_hours', 24)
        self.persist = config.get('persist', True)

        # Directory setup
        self.data_dir = platform.base_path / "data" / feed_name
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Registry TODO: ADD CONFIGURATION TO REGISTRY
        if platform.registry.feed_exists(feed_name):
            print(f"🔄 Resuming existing feed '{feed_name}' (PID: {self.my_pid})")
            self.config = platform.registry.get_metadata(feed_name) # Standin for fetching config
        else:
            platform.registry.create_feed(feed_name)
        
        # NEEDS CONFIGURATION
        self.registry = FeedRegistry(path=platform.base_path / "data" / feed_name / f"{feed_name}.reg", mode="w")

        # will be based on the feed's CONFIGURATION
        #self.index = platform.get_or_create_index(feed_name)

        # Current chunk state
        self.current_chunk = None
        self.current_chunk_id = 0
        self.write_position = 0
        self.total_records = 0

        # Thread safety
        self.lock = threading.Lock()

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
        chunk_name = f"{self.feed_name}-{self.current_chunk_id}"

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
