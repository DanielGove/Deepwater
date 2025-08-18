import time
import struct
from pathlib import Path
from core.chunk import Chunk
from core.index import ChunkIndex
from core.feed_registry import FeedRegistry
from utils.process import ProcessUtils

class Writer:
    def __init__(self, platform, feed_name: str, config: dict):
        self.platform = platform
        self.feed_name = feed_name
        self.config = config
        self.current_chunk_meta = None
        self.my_pid = ProcessUtils.get_current_pid()

        try:
            # Try to resume existing feed
            feed_exists = platform.registry.feed_exists(feed_name)
            if not feed_exists:
                print(f"🆕 Creating new feed '{feed_name}' (PID: {self.my_pid})")
                self.platform.register_feed(feed_name, config)

                # TODO: NEEDS CONFIGURATION
                self.registry = FeedRegistry(path=platform.base_path / "data" / feed_name / f"{feed_name}.reg", mode="w")
            else:
                print(f"🔄 Resuming existing feed '{feed_name}' (PID: {self.my_pid})")
                self.config = platform.registry.get_metadata(feed_name)

                # TODO: NEEDS CONFIGURATION
                self.registry = FeedRegistry(path=platform.base_path / "data" / feed_name / f"{feed_name}.reg", mode="w")
                self.current_chunk_meta = self.registry.get_latest_chunk()

            # self.index = platform.get_or_create_index(feed_name)

            # Extract callback before registry (functions can't be serialized)
            self.index_callback = config.pop('index_callback', None)

            # Configuration
            self.chunk_size = config.get('chunk_size_mb', 64) * 1024 * 1024
            self.retention_hours = config.get('retention_hours', 24)
            self.persist = config.get('persist', True)

            # Directory setup
            self.data_dir = platform.base_path / "data" / feed_name
            self.data_dir.mkdir(parents=True, exist_ok=True)

            # Start up with a new chunk
            self._create_new_chunk()


        except Exception as e:
            # Defensive cleanup if FeedRegistry itself partly initialized
            if hasattr(self, "registry") and self.registry is not None:
                try:
                    self.registry.close()
                except Exception:
                    pass

            if hasattr(self, "current_chunk") and self.current_chunk is not None:
                try:
                    self.current_chunk.unlink()
                except Exception:
                    pass   
            
            raise
    
    def _find_last_valid_record(self, chunk) -> int:
        """Find the last valid record position in chunk"""
        position = 0

        while position < self._write_pos[0]:
            try:
                # Try to read record header
                if position + 12 > chunk.data_size:
                    break

                header_bytes = chunk.read_bytes(position, 12)
                timestamp, data_len = struct.unpack('<QI', header_bytes)

                # Validate record
                total_len = 12 + data_len
                if position + total_len > self._write_pos[0]:
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

        if self.current_chunk_meta is not None:
            new_chunk_id = self.current_chunk_meta.chunk_id + 1
        else:
            new_chunk_id = 1

        chunk_name = f"{self.feed_name}-{new_chunk_id:08d}"

        try:
            self.current_chunk = Chunk(chunk_name, self.chunk_size, create=True)
            self._write_pos = self.current_chunk.header.write_pos
            self._record_count = self.current_chunk.header.record_cnt
            self._start_time = self.current_chunk.header.start_time
            self._end_time = self.current_chunk.header.end_time
            self._last_update = self.current_chunk.header.last_update
            self._owner_pid = self.current_chunk.header.owner_pid

            # Initialize ownership and times
            now = time.time_ns()
            self._write_pos[0] = self.current_chunk.header.SIZE
            self._owner_pid[0] = self.my_pid
            self._start_time[0] = now
            self._end_time[0] = 0
            self._last_update[0] = 0

            # Register with registry
            self.registry.register_chunk(
                now,
                0,
                new_chunk_id,
                self.chunk_size,
                0
            )
            
            print(f"✨ Created new SHM chunk {new_chunk_id} (PID: {self.my_pid})")

        except Exception as e:
            raise RuntimeError(f"Failed to create chunk: {e}")

    def write(self, timestamp: int, record_data: bytes, force_index: bool = False) -> int:
        """ Fast path record writing """
        record_size = len(record_data)

        # Check if need new chunk
        if record_size > self.current_chunk.available_space():
            self._rotate_chunk()

        # ATOMIC OPERATION: All-or-nothing record write

        # 1. Write record data to chunk
        header = struct.pack('<QI', timestamp, len(record_data))
        record_bytes = header + record_data

        new_position = self.current_chunk.write_bytes(self._write_pos[0], record_bytes)

        # 2. Update chunk metadata atomically
        self._write_pos[0] = new_position
        self._record_count[0] += 1
        self._last_update[0] = timestamp

        # 3. Update index if needed (based on feed callback)
        if force_index or (self.index_callback and self.index_callback(memoryview(record_data), timestamp)):
            self.index.add_entry(timestamp, self.current_chunk_id, self._write_pos[0], 1 if force_index else 0)

        # Update local state
        return self._write_pos[0]

    def _rotate_chunk(self):
        """Atomic chunk rotation"""
        if self.current_chunk and self.persist:
            # Persist current chunk to disk
            file_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
            self.current_chunk.persist_to_disk(str(file_path))

            # # Update registry
            # self.registry.register_chunk(
            #     self.feed_name,
            #     self.current_chunk_id,
            #     str(file_path),
            #     False
            # )

            print(f"💾 Persisted chunk {self.current_chunk_id} to {file_path}")

        # Close current chunk and release ownership
        if self.current_chunk:
            self._owner_pid[0] = 0  # Release ownership
            self.current_chunk.close()

        # Create new chunk
        self.current_chunk_id += 1
        self._create_new_chunk()

    def close(self):
        """Clean shutdown with ownership release"""
        print(f"🛑 Shutting down writer for '{self.feed_name}' (PID: {self.my_pid})")

        if self.current_chunk:
            # Release ownership before closing
            self._owner_pid[0]  = 0
            self._end_time = time.time_ns

            if self.persist:
                try:
                    file_path = self.data_dir / f"{self.current_chunk.name}.dat"
                    self.current_chunk.persist_to_disk(str(file_path))

                    # TODO: Update registry with the latest chunk whatever

                    print(f"💾 Final persist of chunk {self.current_chunk.name}")
                except Exception as e:
                    print(f"⚠️  Could not persist final chunk: {e}")

            try:
                self.current_chunk.close()
            except Exception as e:
                print(f"⚠️  Could not close chunk: {e}")
        
        try:
            self.registry.close()
        except Exception as e:
            print(f"⚠️  Could not close registry: {e}")
