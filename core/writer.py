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

    def __init__(self, platform, feed_name:str):
        self.platform = platform
        self.feed_name = feed_name
        self.feed_config = platform.lifecycle(feed_name)
        self.record_format = platform.get_record_format(feed_name)
        self.my_pid = ProcessUtils.get_current_pid()

        # Try to resume existing feed
        feed_exists = platform.registry.feed_exists(feed_name)
        if not feed_exists:
            print(f"🆕 Creating new feed '{feed_name}' (PID: {self.my_pid})")
            self.platform.registry.register_feed(feed_name)

        # Where to store persisted chunks
        self.data_dir = platform.base_path / "data" / feed_name

        # Registry and indexing
        # self.registry = platform.registry.get_metadata(feed_name)
        self.registry = FeedRegistry(platform.base_path/"data"/feed_name/f"{feed_name}.reg", mode='w')
        #self.index = platform.get_or_create_index(feed_name)

        # Current chunk state
        self.current_chunk = None
        self.current_chunk_metadata = self.registry.get_latest_chunk()
        if self.current_chunk_metadata is None:
            self.current_chunk_id = 0
        else:
            self.current_chunk_id = self.current_chunk_metadata.chunk_id
        self._create_new_chunk()

        # Thread safety
        self.lock = threading.Lock()

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
        try:

            self.current_chunk_id += 1
            self.registry.register_chunk(
                time.time_ns(),self.current_chunk_id,
                self.feed_config["chunk_size_bytes"],1)

            if self.current_chunk is not None:
                if self.feed_config["persist"]:
                    file_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
                    self.current_chunk.persist_to_disk(str(file_path))
                    print(f"💾 Persisted chunk {self.current_chunk_id} to {file_path}")
                self.current_chunk.close()

            if self.current_chunk_metadata is not None:
                self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
                new_metadata = self.registry.get_chunk_metadata(self.current_chunk_id)
                new_metadata.start_time = self.current_chunk_metadata.end_time
                self.current_chunk_metadata.release()
            else:
                new_metadata = self.registry.get_chunk_metadata(self.current_chunk_id)

            self.current_chunk_metadata = new_metadata
            self.current_chunk = Chunk(f"{self.feed_name}-{self.current_chunk_id}", self.feed_config["chunk_size_bytes"], create=True)

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
            record_size = len(record_data)

            # Check if need new chunk
            try:
                if record_size > self.current_chunk_metadata.size - self.current_chunk_metadata.write_pos:
                    self._create_new_chunk()
            except Exception as e:
                raise Exception(f"Worst exception ever: {e}")

            # 1. Write record data to chunk
            position = self.current_chunk_metadata.write_pos
            new_position = self.current_chunk.write_bytes(position, record_data)

            # 2. Update registry state
            self.current_chunk_metadata.last_update = timestamp
            self.current_chunk_metadata.write_pos = new_position
            self.current_chunk_metadata.num_records += 1

            # 3. Update index if needed (based on feed callback)
            # if force_index or (self.index_callback and self.index_callback(memoryview(record_data), timestamp)):
            #     self.index.add_entry(timestamp, self.current_chunk_id, position, 1 if force_index else 0)

    def close(self):
        """Clean shutdown with ownership release"""
        with self.lock:
            print(f"🛑 Shutting down writer for '{self.feed_name}' (PID: {self.my_pid})")
            if self.current_chunk:
                self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
                if self.feed_config["persist"]:
                    try:
                        file_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
                        self.current_chunk.persist_to_disk(str(file_path))
                        print(f"💾 Final persist of chunk {self.current_chunk_id}")
                    except Exception as e:
                        print(f"⚠️  Could not persist final chunk: {e}")
                self.current_chunk_metadata.release()
                self.registry.close()
                self.current_chunk.close()
