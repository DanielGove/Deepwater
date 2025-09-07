import threading
import time
import struct
from typing import Union, Optional, Tuple
from pathlib import Path
import numpy as np
from multiprocessing import shared_memory

from .chunk import Chunk
from .index import ChunkIndex
from .feed_registry import FeedRegistry
from .utils.process import ProcessUtils

class Reader:
    def __init__(self, platform, feed_name:str):
        self.platform = platform
        self.feed_name = feed_name
        self.feed_config = platform.lifecycle(feed_name)
        self.record_format = platform.get_record_format(feed_name)
        self.data_dir = platform.base_path / "data" / feed_name
        self.my_pid = ProcessUtils.get_current_pid()

        # Try to resume existing feed
        feed_exists = platform.registry.feed_exists(feed_name)
        if not feed_exists:
            raise RuntimeError(f"Feed '{feed_name}' does not exist; cannot create Reader.")        

        self.registry = FeedRegistry(platform.base_path/"data"/feed_name/f"{feed_name}.reg", mode='r')

        self.chunk_data = []
        self.chunk_metadata = []
        self.chunk_indexes = []

    def get_latest_record(self) -> tuple:
        latest_chunk_meta = self.registry.get_latest_chunk()
        if latest_chunk_meta is None:
            raise RuntimeError(f"Feed '{self.feed_name}' has no chunks; cannot read records.")
        
        chunk_id = latest_chunk_meta.chunk_id
        chunk_path = self.data_dir / f"chunk_{chunk_id:08d}.bin"
        if not chunk_path.exists():
            raise RuntimeError(f"Chunk file '{chunk_path}' does not exist; cannot read records.")

        if len(self.chunk_data) == 0 or self.chunk_metadata[-1].chunk_id != chunk_id:
            # Load new chunk
            chunk = Chunk(str(chunk_path), self.record_format["fmt"], self.record_format["ts_offset"])
            self.chunk_data.append(chunk)
            self.chunk_metadata.append(latest_chunk_meta)

            if self.feed_config.get("index_playback") is True:
                chunk_index = ChunkIndex(name=f"{self.feed_name}_index", directory=self.data_dir, create=False)
                self.chunk_indexes.append(chunk_index)
            else:
                self.chunk_indexes.append(None)

        chunk = self.chunk_data[-1]
        index = self.chunk_indexes[-1]

        if index:
            # Use index to find latest record
            if index._index_count == 0:
                raise RuntimeError(f"Index for chunk '{chunk_id}' is empty; cannot read records.")
            last_index_record = IndexRecord(index._mv, HEADER_SIZE + (index._index_count - 1) * INDEX_SIZE)
            offset = last_index_record.offset
            size = last_index_record.size
            record_bytes = chunk.mmap[offset:offset+size]
            return struct.unpack(self.record_format["fmt"], record_bytes)
        else:
            # No index; read last record directly
            if chunk.record_count == 0:
                raise RuntimeError(f"Chunk '{chunk_id}' has no records; cannot read records.")
            return chunk.get_record(chunk.record_count - 1)

    def close(self):
        """Clean shutdown with ownership release"""
        if self.current_chunk:
            self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
            if self.feed_config["persist"]:
                try:
                    file_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
                    self.current_chunk.persist_to_disk(str(file_path))
                except Exception as e:
                    print(f"⚠️  Could not persist final chunk {e}")
            self.current_chunk_metadata.release()
            if self.feed_config.get("index_playback") is True:
                self.chunk_index.close()
            self.registry.close()
            self._chunk_mv = None
            self.current_chunk.close()
