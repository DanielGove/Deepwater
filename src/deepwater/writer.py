import threading
import time
import struct
from typing import Union, Optional, Tuple
from pathlib import Path
import numpy as np
import numba as _nb
from multiprocessing import shared_memory

from .chunk import Chunk
from .index import ChunkIndex
from .feed_registry import FeedRegistry, IN_MEMORY, ON_DISK, EXPIRED
from .utils.process import ProcessUtils

class Writer:
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
        self.registry = FeedRegistry(platform.base_path/"data"/feed_name/f"{feed_name}.reg", mode='w')

        # Current chunk state
        self.current_chunk = None
        self.chunk_index = None
        self.current_chunk_metadata = None
        self.current_chunk_id = self.registry.get_latest_chunk_idx() or 0

        self._create_new_chunk()
        self._schema_init()

    def _create_new_chunk(self):
        self.current_chunk_id += 1
        
        # Release old metadata BEFORE register_chunk in case it triggers resize
        # (resize will close/remap the mmap, invalidating our metadata's cast views and causing an exported pointers exception)
        _new_start_time = None
        if self.current_chunk_metadata is not None:
            if self.feed_config.get("persist") is True:
                self.current_chunk.close_file()
            else:
                self.current_chunk.close_shm()
            self.current_chunk_metadata.status = ON_DISK if self.feed_config["persist"] else EXPIRED            
            self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
            _new_start_time = self.current_chunk_metadata.end_time
            self.current_chunk_metadata.release()

        if self.feed_config.get("persist") is True:
            self.registry.register_chunk(
                time.time_ns(),self.current_chunk_id,
                self.feed_config["chunk_size_bytes"], status=ON_DISK)
            self.current_chunk = Chunk.create_file(path=str(self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"), size=self.feed_config["chunk_size_bytes"])
        else:
            self.registry.register_chunk(
                time.time_ns(),self.current_chunk_id,
                self.feed_config["chunk_size_bytes"], status=IN_MEMORY)
            self.current_chunk = Chunk.create_shm(name=f"{self.feed_name}-{self.current_chunk_id}", size=self.feed_config["chunk_size_bytes"])

        self.current_chunk_metadata = self.registry.get_chunk_metadata(self.current_chunk_id)
        self.current_chunk_metadata.start_time = _new_start_time if _new_start_time is not None else self.current_chunk_metadata.start_time

        if self.feed_config.get("index_playback") is True:
            if self.feed_config.get("persist") is True:
                if self.chunk_index is not None:
                    self.chunk_index.close_file()
                self.chunk_index = ChunkIndex.create_file(
                    path=str(self.data_dir / f"chunk_{self.current_chunk_id:08d}.idx"),
                    capacity=2047
                )
            else:
                if self.chunk_index is not None:
                    self.chunk_index.close_shm()
                self.chunk_index = ChunkIndex.create_shm(
                    name=f"{self.feed_name}-index-{self.current_chunk_id}",
                    capacity=2047
                )


    def write(self, timestamp: int, record_data: Union[bytes, np.ndarray], create_index: bool = False) -> int:
        if isinstance(record_data, np.ndarray):
            record_data = record_data.tobytes()

        record_size = len(record_data)
        if record_size > self.current_chunk_metadata.size - self.current_chunk_metadata.write_pos:
            self._create_new_chunk()

        position = self.current_chunk_metadata.write_pos
        new_position = self.current_chunk.write_bytes(position, record_data)
        if create_index and self.chunk_index is not None:
            self.chunk_index.create_index(timestamp, position, record_size)

        self.current_chunk_metadata.last_update = timestamp
        self.current_chunk_metadata.write_pos = new_position
        self.current_chunk_metadata.num_records += 1

    def _schema_init(self):
        rf = getattr(self, "record_format", None)
        if not rf or "fmt" not in rf or "fields" not in rf:
            raise RuntimeError("Writer.record_format missing 'fmt'/'fields'; cannot pack values.")
        self._S = struct.Struct(rf["fmt"])
        self._rec_size = self._S.size
        self._value_fields = [f.get("name") for f in rf["fields"] if f.get("name") != "_"]
        # timestamp field name (optional). feed specs used 'ts_col' previously.
        self._ts_field = rf.get("ts_name", None)
        try:
            self._ts_idx = self._value_fields.index(self._ts_field) if self._ts_field else None
        except ValueError:
            self._ts_idx = None
        # staging state
        self._staging_active = False
        self._staging_pos = 0
        self._staging_count = 0
        self._staging_last_ts = None

    def write_values(self, *vals, create_index=False) -> int:
        """Pack positional values according to schema and write directly to SHM.
        Does not accept kwargs. Uses schema order (non-padding fields).
        """
        if self._rec_size > self.current_chunk_metadata.size - self.current_chunk_metadata.write_pos:
            self._create_new_chunk()
        self._S.pack_into(self.current_chunk.buffer, self.current_chunk_metadata.write_pos, *vals)
        if create_index and self.chunk_index is not None:
            self.chunk_index.create_index(
                vals[self._ts_idx],
                self.current_chunk_metadata.write_pos,
                self._rec_size
            )
        self.current_chunk_metadata.last_update = vals[self._ts_idx]
        self.current_chunk_metadata.write_pos += self._rec_size
        self.current_chunk_metadata.num_records += 1
        return self.current_chunk_metadata.write_pos

    # TODO
    def stage_values(self, *vals) -> int:
        """Pack positional values and stage them (write bytes) without updating metadata.
        Call commit_values() to publish staged rows atomically.
        NOTE: this API assumes the same thread uses the writer during staging.
        """
        self._ensure_schema_init()
        self._S.pack_into(self._rowbuf, 0, *vals)
        ts = self._extract_ts_from_vals(vals)
        # initialize staging window if not active
        if not getattr(self, "_staging_active", False):
            self._staging_active = True
            self._staging_pos = self.current_chunk_metadata.write_pos
            self._staging_count = 0
            self._staging_last_ts = None
        # rotate chunk if the staged record won't fit
        if self._staging_pos + self._rec_size > self.current_chunk_metadata.size:
            # start a new chunk for staging
            self._create_new_chunk()
            self._staging_pos = self.current_chunk_metadata.write_pos
            self._staging_count = 0
            self._staging_last_ts = None
        # write bytes at the staging cursor (no registry update)
        new_pos = self.current_chunk.write_bytes(self._staging_pos, self._rowmv[:self._rec_size])
        self._staging_pos = new_pos
        self._staging_count += 1
        self._staging_last_ts = ts
        return new_pos

    # TODO
    def commit_values(self, create_index: bool = False) -> int:
        """Publish previously staged rows by updating metadata once.
        Returns new write_pos. No-op if nothing staged.
        """
        if not getattr(self, "_staging_active", False):
            self._staging_active = False
            self._staging_count = 0
            self._staging_last_ts = None
            return self.current_chunk_metadata.write_pos
        self.current_chunk_metadata.last_update = self._staging_last_ts
        self.current_chunk_metadata.write_pos = self._staging_pos
        self.current_chunk_metadata.num_records += self._staging_count
        self._staging_active = False
        self._staging_count = 0
        self._staging_last_ts = None
        return self.current_chunk_metadata.write_pos

    def close(self):
        if self.current_chunk:
            self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
            if self.feed_config.get("persist", True):
                self.current_chunk.close_file()
                if self.feed_config.get("index_playback", False):
                    self.chunk_index.close_file()
            else:
                self.current_chunk.close_shm()
                if self.feed_config.get("index_playback", False):
                    self.chunk_index.close_shm()
            self.current_chunk_metadata.status = ON_DISK if self.feed_config["persist"] else EXPIRED
            self.current_chunk_metadata.release()
            self.registry.close()
