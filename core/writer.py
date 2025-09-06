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
        self.registry = FeedRegistry(platform.base_path/"data"/feed_name/f"{feed_name}.reg", mode='w')

        # Current chunk state
        self.current_chunk = None
        self.chunk_index = None
        self.current_chunk_metadata = self.registry.get_latest_chunk()
        if self.current_chunk_metadata is None:
            self.current_chunk_id = 0
        else:
            self.current_chunk_id = self.current_chunk_metadata.chunk_id
        self._create_new_chunk()

    def _create_new_chunk(self):
        self.current_chunk_id += 1
        self.registry.register_chunk(
            time.time_ns(),self.current_chunk_id,
            self.feed_config["chunk_size_bytes"],1)

        if self.current_chunk is not None:
            if self.feed_config["persist"]:
                file_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
                self.current_chunk.persist_to_disk(str(file_path))
            self._chunk_mv = None
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
        self._chunk_mv = self.current_chunk.memview()

        if self.feed_config.get("index_playback") is True:
            if self.chunk_index is not None:
                self.chunk_index.close()
            self.chunk_index = ChunkIndex(name=f"{self.feed_name}-{self.current_chunk_id}.idx", create=True, directory=self.platform.data_path/self.feed_name)

    def write(self, timestamp: int, data: Union[bytes, np.ndarray], create_index: bool = False) -> int:
        if isinstance(data, np.ndarray):
            record_data = data.tobytes()
        else:
            record_data = bytes(data)

        record_size = len(record_data)
        if record_size > self.current_chunk_metadata.size - self.current_chunk_metadata.write_pos:
            self._create_new_chunk()

        position = self.current_chunk_metadata.write_pos
        new_position = self.current_chunk.write_bytes(position, record_data)
        if create_index and self.chunk_index is not None:
            self.chunk_index.create_index(timestamp, self.current_chunk_metadata, 0)

        self.current_chunk_metadata.last_update = timestamp
        self.current_chunk_metadata.write_pos = new_position
        self.current_chunk_metadata.num_records += 1

    def _ensure_schema_init(self):
        if getattr(self, "_S", None) is not None:
            return
        rf = getattr(self, "record_format", None)
        if not rf or "fmt" not in rf or "fields" not in rf:
            raise RuntimeError("Writer.record_format missing 'fmt'/'fields'; cannot pack values.")
        self._S = struct.Struct(rf["fmt"])
        self._rec_size = self._S.size
        self._value_fields = [f.get("name") for f in rf["fields"] if f.get("name") != "_"]
        # timestamp field name (optional). feed specs used 'ts_col' previously.
        self._ts_field = rf.get("ts_col") or rf.get("ts") or None
        try:
            self._ts_idx = self._value_fields.index(self._ts_field) if self._ts_field else None
        except ValueError:
            self._ts_idx = None
        # staging state
        self._staging_active = False
        self._staging_pos = 0
        self._staging_count = 0
        self._staging_last_ts = None

    def _extract_ts_from_vals(self, vals):
        if getattr(self, "_ts_idx", None) is not None and self._ts_idx < len(vals):
            try:
                return int(vals[self._ts_idx])
            except Exception:
                pass
        # fallback to wall clock if schema didn't specify ts
        return int(time.time_ns())

    def write_values(self, *vals, create_index=False) -> int:
        """Pack positional values according to schema and write directly to SHM.
        Does not accept kwargs. Uses schema order (non-padding fields).
        """
        self._ensure_schema_init()
        ts = self._extract_ts_from_vals(vals)
        record_size = self._rec_size
        # rotate chunk if needed
        if record_size > self.current_chunk_metadata.size - self.current_chunk_metadata.write_pos:
            self._create_new_chunk()
        self._S.pack_into(self._chunk_mv, self.current_chunk_metadata.write_pos, *vals)
        if create_index and self.chunk_index is not None:
            self.chunk_index.create_index(
                self._chunk_mv[self.current_chunk_metadata.write_pos+self.record_format.get("ts_offset"):self.current_chunk_metadata.write_pos+self.record_format.get("ts_offset")+self.record_format.get("ts_size")].cast("Q")[0],
                self.current_chunk_metadata.write_pos, 0
            )
        self.current_chunk_metadata.last_update = ts
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
    def commit_values(self) -> int:
        """Publish previously staged rows by updating metadata once.
        Returns new write_pos. No-op if nothing staged.
        """
        # If schema hasn't been initialized yet, there is nothing staged.
        if getattr(self, "_S", None) is None:
            return self.current_chunk_metadata.write_pos
        if not getattr(self, "_staging_active", False) or self._staging_count == 0:
            return self.current_chunk_metadata.write_pos
        self.current_chunk_metadata.last_update = (
            self._staging_last_ts if self._staging_last_ts is not None else self.current_chunk_metadata.last_update
        )
        self.current_chunk_metadata.write_pos = self._staging_pos
        self.current_chunk_metadata.num_records += self._staging_count
        # reset staging
        self._staging_active = False
        self._staging_count = 0
        self._staging_last_ts = None
        return self.current_chunk_metadata.write_pos

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