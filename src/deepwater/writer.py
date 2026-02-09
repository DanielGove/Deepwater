import threading
import time
import struct
import logging
from typing import Union, Optional, Tuple
from pathlib import Path
import numpy as np
import numba as _nb
from multiprocessing import shared_memory

from .chunk import Chunk
from .index import ChunkIndex
from .feed_registry import FeedRegistry, IN_MEMORY, ON_DISK, EXPIRED
from .utils.process import ProcessUtils

log = logging.getLogger("dw.writer")

class Writer:
    __slots__ = (
        "platform", "feed_name", "feed_config", "record_format", "my_pid",
        "data_dir", "registry",
        "current_chunk", "chunk_index", "current_chunk_metadata", "current_chunk_id",
        "_S", "_rec_size", "_value_fields", "_ts_field", "_ts_idx",
        "_staging_active", "_staging_pos", "_staging_count", "_staging_last_ts", "_rowbuf",
    )
    """
    Writer for persistent disk-based feeds.
    
    Features:
        - Fixed-size binary chunks (default 128MB)
        - Automatic chunk rotation when full
        - Optional time-based indexing for fast playback
        - Memory-mapped metadata (live visibility to readers)
        - Crash recovery (validates previous chunk on startup)
    
    Threading:
        ⚠️  NOT thread-safe - Writer is NOT protected by locks
        - For multi-threaded access, use external coordination (queue with dedicated writer thread)
        - Writes after close() will silently return 0 (defensive guard prevents crashes)
        - Concurrent writes + close() from different threads may race (use queue pattern)
    
    Usage Patterns:
        # High-frequency writes (trading)
        >>> writer = platform.create_writer('trades')
        >>> for event in websocket:
        ...     writer.write_values(event.price, event.size, event.timestamp_us)
        
        # Batch writes (backfill)
        >>> writer = platform.create_writer('historical')
        >>> for batch in chunks:
        ...     writer.write_tuple(batch)  # Pre-packed tuple
        
        # Always close when done
        >>> writer.close()  # Seals chunk, updates metadata
    
    Methods:
        write_values(*args): Write individual field values
            >>> writer.write_values(123.45, 100.0, 1738368000000000)
        
        write_tuple(record): Write pre-packed tuple (faster for batch)
            >>> writer.write_tuple((123.45, 100.0, 1738368000000000))
        
        write_dict(record): Write from dictionary (slower, convenient)
            >>> writer.write_dict({'price': 123.45, 'size': 100.0, 'timestamp_us': 1738368000000000})
        
        close(): Seal current chunk, release resources
    
    Chunk Rotation:
        - Automatic when chunk reaches size limit
        - Transparent to caller (no special handling needed)
        - New chunk starts immediately after rotation
        - Metadata updated atomically (readers see clean transition)
    
    Multi-Process:
        - Only ONE writer per feed allowed (enforced by platform)
        - Multiple readers can read while writer is active
        - Writer and readers can be in different processes
    
    Gotchas:
        - Must call close() to seal chunk (otherwise readers see incomplete data)
        - Timestamps must be monotonic increasing (not enforced, but expected)
        - Field order must match feed schema (no validation)
        - Writer holds exclusive lock on chunk file
    
    Example:
        >>> from deepwater import Platform
        >>> import time
        >>> 
        >>> p = Platform('./data')
        >>> p.create_feed({
        ...     'feed_name': 'trades',
        ...     'mode': 'UF',
        ...     'fields': [
        ...         {'name': 'price', 'type': 'float64'},
        ...         {'name': 'size', 'type': 'float64'},
        ...         {'name': 'timestamp_us', 'type': 'uint64'},
        ...     ],
        ...     'ts_col': 'timestamp_us',
        ...     'persist': True,
        ... })
        >>> 
        >>> writer = p.create_writer('trades')
        >>> 
        >>> # Write live data
        >>> timestamp_us = int(time.time() * 1e6)
        >>> writer.write_values(123.45, 100.0, timestamp_us)
        >>> writer.write_values(123.50, 200.0, timestamp_us + 1000)
        >>> 
        >>> writer.close()
    """
    def __init__(self, platform, feed_name:str):
        self.platform = platform
        self.feed_name = feed_name
        self.feed_config = platform.lifecycle(feed_name)
        self.record_format = platform.get_record_format(feed_name)
        self.my_pid = ProcessUtils.get_current_pid()

        # Try to resume existing feed
        feed_exists = platform.registry.feed_exists(feed_name)
        if not feed_exists:
            log.info("Creating new feed '%s' (pid=%s)", feed_name, self.my_pid)
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

        # Validate previous chunk before starting (handles corruption/missing files)
        if self.current_chunk_id > 0:
            self._validate_previous_chunk()

        self._create_new_chunk()
        self._schema_init()

    def _create_new_chunk(self):
        self.current_chunk_id += 1
        
        # Release old metadata BEFORE register_chunk in case it triggers resize
        _new_start_time = None
        if self.current_chunk_metadata is not None:
            self.current_chunk.close_file()
            self.current_chunk_metadata.status = ON_DISK
            self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
            _new_start_time = self.current_chunk_metadata.end_time
            self.current_chunk_metadata.release()

        # Writer only handles persist=True (disk chunks)
        chunk_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
        self.current_chunk = Chunk.create_file(path=str(chunk_path), size=self.feed_config["chunk_size_bytes"])
        self.registry.register_chunk(
            time.time_ns() // 1_000, self.current_chunk_id,
            self.feed_config["chunk_size_bytes"], status=ON_DISK)

        self.current_chunk_metadata = self.registry.get_chunk_metadata(self.current_chunk_id)
        self.current_chunk_metadata.start_time = _new_start_time if _new_start_time is not None else self.current_chunk_metadata.start_time

        if self.feed_config.get("index_playback") is True:
            if self.chunk_index is not None:
                self.chunk_index.close_file()
            self.chunk_index = ChunkIndex.create_file(
                path=str(self.data_dir / f"chunk_{self.current_chunk_id:08d}.idx"),
                capacity=2047
            )

    def _validate_previous_chunk(self):
        """Validate previous chunk and auto-repair if corrupted."""
        from . import repair
        
        meta = self.registry.get_chunk_metadata(self.current_chunk_id)
        if meta is None:
            return
        
        try:
            # Delegate to centralized repair logic (persist=True only)
            repair.validate_and_repair_chunk(
                chunk_id=self.current_chunk_id,
                meta=meta,
                feed_name=self.feed_name,
                feed_dir=self.data_dir,
                record_format=self.record_format
            )
        finally:
            meta.release()

    def write(self, timestamp: int, record_data: Union[bytes, np.ndarray], create_index: bool = False) -> int:
        # Guard against write after close
        if self.current_chunk is None:
            return 0  # Silently ignore writes after close
        
        if isinstance(record_data, np.ndarray):
            record_data = record_data.tobytes()

        record_size = len(record_data)
        if record_size > self.current_chunk_metadata.size - self.current_chunk_metadata.write_pos:
            self._create_new_chunk()

        position = self.current_chunk_metadata.write_pos
        self.current_chunk.buffer[position:position+record_size] = record_data
        if create_index and self.chunk_index is not None:
            self.chunk_index.create_index(timestamp, position, record_size)

        self.current_chunk_metadata.last_update = timestamp
        self.current_chunk_metadata.write_pos = position + record_size
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
        self._rowbuf = bytearray(self._rec_size)

    def write_values(self, *vals, create_index=False) -> int:
        """
        Write a record to the current chunk.
        
        Values must match feed schema field order (excluding padding fields).
        Automatically rotates to new chunk when current chunk is full.
        
        Args:
            *vals: Field values in schema order
            create_index: If True, create time-index entry for this record
        
        Returns:
            New write position in chunk
        
        Example:
            >>> writer.write_values(123.45, 100.0, 1738368000000000)
            >>> writer.write_values(124.50, 50.0, 1738368001000000, create_index=True)
        """
        # Guard against write after close
        if self.current_chunk is None:
            return 0  # Silently ignore writes after close
        if self.current_chunk_metadata.write_pos + self._rec_size > self.current_chunk_metadata.size:
            self._create_new_chunk()
        self._S.pack_into(self.current_chunk.buffer, self.current_chunk_metadata.write_pos, *vals)
        if create_index and self.chunk_index is not None:
            self.chunk_index.create_index(vals[self._ts_idx], self.current_chunk_metadata.write_pos)
        self.current_chunk_metadata.last_update = vals[self._ts_idx]
        self.current_chunk_metadata.write_pos = self.current_chunk_metadata.write_pos + self._rec_size
        self.current_chunk_metadata.num_records += 1
        return self.current_chunk_metadata.write_pos
    
    def write_batch_bytes(self, data: bytes, create_index: bool = False) -> int:
        """Write a blob of packed records (len must be multiple of record_size) and update metadata once."""
        # Guard against write after close
        if self.current_chunk is None:
            return 0
        
        if len(data) == 0 or len(data) % self._rec_size != 0:
            raise ValueError("batch length must be a positive multiple of record_size")
        if self.current_chunk_metadata.write_pos + len(data) > self.current_chunk_metadata.size:
            self._create_new_chunk()
        self.current_chunk.buffer[self.current_chunk_metadata.write_pos:self.current_chunk_metadata.write_pos + len(data)] = data
        if create_index and self.chunk_index is not None:
            self.chunk_index.create_index(self.current_chunk_metadata.last_update, self.current_chunk_metadata.write_pos)
        self.current_chunk_metadata.write_pos += len(data)
        self.current_chunk_metadata.num_records += len(data) // self._rec_size
        self.current_chunk_metadata.last_update = int.from_bytes(data[-self._rec_size + self.record_format["ts_offset"]:-self._rec_size + self.record_format["ts_offset"] + 8], "little")
        return self.current_chunk_metadata.write_pos

    def close(self):
        """
        Close writer and flush pending data.
        
        Finalizes current chunk metadata and releases locks.
        Called automatically on exit, but explicit call recommended.
        
        Example:
            >>> writer.close()
        """
        if self.current_chunk:
            self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
            if self.feed_config.get("persist", True):
                self.current_chunk.close_file()
                if self.feed_config.get("index_playback", False) and self.chunk_index:
                    self.chunk_index.close_file()
            else:
                self.current_chunk.close_shm()
                if self.feed_config.get("index_playback", False) and self.chunk_index:
                    self.chunk_index.close_shm()
            self.current_chunk_metadata.status = ON_DISK if self.feed_config["persist"] else EXPIRED
            self.current_chunk_metadata.release()
            self.current_chunk = None  # Prevent double-close
            self.registry.close()

