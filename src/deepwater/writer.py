import time
import struct
import logging
import numpy as np

from typing import Union
from pathlib import Path

from .chunk import Chunk
from .index import ChunkIndex
from .feed_registry import FeedRegistry, IN_MEMORY, ON_DISK, EXPIRED, CHUNK_QMIN_OFFSET, MAX_QUERY_KEYS, UINT64_MAX
from .utils.process import ProcessUtils

log = logging.getLogger("dw.writer")

class Writer:
    __slots__ = (
        "platform", "feed_name", "feed_config", "record_format", "my_pid",
        "data_dir", "registry",
        "current_chunk", "chunk_index", "current_chunk_metadata", "current_chunk_id",
        "_S", "_rec_size", "_clock_level", "_key_min_set", "_u64", "_qmins", "_qmaxs",
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
        ...         {'name': 'timestamp_us', 'type': 'uint64'},  # time axes first
        ...         {'name': 'price', 'type': 'float64'},
        ...         {'name': 'size', 'type': 'float64'},
        ...     ],
        ...     'clock_level': 1,
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
        self._key_min_set = None

        # Validate previous chunk before starting (handles corruption/missing files)
        if self.current_chunk_id > 0:
            self._validate_chunk()

        self._S = struct.Struct(self.record_format["fmt"])
        self._u64 = struct.Struct("<Q")
        self._rec_size = self._S.size
        self._clock_level = self.feed_config.get("clock_level")

        self._create_new_chunk()

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

        # reset per-key bounds for new chunk
        self._key_min_set = [False for _ in range(self._clock_level)]

        # Writer only handles persist=True (disk chunks)
        chunk_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
        self.current_chunk = Chunk.create_file(path=str(chunk_path), size=self.feed_config["chunk_size_bytes"])
        self.registry.register_chunk(
            time.time_ns() // 1_000, self.current_chunk_id,
            self.feed_config["chunk_size_bytes"], status=ON_DISK,
            clock_level=self._clock_level
        )

        self.current_chunk_metadata = self.registry.get_chunk_metadata(self.current_chunk_id)
        self.current_chunk_metadata.start_time = _new_start_time if _new_start_time is not None else self.current_chunk_metadata.start_time
        self.current_chunk_metadata.clock_level = self._clock_level
        self._qmins = self.current_chunk_metadata._qmins
        self._qmaxs = self.current_chunk_metadata._qmaxs

        if self.feed_config.get("index_playback") is True:
            if self.chunk_index is not None:
                self.chunk_index.close_file()
            self.chunk_index = ChunkIndex.create_file(
                path=str(self.data_dir / f"chunk_{self.current_chunk_id:08d}.idx"),
                capacity=2047
            )

    def _validate_chunk(self):
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

        record_mv = memoryview(record_data)
        u64 = self._u64
        position = self.current_chunk_metadata.write_pos
        self.current_chunk.buffer[position:position+record_size] = record_data
        if create_index and self.chunk_index is not None:
            ts0 = u64.unpack_from(record_mv, 0)[0]
            ts1 = u64.unpack_from(record_mv, 8)[0] if self._clock_level >= 2 else 0
            ts2 = u64.unpack_from(record_mv, 16)[0] if self._clock_level >= 3 else 0
            self.chunk_index.create_index((ts0, ts1, ts2), position)

        key_min_set = self._key_min_set
        key_count = self._clock_level
        qmins = self._qmins
        qmaxs = self._qmaxs
        ts0 = u64.unpack_from(record_mv, 0)[0]
        if not key_min_set[0]:
            key_min_set[0] = True
            qmins[0] = ts0
        qmaxs[0] = ts0
        if key_count >= 2:
            ts1 = u64.unpack_from(record_mv, 8)[0]
            if not key_min_set[1]:
                key_min_set[1] = True
                qmins[1] = ts1
            qmaxs[1] = ts1
        if key_count >= 3:
            ts2 = u64.unpack_from(record_mv, 16)[0]
            if not key_min_set[2]:
                key_min_set[2] = True
                qmins[2] = ts2
            qmaxs[2] = ts2
        self.current_chunk_metadata.write_pos = position + record_size
        self.current_chunk_metadata.num_records += 1

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
        pos = self.current_chunk_metadata.write_pos
        self._S.pack_into(self.current_chunk.buffer, pos, *vals)
        if create_index and self.chunk_index is not None:
            ts0 = vals[0]
            ts1 = vals[1] if self._clock_level >= 2 else 0
            ts2 = vals[2] if self._clock_level >= 3 else 0
            self.chunk_index.create_index((ts0, ts1, ts2), pos)

        key_min_set = self._key_min_set
        key_count = self._clock_level
        qmins = self._qmins
        qmaxs = self._qmaxs
        ts0 = vals[0]
        if not key_min_set[0]:
            key_min_set[0] = True
            qmins[0] = ts0
        qmaxs[0] = ts0
        if key_count >= 2:
            ts1 = vals[1]
            if not key_min_set[1]:
                key_min_set[1] = True
                qmins[1] = ts1
            qmaxs[1] = ts1
        if key_count >= 3:
            ts2 = vals[2]
            if not key_min_set[2]:
                key_min_set[2] = True
                qmins[2] = ts2
            qmaxs[2] = ts2
        self.current_chunk_metadata.write_pos = pos + self._rec_size
        self.current_chunk_metadata.num_records += 1
        return self.current_chunk_metadata.write_pos
    
    def write_batch_bytes(self, data: bytes, create_index: bool = False) -> int:
        """Write a blob of packed records (len must be multiple of record_size) and update metadata once."""
        # Guard against write after close
        if self.current_chunk is None:
            return 0
        
        data_len = len(data)
        if data_len == 0 or data_len % self._rec_size != 0:
            raise ValueError("batch length must be a positive multiple of record_size")
        if self.current_chunk_metadata.write_pos + data_len > self.current_chunk_metadata.size:
            self._create_new_chunk()
        start = self.current_chunk_metadata.write_pos
        end = start + data_len
        rec_sz = self._rec_size
        self.current_chunk.buffer[start:end] = data
        # update per-key bounds using first and last records (monotonic timestamps)
        key_min_set = self._key_min_set
        key_count = self._clock_level
        qmins = self._qmins
        qmaxs = self._qmaxs
        mv_data = memoryview(data)
        u64 = self._u64
        ts_first = u64.unpack_from(mv_data, 0)[0]
        ts_last = u64.unpack_from(mv_data, data_len - rec_sz + 0)[0]
        if not key_min_set[0]:
            key_min_set[0] = True
            qmins[0] = ts_first
        qmaxs[0] = ts_last
        if key_count >= 2:
            ts_first = u64.unpack_from(mv_data, 8)[0]
            ts_last = u64.unpack_from(mv_data, data_len - rec_sz + 8)[0]
            if not key_min_set[1]:
                key_min_set[1] = True
                qmins[1] = ts_first
            qmaxs[1] = ts_last
        if key_count >= 3:
            ts_first = u64.unpack_from(mv_data, 16)[0]
            ts_last = u64.unpack_from(mv_data, data_len - rec_sz + 16)[0]
            if not key_min_set[2]:
                key_min_set[2] = True
                qmins[2] = ts_first
            qmaxs[2] = ts_last
        if create_index and self.chunk_index is not None:
            ts0_idx = 0
            ts1_idx = 8 if key_count >= 2 else None
            ts2_idx = 16 if key_count >= 3 else None
            ts0 = u64.unpack_from(mv_data, ts0_idx)[0]
            ts1 = u64.unpack_from(mv_data, ts1_idx)[0] if ts1_idx is not None else 0
            ts2 = u64.unpack_from(mv_data, ts2_idx)[0] if ts2_idx is not None else 0
            self.chunk_index.create_index((ts0, ts1, ts2), start)
        self.current_chunk_metadata.write_pos = end
        self.current_chunk_metadata.num_records += data_len // rec_sz
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
            if self.feed_config.get("persist", True):
                self.current_chunk.close_file()
                if self.feed_config.get("index_playback", False) and self.chunk_index:
                    self.chunk_index.close_file()
            else:
                self.current_chunk.close_shm()
                if self.feed_config.get("index_playback", False) and self.chunk_index:
                    self.chunk_index.close_shm()
            self.current_chunk_metadata.status = ON_DISK
            self.current_chunk_metadata.release()
            self.current_chunk = None  # Prevent double-close
            self.registry.close()
