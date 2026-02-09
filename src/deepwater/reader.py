import time
import struct
from typing import Optional, Iterator, Union, List, Tuple

from .chunk import Chunk
from .index import ChunkIndex
from .feed_registry import FeedRegistry, IN_MEMORY, ON_DISK, EXPIRED, UINT64_MAX
from .utils.process import ProcessUtils

# Try to import Cython-optimized hot paths
try:
    from .reader_fast import binary_search_fast, range_tuples_fast
    HAVE_FAST = True
except ImportError:
    HAVE_FAST = False


class Reader:
    """
    Zero-copy reader for persistent disk-based feeds.
    
    Three Read Modes:
        stream(start=None, format='tuple') → Iterator (infinite, live)
            - Live tail: start=None jumps to current write head
            - Historical: start=timestamp_us streams from that point forward
            - Spin-waits for new data (CPU-intensive)
            - Use for: Live trading, real-time monitoring
        
        range(start, end, format='tuple') → List (finite, historical)
            - Returns all records in [start, end) microseconds
            - Use for: Backtesting, analysis, batch processing
        
        latest(seconds, format='tuple') → List (convenience)
            - Returns last N seconds of data
            - Equivalent to: range(now - seconds*1e6, now)
            - Use for: Quick lookback, recent data checks
    
    Four Output Formats:
        'tuple': Fast tuples (default)
            >>> for record in reader.stream(format='tuple'):
            ...     price, size, timestamp = record
        
        'dict': Named fields (readable, ~2x slower)
            >>> for record in reader.stream(format='dict'):
            ...     print(record['price'], record['size'])
        
        'numpy': Structured array (batch vectorization)
            >>> data = reader.range(start, end, format='numpy')
            >>> avg_price = data['price'].mean()
        
        'raw': Memoryview (zero-copy, expert mode)
            >>> for raw_bytes in reader.stream(format='raw'):
            ...     # Manual parsing with struct.unpack
    
    Common Patterns:
        # Live trading (spin-wait)
        >>> for trade in reader.stream():  # start=None = live
        ...     price, size, timestamp = trade
        ...     # Process immediately (no batching)
        
        # Backtest
        >>> data = reader.range(start_us, end_us, format='numpy')
        >>> # Vectorized operations on entire range
        
        # Replay from specific point
        >>> for trade in reader.stream(start=yesterday_us):
        ...     # Stream from yesterday to now, then continue live
    
    Gotchas:
        - stream() with start=None: Jumps to CURRENT write head (skips history)
        - stream() with start=timestamp: Starts from that timestamp (includes history)
        - stream() is CPU-intensive (spin-wait), use range() for analysis
        - Reader keeps file handles open, call reader.close() when done
        - Chunk rotation: Automatic, transparent, handles writer rotation
    
    Example:
        >>> from deepwater import Platform
        >>> p = Platform('./data')
        >>> reader = p.create_reader('trades')
        >>> 
        >>> # Get last 60 seconds
        >>> recent = reader.latest(60)
        >>> print(f'Last minute: {len(recent)} trades')
        >>> 
        >>> # Live stream
        >>> for trade in reader.stream():
        ...     price, size, ts = trade
        ...     if price > 100:
        ...         break
        >>> 
        >>> reader.close()
    """
    __slots__ = ('platform', 'feed_name', 'feed_config', 'record_format', 
                 'data_dir', 'registry', '_chunk', '_chunk_meta', '_chunk_id',
                 '_unpack', '_rec_size', '_index_available', '_ts_offset', 
                 '_ts_byteorder', '_field_names', '_dtype', '_read_head', '_query_keys')

    def __init__(self, platform, feed_name: str):
        self.platform = platform
        self.feed_name = feed_name
        self.feed_config = platform.lifecycle(feed_name)
        self.record_format = platform.get_record_format(feed_name)
        self.data_dir = platform.base_path / "data" / feed_name
        
        reg_path = platform.base_path / "data" / feed_name / f"{feed_name}.reg"
        self.registry = FeedRegistry(reg_path, mode="r")
        
        self._chunk: Optional[Chunk] = None
        self._chunk_meta = None
        self._chunk_id: Optional[int] = None
        
        # Hot path optimizations - pre-cache everything
        self._unpack = struct.Struct(self.record_format["fmt"]).unpack_from
        self._rec_size = self.record_format["record_size"]
        self._index_available = bool(self.feed_config.get("index_playback"))
        self._ts_offset = self.record_format.get("ts_offset") or self.record_format.get("ts", {}).get("offset")
        self._ts_byteorder = "little" if self.record_format.get("ts_endian", "<") == "<" else "big"
        
        # Query keys: primary ts_col + any additional query_cols
        # Format: {col_name: {offset: int, size: 8}}
        self._query_keys = self.record_format.get("query_keys", {})
        # Add primary timestamp to query_keys for unified access
        if self._ts_offset is not None:
            ts_name = self.record_format.get("ts_name")
            if ts_name and ts_name not in self._query_keys:
                self._query_keys[ts_name] = {"offset": self._ts_offset, "size": 8}
        
        # Filter field_names to exclude padding fields (marked with _N in type like _6, _8)
        # These fields are skipped by struct.unpack due to 'x' in format string
        self._field_names = tuple(f["name"] for f in self.record_format.get("fields", []) 
                                 if not f.get("type", "").startswith("_") or f["name"] != "_")
        
        # Lazy-load numpy dtype
        self._dtype = None
        
        # Read head for non-blocking reads (read_available)
        self._read_head: Optional[int] = None

    # ================================================================ METADATA
    
    @property
    def format(self) -> str:
        """Struct format string (e.g., '<1s1s6xQQQQQdd')"""
        return self.record_format["fmt"]
    
    @property
    def field_names(self) -> tuple:
        """Field names in order (e.g., ('type', 'side', 'trade_id', ...))"""
        return self._field_names
    
    @property
    def dtype(self):
        """Numpy dtype dict for structured arrays"""
        if self._dtype is None:
            import numpy as np
            # Fix duplicate field names by appending index
            dtype_spec = self.record_format["dtype"].copy()
            names = dtype_spec["names"]
            seen = {}
            unique_names = []
            for name in names:
                if name in seen:
                    seen[name] += 1
                    unique_names.append(f"{name}_{seen[name]}")
                else:
                    seen[name] = 0
                    unique_names.append(name)
            dtype_spec["names"] = unique_names
            self._dtype = np.dtype(dtype_spec)
        return self._dtype
    
    @property
    def record_size(self) -> int:
        """Size of each record in bytes"""
        return self._rec_size

    # ================================================================ INTERNAL

    def _resolve_ts_key(self, ts_key: Optional[str]) -> Tuple[int, str]:
        """Resolve ts_key to (offset, byteorder). Returns primary if ts_key is None."""
        if ts_key is None:
            if self._ts_offset is None:
                raise RuntimeError("Feed missing ts_offset for time queries")
            return self._ts_offset, self._ts_byteorder
        
        if ts_key not in self._query_keys:
            available = list(self._query_keys.keys())
            raise ValueError(f"ts_key '{ts_key}' not configured as queryable. Available: {available}")
        
        key_info = self._query_keys[ts_key]
        return key_info["offset"], self._ts_byteorder

    def _close_chunk(self) -> None:
        if self._chunk is not None:
            self._chunk.close_shm() if self._chunk.is_shm else self._chunk.close_file()
            self._chunk = None
        if self._chunk_meta is not None:
            self._chunk_meta.release()
            self._chunk_meta = None
        self._chunk_id = None

    def _open_chunk(self, chunk_id: int) -> None:
        if self._chunk_id == chunk_id:
            return
        self._close_chunk()
        
        meta = self.registry.get_chunk_metadata(chunk_id)
        if meta is None:
            raise RuntimeError(f"Chunk {chunk_id} metadata not found")
        
        if meta.status == IN_MEMORY:
            chunk = Chunk.open_shm(name=f"{self.feed_name}-{chunk_id}")
        elif meta.status == ON_DISK:
            chunk_path = self.data_dir / f"chunk_{chunk_id:08d}.bin"
            if not chunk_path.exists():
                meta.release()
                raise FileNotFoundError(f"Chunk file missing: {chunk_path}")
            chunk = Chunk.open_file(file_path=str(chunk_path))
        elif meta.status == EXPIRED:
            raise RuntimeError(f"Chunk {chunk_id} is expired")
        else:
            raise RuntimeError(f"Unknown chunk status {meta.status}")
        
        self._chunk_meta = meta
        self._chunk = chunk
        self._chunk_id = chunk_id

    def _ensure_latest_chunk(self) -> None:
        latest = self.registry.get_latest_chunk_idx()
        if latest is None:
            raise RuntimeError(f"Feed '{self.feed_name}' has no chunks")
        self._open_chunk(latest)

    def _get_current_timestamp_us(self) -> int:
        """Get current timestamp for latest() queries"""
        return time.time_ns() // 1000

    def _iter_chunks_in_range(self, start_us: int, end_us: Optional[int] = None, ts_key: Optional[str] = None):
        """Iterate chunk IDs that overlap with time range for the requested ts_key."""
        if self.registry.get_latest_chunk_idx() is None:
            return

        # Determine which key index to use for filtering
        ts_keys = [self.record_format.get("ts_name")] + list(self.record_format.get("query_keys", {}).keys())
        ts_idx = 0
        if ts_key:
            try:
                ts_idx = ts_keys.index(ts_key)
            except ValueError:
                ts_idx = 0  # fallback to primary

        latest = self.registry.get_latest_chunk_idx() or 0
        # Use per-key bounds for all ts_keys (including primary)
        chunk_iter = range(1, latest + 1)

        for chunk_id in chunk_iter:
            self._open_chunk(chunk_id)
            meta = self._chunk_meta
            if meta.num_records == 0:
                continue
            if ts_idx >= meta.query_key_count:
                yield chunk_id
                continue
            qmin = meta.get_qmin(ts_idx)
            qmax = meta.get_qmax(ts_idx)
            # If bounds are uninitialized, include conservatively
            if qmin == UINT64_MAX or qmax == 0:
                yield chunk_id
                continue
            if end_us is None:
                if qmax >= start_us:
                    yield chunk_id
            else:
                if not (qmax < start_us or qmin > end_us):
                    yield chunk_id

    def _binary_search_start(self, start_us: int, ts_offset: Optional[int] = None, ts_byteorder: Optional[str] = None) -> int:
        """Binary search for start position in current chunk (assumes sorted!)"""
        if ts_offset is None:
            ts_offset = self._ts_offset
        if ts_byteorder is None:
            ts_byteorder = self._ts_byteorder
        
        if HAVE_FAST:
            return binary_search_fast(
                self._chunk.buffer,
                start_us,
                self._chunk_meta.write_pos,
                self._rec_size,
                ts_offset
            )
        
        # Pure Python fallback
        buf = self._chunk.buffer
        write_pos = self._chunk_meta.write_pos
        rec_size = self._rec_size
        ts_off = ts_offset
        ts_end = ts_off + 8
        from_bytes = int.from_bytes
        byteorder = ts_byteorder
        
        left = 0
        right = write_pos // rec_size
        
        while left < right:
            mid = (left + right) // 2
            mid_pos = mid * rec_size
            ts = from_bytes(buf[mid_pos + ts_off:mid_pos + ts_end], byteorder)
            
            if ts < start_us:
                left = mid + 1
            else:
                right = mid
        
        return left * rec_size

    # ================================================================ TUPLE FORMAT

    def _range_tuples(self, start_us: int, end_us: Optional[int], playback: bool = False, ts_key: Optional[str] = None) -> List[tuple]:
        """Read range as list of tuples (FAST)"""
        # Resolve which timestamp column to query on
        ts_offset, ts_byteorder = self._resolve_ts_key(ts_key)
        
        # If caller requests playback and index is available, start from latest snapshot before start_us
        if playback and self._index_available:
            snapshot_ts = self._get_snapshot_before(start_us)
            if snapshot_ts is not None and snapshot_ts < start_us:
                start_us = snapshot_ts
        
        result = []
        
        for chunk_id in self._iter_chunks_in_range(start_us, end_us, ts_key=ts_key):
            buf = self._chunk.buffer
            write_pos = self._chunk_meta.write_pos
            
            # Binary search for start
            pos = self._binary_search_start(start_us, ts_offset, ts_byteorder)
            
            # Use Cython fast path for inner loop if available
            if HAVE_FAST:
                chunk_results = range_tuples_fast(
                    buf,
                    pos,
                    write_pos,
                    start_us,
                    end_us or 0,  # 0 means no end limit
                    self._rec_size,
                    ts_offset,
                    self._unpack
                )
                result.extend(chunk_results)
            else:
                # Pure Python fallback
                unpack = self._unpack
                rec_size = self._rec_size
                ts_off = ts_offset
                ts_end = ts_off + 8
                from_bytes = int.from_bytes
                byteorder = ts_byteorder
                
                # Optimized tight loop
                while pos + rec_size <= write_pos:
                    ts = from_bytes(buf[pos + ts_off:pos + ts_end], byteorder)
                    if ts < start_us:
                        pos += rec_size
                        continue
                    if end_us is not None and ts > end_us:
                        break
                    result.append(unpack(buf, pos))
                    pos += rec_size
        
        return result

    def _stream_tuples(self, start_us: Optional[int], ts_key: Optional[str] = None) -> Iterator[tuple]:
        """Stream tuples (historical replay → live)"""
        unpack = self._unpack
        rec_size = self._rec_size
        
        # Historical replay if start_us provided
        if start_us is not None:
            # Resolve which timestamp column to query on
            ts_offset, ts_byteorder = self._resolve_ts_key(ts_key)
            if ts_offset is None:
                raise RuntimeError("Feed missing ts_offset for time-based streaming")
            
            # Get latest chunk ID to know when to stop historical replay
            latest_chunk_id = self.registry.get_latest_chunk_idx()
            
            for chunk_id in self._iter_chunks_in_range(start_us, None, ts_key=ts_key):
                buf = self._chunk.buffer
                write_pos = self._chunk_meta.write_pos
                pos = self._binary_search_start(start_us, ts_offset, ts_byteorder)
                
                while pos + rec_size <= write_pos:
                    yield unpack(buf, pos)
                    pos += rec_size
                
                # Stop after processing the latest (active) chunk
                if chunk_id >= latest_chunk_id:
                    break
            
            # After historical replay, we're already on the latest chunk
            # Just set read_head to current write position
            if self._chunk is not None:
                read_head = self._chunk_meta.write_pos
            else:
                self._ensure_latest_chunk()
                read_head = self._chunk_meta.write_pos
        else:
            # Live streaming from current write head
            self._ensure_latest_chunk()
            # Initialize read_head to current write position immediately
            # Don't defer this - it causes iteration through entire chunk!
            read_head = self._chunk_meta.write_pos
        
        # Continue with live polling
        
        while True:
            if self._chunk_meta is None:
                self._ensure_latest_chunk()
                read_head = self._chunk_meta.write_pos
            
            # Refresh write position on each iteration
            buf = self._chunk.buffer
            write_pos = self._chunk_meta.write_pos
            
            # Read new records
            while read_head + rec_size <= write_pos:
                yield unpack(buf, read_head)
                read_head += rec_size
            
            # Check for chunk rotation (status changes to ON_DISK when sealed)
            # But only rotate if we're not already on the latest chunk
            if self._chunk_meta.status == ON_DISK:
                latest_chunk_id = self.registry.get_latest_chunk_idx()
                if self._chunk_id < latest_chunk_id:
                    # There's a newer chunk available
                    next_id = self._chunk_id + 1
                    next_meta = self.registry.get_chunk_metadata(next_id)
                    if next_meta is not None:
                        # Check if next chunk file actually exists before rotating
                        next_path = self.data_dir / f"chunk_{next_id:08d}.bin"
                        if next_path.exists():
                            self._open_chunk(next_id)
                            read_head = 0
                            next_meta.release()
                            continue
                        next_meta.release()
                # else: we're on the latest chunk, even though it's sealed
            
            # Spin-wait for new data (no sleep - burn cycles for minimal latency)

    # ================================================================ DICT FORMAT

    def _range_dicts(self, start_us: int, end_us: Optional[int], playback: bool = False, ts_key: Optional[str] = None) -> List[dict]:
        """Read range as list of dicts (readable, slower)"""
        names = self._field_names
        # Dict comprehension faster than zip - use len(rec) not len(names) due to unpacking
        return [{names[i]: rec[i] for i in range(len(names))} for rec in self._range_tuples(start_us, end_us, playback=playback, ts_key=ts_key)]

    def _stream_dicts(self, start_us: Optional[int], ts_key: Optional[str] = None) -> Iterator[dict]:
        """Stream dicts"""
        names = self._field_names
        # Dict comprehension faster than zip
        for rec in self._stream_tuples(start_us, ts_key=ts_key):
            yield {names[i]: rec[i] for i in range(len(names))}

    # ================================================================ NUMPY FORMAT

    def _range_numpy(self, start_us: int, end_us: Optional[int], playback: bool = False, ts_key: Optional[str] = None):
        """Read range as single numpy structured array"""
        import numpy as np
        
        # Resolve which timestamp column to query on
        ts_offset, ts_byteorder = self._resolve_ts_key(ts_key)
        
        # If caller requests playback and index is available, start from latest snapshot before start_us
        if playback and self._index_available:
            snapshot_ts = self._get_snapshot_before(start_us)
            if snapshot_ts is not None and snapshot_ts < start_us:
                start_us = snapshot_ts
        
        # Collect all matching data as contiguous blocks
        blocks = []
        rec_size = self._rec_size
        ts_off = ts_offset
        ts_end = ts_off + 8
        from_bytes = int.from_bytes
        byteorder = ts_byteorder
        
        for chunk_id in self._iter_chunks_in_range(start_us, end_us, ts_key=ts_key):
            buf = self._chunk.buffer
            write_pos = self._chunk_meta.write_pos
            pos = self._binary_search_start(start_us, ts_offset, ts_byteorder)
            
            # Find end position
            end_pos = pos
            while end_pos + rec_size <= write_pos:
                ts = from_bytes(buf[end_pos + ts_off:end_pos + ts_end], byteorder)
                if ts < start_us:
                    end_pos += rec_size
                    pos = end_pos
                    continue
                if end_us is not None and ts > end_us:
                    break
                end_pos += rec_size
            
            if end_pos > pos:
                blocks.append(bytes(buf[pos:end_pos]))
        
        # Concatenate and convert to numpy
        if not blocks:
            return np.array([], dtype=self.dtype)
        
        combined = b''.join(blocks)
        return np.frombuffer(combined, dtype=self.dtype)

    def _stream_numpy(self, start_us: Optional[int], ts_key: Optional[str] = None) -> Iterator:
        """Stream numpy arrays (yields array per chunk)"""
        import numpy as np
        
        for rec in self._stream_tuples(start_us, ts_key=ts_key):
            # For streaming, yield individual records as 1-element arrays
            arr = np.array([rec], dtype=self.dtype)
            yield arr[0]

    # ================================================================ RAW FORMAT

    def _range_raw(self, start_us: int, end_us: Optional[int], playback: bool = False, ts_key: Optional[str] = None) -> memoryview:
        """Read range as single contiguous memoryview (zero-copy if single chunk)"""
        # Resolve which timestamp column to query on
        ts_offset, ts_byteorder = self._resolve_ts_key(ts_key)
        
        # If caller requests playback and index is available, start from latest snapshot before start_us
        if playback and self._index_available:
            snapshot_ts = self._get_snapshot_before(start_us)
            if snapshot_ts is not None and snapshot_ts < start_us:
                start_us = snapshot_ts
        
        blocks = []
        rec_size = self._rec_size
        ts_off = ts_offset
        ts_end = ts_off + 8
        from_bytes = int.from_bytes
        byteorder = ts_byteorder
        
        for chunk_id in self._iter_chunks_in_range(start_us, end_us, ts_key=ts_key):
            buf = self._chunk.buffer
            write_pos = self._chunk_meta.write_pos
            pos = self._binary_search_start(start_us, ts_offset, ts_byteorder)
            
            # Find end position
            end_pos = pos
            while end_pos + rec_size <= write_pos:
                ts = from_bytes(buf[end_pos + ts_off:end_pos + ts_end], byteorder)
                if ts < start_us:
                    end_pos += rec_size
                    pos = end_pos
                    continue
                if end_us is not None and ts > end_us:
                    break
                end_pos += rec_size
            
            if end_pos > pos:
                blocks.append(bytes(buf[pos:end_pos]))
        
        # Return contiguous memoryview
        if not blocks:
            return memoryview(b'')
        if len(blocks) == 1:
            return memoryview(blocks[0])
        return memoryview(b''.join(blocks))

    def _stream_raw(self, start_us: Optional[int], ts_key: Optional[str] = None) -> Iterator[memoryview]:
        """Stream raw memoryview slices (64 bytes each)"""
        rec_size = self._rec_size
        
        # Historical replay
        if start_us is not None:
            # Resolve ts_offset if ts_key provided
            ts_offset, ts_byteorder = self._resolve_ts_key(ts_key) if ts_key else (self._ts_offset, self._ts_byteorder)
            for chunk_id in self._iter_chunks_in_range(start_us, None, ts_key=ts_key):
                buf = self._chunk.buffer
                write_pos = self._chunk_meta.write_pos
                pos = self._binary_search_start(start_us, ts_offset, ts_byteorder)
                
                while pos + rec_size <= write_pos:
                    yield buf[pos:pos + rec_size]
                    pos += rec_size
        
        # Live streaming
        self._ensure_latest_chunk()
        read_head = self._chunk_meta.write_pos
        
        while True:
            if self._chunk_meta is None:
                self._ensure_latest_chunk()
                read_head = 0
            
            buf = self._chunk.buffer
            write_pos = self._chunk_meta.write_pos
            
            while read_head + rec_size <= write_pos:
                yield buf[read_head:read_head + rec_size]
                read_head += rec_size
            
            if self._chunk_meta.status == ON_DISK:
                next_id = self._chunk_id + 1
                try:
                    if self.registry.get_chunk_metadata(next_id) is not None:
                        self._open_chunk(next_id)
                        read_head = 0
                        continue
                except FileNotFoundError:
                    # Next chunk not ready yet, wait
                    pass
            
            time.sleep(0.0001)

    # ================================================================ PUBLIC API

    def stream(self, start: Optional[int] = None, format: str = 'tuple', ts_key: Optional[str] = None) -> Iterator:
        """
        Stream records (infinite iterator, live updates).
        
        Args:
            start: Start timestamp in microseconds
                - None: Jump to current write head (skip history)
                - timestamp_us: Stream from that point (historical + live)
            format: 'tuple' (default, fast), 'dict' (readable), 'numpy' (batch), 'raw' (memoryview)
            ts_key: Which timestamp column to query on (None = primary ts_col, or specify 'recv_us', 'proc_us', 'ev_us', etc.)
        
        Returns:
            Iterator yielding records in specified format (never ends)
        
        Behavior:
            start=None → Live only (skips all existing data)
                Best for: Real-time trading, live monitoring
            
            start=timestamp_us → Historical replay + live
                Best for: Resume from checkpoint, replay strategies
        
        Example:
            # Live trading
            >>> for trade in reader.stream():  # start=None
            ...     price, size, timestamp = trade
            ...     if price > 100:
            ...         execute_order()
            
            # Resume from checkpoint
            >>> last_ts = 1738368000000000
            >>> for trade in reader.stream(start=last_ts):
            ...     process(trade)
            
            # Query by exchange event time (backtesting)
            >>> for update in reader.stream(start=ev_time, ts_key='ev_us'):
            ...     orderbook.apply(update)
            
            # Dictionary format (readable)
            >>> for trade in reader.stream(format='dict'):
            ...     print(f"Price: {trade['price']}, Size: {trade['size']}")
        
        Notes:
            - Infinite loop (Ctrl+C or break to exit)
            - Spin-waits for new data (100% CPU core usage)
            - Use range() for batch analysis (more efficient)
        """
        if format == 'tuple':
            return self._stream_tuples(start, ts_key=ts_key)
        elif format == 'dict':
            return self._stream_dicts(start, ts_key=ts_key)
        elif format == 'numpy':
            return self._stream_numpy(start, ts_key=ts_key)
        elif format == 'raw':
            return self._stream_raw(start, ts_key=ts_key)
        else:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")

    def range(self, start: int, end: int, format: str = 'tuple', playback: bool = False, ts_key: Optional[str] = None) -> Union[List, memoryview]:
        """
        Read historical time range (finite block).
        
        Args:
            start: Start timestamp in microseconds (inclusive)
            end: End timestamp in microseconds (exclusive)
            format: 'tuple' (default, fast), 'dict' (readable), 'numpy' (vectorized), 'raw' (memoryview)
            playback: For indexed feeds (L2), start from latest snapshot before start (default False)
            ts_key: Which timestamp column to query on (None = primary ts_col, or specify 'recv_us', 'proc_us', 'ev_us', etc.)
        
        Returns:
            List of records (or numpy array if format='numpy', or memoryview if format='raw')
        
        Example:
            # Get last hour of trades
            >>> now_us = int(time.time() * 1e6)
            >>> hour_ago = now_us - 3600_000_000
            >>> trades = reader.range(hour_ago, now_us)
            >>> print(f'Last hour: {len(trades)} trades')
            
            # L2 orderbook with playback from snapshot (consistent state)
            >>> data = reader.range(start, end, playback=True)
            >>> # Starts from snapshot before start, includes all deltas
            
            # Vectorized analysis with numpy
            >>> data = reader.range(start, end, format='numpy')
            >>> avg_price = data['price'].mean()
            >>> print(f'Average price: {avg_price:.2f}')
            
            # Dictionary format (readable)
            >>> trades = reader.range(start, end, format='dict')
            >>> for trade in trades:
            ...     print(f"{trade['timestamp_us']}: ${trade['price']}")
        
        Notes:
            - Much faster than streaming for batch analysis
            - All data returned at once (not lazy)
            - Memory usage: num_records * record_size
            - Use latest(seconds) for convenience
            - playback=True only works if feed has index_playback enabled
        """
        if format == 'tuple':
            return self._range_tuples(start, end, playback=playback, ts_key=ts_key)
        elif format == 'dict':
            return self._range_dicts(start, end, playback=playback, ts_key=ts_key)
        elif format == 'numpy':
            return self._range_numpy(start, end, playback=playback, ts_key=ts_key)
        elif format == 'raw':
            return self._range_raw(start, end, playback=playback, ts_key=ts_key)
        else:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")

    def latest(self, seconds: float = 60.0, format: str = 'tuple', ts_key: Optional[str] = None) -> Union[List, memoryview]:
        """
        Get recent records (rolling time window).
        
        Args:
            seconds: Duration to look back (default 60 seconds)
            format: 'tuple' (default), 'dict', 'numpy', or 'raw'
            ts_key: Which timestamp column to query on (None = primary ts_col)
        
        Returns:
            List of records (tuple/dict), numpy array, or memoryview
        
        Example:
            # Last minute
            >>> recent = reader.latest(60)
            >>> print(f'Last minute: {len(recent)} trades')
            
            # Last hour as numpy
            >>> data = reader.latest(3600, format='numpy')
            >>> volume = data['size'].sum()
            >>> print(f'Hourly volume: {volume:.0f}')
            
            # Check market activity
            >>> if len(reader.latest(10)) > 0:
            ...     print('Market is active')
        
        Notes:
            - Equivalent to: range(now - seconds*1e6, now)
            - For live data, use stream() instead
        """
        now_us = self._get_current_timestamp_us()
        start_us = now_us - int(seconds * 1_000_000)
        return self.range(start_us, now_us, format=format, ts_key=ts_key)

    def read_available(self, max_records: Optional[int] = None, format: str = 'tuple') -> List:
        """
        Read available records without blocking (returns immediately).
        
        Args:
            max_records: Maximum records to return (None = all available)
            format: 'tuple' (default), 'dict', 'numpy', or 'raw'
        
        Returns:
            List of available records (empty list if no new data)
        
        Behavior:
            - First call: Initializes read head to current write position
            - Subsequent calls: Returns new records since last call
            - Never blocks - returns empty list if no new data
            - Perfect for "consume up to N per cycle" patterns
        
        Example:
            # Event loop pattern (no blocking!)
            >>> reader = platform.create_reader('trades')
            >>> while True:
            ...     # Read up to 10 records, returns immediately
            ...     records = reader.read_available(max_records=10)
            ...     for rec in records:
            ...         price, size, ts = rec
            ...         process(rec)
            ...     
            ...     # Do other work without blocking
            ...     do_background_tasks()
            ...     time.sleep(0.001)  # Control loop frequency
            
            # Async pattern
            >>> async def consume():
            ...     while True:
            ...         records = reader.read_available(max_records=100)
            ...         if records:
            ...             await process_batch(records)
            ...         await asyncio.sleep(0.001)
            
            # Dictionary format
            >>> records = reader.read_available(max_records=5, format='dict')
            >>> for rec in records:
            ...     print(f"Price: {rec['price']}, Size: {rec['size']}")
        
        Notes:
            - Non-blocking: Perfect for event loops, async code
            - Stateful: Maintains position between calls (use separate readers per consumer)
            - Use stream() if you want blocking/spin-wait behavior
            - Returns empty list if no data (check length before processing)
        """
        # Initialize read head on first call
        if self._read_head is None:
            self._ensure_latest_chunk()
            self._read_head = self._chunk_meta.write_pos
        
        # Ensure we're on the latest chunk
        if self._chunk_meta is None:
            self._ensure_latest_chunk()
            self._read_head = self._chunk_meta.write_pos
        
        # Check for chunk rotation
        latest_chunk_id = self.registry.get_latest_chunk_idx()
        if self._chunk_id < latest_chunk_id:
            # Move to newer chunk
            self._open_chunk(latest_chunk_id)
            self._read_head = 0
        
        # Read available records (non-blocking)
        result = []
        buf = self._chunk.buffer
        write_pos = self._chunk_meta.write_pos
        rec_size = self._rec_size
        
        while self._read_head + rec_size <= write_pos:
            if format == 'tuple':
                result.append(self._unpack(buf, self._read_head))
            elif format == 'dict':
                rec = self._unpack(buf, self._read_head)
                result.append({self._field_names[i]: rec[i] for i in range(len(self._field_names))})
            elif format == 'raw':
                result.append(buf[self._read_head:self._read_head + rec_size])
            else:
                raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', or 'raw'")
            
            self._read_head += rec_size
            
            if max_records and len(result) >= max_records:
                break
        
        return result

    # ================================================================ LEGACY API (for backward compatibility)
    
    def read_all(self) -> Iterator[tuple]:
        """Legacy: use stream(start=0) instead"""
        # For finite historical read, don't use live streaming
        unpack = self._unpack
        rec_size = self._rec_size
        
        for chunk_id in self.registry.get_chunks_after(0):
            try:
                self._open_chunk(chunk_id)
                buf = self._chunk.buffer
                write_pos = self._chunk_meta.write_pos
                
                # Optimized while loop
                pos = 0
                limit = write_pos - rec_size
                while pos <= limit:
                    yield unpack(buf, pos)
                    pos += rec_size
            except FileNotFoundError:
                continue
    
    def read_range(self, start_us: int, end_us: Optional[int] = None) -> Iterator[tuple]:
        """Legacy: use range() instead (returns list, not iterator)"""
        for rec in self.range(start_us, end_us, format='tuple'):
            yield rec
    
    def get_latest(self, n: int = 1) -> list:
        """Legacy: returns last N records (not time-based like new latest())"""
        self._ensure_latest_chunk()
        write_pos = self._chunk_meta.write_pos
        num_in_chunk = write_pos // self._rec_size
        
        if n <= num_in_chunk:
            buf = self._chunk.buffer
            unpack = self._unpack
            rec_size = self._rec_size
            start = max(0, write_pos - n * rec_size)
            result = []
            for pos in range(start, write_pos, rec_size):
                result.append(unpack(buf, pos))
            return result
        
        return []
    
    def stream_live(self, playback: bool = False) -> Iterator[tuple]:
        """
        Legacy: use stream() instead.
        
        For feeds with index_playback enabled (L2 orderbooks):
            playback=True → Start from latest snapshot marker in index
            playback=False → Live tail only (skip history)
        
        For feeds without index_playback:
            playback=True → Read from beginning (start=0)
            playback=False → Live tail only
        """
        if playback and self._index_available:
            # For indexed feeds (L2): Start from latest snapshot
            start = self._get_latest_snapshot_timestamp()
        elif playback:
            # For non-indexed feeds: Start from beginning
            start = 0
        else:
            # Live tail only
            start = None
        
        return self.stream(start=start, format='tuple')
    
    def _get_latest_snapshot_timestamp(self) -> int:
        """Get timestamp of latest snapshot from index (for playback)"""
        if not self._index_available:
            return 0
        
        # Find the latest chunk with an index
        latest_chunk_id = self.registry.get_latest_chunk_idx()
        if latest_chunk_id is None:
            return 0
        
        # Search backward from latest chunk to find one with index entries
        for chunk_id in range(latest_chunk_id, -1, -1):
            idx_path = self.data_dir / f"chunk_{chunk_id:08d}.idx"
            if not idx_path.exists():
                continue
            
            try:
                from .index import ChunkIndex
                idx = ChunkIndex.open_file(str(idx_path))
                latest_index = idx.get_latest_index()
                
                if latest_index is not None:
                    timestamp = latest_index.timestamp
                    latest_index.release()
                    idx.close_file()
                    return timestamp
                
                idx.close_file()
            except Exception:
                continue
        
        # No index found, start from beginning
        return 0
    
    def _get_snapshot_before(self, target_us: int) -> Optional[int]:
        """Get timestamp of latest snapshot at or before target_us (for range queries)"""
        if not self._index_available:
            return None
        
        # Find chunks that could contain snapshots before target_us
        latest_chunk_id = self.registry.get_latest_chunk_idx()
        if latest_chunk_id is None:
            return None
        
        # Search backward from latest chunk to find snapshot before target
        best_snapshot_ts = None
        
        for chunk_id in range(latest_chunk_id, -1, -1):
            idx_path = self.data_dir / f"chunk_{chunk_id:08d}.idx"
            if not idx_path.exists():
                continue
            
            try:
                from .index import ChunkIndex
                idx = ChunkIndex.open_file(str(idx_path))
                
                # Search for latest index entry before target_us
                for i in range(idx.count - 1, -1, -1):
                    index_rec = idx.get_index(i)
                    ts = index_rec.timestamp
                    index_rec.release()
                    
                    if ts <= target_us:
                        # Found a snapshot at or before target
                        if best_snapshot_ts is None or ts > best_snapshot_ts:
                            best_snapshot_ts = ts
                        idx.close_file()
                        break
                else:
                    idx.close_file()
                    continue
                
                # If we found a snapshot, we can stop searching
                if best_snapshot_ts is not None:
                    break
                    
            except Exception:
                continue
        
        return best_snapshot_ts
    
    def as_dict(self, record: tuple) -> dict:
        """Legacy: convert tuple to dict"""
        return {self._field_names[i]: record[i] for i in range(len(self._field_names))}
    
    def stream_raw(self) -> Iterator[memoryview]:
        """Legacy: use stream(format='raw') instead"""
        return self.stream(format='raw')

    def close(self) -> None:
        """Release resources"""
        self._close_chunk()
        if self.registry:
            self.registry.close()
