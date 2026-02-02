import time
import struct
from typing import Optional, Iterator

from .chunk import Chunk
from .index import ChunkIndex
from .feed_registry import FeedRegistry, IN_MEMORY, ON_DISK, EXPIRED
from .utils.process import ProcessUtils


class Reader:
    """
    Zero-copy reader for persistent disk-based feeds.
    
    Optimized for speed - minimal allocations, direct buffer access.
    """
    __slots__ = ('platform', 'feed_name', 'feed_config', 'record_format', 
                 'data_dir', 'registry', '_chunk', '_chunk_meta', '_chunk_id',
                 '_unpack', '_rec_size', '_index_enabled', '_ts_offset', 
                 '_ts_byteorder', '_field_names')

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
        self._index_enabled = bool(self.feed_config.get("index_playback"))
        self._ts_offset = self.record_format.get("ts_offset") or self.record_format.get("ts", {}).get("offset")
        self._ts_byteorder = "little" if self.record_format.get("ts_endian", "<") == "<" else "big"
        self._field_names = tuple(f["name"] for f in self.record_format.get("fields", []))

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

    # ================================================================ PUBLIC API
    
    def get_latest(self, n: int = 1) -> list:
        """
        Get last N records as tuples (FAST - no dict overhead).
        
        Returns list of tuples in chronological order.
        Use field_names property to map tuple positions.
        """
        self._ensure_latest_chunk()
        write_pos = self._chunk_meta.write_pos
        num_in_chunk = write_pos // self._rec_size
        
        # Fast path: all in current chunk
        if n <= num_in_chunk:
            buf = self._chunk.buffer
            unpack = self._unpack
            rec_size = self._rec_size
            start_pos = write_pos - (n * rec_size)
            return [unpack(buf, start_pos + i * rec_size) for i in range(n)]
        
        # Slow path: cross chunks
        results = []
        remaining = n
        chunk_id = self._chunk_id
        
        while chunk_id > 0 and remaining > 0:
            self._open_chunk(chunk_id)
            write_pos = self._chunk_meta.write_pos
            num_in_chunk = write_pos // self._rec_size
            take = min(remaining, num_in_chunk)
            
            buf = self._chunk.buffer
            unpack = self._unpack
            rec_size = self._rec_size
            start_pos = write_pos - (take * rec_size)
            chunk_records = [unpack(buf, start_pos + i * rec_size) for i in range(take)]
            
            results = chunk_records + results
            remaining -= take
            chunk_id -= 1
        
        return results

    def read_all(self) -> Iterator[tuple]:
        """
        Read all historical records as tuples (FINITE, FAST).
        
        Zero dict overhead - pure tuples from mmap buffer.
        """
        latest_chunk = self.registry.get_latest_chunk_idx()
        if latest_chunk is None:
            return
        
        unpack = self._unpack
        rec_size = self._rec_size
        
        for chunk_id in range(1, latest_chunk + 1):
            try:
                self._open_chunk(chunk_id)
                buf = self._chunk.buffer
                write_pos = self._chunk_meta.write_pos
                pos = 0
                
                # Tight loop - minimal overhead
                while pos + rec_size <= write_pos:
                    yield unpack(buf, pos)
                    pos += rec_size
            except FileNotFoundError:
                continue

    def read_range(self, start_us: int, end_us: Optional[int] = None) -> Iterator[tuple]:
        """
        Read time range as tuples (FINITE, FAST, INDEXED).
        
        Uses chunk index when available for fast seeks.
        """
        if self._ts_offset is None:
            raise RuntimeError("Feed missing ts_offset for time queries")
        
        if self.registry.get_latest_chunk_idx() is None:
            return
        
        chunk_iter = (self.registry.get_chunks_after(start_us) if end_us is None 
                     else self.registry.get_chunks_in_range(start_us, end_us))
        
        for chunk_id in chunk_iter:
            self._open_chunk(chunk_id)
            if self._chunk_meta.num_records == 0:
                continue
            yield from self._read_chunk_range(start_us, end_us)

    def _read_chunk_range(self, start_us: int, end_us: Optional[int]) -> Iterator[tuple]:
        """Hot path - binary search + scan for sorted timestamps."""
        buf = self._chunk.buffer
        write_pos = self._chunk_meta.write_pos
        unpack = self._unpack
        rec_size = self._rec_size
        ts_off = self._ts_offset
        ts_end = ts_off + 8
        from_bytes = int.from_bytes
        byteorder = self._ts_byteorder
        
        # Binary search to find start position (assumes sorted timestamps)
        left = 0
        right = write_pos // rec_size
        start_idx = 0
        
        while left < right:
            mid = (left + right) // 2
            mid_pos = mid * rec_size
            ts = from_bytes(buf[mid_pos + ts_off:mid_pos + ts_end], byteorder)
            
            if ts < start_us:
                left = mid + 1
            else:
                right = mid
        
        start_idx = left
        pos = start_idx * rec_size
        
        # Scan from found position, checking timestamps
        while pos + rec_size <= write_pos:
            ts = from_bytes(buf[pos + ts_off:pos + ts_end], byteorder)
            if ts < start_us:
                # Binary search can overshoot slightly, skip until we hit range
                pos += rec_size
                continue
            if end_us is not None and ts > end_us:
                break
            yield unpack(buf, pos)
            pos += rec_size

    def stream_live(self, playback: bool = False) -> Iterator[tuple]:
        """
        Stream live updates as tuples (INFINITE, FAST).
        
        Minimal overhead - no dict conversions, direct buffer reads.
        """
        # Find starting point
        if playback and self._index_enabled:
            chunk_id = self.registry.get_latest_chunk_idx()
            start_offset = 0
            while chunk_id and chunk_id > 0:
                idx_path = self.data_dir / f"chunk_{chunk_id:08d}.idx"
                if idx_path.exists():
                    idx = ChunkIndex.open_file(str(idx_path))
                    try:
                        rec = idx.get_latest_index()
                        if rec:
                            self._open_chunk(chunk_id)
                            read_head = rec.offset
                            rec.release()
                            break
                    finally:
                        idx.close_file()
                chunk_id -= 1
            else:
                self._ensure_latest_chunk()
                read_head = self._chunk_meta.write_pos
        else:
            self._ensure_latest_chunk()
            read_head = self._chunk_meta.write_pos
        
        # Stream loop - hyper-optimized
        unpack = self._unpack
        rec_size = self._rec_size
        
        while True:
            if self._chunk_meta is None:
                time.sleep(0.001)
                continue
            
            buf = self._chunk.buffer
            write_pos = self._chunk_meta.write_pos
            
            # Read available records
            while read_head + rec_size <= write_pos:
                yield unpack(buf, read_head)
                read_head += rec_size
            
            # Check for chunk rotation
            latest_available = self.registry.get_latest_chunk_idx()
            if latest_available and self._chunk_id is not None and latest_available > self._chunk_id:
                try:
                    self._open_chunk(self._chunk_id + 1)
                    read_head = 0
                    continue
                except FileNotFoundError:
                    try:
                        self._ensure_latest_chunk()
                        read_head = self._chunk_meta.write_pos
                    except FileNotFoundError:
                        raise
            
            time.sleep(0.0005)  # 500µs backoff

    def stream_raw(self, playback: bool = False) -> Iterator[memoryview]:
        """
        Stream live updates as memoryviews (INFINITE, ZERO-COPY).
        
        Fastest possible - no unpacking, direct buffer slices.
        Use for maximum throughput when you'll unpack later.
        """
        if playback and self._index_enabled:
            chunk_id = self.registry.get_latest_chunk_idx()
            while chunk_id and chunk_id > 0:
                idx_path = self.data_dir / f"chunk_{chunk_id:08d}.idx"
                if idx_path.exists():
                    idx = ChunkIndex.open_file(str(idx_path))
                    try:
                        rec = idx.get_latest_index()
                        if rec:
                            self._open_chunk(chunk_id)
                            read_head = rec.offset
                            rec.release()
                            break
                    finally:
                        idx.close_file()
                chunk_id -= 1
            else:
                self._ensure_latest_chunk()
                read_head = self._chunk_meta.write_pos
        else:
            self._ensure_latest_chunk()
            read_head = self._chunk_meta.write_pos
        
        rec_size = self._rec_size
        
        while True:
            if self._chunk_meta is None:
                time.sleep(0.001)
                continue
            
            buf = self._chunk.buffer
            write_pos = self._chunk_meta.write_pos
            
            while read_head + rec_size <= write_pos:
                yield memoryview(buf)[read_head:read_head + rec_size]
                read_head += rec_size
            
            latest_available = self.registry.get_latest_chunk_idx()
            if latest_available and self._chunk_id is not None and latest_available > self._chunk_id:
                try:
                    self._open_chunk(self._chunk_id + 1)
                    read_head = 0
                    continue
                except FileNotFoundError:
                    try:
                        self._ensure_latest_chunk()
                        read_head = self._chunk_meta.write_pos
                    except FileNotFoundError:
                        raise
            
            time.sleep(0.0005)

    @property
    def field_names(self) -> tuple:
        """Field names for mapping tuple positions to field names."""
        return self._field_names

    def as_dict(self, record: tuple) -> dict:
        """Convert tuple record to dict (use sparingly - dicts are slow)."""
        return dict(zip(self._field_names, record))

    def close(self):
        """Release all resources."""
        self._close_chunk()
        self.registry.close()
