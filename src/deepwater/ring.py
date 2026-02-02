import struct
import time
import atexit
from multiprocessing import shared_memory
from typing import Optional, Iterator, Union, List

from .utils.process import ProcessUtils


# Global registry of ring buffers to clean up at exit
_RING_BUFFERS_TO_CLEANUP = []


def _cleanup_ring_buffers():
    """Clean up any remaining ring buffers at process exit"""
    for ring_name in _RING_BUFFERS_TO_CLEANUP:
        try:
            shm = shared_memory.SharedMemory(name=ring_name)
            shm.close()
            shm.unlink()
        except FileNotFoundError:
            pass  # Already cleaned up
        except Exception:
            pass  # Best effort


atexit.register(_cleanup_ring_buffers)


class RingBuffer:
    """
    Shared-memory ring buffer for fixed-size records.
    Layout: header (write_pos, start_pos, generation, last_ts, record_count) followed by data region.
    - write_pos: where next record will be written
    - start_pos: where oldest valid record begins
    - generation: wrap count (for debugging)
    - record_count: total records written
    """

    _HEADER = struct.Struct("<QQQqq")  # write_pos, start_pos, generation, last_ts, record_count

    def __init__(self, name: str, data_size: int, create: bool = False):
        if data_size <= 0:
            raise ValueError("data_size must be > 0")
        # align to page to reduce fragmentation
        if data_size % 4096 != 0:
            data_size = (data_size + 4095) & ~4095
        self.name = name
        self.data_size = data_size
        self.total_size = self._HEADER.size + data_size
        try:
            self.shm = shared_memory.SharedMemory(name=name, create=create, size=self.total_size)
            self.created = create
        except FileExistsError:
            self.shm = shared_memory.SharedMemory(name=name, create=False)
            self.created = False
        self.buf = self.shm.buf
        self.data = self.buf[self._HEADER.size:]
        if self.created:
            self._HEADER.pack_into(self.buf, 0, 0, 0, 0, 0, 0)

    def header(self) -> tuple[int, int, int, int, int]:
        """Returns (write_pos, start_pos, generation, last_ts, record_count)"""
        return self._HEADER.unpack_from(self.buf, 0)

    def update_header(self, write_pos: int, start_pos: int, generation: int, last_ts: int, record_count: int) -> None:
        self._HEADER.pack_into(self.buf, 0, write_pos, start_pos, generation, last_ts, record_count)

    def close(self, unlink: bool = False) -> None:
        try:
            self.data.release()
        except Exception:
            pass
        try:
            self.buf.release()
        except Exception:
            pass
        self.shm.close()
        if unlink:
            try:
                self.shm.unlink()
            except FileNotFoundError:
                pass


class RingWriter:
    """Single-writer for live-only feeds backed by a shared-memory ring."""

    def __init__(self, platform, feed_name: str):
        self.platform = platform
        self.feed_name = feed_name
        self.feed_config = platform.lifecycle(feed_name)
        self.record_format = platform.get_record_format(feed_name)
        self.my_pid = ProcessUtils.get_current_pid()

        self._S = struct.Struct(self.record_format["fmt"])
        self._rec_size = self._S.size
        value_fields = [f.get("name") for f in self.record_format["fields"] if f.get("name") != "_"]
        ts_name = self.record_format.get("ts_name")
        try:
            self._ts_idx = value_fields.index(ts_name) if ts_name else None
        except ValueError:
            self._ts_idx = None

        self.ring_bytes = int(self.feed_config.get("chunk_size_bytes", 8 * 1024 * 1024))
        if self.ring_bytes < self._rec_size:
            raise ValueError(f"ring size {self.ring_bytes} too small for record size {self._rec_size}")

        # Shared memory name is the feed name (no "-ring" suffix needed)
        self.ring = RingBuffer(self.feed_name, data_size=self.ring_bytes, create=True)
        
        # Register for cleanup at exit (writer owns the shared memory)
        if self.feed_name not in _RING_BUFFERS_TO_CLEANUP:
            _RING_BUFFERS_TO_CLEANUP.append(self.feed_name)

    def _write_bytes(self, payload: bytes, last_ts: int) -> int:
        if len(payload) != self._rec_size:
            raise ValueError(f"record length {len(payload)} != expected {self._rec_size}")
        hdr = self.ring.header()
        write_pos, start_pos, generation, _, record_count = hdr
        
        # Check if we need to wrap
        if write_pos + self._rec_size > self.ring_bytes:
            write_pos = 0
            generation += 1
        
        # Calculate start_pos BEFORE writing (oldest = next to be overwritten)
        ring_capacity = self.ring_bytes // self._rec_size
        if record_count >= ring_capacity:
            # Ring is full or will be full after this write
            # Oldest data is at current write position (about to be overwritten)
            start_pos = write_pos
        # else: ring not yet full, start_pos stays at 0
        
        # Write the record
        self.ring.data[write_pos:write_pos + self._rec_size] = payload
        
        # Update positions
        write_pos += self._rec_size
        record_count += 1
        
        self.ring.update_header(write_pos, start_pos, generation, last_ts, record_count)
        return write_pos

    def write(self, timestamp: int, record_data: bytes, create_index: bool = False) -> int:
        """Byte-oriented write; create_index is ignored for rings."""
        _ = create_index
        return self._write_bytes(record_data, last_ts=timestamp)

    def write_values(self, *vals) -> int:
        """Pack values into the ring; overwrites oldest data when full."""
        last_ts = vals[self._ts_idx] if self._ts_idx is not None else 0
        buf = bytearray(self._rec_size)
        self._S.pack_into(buf, 0, *vals)
        return self._write_bytes(bytes(buf), last_ts=last_ts)

    def write_tuple(self, record: tuple) -> int:
        """Write a tuple record (same as write_values but accepts tuple)."""
        return self.write_values(*record)

    def write_dict(self, record: dict) -> int:
        """Write a dictionary record (converts to tuple based on field order)."""
        field_names = [f['name'] for f in self.record_format['fields'] if f.get('name') != '_']
        vals = tuple(record[name] for name in field_names)
        return self.write_values(*vals)

    def write_batch_bytes(self, data: bytes) -> int:
        """Write a blob of packed records (len must be multiple of record_size) with one header update."""
        rec_sz = self._rec_size
        n = len(data)
        if n == 0 or n % rec_sz != 0:
            raise ValueError("batch length must be a multiple of record_size")
        write_pos, generation, _ = self.ring.header()
        # If batch won't fit, wrap and overwrite oldest
        if write_pos + n > self.ring_bytes:
            write_pos = 0
            generation += 1
            if n > self.ring_bytes:
                raise ValueError("batch larger than ring capacity")
        end = write_pos + n
        self.ring.data[write_pos:end] = data
        # last ts is unknown here; keep previous
        self.ring.update_header(end, generation, 0)
        return end

    def resize(self, new_size_bytes: int):
        """Recreate the ring with a new size."""
        if new_size_bytes <= 0:
            raise ValueError("new ring size must be >0")
        new_size_bytes = int(new_size_bytes)
        old = self.ring
        try:
            old.close(unlink=True)
        except Exception:
            pass
        self.feed_config["chunk_size_bytes"] = new_size_bytes
        try:
            self.platform.registry.update_metadata(self.feed_name, chunk_size_bytes=new_size_bytes)
        except Exception:
            pass
        self.ring_bytes = new_size_bytes
        self.ring = RingBuffer(f"{self.feed_name}-ring", data_size=new_size_bytes, create=True)

    def close(self):
        """Close ring buffer (don't unlink - let OS clean up when all processes exit)."""
        try:
            # Don't unlink! Readers might still be attached. Let OS clean up.
            self.ring.close(unlink=False)
        except Exception:
            pass


class RingReader:
    """Reader that tails a shared-memory ring without chunk/index files."""

    def __init__(self, platform, feed_name: str):
        self.platform = platform
        self.feed_name = feed_name
        self.record_format = platform.get_record_format(feed_name)
        self._S = struct.Struct(self.record_format["fmt"])
        self._rec_size = self._S.size
        self.ring_bytes = int(platform.lifecycle(feed_name).get("chunk_size_bytes", 8 * 1024 * 1024))
        # Shared memory name is the feed name (no "-ring" suffix needed)
        self.ring = RingBuffer(self.feed_name, data_size=self.ring_bytes, create=False)
        
        # Start from oldest valid data (start_pos in header)
        write_pos, start_pos, generation, _, record_count = self.ring.header()
        self.read_pos = start_pos
        
        # Calculate how many records we've "read" already (skipped to start_pos)
        ring_capacity = self.ring_bytes // self._rec_size
        if record_count <= ring_capacity:
            self.records_read = 0  # Ring not full, read from beginning
        else:
            self.records_read = record_count - ring_capacity  # Ring full, skip to last N records
        
        # Cache field names for dict format
        self._field_names = tuple(f['name'] for f in self.record_format.get('fields', []) 
                                 if not f.get('type', '').startswith('_') or f['name'] != '_')

    # ================================================================ PUBLIC API (matches Reader)
    
    @property
    def format(self) -> str:
        """Struct format string"""
        return self.record_format["fmt"]
    
    @property
    def field_names(self) -> tuple:
        """Field names in order"""
        return self._field_names

    def stream(self, start: Optional[int] = None, format: str = 'tuple'):
        """
        Stream records (infinite iterator, live updates).
        
        Args:
            start: Start timestamp in microseconds (rings only support None)
                - None: Stream from current position in ring buffer
                - timestamp_us: Not supported (raises ValueError)
            format: 'tuple' (default), 'dict', 'numpy', or 'raw'
        
        Returns:
            Iterator yielding records in specified format (never ends)
        
        Notes:
            - Ring buffers are live-only (no historical range support)
            - Use range() or latest() on disk feeds for historical queries
        """
        if start is not None:
            raise ValueError("Ring feeds don't support historical start times (start must be None)")
        
        if format == 'tuple':
            yield from self.stream_live()
        elif format == 'dict':
            for rec in self.stream_live():
                yield {self._field_names[i]: rec[i] for i in range(len(self._field_names))}
        elif format == 'numpy':
            raise NotImplementedError("Ring feeds don't support numpy format (use 'tuple', 'dict', or 'raw')")
        elif format == 'raw':
            yield from self.stream_latest_records_raw()
        else:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")

    def range(self, start: int, end: int, format: str = 'tuple'):
        """
        Read time range from current ring buffer contents.
        
        Performance:
            - Fast in-memory scan (no disk I/O)
            - Only searches data currently in ring buffer
            - May return partial results if data has wrapped
        
        Args:
            start: Start timestamp in microseconds (inclusive)
            end: End timestamp in microseconds (exclusive)
            format: 'tuple' (default), 'dict', 'numpy', or 'raw'
        
        Returns:
            List of records matching time range (or memoryview if format='raw')
        
        Notes:
            - Ring buffers only contain recent data (last N records based on buffer size)
            - If requested range is older than buffer, returns empty list
            - For full historical queries, use disk-based feed
        
        Example:
            # Get last 5 seconds from ring
            >>> now_us = int(time.time() * 1e6)
            >>> five_sec_ago = now_us - 5_000_000
            >>> recent = ring_reader.range(five_sec_ago, now_us)
            >>> print(f'Last 5s: {len(recent)} records')
        """
        # Get current ring state
        write_pos, start_pos, _, last_ts, record_count = self.ring.header()
        
        if record_count == 0:
            return [] if format != 'raw' else memoryview(b'')
        
        # Scan all records currently in buffer
        results = []
        data = self.ring.data
        rec_sz = self._rec_size
        ring_sz = self.ring_bytes
        ring_capacity = ring_sz // rec_sz
        
        # Get timestamp offset for filtering
        ts_offset = self.record_format.get('ts_offset', 0)
        ts_fmt = '<Q'  # Assume uint64 timestamp
        
        # Determine how many records to scan (min of record_count and ring_capacity)
        num_records = min(record_count, ring_capacity)
        read_pos = start_pos
        
        for _ in range(num_records):
            if read_pos >= ring_sz:
                read_pos = 0
            
            # Extract timestamp to check range
            ts = struct.unpack_from(ts_fmt, data, read_pos + ts_offset)[0]
            
            if start <= ts < end:
                record = self._S.unpack_from(data, read_pos)
                results.append(record)
            
            read_pos += rec_sz
        
        # Convert to requested format
        if format == 'tuple':
            return results
        elif format == 'dict':
            return [{self._field_names[i]: rec[i] for i in range(len(self._field_names))} for rec in results]
        elif format == 'numpy':
            raise NotImplementedError("Ring feeds don't support numpy format yet")
        elif format == 'raw':
            # Return raw bytes for matching records
            if not results:
                return memoryview(b'')
            raw_data = bytearray(len(results) * rec_sz)
            for i, rec in enumerate(results):
                self._S.pack_into(raw_data, i * rec_sz, *rec)
            return memoryview(raw_data)
        else:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")

    def latest(self, seconds: float = 60.0, format: str = 'tuple'):
        """
        Get recent records from ring buffer (rolling time window).
        
        Performance:
            - Fast in-memory scan (no disk I/O)
            - Only searches data currently in ring buffer
        
        Args:
            seconds: Duration to look back (default 60 seconds)
            format: 'tuple' (default), 'dict', 'numpy', or 'raw'
        
        Returns:
            List of records from last N seconds (or memoryview if format='raw')
        
        Notes:
            - Ring buffer size determines maximum lookback
            - If buffer is smaller than requested duration, returns all available data
            - For guaranteed historical depth, use disk-based feed
        
        Example:
            # Last minute from ring
            >>> recent = ring_reader.latest(60)
            >>> print(f'Last minute: {len(recent)} records')
            
            # Check for any data in last 10 seconds
            >>> if len(ring_reader.latest(10)) > 0:
            ...     print('Ring has recent activity')
        """
        # Get last timestamp from ring header
        write_pos, start_pos, _, last_ts, record_count = self.ring.header()
        
        if record_count == 0 or last_ts == 0:
            return [] if format != 'raw' else memoryview(b'')
        
        # Calculate start time
        start_us = last_ts - int(seconds * 1_000_000)
        
        return self.range(start_us, last_ts + 1, format=format)

    # ================================================================ RING-SPECIFIC API

    def stream_live(self):
        """Yield records as they appear; skips data if the reader falls behind."""
        data = self.ring.data
        rec_sz = self._rec_size
        ring_sz = self.ring_bytes
        read_pos = self.read_pos
        records_read = self.records_read

        while True:
            write_pos, start_pos, _, _, record_count = self.ring.header()

            # Check if we're caught up
            if records_read >= record_count:
                time.sleep(0.00001)  # 10µs sleep instead of 500µs
                continue

            # Check if writer lapped us
            ring_capacity = ring_sz // rec_sz
            if record_count - records_read > ring_capacity:
                # We fell behind, skip to start_pos
                read_pos = start_pos
                records_read = record_count - ring_capacity
                time.sleep(0.00001)
                continue

            # Read next record
            # Handle circular buffer: if read_pos >= ring_sz, wrap to 0
            if read_pos >= ring_sz:
                read_pos = 0
            
            # If we're at write_pos, we're caught up (even though records_read < record_count means writer is mid-write)
            if read_pos == write_pos:
                time.sleep(0.00001)  # 10µs sleep
                continue
            
            # Read the record
            record = self._S.unpack_from(data, read_pos)
            read_pos += rec_sz
            records_read += 1
            yield record
        
        # Save position
        self.read_pos = read_pos
        self.records_read = records_read

    def stream_latest_records(self, playback: bool = False):
        """Compatibility shim to mirror chunked Reader API."""
        _ = playback  # playback is not supported on rings
        yield from self.stream_live()

    def stream_latest_records_raw(self):
        """Yield raw memoryviews from the ring."""
        data = self.ring.data
        rec_sz = self._rec_size
        ring_sz = self.ring_bytes
        read_pos = self.read_pos
        read_gen = self.read_gen

        while True:
            write_pos, write_gen, _ = self.ring.header()
            if write_gen > read_gen + 1 or (write_gen > read_gen and write_pos >= read_pos):
                read_gen = write_gen
                read_pos = write_pos

            if read_gen == write_gen:
                if read_pos < write_pos:
                    mv = memoryview(data)[read_pos:read_pos+rec_sz]
                    read_pos += rec_sz
                    if read_pos >= ring_sz:
                        read_pos = 0
                        read_gen += 1
                    yield mv
                    continue
            else:
                if read_pos + rec_sz <= ring_sz:
                    mv = memoryview(data)[read_pos:read_pos+rec_sz]
                    read_pos += rec_sz
                    if read_pos >= ring_sz:
                        read_pos = 0
                        read_gen = write_gen
                    yield mv
                    continue
            time.sleep(0.0005)

    def read_batch(self, count: int):
        """Yield batches of packed records."""
        batch = []
        for rec in self.stream_latest_records(playback=False):
            batch.append(rec)
            if len(batch) >= count:
                yield batch
                batch = []

    def read_batch_raw(self, count: int):
        """Yield batches of raw memoryviews."""
        batch = []
        for rec in self.stream_latest_records_raw():
            batch.append(rec)
            if len(batch) >= count:
                yield batch
                batch = []

    def stream_time_range(self, start_time: int, end_time: Optional[int] = None):
        """Rings are live-only; time windows are unsupported."""
        raise NotImplementedError("Ring feeds are live-only; time windows unavailable.")

    def get_latest_record(self) -> tuple:
        """Return the most recent record currently written."""
        write_pos, write_gen, _ = self.ring.header()
        if write_pos == 0 and write_gen == 0:
            raise RuntimeError("No records written yet.")
        if write_pos == 0:
            pos = self.ring_bytes - self._rec_size
        else:
            pos = max(0, write_pos - self._rec_size)
        return self._S.unpack_from(self.ring.data, pos)

    def close(self):
        """Close ring buffer without unlinking (reader doesn't own it)."""
        try:
            self.ring.close(unlink=False)  # Reader doesn't own, don't unlink
        except Exception:
            pass
