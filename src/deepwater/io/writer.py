import time
import struct
import logging
import os
import threading
import numpy as np

from collections import deque
from dataclasses import dataclass
from typing import Union
from pathlib import Path

from .chunk import Chunk
from .index import ChunkIndex
from ..metadata.feed_registry import FeedRegistry, IN_MEMORY, ON_DISK, EXPIRED, CHUNK_QMIN_OFFSET, MAX_QUERY_KEYS, UINT64_MAX
from ..metadata.segments import SegmentStore
from ..utils.process import ProcessUtils

log = logging.getLogger("dw.writer")

_MAX_PENDING_SEALED_CHUNKS = 8


@dataclass(slots=True)
class _SealedChunk:
    chunk: Chunk
    chunk_index: ChunkIndex | None


@dataclass(slots=True)
class _PreparedChunk:
    chunk_id: int
    chunk_path: Path
    chunk: Chunk
    chunk_index: ChunkIndex | None
    index_path: Path | None


@dataclass(slots=True)
class _TimingAccum:
    count: int = 0
    total_ns: int = 0
    max_ns: int = 0

    def add(self, dt_ns: int) -> None:
        self.count += 1
        self.total_ns += dt_ns
        if dt_ns > self.max_ns:
            self.max_ns = dt_ns

    def status(self) -> dict[str, float | int]:
        avg_ns = (self.total_ns / self.count) if self.count else 0.0
        return {
            "count": self.count,
            "total_ms": self.total_ns / 1_000_000.0,
            "avg_us": avg_ns / 1_000.0,
            "max_ms": self.max_ns / 1_000_000.0,
        }


class Writer:
    __slots__ = (
        "platform", "feed_name", "feed_config", "record_format", "my_pid",
        "data_dir", "registry",
        "current_chunk", "chunk_index", "current_chunk_metadata", "current_chunk_id",
        "_S", "_rec_size", "_clock_level", "_key_min_set", "_u64", "_qmins", "_qmaxs",
        "_index_enabled", "segment_store", "_segment_note_write", "_segment_note_batch",
        "_persist_cv", "_persist_queue", "_persist_inflight", "_persist_stop",
        "_persist_error", "_persist_thread", "_prepared_chunk",
        "persist_queue_high_water", "persist_enqueue_wait_count",
        "persist_enqueue_wait_ns_total", "persist_enqueue_wait_ns_max",
        "rotate_total_ns", "rotate_finalize_ns", "rotate_enqueue_ns",
        "rotate_prepare_wait_ns", "rotate_register_chunk_ns",
        "rotate_meta_open_ns", "rotate_index_create_ns",
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
        self._cleanup_unregistered_future_chunks()

        # Validate previous chunk before starting (handles corruption/missing files)
        if self.current_chunk_id > 0:
            self._validate_chunk()

        self._S = struct.Struct(self.record_format["fmt"])
        self._u64 = struct.Struct("<Q")
        self._rec_size = self._S.size
        self._clock_level = self.feed_config.get("clock_level")
        self._index_enabled = bool(self.feed_config.get("index_playback"))
        self.segment_store = SegmentStore(self.data_dir, self.feed_name)
        self._segment_note_write = self.segment_store.note_write
        self._segment_note_batch = self.segment_store.note_batch
        if self.segment_store.has_open_segment():
            self.segment_store.recover_open_segment(self._latest_level1_timestamp())

        self._persist_cv = threading.Condition()
        self._persist_queue = deque()
        self._persist_inflight = 0
        self._persist_stop = False
        self._persist_error = None
        self._prepared_chunk = None
        self.persist_queue_high_water = 0
        self.persist_enqueue_wait_count = 0
        self.persist_enqueue_wait_ns_total = 0
        self.persist_enqueue_wait_ns_max = 0
        self.rotate_total_ns = _TimingAccum()
        self.rotate_finalize_ns = _TimingAccum()
        self.rotate_enqueue_ns = _TimingAccum()
        self.rotate_prepare_wait_ns = _TimingAccum()
        self.rotate_register_chunk_ns = _TimingAccum()
        self.rotate_meta_open_ns = _TimingAccum()
        self.rotate_index_create_ns = _TimingAccum()
        self._persist_thread = threading.Thread(
            target=self._persist_loop,
            name=f"dw-persist-{self.feed_name}",
            daemon=True,
        )
        self._persist_thread.start()

        self._create_new_chunk()

    def _latest_level1_timestamp(self):
        """Return latest level-1 timestamp observed in feed metadata, or None."""
        latest_idx = self.registry.get_latest_chunk_idx()
        if latest_idx is None:
            return None

        idx = int(latest_idx)
        while idx > 0:
            meta = self.registry.get_chunk_metadata(idx)
            try:
                qmin = meta.get_qmin(0)
                qmax = meta.get_qmax(0)
                if qmax != 0 and qmin != UINT64_MAX and qmin <= qmax:
                    return int(qmax)
            finally:
                meta.release()
            idx -= 1
        return None

    def _create_new_chunk(self):
        rotate_started = time.perf_counter_ns()

        # Release old metadata BEFORE register_chunk in case it triggers resize
        _new_start_time = None
        if self.current_chunk_metadata is not None:
            finalize_started = time.perf_counter_ns()
            sealed_chunk = self.current_chunk
            sealed_index = self.chunk_index if self._index_enabled else None
            self.current_chunk_metadata.status = ON_DISK
            self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
            _new_start_time = self.current_chunk_metadata.end_time
            self.current_chunk_metadata.release()
            self.current_chunk = None
            self.chunk_index = None
            self.rotate_finalize_ns.add(time.perf_counter_ns() - finalize_started)

            enqueue_started = time.perf_counter_ns()
            self._enqueue_sealed_chunk(sealed_chunk, sealed_index)
            self.rotate_enqueue_ns.add(time.perf_counter_ns() - enqueue_started)

        # reset per-key bounds for new chunk
        self._key_min_set = [False for _ in range(self._clock_level)]

        prepared_started = time.perf_counter_ns()
        prepared = self._acquire_prepared_chunk()
        self.rotate_prepare_wait_ns.add(time.perf_counter_ns() - prepared_started)
        self.current_chunk_id = prepared.chunk_id
        self.current_chunk = prepared.chunk
        self.chunk_index = prepared.chunk_index

        register_started = time.perf_counter_ns()
        self.registry.register_chunk(
            time.time_ns() // 1_000, self.current_chunk_id,
            self.feed_config["chunk_size_bytes"], status=ON_DISK,
            clock_level=self._clock_level
        )
        self.rotate_register_chunk_ns.add(time.perf_counter_ns() - register_started)

        meta_started = time.perf_counter_ns()
        self.current_chunk_metadata = self.registry.get_chunk_metadata(self.current_chunk_id)
        self.current_chunk_metadata.start_time = _new_start_time if _new_start_time is not None else self.current_chunk_metadata.start_time
        self.current_chunk_metadata.clock_level = self._clock_level
        self._qmins = self.current_chunk_metadata._qmins
        self._qmaxs = self.current_chunk_metadata._qmaxs
        self.rotate_meta_open_ns.add(time.perf_counter_ns() - meta_started)

        if self._index_enabled:
            # index creation is handled during background preallocation
            self.rotate_index_create_ns.add(0)

        self.rotate_total_ns.add(time.perf_counter_ns() - rotate_started)

    def _check_persist_error(self) -> None:
        err = self._persist_error
        if err is not None:
            raise RuntimeError(f"background persist failed for feed '{self.feed_name}'") from err

    def _enqueue_sealed_chunk(self, chunk: Chunk, chunk_index: ChunkIndex | None) -> None:
        self._check_persist_error()
        with self._persist_cv:
            wait_started = None
            while (
                (len(self._persist_queue) + self._persist_inflight) >= _MAX_PENDING_SEALED_CHUNKS
                and self._persist_error is None
            ):
                if wait_started is None:
                    wait_started = time.perf_counter_ns()
                self._persist_cv.wait()
            if wait_started is not None:
                waited_ns = time.perf_counter_ns() - wait_started
                self.persist_enqueue_wait_count += 1
                self.persist_enqueue_wait_ns_total += waited_ns
                if waited_ns > self.persist_enqueue_wait_ns_max:
                    self.persist_enqueue_wait_ns_max = waited_ns
            self._check_persist_error()
            self._persist_queue.append(_SealedChunk(chunk=chunk, chunk_index=chunk_index))
            queue_depth = len(self._persist_queue) + self._persist_inflight
            if queue_depth > self.persist_queue_high_water:
                self.persist_queue_high_water = queue_depth
            self._persist_cv.notify()

    def _acquire_prepared_chunk(self) -> _PreparedChunk:
        self._check_persist_error()
        with self._persist_cv:
            while self._prepared_chunk is None and self._persist_error is None:
                self._persist_cv.notify()
                self._persist_cv.wait()
            self._check_persist_error()
            if self._prepared_chunk is None:
                raise RuntimeError(f"no prepared chunk available for feed '{self.feed_name}'")
            prepared = self._prepared_chunk
            self._prepared_chunk = None
            self._persist_cv.notify()
            return prepared

    def _make_prepared_chunk(self, chunk_id: int) -> _PreparedChunk:
        chunk_path = self.data_dir / f"chunk_{chunk_id:08d}.bin"
        chunk = Chunk.create_file(path=str(chunk_path), size=self.feed_config["chunk_size_bytes"])
        chunk_index = None
        index_path = None
        if self._index_enabled:
            index_path = self.data_dir / f"chunk_{chunk_id:08d}.idx"
            chunk_index = ChunkIndex.create_file(
                path=str(index_path),
                capacity=2047,
            )
        return _PreparedChunk(
            chunk_id=chunk_id,
            chunk_path=chunk_path,
            chunk=chunk,
            chunk_index=chunk_index,
            index_path=index_path,
        )

    def _discard_prepared_chunk(self, prepared: _PreparedChunk) -> None:
        prepared.chunk.close_file()
        if prepared.chunk_index is not None:
            prepared.chunk_index.close_file()
        if prepared.chunk_path.exists():
            os.unlink(prepared.chunk_path)
        if prepared.index_path is not None and prepared.index_path.exists():
            os.unlink(prepared.index_path)

    def _persist_loop(self) -> None:
        while True:
            sealed = None
            prepare_chunk_id = None
            with self._persist_cv:
                while (
                    not self._persist_queue
                    and self._prepared_chunk is not None
                    and not self._persist_stop
                ):
                    self._persist_cv.wait()
                if self._persist_stop and not self._persist_queue:
                    self._persist_cv.notify_all()
                    return
                if self._persist_queue:
                    sealed = self._persist_queue.popleft()
                    self._persist_inflight += 1
                elif self._prepared_chunk is None:
                    prepare_chunk_id = self.current_chunk_id + 1
                else:
                    continue
            try:
                if sealed is not None:
                    sealed.chunk.close_file()
                    if sealed.chunk_index is not None:
                        sealed.chunk_index.close_file()
                    self.registry.flush()
                elif prepare_chunk_id is not None:
                    prepared = self._make_prepared_chunk(prepare_chunk_id)
                    with self._persist_cv:
                        expected_chunk_id = self.current_chunk_id + 1
                        if (
                            self._prepared_chunk is None
                            and not self._persist_stop
                            and prepare_chunk_id == expected_chunk_id
                        ):
                            self._prepared_chunk = prepared
                            self._persist_cv.notify_all()
                        else:
                            prepared_to_discard = prepared
                            prepared = None
                    if prepared is None:
                        self._discard_prepared_chunk(prepared_to_discard)
            except BaseException as exc:
                with self._persist_cv:
                    if self._persist_error is None:
                        self._persist_error = exc
                        self._persist_stop = True
                    if sealed is not None:
                        self._persist_inflight -= 1
                    self._persist_cv.notify_all()
                return
            else:
                if sealed is not None:
                    with self._persist_cv:
                        self._persist_inflight -= 1
                        self._persist_cv.notify_all()

    def _stop_persister(self) -> None:
        with self._persist_cv:
            self._persist_stop = True
            self._persist_cv.notify_all()
            while (self._persist_queue or self._persist_inflight) and self._persist_error is None:
                self._persist_cv.wait()
            self._persist_cv.notify_all()
        if self._persist_thread.is_alive():
            self._persist_thread.join()
        self._check_persist_error()

    def _cleanup_unregistered_future_chunks(self) -> None:
        latest_chunk_id = int(self.current_chunk_id)
        if not self.data_dir.exists():
            return
        for pattern in ("chunk_*.bin", "chunk_*.idx"):
            for path in self.data_dir.glob(pattern):
                stem = path.stem
                try:
                    chunk_id = int(stem.split("_", 1)[1])
                except (IndexError, ValueError):
                    continue
                if chunk_id > latest_chunk_id:
                    try:
                        os.unlink(path)
                    except FileNotFoundError:
                        pass

    def _validate_chunk(self):
        """Validate previous chunk and auto-repair if corrupted."""
        from ..ops import repair
        
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
        self._check_persist_error()
        
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
        self._segment_note_write(ts0, 1)
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
        self._check_persist_error()
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
        self._segment_note_write(ts0, 1)
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
        self._check_persist_error()
        
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
        self._segment_note_batch(ts_first, ts_last, data_len // rec_sz)
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
        persist_error = None
        try:
            if self.current_chunk:
                empty = int(self.current_chunk_metadata.num_records) == 0
                chunk_id = self.current_chunk_id
                active_chunk = self.current_chunk
                active_index = self.chunk_index if self._index_enabled else None
                meta = self.current_chunk_metadata
                self.current_chunk = None
                self.chunk_index = None
                self.current_chunk_metadata = None
                if empty:
                    active_chunk.close_file()
                    if active_index is not None:
                        active_index.close_file()
                    meta.release()
                    self.registry.pop_latest_chunk()
                    os.unlink(self.data_dir / f"chunk_{chunk_id:08d}.bin")
                    if self._index_enabled:
                        idx_path = self.data_dir / f"chunk_{chunk_id:08d}.idx"
                        if idx_path.exists():
                            os.unlink(idx_path)
                else:
                    meta.status = ON_DISK
                    meta.end_time = meta.last_update
                    meta.release()
                    self._enqueue_sealed_chunk(active_chunk, active_index)
            self._stop_persister()
            if self._prepared_chunk is not None:
                prepared = self._prepared_chunk
                self._prepared_chunk = None
                self._discard_prepared_chunk(prepared)
        except Exception as exc:
            persist_error = exc
        finally:
            try:
                if self.registry is not None:
                    self.registry.close()
            finally:
                self.segment_store.close_open_segment("writer_close")
        if persist_error is not None:
            raise persist_error

    def mark_segment_boundary(self, reason: str = "disconnect") -> bool:
        """
        Close current segment without closing the writer.

        Next write will automatically open a new segment.
        Useful for websocket disconnect/reconnect boundaries.
        """
        if self.current_chunk is None:
            return False
        return self.segment_store.close_open_segment(reason)
