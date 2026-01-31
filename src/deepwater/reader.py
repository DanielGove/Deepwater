import time
import os
import struct
from typing import Optional, Tuple

from .chunk import Chunk
from .index import ChunkIndex
from .feed_registry import FeedRegistry, IN_MEMORY, ON_DISK, EXPIRED
from .utils.process import ProcessUtils


class Reader:
    """Feed reader with optional indexed playback from snapshot markers."""

    def __init__(self, platform, feed_name: str):
        self.platform = platform
        self.feed_name = feed_name
        self.feed_config = platform.lifecycle(feed_name)
        self.record_format = platform.get_record_format(feed_name)
        # Timestamp decoding for range queries
        self._ts_offset = self.record_format.get("ts_offset") or self.record_format.get("ts", {}).get("offset")
        self._ts_byteorder = "little" if self.record_format.get("ts_endian", "<") == "<" else "big"
        self.data_dir = platform.base_path / "data" / feed_name
        self.my_pid = ProcessUtils.get_current_pid()

        if not platform.registry.feed_exists(feed_name):
            raise RuntimeError(f"Feed '{feed_name}' does not exist; cannot create Reader.")

        reg_path = platform.base_path / "data" / feed_name / f"{feed_name}.reg"
        self.registry = FeedRegistry(reg_path, mode="r")

        self._chunk: Optional[Chunk] = None
        self._chunk_meta = None
        self._chunk_id: Optional[int] = None

        self._record_struct = struct.Struct(self.record_format["fmt"])
        self._record_size = self.record_format["record_size"]
        self._index_enabled = bool(self.feed_config.get("index_playback"))
        self._ts_offset = self.record_format.get("ts_offset") or self.record_format.get("ts", {}).get("offset")
        self._ts_byteorder = "little" if self.record_format.get("ts_endian", "<") == "<" else "big"

    # ------------------------------------------------------------------ helpers
    def _close_chunk(self) -> None:
        if self._chunk is not None:
            if self._chunk.is_shm:
                self._chunk.close_shm()
            else:
                self._chunk.close_file()
            self._chunk = None
        if self._chunk_meta is not None:
            self._chunk_meta.release()
            self._chunk_meta = None
        self._chunk_id = None

    def _open_chunk(self, chunk_id: int) -> None:
        if chunk_id is None or chunk_id <= 0:
            raise RuntimeError("Invalid chunk id")
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
            
            # Optional validation for ON_DISK chunks (only if feed is idle)
            # Skip validation if feed is actively being written (race condition)
            if self._should_validate_chunk(meta):
                self._validate_chunk_integrity(chunk_path, meta)
        elif meta.status == EXPIRED:
            raise RuntimeError(f"Chunk {chunk_id} is expired; cannot read.")
        else:
            raise RuntimeError(f"Unknown chunk status {meta.status}")

        self._chunk_meta = meta
        self._chunk = chunk
        self._chunk_id = chunk_id

    def _should_validate_chunk(self, meta) -> bool:
        """Check if chunk should be validated (only if feed appears idle)."""
        now_us = time.time_ns() // 1_000
        age_us = now_us - meta.last_update
        # If updated within last 5 seconds, assume feed is active (skip validation)
        return age_us > 5_000_000

    def _validate_chunk_integrity(self, chunk_path, meta):
        """
        Optional validation: warn if chunk file doesn't match registry.
        Does not block reads - just logs warnings for operator awareness.
        """
        try:
            actual_size = chunk_path.stat().st_size
            actual_records = actual_size // self._record_size
            expected_records = meta.num_records
            
            if actual_records < expected_records:
                import logging
                log = logging.getLogger("dw.reader")
                log.warning(
                    f"Chunk {meta.chunk_id} validation failed: "
                    f"file has {actual_records} records, registry claims {expected_records}. "
                    f"Consider running: python -m deepwater.repair --feed {self.feed_name}"
                )
        except Exception:
            pass  # Validation is best-effort, don't block reads

    def _ensure_latest_chunk(self) -> None:
        latest = self.registry.get_latest_chunk_idx()
        if latest is None:
            raise RuntimeError(f"Feed '{self.feed_name}' has no chunks; cannot read records.")
        self._open_chunk(latest)

    def _find_latest_snapshot_pointer(self) -> Optional[Tuple[int, int]]:
        """Return (chunk_id, offset) for most recent snapshot entry, if any."""
        if not self._index_enabled:
            return None
        chunk_id = self.registry.get_latest_chunk_idx()
        while chunk_id and chunk_id > 0:
            idx_path = self.data_dir / f"chunk_{chunk_id:08d}.idx"
            if idx_path.exists():
                idx = ChunkIndex.open_file(str(idx_path))
                try:
                    rec = idx.get_latest_index()
                    if rec:
                        offset = rec.offset
                        rec.release()
                        return chunk_id, offset
                finally:
                    idx.close_file()
            chunk_id -= 1
        return None

    # ------------------------------------------------------------------ public
    def stream_latest_records(self, playback: bool = False):
        """
        Yield packed records as they arrive.
        playback=True replays from the newest snapshot marker (if present) before
        continuing with live deltas.
        """
        start_chunk = None
        start_offset = 0

        if playback:
            snapshot = self._find_latest_snapshot_pointer()
            if snapshot:
                start_chunk, start_offset = snapshot

        if start_chunk is not None:
            self._open_chunk(start_chunk)
            read_head = start_offset
        else:
            self._ensure_latest_chunk()
            # tail new data only
            read_head = self._chunk_meta.write_pos

        while True:
            if self._chunk_meta is None:
                time.sleep(0.005)
                continue

            write_pos = self._chunk_meta.write_pos
            if read_head < write_pos:
                record = self._record_struct.unpack_from(self._chunk.buffer, read_head)
                read_head += self._record_size
                yield record
                continue

            latest_available = self.registry.get_latest_chunk_idx()
            if latest_available and self._chunk_id is not None and latest_available > self._chunk_id:
                try:
                    self._open_chunk(self._chunk_id + 1)
                    read_head = 0
                    continue
                except FileNotFoundError:
                    # Chunk file missing (pruned or not yet materialized); retry by reloading latest
                    try:
                        self._ensure_latest_chunk()
                        read_head = self._chunk_meta.write_pos
                        continue
                    except FileNotFoundError as e:
                        raise e

            time.sleep(0.001)

    def stream_latest_records_raw(self, playback: bool = False):
        """Yield memoryviews over packed records (no struct unpack)."""
        start_chunk = None
        start_offset = 0

        if playback:
            snapshot = self._find_latest_snapshot_pointer()
            if snapshot:
                start_chunk, start_offset = snapshot

        if start_chunk is not None:
            self._open_chunk(start_chunk)
            read_head = start_offset
        else:
            self._ensure_latest_chunk()
            read_head = self._chunk_meta.write_pos

        rs = self._record_size
        buf = self._chunk.buffer
        while True:
            if self._chunk_meta is None:
                time.sleep(0.001)
                continue
            write_pos = self._chunk_meta.write_pos
            if read_head < write_pos:
                yield memoryview(buf)[read_head:read_head+rs]
                read_head += rs
                continue
            latest_available = self.registry.get_latest_chunk_idx()
            if latest_available and self._chunk_id is not None and latest_available > self._chunk_id:
                try:
                    self._open_chunk(self._chunk_id + 1)
                    buf = self._chunk.buffer
                    read_head = 0
                    continue
                except FileNotFoundError:
                    try:
                        self._ensure_latest_chunk()
                        buf = self._chunk.buffer
                        read_head = self._chunk_meta.write_pos
                        continue
                    except FileNotFoundError as e:
                        raise e
            time.sleep(0.0005)

    def read_batch(self, count: int, playback: bool = False):
        """Yield lists of records in batches of size count."""
        batch = []
        for rec in self.stream_latest_records(playback=playback):
            batch.append(rec)
            if len(batch) >= count:
                yield batch
                batch = []

    def read_batch_raw(self, count: int, playback: bool = False):
        """Yield lists of memoryviews in batches of size count."""
        batch = []
        for rec in self.stream_latest_records_raw(playback=playback):
            batch.append(rec)
            if len(batch) >= count:
                yield batch
                batch = []

    def get_latest_record(self) -> tuple:
        """Return the most recent record currently written."""
        self._ensure_latest_chunk()
        if self._chunk_meta.num_records == 0:
            raise RuntimeError(f"Chunk {self._chunk_id} has no records; cannot read.")

        write_pos = self._chunk_meta.write_pos
        last_offset = write_pos - self._record_size
        if last_offset < 0:
            raise RuntimeError(f"Invalid write_pos {write_pos} for record_size {self._record_size}.")
        return self._record_struct.unpack_from(self._chunk.buffer, last_offset)

    # ------------------------------------------------------------------ range playback
    def stream_time_range(self, start_time: int, end_time: Optional[int] = None):
        """
        Yield packed records whose timestamp is within [start_time, end_time].
        Requires feed layout to define a uint64 ts field (ts_offset).
        """
        if self._ts_offset is None:
            raise RuntimeError("Feed layout missing ts_offset; cannot perform time-based reads.")

        if self.registry.get_latest_chunk_idx() is None:
            return

        # choose chunk ids via registry helpers (1-based)
        if end_time is None:
            chunk_iter = self.registry.get_chunks_after(start_time)
        else:
            chunk_iter = self.registry.get_chunks_in_range(start_time, end_time)

        for chunk_id in chunk_iter:
            # open_chunk loads fresh metadata; skip empty chunks
            self._open_chunk(chunk_id)
            if self._chunk_meta is None or self._chunk_meta.num_records == 0:
                continue
            yield from self._stream_chunk_range(start_time, end_time)

    def _stream_chunk_range(self, start_time: int, end_time: Optional[int]):
        """Yield records from the currently opened chunk within the time window."""
        buffer = self._chunk.buffer
        write_pos = self._chunk_meta.write_pos

        idx_path = self.data_dir / f"chunk_{self._chunk_id:08d}.idx"
        have_index = self._index_enabled and idx_path.exists()

        if have_index:
            idx = ChunkIndex.open_file(str(idx_path))
            try:
                start_idx = idx._binary_search_start_time(start_time)
                start_offset = 0
                if start_idx < idx.count:
                    rec = idx.get_index(start_idx)
                    try:
                        start_offset = rec.offset
                    finally:
                        rec.release()
            finally:
                idx.close_file()
            # Continue with sequential scan from start_offset to include non-indexed records too
            pos = start_offset
        else:
            # Fallback: scan records sequentially
            pos = 0

        ts_off = self._ts_offset
        ts_end = ts_off + 8
        while pos + self._record_size <= write_pos:
            ts = int.from_bytes(buffer[pos + ts_off:pos + ts_end], self._ts_byteorder)
            if ts >= start_time:
                if end_time is not None and ts > end_time:
                    break
                yield self._record_struct.unpack_from(buffer, pos)
            pos += self._record_size

    def close(self):
        self._close_chunk()
        self.registry.close()
