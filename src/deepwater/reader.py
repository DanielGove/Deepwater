import time
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
            chunk = Chunk.open_file(file_path=str(chunk_path))
        elif meta.status == EXPIRED:
            raise RuntimeError(f"Chunk {chunk_id} is expired; cannot read.")
        else:
            raise RuntimeError(f"Unknown chunk status {meta.status}")

        self._chunk_meta = meta
        self._chunk = chunk
        self._chunk_id = chunk_id

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
                self._open_chunk(self._chunk_id + 1)
                read_head = 0
                continue

            time.sleep(0.001)

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

    def close(self):
        self._close_chunk()
        self.registry.close()
