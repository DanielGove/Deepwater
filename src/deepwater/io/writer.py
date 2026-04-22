import os
import time
import struct
import logging
import numpy as np

from typing import Union

from .chunk import Chunk
from .index import ChunkIndex
from ..metadata.feed_registry import FeedRegistry, ON_DISK, UINT64_MAX
from ..metadata.segments import SegmentStore
from ..utils.process import ProcessUtils

log = logging.getLogger("dw.writer")


def _noop_note_write(*_args, **_kwargs) -> None:
    return None


def _noop_note_batch(*_args, **_kwargs) -> None:
    return None


class ChunkWriter:
    __slots__ = (
        "platform", "feed_name", "feed_config", "record_format", "my_pid",
        "data_dir", "registry",
        "current_chunk", "chunk_index", "current_chunk_metadata", "current_chunk_id",
        "_S", "_rec_size", "_clock_level", "_key_min_set", "_u64", "_qmins", "_qmaxs",
        "_index_enabled", "segment_store", "_segment_note_write", "_segment_note_batch",
    )

    def __init__(self, platform, feed_name: str, *, segment_tracking: bool | None = None):
        self.platform = platform
        self.feed_name = feed_name
        self.feed_config = platform.lifecycle(feed_name)
        self.record_format = platform.get_record_format(feed_name)
        self.my_pid = ProcessUtils.get_current_pid()

        if not platform.registry.feed_exists(feed_name):
            log.info("Creating new feed '%s' (pid=%s)", feed_name, self.my_pid)
            self.platform.registry.register_feed(feed_name)

        self.data_dir = platform.base_path / "data" / feed_name
        self.registry = FeedRegistry(self.data_dir / f"{feed_name}.reg", mode="w")

        self.current_chunk = None
        self.chunk_index = None
        self.current_chunk_metadata = None
        self.current_chunk_id = self.registry.get_latest_chunk_idx() or 0
        self._key_min_set = None

        if self.current_chunk_id > 0:
            self._validate_chunk()

        self._S = struct.Struct(self.record_format["fmt"])
        self._u64 = struct.Struct("<Q")
        self._rec_size = self._S.size
        self._clock_level = self.feed_config.get("clock_level")
        self._index_enabled = bool(self.feed_config.get("index_playback"))
        if bool(self.feed_config.get("segment_tracking", True) if segment_tracking is None else segment_tracking):
            self.segment_store = SegmentStore(self.data_dir, self.feed_name)
            self._segment_note_write = self.segment_store.note_write_one
            self._segment_note_batch = self.segment_store.note_batch
            if self.segment_store.has_open_segment():
                self.segment_store.recover_open_segment(self._latest_level1_timestamp())
        else:
            self.segment_store = None
            self._segment_note_write = _noop_note_write
            self._segment_note_batch = _noop_note_batch

        self._create_new_chunk()

    @property
    def chunk_record_capacity(self) -> int:
        return int(self.feed_config["chunk_size_bytes"]) // self._rec_size

    def _latest_level1_timestamp(self):
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
        self.current_chunk_id += 1

        new_start_time = None
        if self.current_chunk_metadata is not None:
            self._close_current_chunk_file()
            self.current_chunk_metadata.status = ON_DISK
            self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
            new_start_time = self.current_chunk_metadata.end_time
            self.current_chunk_metadata.release()
            self.current_chunk_metadata = None

        self._key_min_set = [False for _ in range(self._clock_level)]

        chunk_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
        self.current_chunk = Chunk.create_file(
            path=str(chunk_path),
            size=self.feed_config["chunk_size_bytes"],
        )
        self.registry.register_chunk(
            time.time_ns() // 1_000,
            self.current_chunk_id,
            self.feed_config["chunk_size_bytes"],
            status=ON_DISK,
            clock_level=self._clock_level,
        )

        self.current_chunk_metadata = self.registry.get_chunk_metadata(self.current_chunk_id)
        if new_start_time is not None:
            self.current_chunk_metadata.start_time = new_start_time
        self.current_chunk_metadata.clock_level = self._clock_level
        self._qmins = self.current_chunk_metadata._qmins
        self._qmaxs = self.current_chunk_metadata._qmaxs

        if self._index_enabled:
            if self.chunk_index is not None:
                self.chunk_index.close_file()
            self.chunk_index = ChunkIndex.create_file(
                path=str(self.data_dir / f"chunk_{self.current_chunk_id:08d}.idx"),
                capacity=2047,
            )

    def _close_current_chunk_file(self) -> None:
        if self.current_chunk is not None:
            self.current_chunk.close_file()
        if self._index_enabled and self.chunk_index is not None:
            self.chunk_index.close_file()

    def _discard_current_chunk(self) -> None:
        if self.current_chunk is None or self.current_chunk_metadata is None:
            return
        chunk_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.bin"
        try:
            self.current_chunk.buffer.release()
        except Exception:
            pass
        mm = self.current_chunk.closeables[0]
        fd = self.current_chunk.closeables[1]
        try:
            mm.close()
        except Exception:
            pass
        try:
            os.close(fd)
        except Exception:
            pass
        if self._index_enabled and self.chunk_index is not None:
            try:
                idx_path = self.data_dir / f"chunk_{self.current_chunk_id:08d}.idx"
                self.chunk_index.close_file()
                try:
                    os.unlink(idx_path)
                except FileNotFoundError:
                    pass
            except Exception:
                pass
        self.current_chunk_metadata.release()
        self.current_chunk = None
        self.chunk_index = None
        self.current_chunk_metadata = None
        try:
            os.unlink(chunk_path)
        except FileNotFoundError:
            pass
        self.registry.pop_latest_chunk()
        self.current_chunk_id -= 1

    def _finalize_current_chunk(self, *, discard_empty: bool = False) -> None:
        if self.current_chunk is None or self.current_chunk_metadata is None:
            return
        if discard_empty and int(self.current_chunk_metadata.num_records) == 0:
            self._discard_current_chunk()
            return
        self._close_current_chunk_file()
        self.current_chunk_metadata.status = ON_DISK
        self.current_chunk_metadata.end_time = self.current_chunk_metadata.last_update
        self.current_chunk_metadata.release()
        self.current_chunk = None
        self.chunk_index = None
        self.current_chunk_metadata = None

    def _validate_chunk(self):
        from ..ops import repair

        meta = self.registry.get_chunk_metadata(self.current_chunk_id)
        if meta is None:
            return

        try:
            repair.validate_and_repair_chunk(
                chunk_id=self.current_chunk_id,
                meta=meta,
                feed_name=self.feed_name,
                feed_dir=self.data_dir,
                record_format=self.record_format,
            )
        finally:
            meta.release()

    def write(self, timestamp: int, record_data: Union[bytes, np.ndarray], create_index: bool = False) -> int:
        if self.current_chunk is None:
            return 0

        if isinstance(record_data, np.ndarray):
            record_data = record_data.tobytes()

        meta = self.current_chunk_metadata
        position = meta._wp[0]
        record_size = len(record_data)
        if record_size > meta._size[0] - position:
            self._create_new_chunk()
            meta = self.current_chunk_metadata
            position = meta._wp[0]

        u64 = self._u64
        self.current_chunk.buffer[position:position + record_size] = record_data
        ts0 = int(timestamp)

        if create_index and self.chunk_index is not None:
            ts1 = u64.unpack_from(record_data, 8)[0] if self._clock_level >= 2 else 0
            ts2 = u64.unpack_from(record_data, 16)[0] if self._clock_level >= 3 else 0
            self.chunk_index.create_index((ts0, ts1, ts2), position)

        key_min_set = self._key_min_set
        key_count = self._clock_level
        qmins = self._qmins
        qmaxs = self._qmaxs
        self._segment_note_write(ts0)
        if not key_min_set[0]:
            key_min_set[0] = True
            qmins[0] = ts0
        qmaxs[0] = ts0
        if key_count >= 2:
            ts1 = u64.unpack_from(record_data, 8)[0]
            if not key_min_set[1]:
                key_min_set[1] = True
                qmins[1] = ts1
            qmaxs[1] = ts1
        if key_count >= 3:
            ts2 = u64.unpack_from(record_data, 16)[0]
            if not key_min_set[2]:
                key_min_set[2] = True
                qmins[2] = ts2
            qmaxs[2] = ts2
        end = position + record_size
        meta._wp[0] = end
        meta._nr[0] += 1
        return end

    write_fast = write

    def write_values(self, *vals, create_index: bool = False) -> int:
        if self.current_chunk is None:
            return 0
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
        self._segment_note_write(ts0)
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

    def write_batch_bytes(self, data, create_index: bool = False) -> int:
        if self.current_chunk is None:
            return 0

        view = data if isinstance(data, memoryview) else memoryview(data)
        data_len = len(view)
        if data_len == 0 or data_len % self._rec_size != 0:
            raise ValueError("batch length must be a positive multiple of record_size")
        if self.current_chunk_metadata.write_pos + data_len > self.current_chunk_metadata.size:
            self._create_new_chunk()
        start = self.current_chunk_metadata.write_pos
        end = start + data_len
        rec_sz = self._rec_size
        self.current_chunk.buffer[start:end] = view

        key_min_set = self._key_min_set
        key_count = self._clock_level
        qmins = self._qmins
        qmaxs = self._qmaxs
        u64 = self._u64
        ts_first = u64.unpack_from(view, 0)[0]
        ts_last = u64.unpack_from(view, data_len - rec_sz)[0]
        self._segment_note_batch(ts_first, ts_last, data_len // rec_sz)
        if not key_min_set[0]:
            key_min_set[0] = True
            qmins[0] = ts_first
        qmaxs[0] = ts_last
        if key_count >= 2:
            ts_first = u64.unpack_from(view, 8)[0]
            ts_last = u64.unpack_from(view, data_len - rec_sz + 8)[0]
            if not key_min_set[1]:
                key_min_set[1] = True
                qmins[1] = ts_first
            qmaxs[1] = ts_last
        if key_count >= 3:
            ts_first = u64.unpack_from(view, 16)[0]
            ts_last = u64.unpack_from(view, data_len - rec_sz + 16)[0]
            if not key_min_set[2]:
                key_min_set[2] = True
                qmins[2] = ts_first
            qmaxs[2] = ts_last
        if create_index and self.chunk_index is not None:
            ts0_idx = 0
            ts1_idx = 8 if key_count >= 2 else None
            ts2_idx = 16 if key_count >= 3 else None
            ts0 = u64.unpack_from(view, ts0_idx)[0]
            ts1 = u64.unpack_from(view, ts1_idx)[0] if ts1_idx is not None else 0
            ts2 = u64.unpack_from(view, ts2_idx)[0] if ts2_idx is not None else 0
            self.chunk_index.create_index((ts0, ts1, ts2), start)
        self.current_chunk_metadata.write_pos = end
        self.current_chunk_metadata.num_records += data_len // rec_sz
        return self.current_chunk_metadata.write_pos

    def commit_chunk_bytes(self, data, create_index: bool = False) -> int:
        view = data if isinstance(data, memoryview) else memoryview(data)
        data_len = len(view)
        if data_len == 0 or data_len % self._rec_size != 0:
            raise ValueError("chunk length must be a positive multiple of record_size")
        if data_len > int(self.feed_config["chunk_size_bytes"]):
            raise ValueError("chunk payload exceeds configured chunk size")
        self.begin_chunk_commit()
        written = self.write_batch_bytes(view, create_index=create_index)
        self.finish_chunk_commit()
        return written

    def begin_chunk_commit(self) -> None:
        if self.current_chunk is None:
            self._create_new_chunk()
        elif int(self.current_chunk_metadata.num_records) != 0:
            self._create_new_chunk()

    def finish_chunk_commit(self) -> None:
        self._finalize_current_chunk(discard_empty=True)

    def close(self):
        try:
            if self.current_chunk is not None:
                self._finalize_current_chunk(discard_empty=True)
        finally:
            try:
                if self.registry is not None:
                    self.registry.close()
            finally:
                if self.segment_store is not None:
                    self.segment_store.close_open_segment("writer_close")

    def mark_segment_boundary(self, reason: str = "disconnect") -> bool:
        if self.current_chunk is None or self.segment_store is None:
            return False
        return self.segment_store.close_open_segment(reason)
