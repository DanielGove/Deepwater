from __future__ import annotations

import os
import struct
import time
from pathlib import Path
from typing import Iterator, Optional

import numpy as np

from ..blob_sidecar import BlobRef, BlobSidecarReaders, codec_name
from ..chunk import Chunk
from ..formatting import format_empty_batch, format_raw_batch, format_record_at, raw_record_formatter
from ..ring import RingBuffer, ring_buffer_shm_names
from .traversal import (
    chunk_lower_bound,
    chunk_raw_batches,
    ring_copy_records,
    ring_lower_bound,
    ring_raw_batches,
)
from ...metadata.feed_registry import EXPIRED, IN_MEMORY, ON_DISK, FeedRegistry
from ...metadata.feed_metadata import load_feed_metadata
from ...metadata.feed_schema import load_record_schema_for_feed
from ...network.path import DEFAULT_PORT, NetworkTarget, parse_target


class Reader:
    """Unified Deepwater reader.

    A Reader owns both readable surfaces for one feed:
    durable chunk files, tracked by FeedRegistry, and the live shared-memory
    ring tail. The feed metadata decides which paths exist.

    Public methods:
        schema(): schema fields and timestamp layout.
        range(start, end): finite historical window, [start, end).
        range_batches(start, end): finite historical window in batches.
        stream(start=None): infinite live stream, optionally replaying from time.
        first_after(start): first record with key >= start.
        first_before(ts): last record with key <= ts.
        last_timestamp(ts_key=None): latest timestamp value for this feed.
        read_available(): stateful nonblocking tail read.
        ring_state(): live ring cursor/header state when a ring exists.
        blob_ref(row, sidecar=...): decode sidecar payload coordinates from an index row.
        blob(row, sidecar=...): load one blob sidecar payload.
        blobs(rows, sidecar=...): load payloads for a batch of index rows.
    """

    __slots__ = (
        "base_path", "data_dir", "feed_name",
        "record_format", "_metadata",
        "_persistent", "_uses_ring", "feed_registry",
        "_chunk", "_chunk_meta", "_chunk_id",
        "_S", "_u64", "_unpack", "_rec_size", "_ts_offset_by_name",
        "_field_names", "_dtype", "_read_head",
        "_ring", "_ring_record_capacity",
        "_primary_ts_off", "_tail_read_seq",
        "_chunk_lower_bound", "_chunk_raw_batches",
        "_ring_lower_bound", "_ring_raw_batches", "_ring_copy_records",
        "_sidecars", "_blob_readers",
    )

    def __new__(
        cls,
        base_path: str | os.PathLike[str] | NetworkTarget,
        feed_name: str,
        port: int = DEFAULT_PORT,
        timeout: float = 10.0,
        status_callback=None,
    ):
        if cls is not Reader:
            return super().__new__(cls)
        target = base_path if isinstance(base_path, NetworkTarget) else parse_target(base_path, default_port=port)
        if target.is_remote:
            from ...network.client import RemoteReader

            assert target.host is not None
            return RemoteReader(
                target.host,
                target.path,
                feed_name,
                port=target.port,
                timeout=timeout,
                status_callback=status_callback,
            )
        return super().__new__(cls)

    def __init__(
        self,
        base_path: str | os.PathLike[str],
        feed_name: str
    ):
        self.base_path = Path(base_path)
        self.data_dir = self.base_path / "data" / feed_name
        self.feed_name = feed_name

        # Durable chunk cursor.
        self._chunk: Chunk | None = None
        self._chunk_meta = None
        self._chunk_id: Optional[int] = None
        self._read_head: Optional[int] = None
        self._ring: RingBuffer | None = None
        self._ring_record_capacity = 0
        self._tail_read_seq: Optional[int] = None
        self._sidecars = None
        self._blob_readers = None

        self._metadata = load_feed_metadata(self.base_path, feed_name)
        if self._metadata is None:
            raise KeyError(feed_name)

        self.record_format = load_record_schema_for_feed(self.base_path, feed_name)
        self._persistent = self._metadata.persist
        self._uses_ring = self._metadata.uses_ring
        
        self.feed_registry = (
            FeedRegistry(self.data_dir / f"{feed_name}.reg", mode="r")
            if self._persistent
            else None
        )

        # Record layout and traversal functions.
        self._S = struct.Struct(self.record_format.fmt)
        self._u64 = struct.Struct("<Q")
        self._unpack = self._S.unpack_from
        self._rec_size = self._S.size
        self._chunk_lower_bound = chunk_lower_bound
        self._chunk_raw_batches = chunk_raw_batches
        self._ring_lower_bound = ring_lower_bound
        self._ring_raw_batches = ring_raw_batches
        self._ring_copy_records = ring_copy_records

        ts_fields = self.record_format.timestamp_fields
        self._ts_offset_by_name = {field.name: field.offset for field in ts_fields}
        self._primary_ts_off = self.record_format.primary_ts_offset
        self._field_names = tuple(
            f.name
            for f in self.record_format.fields
            if not f.type.startswith("_") or f.name != "_"
        )
        self._dtype = self.record_format.numpy_dtype

        if self._uses_ring:
            try:
                self._ring = RingBuffer.open(
                    ring_buffer_shm_names(self.base_path, self.feed_name)[0],
                    data_size=int(self._metadata.ring_size_bytes),
                )
            except FileNotFoundError:
                pass
            else:
                self._ring_record_capacity = self._ring.data_size // self._rec_size

        self._ensure_blob_readers()

    @property
    def format(self) -> str:
        return self.record_format.fmt

    @property
    def field_names(self) -> tuple:
        return self._field_names

    @property
    def dtype(self):
        return self._dtype

    @property
    def record_size(self) -> int:
        return self._rec_size

    @property
    def metadata(self) -> dict:
        storage = "ring" if self._uses_ring else "chunk"
        metadata = self._metadata.to_dict()
        metadata["storage"] = str(storage)
        metadata["uses_ring"] = self._uses_ring
        return metadata

    @property
    def fields(self) -> tuple[dict, ...]:
        return tuple(field.to_dict() for field in self.record_format.fields)

    @property
    def timestamp_fields(self) -> tuple[dict, ...]:
        return tuple(field.to_dict() for field in self.record_format.timestamp_fields)

    @property
    def is_persistent(self) -> bool:
        return self._persistent

    def schema(self) -> dict:
        """Return schema fields and timestamp layout."""
        clock_level = int(self.record_format.clock_level)
        fields = [field.to_dict() for field in self.record_format.fields]
        timestamp_fields = fields[:clock_level]
        ts_offset = self.record_format.primary_ts_offset
        return {
            "clock_level": clock_level,
            "timestamp_fields": timestamp_fields,
            "record_fmt": self.record_format.fmt,
            "record_size": self.record_format.record_size,
            "ts_offset": ts_offset,
            "fields": fields,
        }

    def __repr__(self) -> str:
        return (
            f"Reader(feed_name={self.feed_name!r}, "
            f"persistent={self._persistent!r}, "
            f"record_size={self._rec_size}, "
            f"fields={self._field_names!r})"
        )

    def __enter__(self) -> "Reader":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def _close_chunk(self) -> None:
        if self._chunk is not None:
            self._chunk.close()
            self._chunk = None
        if self._chunk_meta is not None:
            self._chunk_meta.release()
            self._chunk_meta = None
        self._chunk_id = None

    def _open_chunk(self, chunk_id: int) -> None:
        if self._chunk_id == chunk_id:
            return
        self._close_chunk()

        meta = self.feed_registry.get_chunk_metadata(chunk_id)
        if meta is None:
            raise RuntimeError(f"Chunk {chunk_id} metadata not found")

        status = meta.status
        if status == ON_DISK:
            chunk_path = self.data_dir / f"chunk_{chunk_id:08d}.bin"
            if not chunk_path.exists():
                meta.release()
                raise FileNotFoundError(f"Chunk file missing: {chunk_path}")
            chunk = Chunk.open(str(chunk_path))
        elif status == IN_MEMORY:
            meta.release()
            # Prepared index chunks are not in the readable durable prefix.
            raise FileNotFoundError(f"Chunk {chunk_id} is not published")
        elif status == EXPIRED:
            meta.release()
            raise RuntimeError(f"Chunk {chunk_id} is expired")
        else:
            meta.release()
            raise RuntimeError(f"Unknown chunk status {status}")

        self._chunk_meta = meta
        self._chunk = chunk
        self._chunk_id = chunk_id

    def _ensure_latest_chunk(self) -> None:
        latest = self.feed_registry.get_latest_chunk_idx()
        if latest is None:
            raise RuntimeError(f"Feed '{self.feed_name}' has no chunks")
        chunk_id = latest
        while chunk_id and chunk_id > 0:
            meta = self.feed_registry.get_chunk_metadata(chunk_id)
            if meta is None:
                chunk_id -= 1
                continue
            status = meta.status
            meta.release()
            if status in (EXPIRED, IN_MEMORY):
                chunk_id -= 1
                continue
            self._open_chunk(chunk_id)
            return
        raise RuntimeError(f"Feed '{self.feed_name}' has no available chunks (all expired/deleted)")

    def _visible_chunk_write_pos(self, durable_seq, ring_epoch_tag) -> int:
        meta = self._chunk_meta
        write_pos = int(meta.write_pos)
        if (durable_seq is not None and ring_epoch_tag is not None
                and int(meta.ring_epoch_tag) == ring_epoch_tag):
            count = int(meta.num_records)
            start_seq = int(meta.ring_end_seq) - count
            if start_seq < 0:
                raise RuntimeError("invalid committed chunk sequence range")
            allowed = min(count, max(0, durable_seq - start_seq))
            write_pos = min(write_pos, allowed * self._rec_size)
        return write_pos

    def _capture_ring_tail(self, start_time=None, end_time=None, ts_off=0, *, limit=None, from_end=False):
        """Own the live suffix before reading a potentially slow durable prefix.

        A stable durable acknowledgement prevents the producer from reclaiming
        these undurable slots during the copy. Chunk metadata is clipped to the
        same acknowledgement so a commit cannot appear on both read surfaces.
        """
        ring = self._ring
        for _ in range(16):
            version = ring.write_sequence
            if version & 1:
                os.sched_yield()
                continue
            durable_seq = int(ring.durable_record_count[0]) if self._persistent else 0
            record_count = int(ring.record_count[0])
            earliest_seq = max(0, record_count - self._ring_record_capacity, ring.retained_record_floor)
            if self._persistent and (durable_seq < earliest_seq or durable_seq > record_count):
                if ring.write_sequence != version or int(ring.durable_record_count[0]) != durable_seq:
                    continue
                raise RuntimeError("live index window no longer covers its durable frontier")
            start_seq = max(durable_seq, earliest_seq)
            end_seq = record_count
            start_pos = (earliest_seq * self._rec_size) % ring.data_size
            if start_time is not None:
                start_seq = self._ring_lower_bound(
                    ring.data, start_pos, earliest_seq, start_seq, end_seq,
                    int(start_time), ts_off, self._rec_size, ring.data_size,
                )
            if end_time is not None:
                end_seq = self._ring_lower_bound(
                    ring.data, start_pos, earliest_seq, start_seq, end_seq,
                    int(end_time), ts_off, self._rec_size, ring.data_size,
                )
            if limit is not None:
                if from_end:
                    start_seq = max(start_seq, end_seq - limit)
                else:
                    end_seq = min(end_seq, start_seq + limit)
            pos = (start_seq * self._rec_size) % ring.data_size
            raw = bytes(ring.data[pos:pos + (end_seq - start_seq) * self._rec_size])
            if ring.write_sequence != version or start_seq < ring.retained_record_floor:
                continue
            if self._persistent:
                if int(ring.durable_record_count[0]) != durable_seq:
                    continue
            elif int(ring.record_count[0]) != record_count:
                continue
            return durable_seq, record_count, raw
        raise BlockingIOError("live index changed during repeated snapshot attempts; retry the read")

    def _copy_ring_window(self, start_seq: int, end_seq: int) -> bytes:
        """Copy fixed rows while the producer's overwrite guard stays stable."""
        ring = self._ring
        for _ in range(16):
            version = ring.write_sequence
            if version & 1:
                os.sched_yield()
                continue
            record_count = int(ring.record_count[0])
            floor = max(0, record_count - self._ring_record_capacity, ring.retained_record_floor)
            if start_seq < floor or end_seq > record_count:
                if ring.write_sequence != version:
                    continue
                raise RuntimeError("reader fell behind the retained index window; resume from durable history")
            pos = (start_seq * self._rec_size) % ring.data_size
            raw = bytes(ring.data[pos:pos + (end_seq - start_seq) * self._rec_size])
            if ring.write_sequence == version and start_seq >= ring.retained_record_floor:
                return raw
        raise BlockingIOError("live index changed during repeated copy attempts; retry the read")

    def _iter_durable_raw_range(
        self,
        start_us: int,
        end_us: Optional[int],
        ts_off: int,
        batch_records: Optional[int],
        *,
        durable_seq: int | None = None,
        ring_epoch_tag: int | None = None,
    ) -> Iterator[memoryview]:
        """Yield owned raw batches from durable chunks for [start_us, end_us)."""
        rec_size = self._rec_size
        if batch_records is not None and batch_records <= 0:
            raise ValueError("batch_records must be positive")

        batch_bytes = int(batch_records) * rec_size if batch_records is not None else 0
        chunk_ids = (
            self.feed_registry.get_chunks_in_range(start_us, end_us, qoff=ts_off)
            if end_us is not None
            else self.feed_registry.get_chunks_after(start_us, qoff=ts_off)
        )
        for chunk_id in chunk_ids:
            try:
                self._open_chunk(chunk_id)
            except FileNotFoundError:
                continue
            buf = self._chunk.buffer
            write_pos = self._visible_chunk_write_pos(durable_seq, ring_epoch_tag)
            pos = self._chunk_lower_bound(buf, start_us, write_pos, self._rec_size, ts_off)
            end_pos = write_pos if end_us is None else self._chunk_lower_bound(
                buf, end_us, write_pos, self._rec_size, ts_off
            )
            if end_pos <= pos:
                continue

            yield from self._chunk_raw_batches(buf, pos, end_pos, batch_bytes)

    def _stream_durable_raw(
        self,
        start: Optional[int] = None,
        ts_off: int = 0,
    ) -> Iterator[memoryview]:
        rec_size = self._rec_size
        if start is not None:
            latest_chunk_id = self.feed_registry.get_latest_chunk_idx()
            if latest_chunk_id is not None:
                for chunk_id in self.feed_registry.get_chunks_after(start, qoff=ts_off):
                    try:
                        self._open_chunk(chunk_id)
                    except FileNotFoundError:
                        continue
                    buf = self._chunk.buffer
                    write_pos = self._chunk_meta.write_pos
                    pos = self._chunk_lower_bound(buf, start, write_pos, rec_size, ts_off)
                    while pos + rec_size <= write_pos:
                        yield buf[pos:pos + rec_size]
                        pos += rec_size
                    if chunk_id >= latest_chunk_id:
                        break
            read_head = self._chunk_meta.write_pos if self._chunk_meta is not None else 0
        else:
            while self.feed_registry.get_latest_chunk_idx() is None:
                os.sched_yield()
            self._ensure_latest_chunk()
            read_head = self._chunk_meta.write_pos

        while True:
            if self._chunk_meta is None:
                self._ensure_latest_chunk()
                read_head = self._chunk_meta.write_pos

            buf = self._chunk.buffer
            write_pos = self._chunk_meta.write_pos

            while read_head + rec_size <= write_pos:
                yield buf[read_head:read_head + rec_size]
                read_head += rec_size

            latest_chunk_id = self.feed_registry.get_latest_chunk_idx()
            if latest_chunk_id is not None and self._chunk_id < latest_chunk_id:
                try:
                    self._open_chunk(self._chunk_id + 1)
                    read_head = 0
                    continue
                except FileNotFoundError:
                    os.sched_yield()
                    continue
            os.sched_yield()

    def ring_state(self) -> dict:
        """Return live ring counters for diagnostics."""
        if self._ring is None:
            raise FileNotFoundError(f"feed '{self.feed_name}' has no live ring")
        ring = self._ring
        record_count = ring.record_count[0]
        return {
            "write_pos": ring.write_pos[0],
            "start_pos": ring.start_pos[0],
            "last_ts": ring.last_ts[0],
            "record_count": record_count,
            "durable_record_count": ring.durable_record_count[0],
            "durable_last_ts": ring.durable_last_ts[0],
            "overrun_count": ring.overrun_count(),
            "lost_records": ring.lost_records[0],
            "earliest_live_seq": max(0, record_count - self._ring_record_capacity),
            "ring_record_capacity": self._ring_record_capacity,
            "ring_data_size": ring.data_size,
            "ring_total_size": ring.total_size,
        }

    def last_timestamp(self, ts_key: Optional[str] = None) -> int | None:
        """Return the latest readable timestamp value, or None when the feed is empty."""
        if ts_key is None:
            ts_off = self._primary_ts_off
        else:
            try:
                ts_off = self._ts_offset_by_name[ts_key]
            except KeyError:
                raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {tuple(self._ts_offset_by_name)}") from None

        ring = self._ring
        if ring is not None:
            record_count = int(ring.record_count[0])
            if record_count > 0 and self._ring_record_capacity > 0:
                raw = self._copy_ring_window(record_count - 1, record_count)
                return int.from_bytes(raw[ts_off:ts_off + 8], "little")

        if self.feed_registry is None:
            return None

        chunk_id = self.feed_registry.get_latest_chunk_idx()
        while chunk_id and chunk_id > 0:
            meta = self.feed_registry.get_chunk_metadata(chunk_id)
            if meta is None:
                chunk_id -= 1
                continue
            try:
                if meta.status != ON_DISK or int(meta.num_records) <= 0:
                    chunk_id -= 1
                    continue
            finally:
                meta.release()
            try:
                self._open_chunk(chunk_id)
            except FileNotFoundError:
                chunk_id -= 1
                continue
            pos = int(self._chunk_meta.write_pos) - self._rec_size
            if pos >= 0:
                return int.from_bytes(self._chunk.buffer[pos + ts_off:pos + ts_off + 8], "little")
            chunk_id -= 1
        return None

    def _raw_seq_batches(
        self, start_pos: int, earliest_seq: int, start_seq: int, end_seq: int,
        ts_off: int = 0, start_time: Optional[int] = None,
        end_time: Optional[int] = None, batch_records: int = 50_000,
    ):
        if batch_records <= 0:
            raise ValueError("batch_records must be positive")
        if end_seq <= start_seq:
            return
        raw = self._copy_ring_window(start_seq, end_seq)
        begin, finish = 0, len(raw)
        if start_time is not None:
            begin = self._chunk_lower_bound(raw, int(start_time), finish, self._rec_size, ts_off)
        if end_time is not None:
            finish = self._chunk_lower_bound(raw, int(end_time), finish, self._rec_size, ts_off)
        view = memoryview(raw)
        batch_bytes = batch_records * self._rec_size
        for pos in range(begin, finish, batch_bytes):
            yield view[pos:min(pos + batch_bytes, finish)]

    def _stream_ring_raw(
        self, start_seq: int, ts_off: int = 0,
        start_time: Optional[int] = None,
    ) -> Iterator[memoryview]:
        ring = self._ring
        records_read = int(start_seq)
        while True:
            record_count = int(ring.record_count[0])
            if records_read >= record_count:
                os.sched_yield()
                continue
            end_seq = min(record_count, records_read + 1024)
            try:
                raw = self._copy_ring_window(records_read, end_seq)
            except BlockingIOError:
                os.sched_yield()
                continue
            records_read = end_seq
            view = memoryview(raw)
            for pos in range(0, len(raw), self._rec_size):
                if start_time is not None and self._u64.unpack_from(raw, pos + ts_off)[0] < start_time:
                    continue
                yield view[pos:pos + self._rec_size]

    def stream(
        self,
        start: Optional[int] = None,
        format: str = "tuple",
        ts_key: Optional[str] = None,
    ) -> Iterator:
        """Yield records, optionally replaying one consistent historical prefix."""
        try:
            ts_off = self._primary_ts_off if ts_key is None else self._ts_offset_by_name[ts_key]
        except KeyError:
            raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {tuple(self._ts_offset_by_name)}") from None
        format_raw_record = raw_record_formatter(format, self._unpack, self._field_names, self._dtype)
        if self._ring is None:
            if self.feed_registry is None or self.feed_registry.get_latest_chunk_idx() is None:
                return
            raw_records = self._stream_durable_raw(start=start, ts_off=ts_off)
        elif start is None:
            raw_records = self._stream_ring_raw(int(self._ring.record_count[0]), ts_off=ts_off)
        else:
            durable_seq, record_count, tail = self._capture_ring_tail(start_time=start, ts_off=ts_off)
            if self.feed_registry is not None:
                for batch in self._iter_durable_raw_range(
                    start, None, ts_off=ts_off, batch_records=50_000,
                    durable_seq=durable_seq, ring_epoch_tag=self._ring.epoch_tag,
                ):
                    data = memoryview(batch).cast("B")
                    for pos in range(0, data.nbytes, self._rec_size):
                        yield format_raw_record(data[pos:pos + self._rec_size])
            for pos in range(0, len(tail), self._rec_size):
                yield format_raw_record(memoryview(tail)[pos:pos + self._rec_size])
            raw_records = self._stream_ring_raw(record_count, ts_off=ts_off, start_time=start)
        for raw in raw_records:
            yield format_raw_record(raw)

    def range(
        self,
        start: int,
        end: int,
        format: str = "tuple",
        ts_key: Optional[str] = None,
    ) -> list | memoryview | np.ndarray:
        """Return owned records in [start, end) across one durable frontier."""
        try:
            ts_off = self._primary_ts_off if ts_key is None else self._ts_offset_by_name[ts_key]
        except KeyError:
            raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {tuple(self._ts_offset_by_name)}") from None
        if end <= start:
            return format_empty_batch(format, self._dtype)
        data = bytearray()
        for raw in self._iter_raw_range(start, end, ts_off):
            data.extend(raw)
        if not data:
            return format_empty_batch(format, self._dtype)
        return format_raw_batch(memoryview(data), format, self._unpack, self._rec_size, self._field_names, self._dtype)

    def range_columns(
        self,
        start: int,
        end: int,
        columns: list[str] | tuple[str, ...],
        *,
        ts_key: Optional[str] = None,
    ) -> dict[str, np.ndarray]:
        arr = self.range(start, end, format="numpy", ts_key=ts_key)
        return {name: arr[name] for name in columns}

    def _ensure_blob_readers(self) -> BlobSidecarReaders:
        readers = self._blob_readers
        if readers is None:
            readers = BlobSidecarReaders(self.base_path, self.feed_name, parent_ring=self._ring, persistent=self._persistent)
            self._blob_readers = readers
            self._sidecars = readers.sidecars
        return readers

    def _row_value(self, row, name: str):
        if isinstance(row, dict):
            return row[name]
        if isinstance(row, tuple) or isinstance(row, list):
            return row[self._field_names.index(name)]
        return row[name]

    def blob_ref(self, row, *, sidecar: str) -> BlobRef:
        """Decode a blob sidecar reference from a formatted index row."""
        readers = self._ensure_blob_readers()
        meta = readers.sidecars[sidecar]
        fields = meta.ref_fields
        codec_id = int(self._row_value(row, fields.codec))
        return BlobRef(
            sidecar=sidecar,
            chunk_id=int(self._row_value(row, fields.chunk_id)),
            offset=int(self._row_value(row, fields.offset)),
            size=int(self._row_value(row, fields.size)),
            codec=codec_name(codec_id),
            codec_id=codec_id,
            schema_id=int(self._row_value(row, fields.schema_id)),
            flags=int(self._row_value(row, fields.flags)),
            crc=int(self._row_value(row, fields.crc)),
        )

    def blob(self, row, *, sidecar: str, memory_only: bool = False) -> bytes:
        """Read one blob sidecar payload referenced by a formatted index row."""
        readers = self._ensure_blob_readers()
        return readers[sidecar].read(self.blob_ref(row, sidecar=sidecar), memory_only=memory_only)

    def blobs(self, rows, *, sidecar: str, memory_only: bool = False) -> list[bytes]:
        """Read blob sidecar payloads for formatted index rows."""
        return [self.blob(row, sidecar=sidecar, memory_only=memory_only) for row in rows]

    def range_with_blobs(
        self,
        start: int,
        end: int,
        *,
        sidecar: str,
        format: str = "tuple",
        ts_key: Optional[str] = None,
    ):
        """Yield ``(row, payload)`` pairs for a time range and one sidecar."""
        rows = self.range(start, end, format=format, ts_key=ts_key)
        for row in rows:
            yield row, self.blob(row, sidecar=sidecar)

    def _iter_raw_range(
        self,
        start: int,
        end: int,
        ts_off: int,
        batch_records: int = 50_000,
    ) -> Iterator[memoryview]:
        """Yield owned batches without double-reading a concurrent commit."""
        if end <= start:
            return
        if batch_records <= 0:
            raise ValueError("batch_records must be positive")
        durable_seq = ring_epoch_tag = None
        tail = b""
        if self._ring is not None:
            durable_seq, _record_count, tail = self._capture_ring_tail(start, end, ts_off)
            ring_epoch_tag = self._ring.epoch_tag
        if self.feed_registry is not None and self.feed_registry.get_latest_chunk_idx() is not None:
            yield from self._iter_durable_raw_range(
                start, end, ts_off=ts_off, batch_records=batch_records,
                durable_seq=durable_seq, ring_epoch_tag=ring_epoch_tag,
            )
        batch_bytes = batch_records * self._rec_size
        view = memoryview(tail)
        for pos in range(0, len(tail), batch_bytes):
            yield view[pos:pos + batch_bytes]

    def range_batches(
        self,
        start: int,
        end: int,
        format: str = "tuple",
        ts_key: Optional[str] = None,
        batch_records: int = 50_000,
    ) -> Iterator:
        """Yield formatted batches for [start, end)."""
        if batch_records <= 0:
            raise ValueError("batch_records must be positive")
        try:
            ts_off = self._primary_ts_off if ts_key is None else self._ts_offset_by_name[ts_key]
        except KeyError:
            raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {tuple(self._ts_offset_by_name)}") from None
        dtype = self._dtype
        for raw in self._iter_raw_range(
            start,
            end,
            ts_off=ts_off,
            batch_records=batch_records,
        ):
            yield format_raw_batch(raw, format, self._unpack, self._rec_size, self._field_names, dtype)

    def first_after(
        self, start: int, format: str = "tuple", ts_key: Optional[str] = None,
    ):
        """Return the first record with key >= start, or None."""
        try:
            ts_off = self._primary_ts_off if ts_key is None else self._ts_offset_by_name[ts_key]
        except KeyError:
            raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {tuple(self._ts_offset_by_name)}") from None
        durable_seq = ring_epoch_tag = None
        tail = b""
        if self._ring is not None:
            durable_seq, _, tail = self._capture_ring_tail(start_time=start, ts_off=ts_off, limit=1)
            ring_epoch_tag = self._ring.epoch_tag
        if self.feed_registry is not None:
            for chunk_id in self.feed_registry.get_chunks_after(start, qoff=ts_off):
                try:
                    self._open_chunk(chunk_id)
                except FileNotFoundError:
                    continue
                write_pos = self._visible_chunk_write_pos(durable_seq, ring_epoch_tag)
                pos = self._chunk_lower_bound(self._chunk.buffer, start, write_pos, self._rec_size, ts_off)
                if pos < write_pos:
                    raw = bytes(self._chunk.buffer[pos:pos + self._rec_size])
                    return format_record_at(raw, 0, format, self._unpack, self._rec_size, self._field_names, self._dtype)
        if tail:
            return format_record_at(tail, 0, format, self._unpack, self._rec_size, self._field_names, self._dtype)
        return None

    def first_before(
        self, ts: int, format: str = "tuple", ts_key: Optional[str] = None,
    ):
        """Return the nearest record at or before ts, or None."""
        try:
            ts_off = self._primary_ts_off if ts_key is None else self._ts_offset_by_name[ts_key]
        except KeyError:
            raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {tuple(self._ts_offset_by_name)}") from None
        durable_seq = ring_epoch_tag = None
        if self._ring is not None:
            durable_seq, _, tail = self._capture_ring_tail(end_time=ts + 1, ts_off=ts_off, limit=1, from_end=True)
            ring_epoch_tag = self._ring.epoch_tag
            if tail:
                return format_record_at(tail, len(tail) - self._rec_size, format, self._unpack, self._rec_size, self._field_names, self._dtype)
        if self.feed_registry is not None:
            for chunk_id in self.feed_registry.get_chunks_in_range_reverse(0, ts, qoff=ts_off):
                try:
                    self._open_chunk(chunk_id)
                except FileNotFoundError:
                    continue
                write_pos = self._visible_chunk_write_pos(durable_seq, ring_epoch_tag)
                pos = self._chunk_lower_bound(self._chunk.buffer, ts + 1, write_pos, self._rec_size, ts_off) - self._rec_size
                if pos >= 0:
                    raw = bytes(self._chunk.buffer[pos:pos + self._rec_size])
                    return format_record_at(raw, 0, format, self._unpack, self._rec_size, self._field_names, self._dtype)
        return None

    def _read_available_ring(self, max_records=None, format: str = "tuple"):
        record_count = int(self._ring.record_count[0])
        tail_seq = self._tail_read_seq
        if tail_seq is None:
            self._tail_read_seq = record_count
            return format_empty_batch(format, self._dtype)
        if record_count <= tail_seq:
            return format_empty_batch(format, self._dtype)
        end_seq = record_count
        if max_records is not None:
            end_seq = min(end_seq, tail_seq + int(max_records))
        if end_seq <= tail_seq:
            return format_empty_batch(format, self._dtype)
        try:
            raw = self._copy_ring_window(tail_seq, end_seq)
        except BlockingIOError:
            return format_empty_batch(format, self._dtype)
        self._tail_read_seq = end_seq
        return format_raw_batch(raw, format, self._unpack, self._rec_size, self._field_names, self._dtype)

    def _read_available_durable(self, max_records=None, format: str = "tuple"):
        dtype = self._dtype
        if self._read_head is None or self._chunk_meta is None:
            try:
                self._ensure_latest_chunk()
            except RuntimeError:
                return format_empty_batch(format, dtype)
            self._read_head = self._chunk_meta.write_pos
            return format_empty_batch(format, dtype)

        remaining = None if max_records is None else int(max_records)
        if remaining is not None and remaining <= 0:
            return format_empty_batch(format, dtype)

        raw = bytearray()
        while True:
            buf = self._chunk.buffer
            write_pos = self._chunk_meta.write_pos
            read_head = self._read_head
            available_records = (write_pos - read_head) // self._rec_size
            if available_records > 0 and (remaining is None or remaining > 0):
                take_records = available_records if remaining is None else min(available_records, remaining)
                take_bytes = take_records * self._rec_size
                raw.extend(buf[read_head:read_head + take_bytes])
                read_head += take_bytes
                if remaining is not None:
                    remaining -= take_records
            self._read_head = read_head

            if remaining == 0:
                break

            latest_chunk_id = self.feed_registry.get_latest_chunk_idx()
            if latest_chunk_id is None or self._chunk_id >= latest_chunk_id:
                break

            try:
                self._open_chunk(self._chunk_id + 1)
            except FileNotFoundError:
                break
            self._read_head = 0

        return format_raw_batch(memoryview(raw), format, self._unpack, self._rec_size, self._field_names, dtype)

    def read_available(self, max_records=None, format: str = "tuple"):
        """Return newly available live records without blocking.

        The first call positions the reader at the current live head and returns
        empty; subsequent calls return records written since the previous call.
        """
        if self._ring is not None:
            return self._read_available_ring(max_records=max_records, format=format)
        if self.feed_registry is not None and self.feed_registry.get_latest_chunk_idx() is not None:
            return self._read_available_durable(max_records=max_records, format=format)
        return format_empty_batch(format, self._dtype)

    def close(self) -> None:
        """Release open chunk, registry, and ring resources."""
        self._close_chunk()
        if self._blob_readers is not None:
            self._blob_readers.close()
            self._blob_readers = None
        if self.feed_registry is not None:
            self.feed_registry.close()
            self.feed_registry = None
        if self._ring is not None:
            self._ring.close(unlink=False)
            self._ring = None
