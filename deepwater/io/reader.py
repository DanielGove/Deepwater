from __future__ import annotations

import os
import struct
import time
from pathlib import Path
from typing import Iterator, Optional

import numpy as np

from .blob_sidecar import BlobRef, BlobSidecarReaders, codec_name
from .chunk import Chunk
from .formatting import format_empty_batch, format_raw_batch, format_record_at, raw_record_formatter
from .ring import RingBuffer, ring_buffer_shm_names
from .traversal import (
    chunk_lower_bound,
    chunk_raw_batches,
    ring_copy_records,
    ring_lower_bound,
    ring_raw_batches,
)
from ..metadata.feed_registry import EXPIRED, ON_DISK, FeedRegistry
from ..metadata.feed_metadata import load_feed_metadata
from ..metadata.feed_schema import load_record_schema_for_feed
from ..network.path import DEFAULT_PORT, NetworkTarget, parse_target


class Reader:
    """Unified Deepwater reader.

    A Reader owns both readable surfaces for one feed:
    durable chunk files, tracked by FeedRegistry, and the live shared-memory
    ring tail. The feed metadata decides which paths exist.

    Public methods:
        describe(): metadata and schema summary.
        range(start, end): finite historical window, [start, end).
        range_batches(start, end): finite historical window in batches.
        stream(start=None): infinite live stream, optionally replaying from time.
        first_after(start): first record with key >= start.
        first_before(ts): last record with key <= ts.
        read_available(): stateful nonblocking tail read.
        state(): live ring cursor/header state when a ring exists.
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
            from ..network.client import RemoteReader

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

    def describe(self) -> dict:
        """Return feed metadata and schema fields."""
        clock_level = int(self.record_format.clock_level)
        fields = [field.to_dict() for field in self.record_format.fields]
        timestamp_fields = fields[:clock_level]
        ts_offset = self.record_format.primary_ts_offset
        storage = "ring" if self._uses_ring else "chunk"
        return {
            "feed_name": self.feed_name,
            "metadata": {
                "chunk_size_bytes": self._metadata.chunk_size_bytes,
                "ring_size_bytes": self._metadata.ring_size_bytes,
                "retention_hours": self._metadata.retention_hours,
                "persist": self._persistent,
                "segment_tracking": self._metadata.segment_tracking,
                "prefault_ring": self._metadata.prefault_ring,
                "storage": storage,
                "uses_ring": self._uses_ring,
            },
            "clock_level": clock_level,
            "timestamp_fields": timestamp_fields,
            "record_fmt": self.record_format.fmt,
            "record_size": self.record_format.record_size,
            "ts_offset": ts_offset,
            "fields": fields,
            "created_us": self._metadata.created_us,
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

        if meta.status == ON_DISK:
            chunk_path = self.data_dir / f"chunk_{chunk_id:08d}.bin"
            if not chunk_path.exists():
                meta.release()
                raise FileNotFoundError(f"Chunk file missing: {chunk_path}")
            chunk = Chunk.open(str(chunk_path))
        elif meta.status == EXPIRED:
            meta.release()
            raise RuntimeError(f"Chunk {chunk_id} is expired")
        else:
            meta.release()
            raise RuntimeError(f"Unknown chunk status {meta.status}")

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
            if status == EXPIRED:
                chunk_id -= 1
                continue
            self._open_chunk(chunk_id)
            return
        raise RuntimeError(f"Feed '{self.feed_name}' has no available chunks (all expired/deleted)")

    def _iter_durable_raw_range(
        self,
        start_us: int,
        end_us: Optional[int],
        ts_off: int,
        batch_records: Optional[int],
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
            write_pos = self._chunk_meta.write_pos
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

    def state(self) -> dict:
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

    def _raw_seq_batches(
        self,
        start_pos: int,
        earliest_seq: int,
        start_seq: int,
        end_seq: int,
        ts_off: int = 0,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        batch_records: int = 50_000,
    ):
        if batch_records <= 0:
            raise ValueError("batch_records must be positive")
        if end_seq <= start_seq:
            return

        if start_time is not None:
            start_seq = self._ring_lower_bound(
                self._ring.data, start_pos, earliest_seq, start_seq, end_seq,
                int(start_time), ts_off, self._rec_size, self._ring.data_size,
            )
        if end_time is not None:
            end_seq = self._ring_lower_bound(
                self._ring.data, start_pos, earliest_seq, start_seq, end_seq,
                int(end_time), ts_off, self._rec_size, self._ring.data_size,
            )
        if end_seq <= start_seq:
            return

        yield from self._ring_raw_batches(
            self._ring.data,
            start_pos,
            earliest_seq,
            start_seq,
            end_seq,
            self._rec_size,
            self._ring.data_size,
            int(batch_records),
        )

    def _stream_ring_raw(
        self,
        start_seq: int,
        ts_off: int = 0,
        start_time: Optional[int] = None,
    ) -> Iterator[memoryview]:
        ring = self._ring
        record_count = ring.record_count[0]
        earliest_seq = max(0, record_count - self._ring_record_capacity)
        records_read = max(start_seq, earliest_seq)
        records_read = min(records_read, record_count)
        start_pos = ring.start_pos[0]
        if start_time is not None and records_read < record_count:
            records_read = self._ring_lower_bound(
                ring.data,
                start_pos,
                earliest_seq,
                records_read,
                record_count,
                start_time,
                ts_off,
                self._rec_size,
                ring.data_size,
            )
        ring_size = ring.data_size
        read_pos = (
            (start_pos + ((records_read - earliest_seq) * self._rec_size)) % ring_size
            if records_read < record_count
            else ring.write_pos[0]
        )
        data = ring.data
        rec_sz = self._rec_size
        ring_record_capacity = self._ring_record_capacity

        while True:
            record_count = ring.record_count[0]
            earliest_live_seq = max(0, record_count - ring_record_capacity)
            if records_read < earliest_live_seq:
                records_read = earliest_live_seq
                read_pos = ring.start_pos[0]
            if records_read >= record_count:
                os.sched_yield()
                continue
            if read_pos == ring_size:
                read_pos = 0
            pos = read_pos
            read_pos += rec_sz
            records_read += 1
            yield data[pos:pos + rec_sz]

    def stream(
        self,
        start: Optional[int] = None,
        format: str = "tuple",
        ts_key: Optional[str] = None,
    ) -> Iterator:
        """Yield records forever.

        With start=None, streaming begins at the current live head. With start
        set, persistent feeds replay durable chunks first, then continue from
        the live ring tail. format is one of tuple, dict, numpy, or raw.
        """
        try:
            ts_off = self._primary_ts_off if ts_key is None else self._ts_offset_by_name[ts_key]
        except KeyError:
            raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {tuple(self._ts_offset_by_name)}") from None
        format_raw_record = raw_record_formatter(format, self._unpack, self._field_names, self._dtype)
        if self._ring is None:
            if self.feed_registry is None or self.feed_registry.get_latest_chunk_idx() is None:
                return
            raw_records = self._stream_durable_raw(start=start, ts_off=ts_off)
        else:
            ring = self._ring
            if start is None:
                raw_records = self._stream_ring_raw(ring.record_count[0], ts_off=ts_off)
            else:
                durable_seq = ring.durable_record_count[0]
                durable_last_ts = ring.durable_last_ts[0]
                if durable_seq > 0 and durable_last_ts and start <= durable_last_ts:
                    rec_size = self._rec_size
                    for batch in self._iter_durable_raw_range(
                        start,
                        durable_last_ts + 1,
                        ts_off=ts_off,
                        batch_records=50_000,
                    ):
                        data = memoryview(batch).cast("B")
                        for pos in range(0, data.nbytes, rec_size):
                            yield format_raw_record(data[pos:pos + rec_size])
                raw_records = self._stream_ring_raw(durable_seq, ts_off=ts_off, start_time=start)

        for raw in raw_records:
            yield format_raw_record(raw)

    def range(
        self,
        start: int,
        end: int,
        format: str = "tuple",
        ts_key: Optional[str] = None,
    ) -> list | memoryview | np.ndarray:
        """Return all records in [start, end).

        Reads durable chunks first, then the live ring tail when present, and
        formats once from raw record bytes.
        """
        try:
            ts_off = self._primary_ts_off if ts_key is None else self._ts_offset_by_name[ts_key]
        except KeyError:
            raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {tuple(self._ts_offset_by_name)}") from None
        if end <= start:
            return format_empty_batch(format, self._dtype)
        ring = self._ring
        if (
            ring is not None
            and (
                not self._persistent
                or (ring.durable_record_count[0] > 0 and start > ring.durable_last_ts[0])
            )
        ):
            record_count = ring.record_count[0]
            durable_seq = ring.durable_record_count[0]
            earliest_seq = max(0, record_count - self._ring_record_capacity)
            start_seq = max(durable_seq, earliest_seq)
            if record_count <= start_seq:
                return format_empty_batch(format, self._dtype)
            start_pos = ring.start_pos[0]
            start_seq = self._ring_lower_bound(
                ring.data, start_pos, earliest_seq, start_seq, record_count,
                start, ts_off, self._rec_size, ring.data_size,
            )
            end_seq = self._ring_lower_bound(
                ring.data, start_pos, earliest_seq, start_seq, record_count,
                end, ts_off, self._rec_size, ring.data_size,
            )
            if end_seq <= start_seq:
                return format_empty_batch(format, self._dtype)
            pos = (start_pos + ((start_seq - earliest_seq) * self._rec_size)) % ring.data_size
            byte_len = (end_seq - start_seq) * self._rec_size
            raw = bytes(ring.data[pos:pos + byte_len])
            return format_raw_batch(raw, format, self._unpack, self._rec_size, self._field_names, self._dtype)

        data = bytearray()
        if self.feed_registry is not None and self.feed_registry.get_latest_chunk_idx() is not None:
            for chunk_id in self.feed_registry.get_chunks_in_range(start, end, qoff=ts_off):
                try:
                    self._open_chunk(chunk_id)
                except FileNotFoundError:
                    continue
                buf = self._chunk.buffer
                write_pos = self._chunk_meta.write_pos
                pos = self._chunk_lower_bound(buf, start, write_pos, self._rec_size, ts_off)
                end_pos = self._chunk_lower_bound(buf, end, write_pos, self._rec_size, ts_off)
                if end_pos > pos:
                    data.extend(buf[pos:end_pos])

        if ring is not None:
            record_count = ring.record_count[0]
            durable_seq = ring.durable_record_count[0]
            if record_count > durable_seq:
                earliest_seq = max(0, record_count - self._ring_record_capacity)
                start_seq = max(durable_seq, earliest_seq)
                if record_count > start_seq:
                    start_pos = ring.start_pos[0]
                    start_seq = self._ring_lower_bound(
                        ring.data, start_pos, earliest_seq, start_seq, record_count,
                        start, ts_off, self._rec_size, ring.data_size,
                    )
                    end_seq = self._ring_lower_bound(
                        ring.data, start_pos, earliest_seq, start_seq, record_count,
                        end, ts_off, self._rec_size, ring.data_size,
                    )
                    if end_seq > start_seq:
                        pos = (start_pos + ((start_seq - earliest_seq) * self._rec_size)) % ring.data_size
                        byte_len = (end_seq - start_seq) * self._rec_size
                        data.extend(ring.data[pos:pos + byte_len])

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
            readers = BlobSidecarReaders(self.base_path, self.feed_name)
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

    def blob(self, row, *, sidecar: str) -> bytes:
        """Read one blob sidecar payload referenced by a formatted index row."""
        readers = self._ensure_blob_readers()
        return readers[sidecar].read(self.blob_ref(row, sidecar=sidecar))

    def blobs(self, rows, *, sidecar: str) -> list[bytes]:
        """Read blob sidecar payloads for formatted index rows."""
        return [self.blob(row, sidecar=sidecar) for row in rows]

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
        """Yield raw record batches for [start, end)."""
        if end <= start:
            return
        if batch_records <= 0:
            raise ValueError("batch_records must be positive")

        if self._ring is None:
            if self.feed_registry is not None and self.feed_registry.get_latest_chunk_idx() is not None:
                yield from self._iter_durable_raw_range(
                    start,
                    end,
                    ts_off=ts_off,
                    batch_records=batch_records,
                )
            return

        ring = self._ring
        durable_seq = ring.durable_record_count[0]
        if self.feed_registry is not None and self.feed_registry.get_latest_chunk_idx() is not None:
            yield from self._iter_durable_raw_range(
                start,
                end,
                ts_off=ts_off,
                batch_records=batch_records,
            )

        record_count = ring.record_count[0]
        if record_count <= durable_seq:
            return

        earliest_seq = max(0, record_count - self._ring_record_capacity)
        start_seq = max(durable_seq, earliest_seq)
        if record_count <= start_seq:
            return
        yield from self._raw_seq_batches(
            ring.start_pos[0],
            earliest_seq,
            start_seq,
            record_count,
            ts_off=ts_off,
            start_time=start,
            end_time=end,
            batch_records=batch_records,
        )

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
        self,
        start: int,
        format: str = "tuple",
        ts_key: Optional[str] = None,
    ):
        """Return the first record with key >= start, or None."""
        try:
            ts_off = self._primary_ts_off if ts_key is None else self._ts_offset_by_name[ts_key]
        except KeyError:
            raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {tuple(self._ts_offset_by_name)}") from None
        dtype = self._dtype

        if self.feed_registry is not None:
            for chunk_id in self.feed_registry.get_chunks_after(start, qoff=ts_off):
                self._open_chunk(chunk_id)
                pos = self._chunk_lower_bound(
                    self._chunk.buffer, start, self._chunk_meta.write_pos, self._rec_size, ts_off
                )
                if pos < self._chunk_meta.write_pos:
                    return format_record_at(
                        self._chunk.buffer, pos, format, self._unpack,
                        self._rec_size, self._field_names, dtype,
                    )
        if self._ring is not None:
            record_count = self._ring.record_count[0]
            earliest_seq = max(0, record_count - self._ring_record_capacity)
            start_seq = max(earliest_seq, self._ring.durable_record_count[0])
            if record_count > start_seq:
                start_pos = self._ring.start_pos[0]
                seq = self._ring_lower_bound(
                    self._ring.data, start_pos, earliest_seq, start_seq, record_count,
                    start, ts_off, self._rec_size, self._ring.data_size,
                )
                if seq < record_count:
                    pos = (start_pos + ((seq - earliest_seq) * self._rec_size)) % self._ring.data_size
                    return format_record_at(
                        self._ring.data, pos, format, self._unpack,
                        self._rec_size, self._field_names, dtype,
                    )
        return None

    def first_before(
        self,
        ts: int,
        format: str = "tuple",
        ts_key: Optional[str] = None,
    ):
        """Return the nearest record at or before ts, or None."""
        try:
            ts_off = self._primary_ts_off if ts_key is None else self._ts_offset_by_name[ts_key]
        except KeyError:
            raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {tuple(self._ts_offset_by_name)}") from None
        dtype = self._dtype

        if self._ring is not None:
            record_count = self._ring.record_count[0]
            earliest_seq = max(0, record_count - self._ring_record_capacity)
            start_seq = max(earliest_seq, self._ring.durable_record_count[0])
            if record_count > start_seq:
                start_pos = self._ring.start_pos[0]
                seq = self._ring_lower_bound(
                    self._ring.data, start_pos, earliest_seq, start_seq, record_count,
                    ts + 1, ts_off, self._rec_size, self._ring.data_size,
                ) - 1
                if seq >= start_seq:
                    pos = (start_pos + ((seq - earliest_seq) * self._rec_size)) % self._ring.data_size
                    return format_record_at(
                        self._ring.data, pos, format, self._unpack,
                        self._rec_size, self._field_names, dtype,
                    )
        if self.feed_registry is not None:
            for chunk_id in self.feed_registry.get_chunks_in_range_reverse(0, ts, qoff=ts_off):
                self._open_chunk(chunk_id)
                pos = self._chunk_lower_bound(
                    self._chunk.buffer, ts + 1, self._chunk_meta.write_pos, self._rec_size, ts_off
                ) - self._rec_size
                if pos >= 0:
                    return format_record_at(
                        self._chunk.buffer, pos, format, self._unpack,
                        self._rec_size, self._field_names, dtype,
                    )
        return None
    
    def _read_available_ring(self, max_records=None, format: str = "tuple"):
        dtype = self._dtype
        ring = self._ring
        record_count = ring.record_count[0]
        tail_seq = self._tail_read_seq
        if tail_seq is None:
            self._tail_read_seq = record_count
            return format_empty_batch(format, dtype)
        if record_count <= tail_seq:
            return format_empty_batch(format, dtype)

        earliest_seq = max(0, record_count - self._ring_record_capacity)
        if tail_seq < earliest_seq:
            tail_seq = earliest_seq

        end_seq = record_count
        if max_records is not None:
            end_seq = min(end_seq, tail_seq + int(max_records))
        if end_seq <= tail_seq:
            return format_empty_batch(format, dtype)

        self._tail_read_seq = end_seq
        n_records = end_seq - tail_seq
        start_pos = ring.start_pos[0]
        pos = (start_pos + ((tail_seq - earliest_seq) * self._rec_size)) % ring.data_size
        raw = self._ring_copy_records(
            ring.data, pos, n_records, self._rec_size, ring.data_size
        )
        return format_raw_batch(raw, format, self._unpack, self._rec_size, self._field_names, dtype)

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
        if self.feed_registry is not None:
            self.feed_registry.close()
            self.feed_registry = None
        if self._ring is not None:
            self._ring.close(unlink=False)
            self._ring = None
