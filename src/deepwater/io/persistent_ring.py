from __future__ import annotations

import logging
import os
import struct
import sys
import time
from pathlib import Path
from typing import Iterator, Optional

import numpy as np

from .reader import ChunkReader
from .ring import RingBuffer, RingReader, RingWriter, _yield_cpu, ring_buffer_shm_names
from .writer import ChunkWriter


log = logging.getLogger("dw.persistent_ring")

_HEARTBEAT_INTERVAL_S = 0.25
_U64 = struct.Struct("<Q")
_PERSIST_EVENT_FEED = "dw-persist-events"


def _persist_event_spec() -> dict:
    return {
        "feed_name": _PERSIST_EVENT_FEED,
        "mode": "UF",
        "fields": [
            {"name": "event_ts_us", "type": "uint64"},
            {"name": "feed_name", "type": "bytes64"},
            {"name": "records", "type": "uint64"},
            {"name": "bytes_written", "type": "uint64"},
            {"name": "durable_seq_start", "type": "uint64"},
            {"name": "durable_seq_end", "type": "uint64"},
            {"name": "snapshot_seq", "type": "uint64"},
            {"name": "backlog_before", "type": "uint64"},
            {"name": "backlog_after", "type": "uint64"},
            {"name": "chunk_id_before", "type": "uint64"},
            {"name": "chunk_id_after", "type": "uint64"},
            {"name": "elapsed_us", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": False,
        "segment_tracking": False,
        "ring_size_mb": 16,
        "chunk_size_mb": 4,
    }


def _pack_feed_name(value: str, size: int = 64) -> bytes:
    return value.encode("utf-8", errors="replace")[:size]


def _configure_persister_logging(base_path: str) -> None:
    log_path = Path(base_path) / "deepwater.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    target = str(log_path.resolve())
    for handler in log.handlers:
        if isinstance(handler, logging.FileHandler):
            try:
                if str(Path(handler.baseFilename).resolve()) == target:
                    log.setLevel(logging.INFO)
                    log.propagate = False
                    return
            except Exception:
                continue

    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s pid=%(process)d: %(message)s"
        )
    )
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False


def _iter_ring_window(
    ring: RingBuffer,
    record_size: int,
    start_seq: int,
    end_seq: int,
    *,
    earliest_live_seq: int,
    start_pos: int,
    usable_bytes: int,
) -> Iterator[memoryview]:
    if end_seq <= start_seq:
        return
    if usable_bytes <= 0:
        return
    seq = max(int(start_seq), int(earliest_live_seq))
    end_seq = int(end_seq)
    pos = (int(start_pos) + ((seq - int(earliest_live_seq)) * record_size)) % int(usable_bytes)
    while seq < end_seq:
        if pos + record_size > usable_bytes:
            pos = 0
        run_records = min(end_seq - seq, (usable_bytes - pos) // record_size)
        if run_records <= 0:
            pos = 0
            continue
        off = pos
        end = off + (run_records * record_size)
        yield ring.data[off:end]
        seq += run_records
        pos += run_records * record_size
        if pos >= usable_bytes:
            pos = 0


def _pending_window(ring: RingBuffer, record_size: int) -> tuple[int, int, int, int, int, int, int]:
    _, start_pos, _, _, record_count, durable_count, _, overrun_count, lost_records = ring.header()
    snapshot_count = int(record_count)
    durable_count = int(durable_count)
    capacity_records = max(1, ring.data_size // record_size)
    usable_bytes = capacity_records * record_size
    earliest_live = max(0, snapshot_count - capacity_records)
    return (
        snapshot_count,
        durable_count,
        earliest_live,
        int(start_pos),
        int(usable_bytes),
        int(overrun_count),
        int(lost_records),
    )


def _target_flush_records(ring: RingBuffer, writer: ChunkWriter, record_size: int) -> tuple[int, int]:
    max_batch_records = max(1, int(writer.feed_config["chunk_size_bytes"]) // record_size)
    return max_batch_records, max_batch_records


def _persist_feed(
    ring: RingBuffer,
    writer: ChunkWriter,
    record_size: int,
    *,
    event_writer=None,
) -> bool:
    _, _, _, _, _, _, durable_last_ts, _, _ = ring.header()
    (
        snapshot_count,
        durable_count,
        earliest_live,
        start_pos,
        usable_bytes,
        overrun_count,
        lost_records,
    ) = _pending_window(ring, record_size)
    if durable_count < earliest_live:
        lost = earliest_live - durable_count
        overrun_count += 1
        lost_records += lost
        durable_count = earliest_live
        ring.update_durable_header(
            durable_record_count=durable_count,
            overrun_count=overrun_count,
            lost_records=lost_records,
        )
    if snapshot_count <= durable_count:
        return False

    _, max_batch_records = _target_flush_records(ring, writer, record_size)
    end_seq = min(snapshot_count, durable_count + max_batch_records)
    last_batch_ts = durable_last_ts
    bytes_written = 0
    records_written = 0
    first_batch_ts: int | None = None
    chunk_id_before = int(writer.current_chunk_id)
    started_ns = time.perf_counter_ns()
    writer.begin_chunk_commit()
    try:
        for blob in _iter_ring_window(
            ring,
            record_size,
            durable_count,
            end_seq,
            earliest_live_seq=earliest_live,
            start_pos=start_pos,
            usable_bytes=usable_bytes,
        ):
            if not blob:
                continue
            if first_batch_ts is None:
                first_batch_ts = _U64.unpack_from(blob, 0)[0]
            writer.write_batch_bytes(blob)
            bytes_written += len(blob)
            records_written += len(blob) // record_size
            last_batch_ts = _U64.unpack_from(blob, len(blob) - record_size)[0]
        writer.finish_chunk_commit()
    except Exception:
        writer.finish_chunk_commit()
        raise

    ring.update_durable_header(
        durable_record_count=end_seq,
        durable_last_ts=last_batch_ts,
    )
    elapsed_us = (time.perf_counter_ns() - started_ns) // 1_000
    chunk_id_after = int(writer.current_chunk_id)
    backlog_before = snapshot_count - durable_count
    backlog_after = snapshot_count - end_seq
    log.info(
        "persisted feed=%s records=%d bytes=%d durable_seq=%d->%d snapshot_seq=%d "
        "backlog_before=%d backlog_after=%d first_ts=%s last_ts=%d chunk_id=%d->%d elapsed_us=%d",
        writer.feed_name,
        records_written,
        bytes_written,
        durable_count,
        end_seq,
        snapshot_count,
        backlog_before,
        backlog_after,
        first_batch_ts,
        int(last_batch_ts),
        chunk_id_before,
        chunk_id_after,
        int(elapsed_us),
    )
    if event_writer is not None:
        try:
            event_writer.write_values(
                int(time.time_ns() // 1_000),
                _pack_feed_name(writer.feed_name),
                int(records_written),
                int(bytes_written),
                int(durable_count),
                int(end_seq),
                int(snapshot_count),
                int(backlog_before),
                int(backlog_after),
                int(chunk_id_before),
                int(chunk_id_after),
                int(elapsed_us),
            )
        except Exception:
            log.exception("persist event emit failed feed=%s", writer.feed_name)
    return True


def persistent_ring_persister_main(base_path: str) -> None:
    from deepwater import Platform

    _configure_persister_logging(base_path)
    platform = Platform(base_path)
    pid = os.getpid()
    if not platform.registry.claim_persistent_ring_owner(pid):
        log.info("persister exit: owner already claimed base_path=%s pid=%d", base_path, pid)
        return
    log.info("persister start base_path=%s pid=%d", base_path, pid)

    rings: dict[str, RingBuffer] = {}
    writers: dict[str, ChunkWriter] = {}
    record_sizes: dict[str, int] = {}
    last_heartbeat = 0.0
    persist_event_writer = None

    try:
        try:
            if not platform.feed_exists(_PERSIST_EVENT_FEED):
                platform.create_feed(_persist_event_spec())
            persist_event_writer = platform.create_writer(_PERSIST_EVENT_FEED)
        except Exception:
            log.exception("persist event feed init failed")
            persist_event_writer = None

        while True:
            now = time.monotonic()
            if now - last_heartbeat >= _HEARTBEAT_INTERVAL_S:
                if not platform.registry.heartbeat_persistent_ring_owner(pid):
                    break
                last_heartbeat = now

            did_work = False
            for feed_name in platform.list_feeds():
                try:
                    lifecycle = platform.lifecycle(feed_name) or {}
                    if not lifecycle.get("persist", False):
                        continue
                    record_format = platform.get_record_format(feed_name)

                    ring = rings.get(feed_name)
                    if ring is None:
                        try:
                            shm_names = ring_buffer_shm_names(platform.base_path, feed_name)
                            ring = RingBuffer(
                                feed_name,
                                data_size=int(record_format.get("ring_size_bytes") or (64 * 1024 * 1024)),
                                create=False,
                                shm_name=shm_names[0],
                                fallback_names=shm_names[1:],
                            )
                        except FileNotFoundError:
                            continue
                        rings[feed_name] = ring
                        record_sizes[feed_name] = int(record_format["record_size"])
                        log.info(
                            "attached ring feed=%s ring_size_bytes=%d record_size=%d",
                            feed_name,
                            int(record_format.get("ring_size_bytes") or (64 * 1024 * 1024)),
                            int(record_sizes[feed_name]),
                        )

                    writer = writers.get(feed_name)
                    if writer is None:
                        writer = ChunkWriter(platform, feed_name, segment_tracking=False)
                        writers[feed_name] = writer
                        log.info(
                            "opened chunk writer feed=%s chunk_id=%d chunk_size_bytes=%d",
                            feed_name,
                            int(writer.current_chunk_id),
                            int(writer.feed_config["chunk_size_bytes"]),
                        )

                    snapshot_count, durable_count, _, _, _, _, _ = _pending_window(
                        ring,
                        record_sizes[feed_name],
                    )
                    pending_records = snapshot_count - durable_count
                    if pending_records <= 0:
                        if ring.drain_requested():
                            ring.clear_drain_request()
                        continue

                    target_records, _ = _target_flush_records(ring, writer, record_sizes[feed_name])
                    if pending_records < target_records and not ring.drain_requested():
                        continue

                    if _persist_feed(
                        ring,
                        writer,
                        record_sizes[feed_name],
                        event_writer=persist_event_writer,
                    ):
                        did_work = True
                        snapshot_count, durable_count, _, _, _, _, _ = _pending_window(
                            ring,
                            record_sizes[feed_name],
                        )
                        if snapshot_count <= durable_count and ring.drain_requested():
                            ring.clear_drain_request()
                except Exception:
                    log.exception("persistent ring persister failed for %s", feed_name)
            if not did_work:
                _yield_cpu()
    finally:
        if persist_event_writer is not None:
            try:
                persist_event_writer.close()
            except Exception:
                pass
        for writer in writers.values():
            try:
                writer.close()
            except Exception:
                pass
        for ring in rings.values():
            try:
                ring.close(unlink=False)
            except Exception:
                pass
        try:
            platform.registry.release_persistent_ring_owner(pid)
        except Exception:
            pass
        try:
            platform.close()
        except Exception:
            pass
        log.info("persister stop base_path=%s pid=%d", base_path, pid)

class PersistentRingReader:
    def __init__(self, platform, feed_name: str):
        self.platform = platform
        self.feed_name = feed_name
        self.record_format = platform.get_record_format(feed_name)
        self._ring_reader: RingReader | None = None
        self._durable_reader: ChunkReader | None = None
        self._S = struct.Struct(self.record_format["fmt"])
        self._tail_read_seq: Optional[int] = None
        self._dtype = None

    def _get_durable_reader(self) -> ChunkReader:
        if self._durable_reader is None:
            self._durable_reader = ChunkReader(self.platform, self.feed_name)
        return self._durable_reader

    @staticmethod
    def _durable_not_ready(exc: Exception) -> bool:
        return isinstance(exc, (FileNotFoundError, ValueError, RuntimeError))

    def _get_ring_reader(self) -> RingReader:
        if self._ring_reader is None:
            self._ring_reader = RingReader(self.platform, self.feed_name)
        return self._ring_reader

    def _ring_state(self) -> Optional[dict]:
        try:
            return self._get_ring_reader().state()
        except FileNotFoundError:
            return None

    def _resolve_ts_key(self, ts_key: Optional[str]) -> int:
        clock_level = self.record_format.get("clock_level") or 1
        ts_fields = self.record_format.get("fields", [])[:clock_level]
        if ts_key is None:
            return ts_fields[0]["offset"] if ts_fields else 0
        for field in ts_fields:
            if field["name"] == ts_key:
                return field["offset"]
        raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {[f['name'] for f in ts_fields]}")

    def _read_durable_tuples(
        self,
        start: int,
        end: int,
        *,
        playback: bool = False,
        ts_key: Optional[str] = None,
    ) -> list[tuple]:
        if end <= start:
            return []
        try:
            return self._get_durable_reader().range(
                start,
                end,
                format="tuple",
                playback=playback,
                ts_key=ts_key,
            )
        except Exception as exc:
            if not self._durable_not_ready(exc):
                raise
            return []

    def _format_records(self, records: list[tuple], format: str):
        if format == "tuple":
            return records
        if format == "dict":
            field_names = self.field_names
            return [{field_names[i]: rec[i] for i in range(len(field_names))} for rec in records]
        if format == "numpy":
            return np.array(records, dtype=self.dtype) if records else np.array([], dtype=self.dtype)
        if format == "raw":
            if not records:
                return memoryview(b"")
            raw = bytearray(len(records) * self.record_size)
            for i, rec in enumerate(records):
                self._S.pack_into(raw, i * self.record_size, *rec)
            return memoryview(raw)
        raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")

    @property
    def format(self) -> str:
        return self.record_format["fmt"]

    @property
    def field_names(self) -> tuple:
        return tuple(
            f["name"]
            for f in self.record_format.get("fields", [])
            if not f.get("type", "").startswith("_") or f["name"] != "_"
        )

    @property
    def dtype(self):
        if self._dtype is None:
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
        return int(self.record_format["record_size"])

    def stream(
        self,
        start: Optional[int] = None,
        format: str = "tuple",
        ts_key: Optional[str] = None,
        playback: bool = False,
    ):
        ring_state = self._ring_state()
        ts_off = self._resolve_ts_key(ts_key)

        if ring_state is None:
            try:
                yield from self._get_durable_reader().stream(
                    start=start,
                    format=format,
                    ts_key=ts_key,
                    playback=playback,
                )
            except Exception as exc:
                if not self._durable_not_ready(exc):
                    raise
            return

        if start is None and not playback:
            yield from self._get_ring_reader().stream_from_seq(
                ring_state["record_count"],
                format=format,
                ts_off=ts_off,
            )
            return

        durable_seq = int(ring_state["durable_record_count"])
        durable_last_ts = int(ring_state["durable_last_ts"])
        if start is not None and durable_seq > 0 and durable_last_ts and start <= durable_last_ts:
            historical = self._read_durable_tuples(
                start,
                durable_last_ts + 1,
                playback=playback,
                ts_key=ts_key,
            )
            if format == "tuple":
                for record in historical:
                    yield record
            elif format == "dict":
                for record in self._format_records(historical, "dict"):
                    yield record
            elif format == "numpy":
                for record in self._format_records(historical, "numpy"):
                    yield record
            elif format == "raw":
                for record in historical:
                    raw = bytearray(self.record_size)
                    self._S.pack_into(raw, 0, *record)
                    yield memoryview(raw)
            else:
                raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")

        yield from self._get_ring_reader().stream_from_seq(
            durable_seq,
            format=format,
            ts_off=ts_off,
            start_time=start,
        )

    def range(
        self,
        start: int,
        end: int,
        format: str = "tuple",
        playback: bool = False,
        ts_key: Optional[str] = None,
    ):
        ring_state = self._ring_state()
        ts_off = self._resolve_ts_key(ts_key)
        durable_records: list[tuple] = []
        ring_records: list[tuple] = []

        if ring_state is None:
            return self._format_records(
                self._read_durable_tuples(start, end, playback=playback, ts_key=ts_key),
                format,
            )

        durable_seq = int(ring_state["durable_record_count"])
        durable_last_ts = int(ring_state["durable_last_ts"])
        if durable_seq > 0:
            durable_end = end if not durable_last_ts else min(end, durable_last_ts + 1)
            durable_records = self._read_durable_tuples(start, durable_end, playback=playback, ts_key=ts_key)
        if ring_state["record_count"] > durable_seq:
            ring_records = self._get_ring_reader().read_seq_range(
                durable_seq,
                ring_state["record_count"],
                format="tuple",
                ts_off=ts_off,
                start_time=start,
                end_time=end,
            )
        return self._format_records(durable_records + ring_records, format)

    def latest(self, seconds: float = 60.0, format: str = "tuple", ts_key: Optional[str] = None):
        ring_state = self._ring_state()
        if ring_state is None or ring_state["last_ts"] == 0:
            try:
                return self._get_durable_reader().latest(seconds=seconds, format=format, ts_key=ts_key)
            except Exception as exc:
                if not self._durable_not_ready(exc):
                    raise
                return [] if format != "raw" else memoryview(b"")
        ts_off = self._resolve_ts_key(ts_key)
        end_us = int(self._get_ring_reader()._latest_end_us(ts_off)) + 1
        if end_us <= 1:
            return [] if format != "raw" else memoryview(b"")
        start_us = end_us - int(seconds * 1_000_000)
        return self.range(start_us, end_us, format=format, ts_key=ts_key)

    def read_available(self, max_records=None, format: str = "tuple"):
        ring_state = self._ring_state()
        if ring_state is None:
            try:
                return self._get_durable_reader().read_available(max_records=max_records, format=format)
            except Exception as exc:
                if not self._durable_not_ready(exc):
                    raise
                return [] if format != "raw" else memoryview(b"")

        if self._tail_read_seq is None:
            self._tail_read_seq = int(ring_state["record_count"])
            return [] if format != "raw" else memoryview(b"")

        end_seq = int(ring_state["record_count"])
        if max_records is not None:
            end_seq = min(end_seq, self._tail_read_seq + int(max_records))
        records = self._get_ring_reader().read_seq_range(
            self._tail_read_seq,
            end_seq,
            format="tuple",
            ts_off=self._resolve_ts_key(None),
        )
        self._tail_read_seq = end_seq
        return self._format_records(records, format)

    def close(self) -> None:
        try:
            if self._durable_reader is not None:
                self._durable_reader.close()
        finally:
            if self._ring_reader is not None:
                self._ring_reader.close()
                self._ring_reader = None

    def __getattr__(self, name: str):
        return getattr(self._get_durable_reader(), name)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 1:
        print("usage: python -m deepwater.io.persistent_ring <base_path>", file=sys.stderr)
        return 2
    persistent_ring_persister_main(argv[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
