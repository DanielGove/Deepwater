"""
Per-feed segment metadata.

Segments are control-plane metadata only. They do not change reader behavior.
Storage backend is a fixed-record binary registry (`segments.reg`) with mmap.
"""
from __future__ import annotations

import mmap
import os
import struct
import time
from pathlib import Path
from typing import Optional

import orjson


USABLE_SEGMENT_STATUSES = ("closed", "crash_closed")

MAGIC = b"DWSEGv1\x00"
VERSION = 1
INITIAL_SIZE_BYTES = 512 * 1024
I64_NONE = -1

HEADER_STRUCT = struct.Struct("<8sIIIIQQqQ72x")
HEADER_SIZE = HEADER_STRUCT.size  # 128

ENTRY_STRUCT = struct.Struct("<Qqqqqq24s64sI116x")
ENTRY_SIZE = ENTRY_STRUCT.size  # 256

STATUS_MAX = 24
REASON_MAX = 64


def _encode_text(val: Optional[str], max_len: int) -> bytes:
    if val is None:
        return b"\x00" * max_len
    raw = str(val).encode("utf-8", "replace")
    return raw[:max_len].ljust(max_len, b"\x00")


def _decode_text(raw: bytes) -> Optional[str]:
    txt = raw.split(b"\x00", 1)[0].decode("utf-8", "replace").strip()
    return txt or None


def _to_i64(v: Optional[int]) -> int:
    return I64_NONE if v is None else int(v)


def _from_i64(v: int) -> Optional[int]:
    return None if v == I64_NONE else int(v)


class SegmentStore:
    """
    Store and query feed segments in `data/<feed>/segments.reg`.

    Design goals:
    - zero reader UX impact
    - minimal write-path overhead (persist only at segment open/close)
    - crash recovery support (close previously-open segment on writer restart)
    """

    __slots__ = (
        "path",
        "legacy_path",
        "feed_name",
        "_open_segment_id",
        "_open_start_us",
        "_open_last_ts",
        "_open_records",
    )

    def __init__(self, feed_dir: Path, feed_name: str):
        feed_dir = Path(feed_dir)
        self.path = feed_dir / "segments.reg"
        self.legacy_path = feed_dir / "segments.json"
        self.feed_name = feed_name
        self._open_segment_id: Optional[int] = None
        self._open_start_us: Optional[int] = None
        self._open_last_ts: Optional[int] = None
        self._open_records: int = 0
        self._ensure_initialized()

    # ------------------------------------------------------------------ IO
    def _init_file(self, size_bytes: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "wb") as f:
            f.truncate(size_bytes)
        fd = os.open(self.path, os.O_RDWR)
        try:
            mm = mmap.mmap(fd, size_bytes, mmap.MAP_SHARED, mmap.PROT_WRITE | mmap.PROT_READ)
            try:
                capacity = (size_bytes - HEADER_SIZE) // ENTRY_SIZE
                HEADER_STRUCT.pack_into(
                    mm,
                    0,
                    MAGIC,
                    VERSION,
                    HEADER_SIZE,
                    ENTRY_SIZE,
                    0,      # flags
                    0,      # segment_count
                    1,      # next_id
                    0,      # open_index (1-based, 0=none)
                    capacity,
                )
                mm.flush()
            finally:
                mm.close()
        finally:
            os.close(fd)

    def _open_mmap(self) -> tuple[int, mmap.mmap]:
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT)
        try:
            size = os.fstat(fd).st_size
            if size < HEADER_SIZE + ENTRY_SIZE:
                os.close(fd)
                self._init_file(INITIAL_SIZE_BYTES)
                fd = os.open(self.path, os.O_RDWR)
                size = os.fstat(fd).st_size
            mm = mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_WRITE | mmap.PROT_READ)
            return fd, mm
        except Exception:
            try:
                os.close(fd)
            except Exception:
                pass
            raise

    @staticmethod
    def _read_header(mm: mmap.mmap) -> dict:
        raw = HEADER_STRUCT.unpack_from(mm, 0)
        return {
            "magic": raw[0],
            "version": raw[1],
            "header_size": raw[2],
            "entry_size": raw[3],
            "flags": raw[4],
            "segment_count": raw[5],
            "next_id": raw[6],
            "open_index": raw[7],
            "capacity": raw[8],
        }

    @staticmethod
    def _write_header(
        mm: mmap.mmap,
        *,
        segment_count: int,
        next_id: int,
        open_index: int,
        capacity: int,
        flags: int = 0,
    ) -> None:
        HEADER_STRUCT.pack_into(
            mm,
            0,
            MAGIC,
            VERSION,
            HEADER_SIZE,
            ENTRY_SIZE,
            flags,
            int(segment_count),
            int(next_id),
            int(open_index),
            int(capacity),
        )

    @staticmethod
    def _entry_offset(index_1based: int) -> int:
        return HEADER_SIZE + ((index_1based - 1) * ENTRY_SIZE)

    @staticmethod
    def _read_entry(mm: mmap.mmap, index_1based: int) -> dict:
        offset = SegmentStore._entry_offset(index_1based)
        raw = ENTRY_STRUCT.unpack_from(mm, offset)
        return {
            "id": int(raw[0]),
            "start_us": _from_i64(raw[1]),
            "end_us": _from_i64(raw[2]),
            "records": _from_i64(raw[3]),
            "opened_at_us": _from_i64(raw[4]),
            "closed_at_us": _from_i64(raw[5]),
            "status": _decode_text(raw[6]),
            "close_reason": _decode_text(raw[7]),
            "flags": int(raw[8]),
        }

    @staticmethod
    def _write_entry(mm: mmap.mmap, index_1based: int, entry: dict) -> None:
        offset = SegmentStore._entry_offset(index_1based)
        ENTRY_STRUCT.pack_into(
            mm,
            offset,
            int(entry.get("id", 0)),
            _to_i64(entry.get("start_us")),
            _to_i64(entry.get("end_us")),
            _to_i64(entry.get("records")),
            _to_i64(entry.get("opened_at_us")),
            _to_i64(entry.get("closed_at_us")),
            _encode_text(entry.get("status"), STATUS_MAX),
            _encode_text(entry.get("close_reason"), REASON_MAX),
            int(entry.get("flags", 0)),
        )

    @staticmethod
    def _find_open_index(mm: mmap.mmap, segment_count: int) -> int:
        for idx in range(int(segment_count), 0, -1):
            e = SegmentStore._read_entry(mm, idx)
            if e.get("status") == "open":
                return idx
        return 0

    def _ensure_capacity(self, fd: int, mm: mmap.mmap, min_capacity: int) -> mmap.mmap:
        header = self._read_header(mm)
        capacity = int(header["capacity"])
        if capacity >= min_capacity:
            return mm

        size = os.fstat(fd).st_size
        new_size = max(size * 2, INITIAL_SIZE_BYTES)
        while ((new_size - HEADER_SIZE) // ENTRY_SIZE) < min_capacity:
            new_size *= 2

        mm.flush()
        mm.close()
        os.ftruncate(fd, new_size)
        new_mm = mmap.mmap(fd, new_size, mmap.MAP_SHARED, mmap.PROT_WRITE | mmap.PROT_READ)
        new_capacity = (new_size - HEADER_SIZE) // ENTRY_SIZE

        current = self._read_header(new_mm)
        self._write_header(
            new_mm,
            segment_count=current["segment_count"],
            next_id=current["next_id"],
            open_index=current["open_index"],
            capacity=new_capacity,
            flags=current["flags"],
        )
        new_mm.flush()
        return new_mm

    def _migrate_from_json(self) -> None:
        try:
            raw = orjson.loads(self.legacy_path.read_bytes())
        except Exception:
            return
        if not isinstance(raw, dict):
            return

        segs = raw.get("segments")
        if not isinstance(segs, list):
            return

        max_id = 0
        open_idx = 0
        count = len(segs)
        next_id = int(raw.get("next_id") or 1)
        required_size = HEADER_SIZE + max(1, count) * ENTRY_SIZE
        size = INITIAL_SIZE_BYTES
        while size < required_size:
            size *= 2

        self._init_file(size)
        fd, mm = self._open_mmap()
        try:
            for i, seg in enumerate(segs, start=1):
                if not isinstance(seg, dict):
                    continue
                sid = int(seg.get("id") or i)
                max_id = max(max_id, sid)
                status = seg.get("status")
                if status == "open":
                    open_idx = i
                self._write_entry(
                    mm,
                    i,
                    {
                        "id": sid,
                        "start_us": seg.get("start_us"),
                        "end_us": seg.get("end_us"),
                        "records": seg.get("records"),
                        "opened_at_us": seg.get("opened_at_us"),
                        "closed_at_us": seg.get("closed_at_us"),
                        "status": status,
                        "close_reason": seg.get("close_reason"),
                        "flags": 0,
                    },
                )

            self._write_header(
                mm,
                segment_count=count,
                next_id=max(next_id, max_id + 1),
                open_index=open_idx,
                capacity=(os.fstat(fd).st_size - HEADER_SIZE) // ENTRY_SIZE,
                flags=0,
            )
            mm.flush()
        finally:
            mm.close()
            os.close(fd)

    def _ensure_initialized(self) -> None:
        if self.path.exists():
            fd, mm = self._open_mmap()
            try:
                header = self._read_header(mm)
                if (
                    header["magic"] == MAGIC
                    and int(header["version"]) == VERSION
                    and int(header["header_size"]) == HEADER_SIZE
                    and int(header["entry_size"]) == ENTRY_SIZE
                    and int(header["capacity"]) > 0
                ):
                    return
            finally:
                mm.close()
                os.close(fd)
            # Invalid/corrupt file: recreate fresh.
            self._init_file(INITIAL_SIZE_BYTES)
            return

        if self.legacy_path.exists():
            self._migrate_from_json()
            if self.path.exists():
                return

        self._init_file(INITIAL_SIZE_BYTES)

    # ------------------------------------------------------------------ lifecycle
    def recover_open_segment(self, last_ts_level1: Optional[int]) -> bool:
        """
        Close previously-open segment (if any) on writer restart.
        Returns True if an open segment was recovered.
        """
        self._ensure_initialized()
        fd, mm = self._open_mmap()
        try:
            header = self._read_header(mm)
            open_idx = int(header["open_index"])
            count = int(header["segment_count"])
            if open_idx <= 0 or open_idx > count:
                open_idx = self._find_open_index(mm, count)
            if open_idx <= 0:
                self._open_segment_id = None
                self._open_start_us = None
                self._open_last_ts = None
                self._open_records = 0
                return False

            seg = self._read_entry(mm, open_idx)
            now_us = time.time_ns() // 1_000
            seg["closed_at_us"] = now_us
            seg["close_reason"] = "writer_recovery"

            start_us = seg.get("start_us")
            if last_ts_level1 is None and start_us is None:
                seg["status"] = "invalid_empty"
                seg["end_us"] = None
            else:
                if start_us is None:
                    seg["start_us"] = int(last_ts_level1)
                    start_us = seg["start_us"]
                end_us = int(last_ts_level1) if last_ts_level1 is not None else int(start_us)
                seg["end_us"] = end_us
                seg["status"] = "crash_closed"

            if seg.get("records", 0) == 0:
                seg["records"] = None

            self._write_entry(mm, open_idx, seg)
            self._write_header(
                mm,
                segment_count=header["segment_count"],
                next_id=header["next_id"],
                open_index=0,
                capacity=header["capacity"],
                flags=header["flags"],
            )
            mm.flush()
            self._open_segment_id = None
            self._open_start_us = None
            self._open_last_ts = None
            self._open_records = 0
            return True
        finally:
            mm.close()
            os.close(fd)

    def _open_segment(self, start_us: int) -> int:
        self._ensure_initialized()
        fd, mm = self._open_mmap()
        try:
            header = self._read_header(mm)
            count = int(header["segment_count"])
            open_idx = int(header["open_index"])
            if open_idx <= 0 or open_idx > count:
                open_idx = self._find_open_index(mm, count)
            if open_idx > 0:
                seg = self._read_entry(mm, open_idx)
                seg_id = int(seg["id"])
                self._open_segment_id = seg_id
                self._open_start_us = int(seg.get("start_us") or start_us)
                self._open_last_ts = None
                rec = seg.get("records")
                self._open_records = 0 if rec is None else int(rec)
                return seg_id

            next_count = count + 1
            mm = self._ensure_capacity(fd, mm, next_count)
            header = self._read_header(mm)
            seg_id = int(header["next_id"])
            now_us = time.time_ns() // 1_000
            self._write_entry(
                mm,
                next_count,
                {
                    "id": seg_id,
                    "start_us": int(start_us),
                    "end_us": None,
                    "records": 0,
                    "opened_at_us": now_us,
                    "closed_at_us": None,
                    "status": "open",
                    "close_reason": None,
                    "flags": 0,
                },
            )
            self._write_header(
                mm,
                segment_count=next_count,
                next_id=seg_id + 1,
                open_index=next_count,
                capacity=header["capacity"],
                flags=header["flags"],
            )
            mm.flush()
            self._open_segment_id = seg_id
            self._open_start_us = int(start_us)
            self._open_last_ts = None
            self._open_records = 0
            return seg_id
        finally:
            mm.close()
            os.close(fd)

    def note_write(self, ts_level1: int, records: int = 1) -> None:
        ts = int(ts_level1)
        if self._open_segment_id is None:
            self._open_segment(ts)
        self._open_last_ts = ts
        self._open_records += int(records)

    def note_batch(self, start_ts_level1: int, end_ts_level1: int, records: int) -> None:
        start_ts = int(start_ts_level1)
        end_ts = int(end_ts_level1)
        if self._open_segment_id is None:
            self._open_segment(start_ts)
        self._open_last_ts = end_ts
        self._open_records += int(records)

    def close_open_segment(self, reason: str = "writer_close") -> bool:
        if self._open_segment_id is None:
            return False

        self._ensure_initialized()
        fd, mm = self._open_mmap()
        try:
            header = self._read_header(mm)
            count = int(header["segment_count"])
            open_idx = int(header["open_index"])
            if open_idx <= 0 or open_idx > count:
                open_idx = self._find_open_index(mm, count)

            if open_idx <= 0:
                self._open_segment_id = None
                self._open_start_us = None
                self._open_last_ts = None
                self._open_records = 0
                return False

            seg = self._read_entry(mm, open_idx)
            end_us = self._open_last_ts
            start_us = self._open_start_us or seg.get("start_us")
            seg["start_us"] = start_us
            seg["end_us"] = end_us
            seg["records"] = int(self._open_records) if self._open_records > 0 else 0
            seg["closed_at_us"] = time.time_ns() // 1_000
            seg["close_reason"] = reason

            if start_us is None or end_us is None:
                seg["status"] = "invalid_empty"
            else:
                seg["status"] = "closed"

            self._write_entry(mm, open_idx, seg)
            self._write_header(
                mm,
                segment_count=header["segment_count"],
                next_id=header["next_id"],
                open_index=0,
                capacity=header["capacity"],
                flags=header["flags"],
            )
            mm.flush()
            self._open_segment_id = None
            self._open_start_us = None
            self._open_last_ts = None
            self._open_records = 0
            return True
        finally:
            mm.close()
            os.close(fd)

    # ------------------------------------------------------------------ query
    def list_segments(self, status: Optional[str] = None) -> list[dict]:
        self._ensure_initialized()
        fd, mm = self._open_mmap()
        try:
            header = self._read_header(mm)
            count = int(header["segment_count"])
            segments: list[dict] = []
            for idx in range(1, count + 1):
                seg = self._read_entry(mm, idx)
                seg_doc = {
                    "id": seg["id"],
                    "feed_name": self.feed_name,
                    "status": seg.get("status"),
                    "start_us": seg.get("start_us"),
                    "end_us": seg.get("end_us"),
                    "records": seg.get("records"),
                    "opened_at_us": seg.get("opened_at_us"),
                    "closed_at_us": seg.get("closed_at_us"),
                    "close_reason": seg.get("close_reason"),
                }
                segments.append(seg_doc)

            if status is None or status == "all":
                return segments
            if status == "usable":
                return [s for s in segments if s.get("status") in USABLE_SEGMENT_STATUSES]
            return [s for s in segments if s.get("status") == status]
        finally:
            mm.close()
            os.close(fd)

    def suggested_timestamp_range(self) -> Optional[tuple[int, int]]:
        usable = self.list_segments(status="usable")
        if not usable:
            return None

        starts = [int(s["start_us"]) for s in usable if s.get("start_us") is not None]
        ends = [int(s["end_us"]) for s in usable if s.get("end_us") is not None]
        if not starts or not ends:
            return None
        return min(starts), max(ends)
