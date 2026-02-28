"""
Per-feed segment metadata.

Segments are control-plane metadata only. They do not change reader behavior.
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Optional

import orjson


USABLE_SEGMENT_STATUSES = ("closed", "crash_closed")


class SegmentStore:
    """
    Store and query feed segments in `data/<feed>/segments.json`.

    Design goals:
    - zero reader UX impact
    - minimal write-path overhead (persist only at segment open/close)
    - crash recovery support (close previously-open segment on writer restart)
    """

    __slots__ = (
        "path",
        "feed_name",
        "_open_segment_id",
        "_open_start_us",
        "_open_last_ts",
        "_open_records",
    )

    def __init__(self, feed_dir: Path, feed_name: str):
        self.path = Path(feed_dir) / "segments.json"
        self.feed_name = feed_name
        self._open_segment_id: Optional[int] = None
        self._open_start_us: Optional[int] = None
        self._open_last_ts: Optional[int] = None
        self._open_records: int = 0

    # ------------------------------------------------------------------ IO
    def _default_doc(self) -> dict:
        return {
            "format_version": 1,
            "feed_name": self.feed_name,
            "next_id": 1,
            "segments": [],
        }

    def _load(self) -> dict:
        if not self.path.exists():
            return self._default_doc()
        try:
            raw = orjson.loads(self.path.read_bytes())
        except Exception:
            # Corrupt metadata should not block writers. Start fresh doc.
            return self._default_doc()
        if not isinstance(raw, dict):
            return self._default_doc()
        if "segments" not in raw or "next_id" not in raw:
            return self._default_doc()
        return raw

    def _save(self, doc: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_bytes(orjson.dumps(doc, option=orjson.OPT_INDENT_2))
        os.replace(tmp, self.path)

    @staticmethod
    def _find_open_idx(doc: dict) -> Optional[int]:
        segs = doc.get("segments", [])
        for idx in range(len(segs) - 1, -1, -1):
            if segs[idx].get("status") == "open":
                return idx
        return None

    # ------------------------------------------------------------------ lifecycle
    def recover_open_segment(self, last_ts_level1: Optional[int]) -> bool:
        """
        Close previously-open segment (if any) on writer restart.
        Returns True if an open segment was recovered.
        """
        doc = self._load()
        idx = self._find_open_idx(doc)
        if idx is None:
            self._open_segment_id = None
            self._open_start_us = None
            self._open_last_ts = None
            self._open_records = 0
            return False

        seg = doc["segments"][idx]
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

        # record count from crashed process may be unknown
        if seg.get("records", 0) == 0:
            seg["records"] = None

        self._save(doc)
        self._open_segment_id = None
        self._open_start_us = None
        self._open_last_ts = None
        self._open_records = 0
        return True

    def _open_segment(self, start_us: int) -> int:
        doc = self._load()
        idx = self._find_open_idx(doc)
        if idx is not None:
            seg = doc["segments"][idx]
            seg_id = int(seg["id"])
            self._open_segment_id = seg_id
            self._open_start_us = int(seg.get("start_us") or start_us)
            self._open_last_ts = None
            self._open_records = int(seg.get("records") or 0)
            return seg_id

        seg_id = int(doc.get("next_id", 1))
        doc["next_id"] = seg_id + 1
        doc["segments"].append(
            {
                "id": seg_id,
                "feed_name": self.feed_name,
                "status": "open",
                "start_us": int(start_us),
                "end_us": None,
                "records": 0,
                "opened_at_us": time.time_ns() // 1_000,
                "closed_at_us": None,
                "close_reason": None,
            }
        )
        self._save(doc)
        self._open_segment_id = seg_id
        self._open_start_us = int(start_us)
        self._open_last_ts = None
        self._open_records = 0
        return seg_id

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

        doc = self._load()
        idx = self._find_open_idx(doc)
        if idx is None:
            self._open_segment_id = None
            self._open_start_us = None
            self._open_last_ts = None
            self._open_records = 0
            return False

        seg = doc["segments"][idx]
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
            seg["status"] = "closed" if reason == "writer_close" else "crash_closed"

        self._save(doc)
        self._open_segment_id = None
        self._open_start_us = None
        self._open_last_ts = None
        self._open_records = 0
        return True

    # ------------------------------------------------------------------ query
    def list_segments(self, status: Optional[str] = None) -> list[dict]:
        doc = self._load()
        segments = list(doc.get("segments", []))
        if status is None or status == "all":
            return segments
        if status == "usable":
            return [s for s in segments if s.get("status") in USABLE_SEGMENT_STATUSES]
        return [s for s in segments if s.get("status") == status]

    def suggested_timestamp_range(self) -> Optional[tuple[int, int]]:
        usable = self.list_segments(status="usable")
        if not usable:
            return None

        starts = [int(s["start_us"]) for s in usable if s.get("start_us") is not None]
        ends = [int(s["end_us"]) for s in usable if s.get("end_us") is not None]
        if not starts or not ends:
            return None
        return min(starts), max(ends)
