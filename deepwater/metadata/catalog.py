from __future__ import annotations

from pathlib import Path

from ..metadata.feed_registry import FeedRegistry, UINT64_MAX
from .discovery import describe_feed, list_segments
from .feed_metadata import list_feeds, load_feed_metadata


def _chunk_coverage(base_path: Path, feed_name: str) -> dict:
    reg_path = base_path / "data" / feed_name / f"{feed_name}.reg"
    if not reg_path.exists():
        return {
            "feed_name": feed_name,
            "source": "chunks",
            "start_us": None,
            "end_us": None,
            "duration_us": 0,
            "records": 0,
        }

    registry = FeedRegistry(reg_path, mode="r")
    try:
        latest = int(registry.get_latest_chunk_idx() or 0)
        start_us = None
        end_us = None
        records = 0
        for chunk_id in range(1, latest + 1):
            meta = registry.get_chunk_metadata(chunk_id)
            if meta is None:
                continue
            try:
                qmin = int(meta.get_qmin(0))
                qmax = int(meta.get_qmax(0))
                n = int(meta.num_records)
            finally:
                meta.release()
            if n <= 0 or qmin == UINT64_MAX or qmax == 0 or qmax < qmin:
                continue
            records += n
            start_us = qmin if start_us is None else min(start_us, qmin)
            end_us = qmax if end_us is None else max(end_us, qmax)

        if start_us is None or end_us is None:
            return {
                "feed_name": feed_name,
                "source": "chunks",
                "start_us": None,
                "end_us": None,
                "duration_us": 0,
                "records": 0,
            }
        return {
            "feed_name": feed_name,
            "source": "chunks",
            "start_us": int(start_us),
            "end_us": int(end_us),
            "duration_us": int(end_us) - int(start_us) + 1,
            "records": int(records),
        }
    finally:
        registry.close()


def feed_coverage(base_path, feed_name: str):
    base = Path(base_path)
    feed_metadata = load_feed_metadata(base, feed_name)
    segment_tracking = True if feed_metadata is None else feed_metadata.segment_tracking

    if segment_tracking:
        segments = list_segments(base, feed_name, status="usable")
        if segments:
            start_us = min(int(seg["start_us"]) for seg in segments)
            end_us = max(int(seg["end_us"]) for seg in segments)
            records = sum(int(seg.get("records") or 0) for seg in segments)
            return {
                "feed_name": feed_name,
                "source": "segments",
                "start_us": start_us,
                "end_us": end_us,
                "duration_us": end_us - start_us + 1,
                "records": records,
            }

    return _chunk_coverage(base, feed_name)


def catalog(base_path, include_fields: bool = False):
    base = Path(base_path)
    feeds = []
    for entry in list_feeds(base):
        name = entry["feed_name"] if isinstance(entry, dict) else str(entry)
        try:
            desc = describe_feed(base, name)
        except Exception:
            continue
        item = {
            "feed_name": name,
            "clock_level": int(desc.get("clock_level") or 1),
            "record_size": int(desc.get("record_size") or 0),
            "coverage": feed_coverage(base_path, name),
        }
        if include_fields:
            item["fields"] = list(desc.get("fields", []))
        feeds.append(item)
    return {
        "base_path": str(Path(base_path)),
        "feeds": feeds,
    }
