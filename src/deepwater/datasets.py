"""
Utilities for computing contiguous multi-feed dataset windows.
"""
from __future__ import annotations

import time
from typing import Iterable


Interval = tuple[int, int]


def normalize_intervals(intervals: Iterable[Interval], *, merge_touching: bool = True) -> list[Interval]:
    """
    Sort and merge overlapping intervals.
    If merge_touching=True, [a,b] and [b+1,c] are merged into [a,c].
    """
    arr = sorted((int(s), int(e)) for s, e in intervals if s is not None and e is not None and int(s) <= int(e))
    if not arr:
        return []

    out: list[Interval] = [arr[0]]
    for s, e in arr[1:]:
        ps, pe = out[-1]
        if s <= pe or (merge_touching and s == pe + 1):
            out[-1] = (ps, max(pe, e))
        else:
            out.append((s, e))
    return out


def intersect_intervals(left: list[Interval], right: list[Interval]) -> list[Interval]:
    """Intersect two normalized interval lists."""
    i = 0
    j = 0
    out: list[Interval] = []
    while i < len(left) and j < len(right):
        ls, le = left[i]
        rs, re = right[j]
        s = max(ls, rs)
        e = min(le, re)
        if s <= e:
            out.append((s, e))
        if le < re:
            i += 1
        else:
            j += 1
    return out


def common_intervals(interval_sets: list[list[Interval]], *, merge_touching: bool = True) -> list[Interval]:
    """Return contiguous intersections across all interval sets."""
    if not interval_sets:
        return []

    cur = normalize_intervals(interval_sets[0], merge_touching=merge_touching)
    for nxt in interval_sets[1:]:
        cur = intersect_intervals(cur, normalize_intervals(nxt, merge_touching=merge_touching))
        if not cur:
            return []
    return normalize_intervals(cur, merge_touching=merge_touching)


def with_duration(intervals: Iterable[Interval], *, min_duration_us: int = 0) -> list[dict]:
    out: list[dict] = []
    min_d = int(min_duration_us)
    for s, e in intervals:
        duration = int(e) - int(s) + 1
        if duration >= min_d:
            out.append({"start_us": int(s), "end_us": int(e), "duration_us": duration})
    return out


def recommend_train_validation(intervals: list[dict], train_ratio: float = 0.8) -> dict | None:
    """
    Pick longest interval and split into train/validation contiguous windows.
    Returns None if no valid split is possible.
    """
    if not intervals:
        return None
    if not (0.0 < train_ratio < 1.0):
        raise ValueError("train_ratio must be between 0 and 1")

    best = max(intervals, key=lambda x: int(x["duration_us"]))
    start_us = int(best["start_us"])
    end_us = int(best["end_us"])
    duration = int(best["duration_us"])
    if duration < 2:
        return None

    train_len = int(duration * float(train_ratio))
    # Ensure both windows are non-empty.
    if train_len <= 0:
        train_len = 1
    if train_len >= duration:
        train_len = duration - 1

    train_start = start_us
    train_end = start_us + train_len - 1
    val_start = train_end + 1
    val_end = end_us

    return {
        "interval": {"start_us": start_us, "end_us": end_us, "duration_us": duration},
        "train": {"start_us": train_start, "end_us": train_end},
        "validation": {"start_us": val_start, "end_us": val_end},
        "train_ratio": float(train_ratio),
    }


def build_manifest(
    *,
    base_path: str,
    feeds: list[str],
    status_filter: str,
    intervals: list[dict],
    recommendation: dict | None,
) -> dict:
    return {
        "format_version": 1,
        "created_at_us": time.time_ns() // 1_000,
        "base_path": str(base_path),
        "feeds": list(feeds),
        "status_filter": status_filter,
        "intervals": intervals,
        "recommended": recommendation,
    }


def build_multi_manifest(
    *,
    sources: dict[str, str],
    feed_refs: list[str],
    status_filter: str,
    intervals: list[dict],
    recommendation: dict | None,
) -> dict:
    return {
        "format_version": 1,
        "created_at_us": time.time_ns() // 1_000,
        "mode": "multi_source",
        "sources": dict(sources),
        "feed_refs": list(feed_refs),
        "status_filter": status_filter,
        "intervals": intervals,
        "recommended": recommendation,
    }
