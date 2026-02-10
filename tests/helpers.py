"""
Test helpers for Deepwater
-------------------------
Centralized feed specs and convenience routines so tests don't need to
redefine schemas. This reduces churn when the feed UX changes.
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Tuple

from deepwater import Platform

# Canonical feed templates (clock_level model: leading uint64 time axes)
SPEC_TEMPLATES = {
    "clock1": {
        "feed_name": "clock1",
        "mode": "UF",
        "fields": [
            {"name": "timestamp_us", "type": "uint64"},
            {"name": "value", "type": "float64"},
        ],
        "clock_level": 1,
        "persist": True,
    },
    "clock3": {
        "feed_name": "clock3",
        "mode": "UF",
        "fields": [
            {"name": "recv_us", "type": "uint64"},
            {"name": "proc_us", "type": "uint64"},
            {"name": "ev_us", "type": "uint64"},
            {"name": "trade_id", "type": "uint64"},
            {"name": "price", "type": "float64"},
        ],
        "clock_level": 3,
        "persist": True,
    },
    "ring1": {
        "feed_name": "ring1",
        "mode": "UF",
        "fields": [
            {"name": "timestamp_us", "type": "uint64"},
            {"name": "value", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": False,  # SHM ring buffer path
    },
}


def make_platform(kind: str, base: Path | None = None) -> Tuple[Platform, str]:
    """Create (or reuse) a Platform with a canonical feed.

    Returns platform and feed_name. Feed is created only once per base dir.
    """
    if kind not in SPEC_TEMPLATES:
        raise ValueError(f"Unknown feed kind '{kind}'")
    if base is None:
        base = Path(tempfile.mkdtemp(prefix=f"dw-{kind}-"))
    base = Path(base)
    p = Platform(str(base))
    spec = SPEC_TEMPLATES[kind].copy()
    fname = spec["feed_name"]
    # Create feed if missing (idempotent as Platform.create_feed already is)
    p.create_feed(spec)
    return p, fname


def seed_records(writer, n: int, *, start_ts: int = 1_000_000):
    """Seed n records with monotonic timestamps across available axes.

    For clock1: (ts, value)
    For clock3: (recv, proc, ev, trade_id, price)
    """
    if writer.record_format.get("clock_level", 1) >= 3:
        for i in range(n):
            recv = start_ts + i * 10
            proc = recv + 10
            ev = recv - 10
            trade_id = i + 1
            price = 100.0 + i
            writer.write_values(recv, proc, ev, trade_id, price)
    else:
        for i in range(n):
            ts = start_ts + i * 10
            writer.write_values(ts, float(i))
    writer.close()


def close_all(*objs):
    for o in objs:
        try:
            o.close()
        except Exception:
            pass
