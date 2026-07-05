#!/usr/bin/env python3
"""Regression coverage for chunk metadata repair."""
from __future__ import annotations

import tempfile
from pathlib import Path

from deepwater import Reader, Writer, create_feed
from deepwater.metadata.feed_registry import FeedRegistry, UINT64_MAX
from deepwater.ops.repair import repair_feed


def _make_chunk_feed(base: Path) -> None:
    create_feed(base, {
        "feed_name": "events",
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "value", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": True,
        "storage": "chunk",
        "chunk_size_mb": 1,
    })


def test_repair_feed_rebuilds_chunk_counts_write_pos_and_qbounds():
    with tempfile.TemporaryDirectory(prefix="dw-repair-chunk-") as td:
        base = Path(td)
        _make_chunk_feed(base)
        start = 2_500_000_000_000_000
        writer = Writer(base, "events")
        for i in range(4):
            writer.write_values(start + i * 10, i)
        writer.close()

        reg_path = base / "data" / "events" / "events.reg"
        registry = FeedRegistry(reg_path, mode="w")
        try:
            meta = registry.get_chunk_metadata(1)
            try:
                meta.num_records = 1
                meta.write_pos = 64
                meta.start_time = 0
                meta.end_time = 0
                meta.set_qbounds(0, UINT64_MAX, 0)
            finally:
                meta.release()
        finally:
            registry.close()

        checked, repaired = repair_feed(base, "events")
        assert (checked, repaired) == (1, 1)

        registry = FeedRegistry(reg_path, mode="r")
        try:
            meta = registry.get_chunk_metadata(1)
            try:
                assert meta.num_records == 4
                assert meta.write_pos == 64
                assert meta.start_time == start
                assert meta.end_time == start + 30
                assert meta.get_qmin(0) == start
                assert meta.get_qmax(0) == start + 30
            finally:
                meta.release()
        finally:
            registry.close()

        reader = Reader(base, "events")
        try:
            assert reader.range(start, start + 40) == [(start + i * 10, i) for i in range(4)]
        finally:
            reader.close()
