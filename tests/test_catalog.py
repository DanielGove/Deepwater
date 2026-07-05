#!/usr/bin/env python3
"""Standalone catalog and coverage helpers do not require Platform."""
import tempfile
from pathlib import Path


from deepwater import Writer, catalog, create_feed, feed_coverage


def _spec(name: str) -> dict:
    return {
        "feed_name": name,
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "v", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": True,
        "storage": "chunk",
        "chunk_size_mb": 0.01,
    }


def test_feed_coverage_uses_chunks():
    with tempfile.TemporaryDirectory(prefix="dw-catalog-chunks-") as td:
        base = Path(td)
        create_feed(base, _spec("events"))
        w = Writer(base, "events")
        for i in range(5):
            w.write_values(1_000 + i, i)
        w.close()

        coverage = feed_coverage(base, "events")
        assert coverage["source"] == "chunks"
        assert coverage["start_us"] == 1_000
        assert coverage["end_us"] == 1_004
        assert coverage["duration_us"] == 5
        assert coverage["records"] == 5

        info = catalog(base, include_fields=True)
        assert info["base_path"] == str(base)
        assert [feed["feed_name"] for feed in info["feeds"]] == ["events"]
        assert info["feeds"][0]["coverage"]["duration_us"] == 5
        assert info["feeds"][0]["fields"][0]["name"] == "ts"
