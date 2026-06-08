#!/usr/bin/env python3
"""Standalone catalog and coverage helpers do not require Platform."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater import Writer, catalog, create_feed, feed_coverage


def _spec(name: str, *, segment_tracking: bool = True) -> dict:
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
        "segment_tracking": segment_tracking,
    }


def test_feed_coverage_prefers_segments():
    with tempfile.TemporaryDirectory(prefix="dw-catalog-segments-") as td:
        base = Path(td)
        create_feed(base, _spec("events"))
        w = Writer(base, "events")
        for i in range(5):
            w.write_values(1_000 + i, i)
        w.close()

        coverage = feed_coverage(base, "events")
        assert coverage["source"] == "segments"
        assert coverage["start_us"] == 1_000
        assert coverage["end_us"] == 1_004
        assert coverage["duration_us"] == 5
        assert coverage["records"] == 5

        info = catalog(base, include_fields=True)
        assert info["base_path"] == str(base)
        assert [feed["feed_name"] for feed in info["feeds"]] == ["events"]
        assert info["feeds"][0]["coverage"]["duration_us"] == 5
        assert info["feeds"][0]["fields"][0]["name"] == "ts"


def test_feed_coverage_falls_back_to_chunks_without_segment_side_effect():
    with tempfile.TemporaryDirectory(prefix="dw-catalog-chunks-") as td:
        base = Path(td)
        create_feed(base, _spec("events", segment_tracking=False))
        w = Writer(base, "events")
        for i in range(3):
            w.write_values(2_000 + i, i)
        w.close()

        segments_path = base / "data" / "events" / "segments.reg"
        assert not segments_path.exists()

        coverage = feed_coverage(base, "events")
        assert coverage["source"] == "chunks"
        assert coverage["start_us"] == 2_000
        assert coverage["end_us"] == 2_002
        assert coverage["duration_us"] == 3
        assert coverage["records"] == 3
        assert not segments_path.exists()


def run_tests():
    tests = [
        ("feed_coverage_prefers_segments", test_feed_coverage_prefers_segments),
        ("feed_coverage_falls_back_to_chunks_without_segment_side_effect", test_feed_coverage_falls_back_to_chunks_without_segment_side_effect),
    ]
    print("Catalog Tests")
    print("=" * 60)
    passed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"✅ {name}")
            passed += 1
        except Exception as e:
            print(f"❌ {name} - {e}")
            raise
    print(f"\nPassed: {passed}/{len(tests)}")
    if passed != len(tests):
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
