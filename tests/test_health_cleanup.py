#!/usr/bin/env python3
"""Tests for health_check and cleanup utilities."""
import sys
import tempfile
from pathlib import Path
import time

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform
from deepwater.health_check import check_manifest, check_global_registry, check_feeds
from deepwater.cleanup import cleanup_all_feeds


def _make_feed(base: Path, name: str, retention_hours: int = 0, clock_level: int = 1, persist=True):
    p = Platform(str(base))
    spec = {
        "feed_name": name,
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "v", "type": "uint64"},
        ],
        "clock_level": clock_level,
        "persist": persist,
        "chunk_size_mb": 0.5,
        "retention_hours": retention_hours,
    }
    p.create_feed(spec)
    return p


def test_health_checks_pass_on_fresh_platform():
    with tempfile.TemporaryDirectory(prefix="dw-health-") as td:
        base = Path(td)
        p = _make_feed(base, "hfeed", retention_hours=0)
        ok, msg = check_manifest(base)
        assert ok, msg
        ok, msg = check_global_registry(base)
        assert ok, msg
        ok, msg = check_feeds(base)
        assert ok, msg
        p.close()


def test_cleanup_deletes_expired_chunks():
    with tempfile.TemporaryDirectory(prefix="dw-clean-") as td:
        base = Path(td)
        p = _make_feed(base, "cfeed", retention_hours=1)
        w = p.create_writer("cfeed")
        base_ts = int(time.time() * 1e6) - 3_600_000_000  # 1 hour ago
        # Write a couple records to ensure chunk sealed
        for i in range(10):
            w.write_values(base_ts + i, i)
        w.close()

        reg_path = base / "data" / "cfeed" / "cfeed.reg"
        assert reg_path.exists()
        chunk_file = base / "data" / "cfeed" / "chunk_00000001.bin"
        assert chunk_file.exists()
        size_before = chunk_file.stat().st_size

        # Force metadata end_time/last_update to old timestamp to trigger retention
        from deepwater.feed_registry import FeedRegistry, ON_DISK
        reg = FeedRegistry(str(reg_path), mode="w")
        meta = reg.get_latest_chunk()
        meta.end_time = base_ts
        meta.last_update = base_ts
        meta.status = ON_DISK
        meta.release()
        reg.close()

        # Run cleanup; should delete the expired chunk
        cleanup_all_feeds(base, dry_run=False)

        assert not chunk_file.exists(), "cleanup did not delete expired chunk"
        p.close()


def run_tests():
    tests = [
        ("health_checks_pass_on_fresh_platform", test_health_checks_pass_on_fresh_platform),
        ("cleanup_deletes_expired_chunks", test_cleanup_deletes_expired_chunks),
    ]
    print("Health/Cleanup Tests")
    print("=" * 60)
    passed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"✅ {name}")
            passed += 1
        except Exception as e:
            print(f"❌ {name} - {e}")
    print(f"\nPassed: {passed}/{len(tests)}")
    if passed != len(tests):
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
