#!/usr/bin/env python3
"""
Smoke tests using the shared helper harness.
Keeps feed creation centralized to cut churn when UX changes.
"""
import sys
import time
from pathlib import Path
import tempfile

sys.path.insert(0, str(Path(__file__).parent.parent))

from helpers import make_feed, seed_records, close_all
from deepwater import Reader, Writer
from deepwater.metadata.feed_registry import FeedRegistry
from deepwater.metadata.discovery import feed_exists


def test_reuse_single_creation():
    with tempfile.TemporaryDirectory(prefix="dw-helper-") as td:
        base = Path(td)
        _base1, feed = make_feed("clock1", base)
        _base2, _ = make_feed("clock1", base)
        # Registry should have exactly one feed; create_feed is idempotent
        assert feed_exists(base, feed)
        assert feed_exists(base, feed)


def test_clock3_bounds_and_ranges():
    with tempfile.TemporaryDirectory(prefix="dw-helper-") as td:
        base = Path(td)
        base, feed = make_feed("clock3", base)
        w = Writer(base, feed)
        seed_records(w, 5, start_ts=1_000_000)

        # Verify registry metadata bounds per axis
        reg = FeedRegistry(str(base / "data" / feed / f"{feed}.reg"), mode="r")
        meta = reg.get_latest_chunk()
        assert meta.clock_level == 3, "registry lost clock_level=3"
        assert meta.get_qmin(0) == 1_000_000, "recv_us qmin wrong"
        assert meta.get_qmax(0) == 1_000_040, "recv_us qmax wrong"
        assert meta.get_qmin(1) == 1_000_010, "proc_us qmin wrong"
        assert meta.get_qmax(1) == 1_000_050, "proc_us qmax wrong"
        assert meta.get_qmin(2) == 999_990, "ev_us qmin wrong"
        assert meta.get_qmax(2) == 1_000_030, "ev_us qmax wrong"
        meta.release(); reg.close()

        r = Reader(base, feed)
        # Range on proc_us
        out_proc = r.range(1_000_010, 1_000_051, ts_key="proc_us")
        assert len(out_proc) == 5, "range on proc_us should return all rows"
        # Range on recv_us (primary axis)
        out_recv = r.range(1_000_000, 1_000_041, ts_key="recv_us")
        assert len(out_recv) == 5, "range on recv_us should return all rows"
        # Range on ev_us (earlier timeline)
        out_ev = r.range(999_990, 1_000_031, ts_key="ev_us")
        assert len(out_ev) == 5, "range on ev_us should return all rows"
        close_all(r)


def run_tests():
    tests = [
        ("Reuse Single Creation", test_reuse_single_creation),
        ("Clock3 Bounds and Ranges", test_clock3_bounds_and_ranges),
    ]
    print("Helper Harness Tests")
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
