#!/usr/bin/env python3
"""Reader range/ts_key coverage using shared helpers."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from helpers import make_platform, seed_records, close_all


def test_range_per_axis_clock3():
    with tempfile.TemporaryDirectory(prefix="dw-range-") as td:
        base = Path(td)
        p, feed = make_platform("clock3", base)
        w = p.create_writer(feed)
        seed_records(w, 5, start_ts=1_000_000)

        r = p.create_reader(feed)
        # recv_us axis (default first field)
        out_recv = r.range(1_000_000, 1_000_041, ts_key="recv_us")
        assert len(out_recv) == 5, "recv_us range returned wrong count"
        # proc_us axis
        out_proc = r.range(1_000_010, 1_000_051, ts_key="proc_us")
        assert len(out_proc) == 5, "proc_us range returned wrong count"
        # ev_us axis (earliest)
        out_ev = r.range(999_990, 1_000_031, ts_key="ev_us")
        assert len(out_ev) == 5, "ev_us range returned wrong count"
        close_all(r, p)


def run_tests():
    tests = [("range_per_axis_clock3", test_range_per_axis_clock3)]
    print("Reader Range Tests")
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
