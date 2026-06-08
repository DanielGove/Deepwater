#!/usr/bin/env python3
"""Reader range/ts_key coverage using shared helpers."""
import sys
import tempfile
import struct
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater import Reader, Writer, create_feed
from helpers import make_feed, seed_records, close_all


def make_chunk_feed(base: Path):
    create_feed(base, {
        "feed_name": "chunk_points",
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "v", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": True,
        "storage": "chunk",
        "chunk_size_mb": 0.00016,
    })
    return base, "chunk_points"


def test_range_per_axis_clock3():
    with tempfile.TemporaryDirectory(prefix="dw-range-") as td:
        base = Path(td)
        base, feed = make_feed("clock3", base)
        w = Writer(base, feed)
        seed_records(w, 5, start_ts=1_000_000)

        r = Reader(base, feed)
        # recv_us axis (default first field)
        out_recv = r.range(1_000_000, 1_000_041, ts_key="recv_us")
        assert len(out_recv) == 5, "recv_us range returned wrong count"
        # proc_us axis
        out_proc = r.range(1_000_010, 1_000_051, ts_key="proc_us")
        assert len(out_proc) == 5, "proc_us range returned wrong count"
        # ev_us axis (earliest)
        out_ev = r.range(999_990, 1_000_031, ts_key="ev_us")
        assert len(out_ev) == 5, "ev_us range returned wrong count"
        close_all(r)


def test_reader_point_lookups_per_axis():
    with tempfile.TemporaryDirectory(prefix="dw-point-") as td:
        base = Path(td)
        base, feed = make_feed("clock3", base)
        w = Writer(base, feed)
        seed_records(w, 8, start_ts=1_000_000)

        r = Reader(base, feed)
        first = r.first_after(1_000_015, ts_key="proc_us")
        last_before = r.first_before(1_000_035, ts_key="proc_us")
        first_dict = r.first_after(999_990, format="dict", ts_key="ev_us")

        assert first[3] == 2
        assert last_before[3] == 3
        assert first_dict["trade_id"] == 1
        assert r.first_after(9_999_999, ts_key="proc_us") is None
        assert r.first_before(999_999, ts_key="proc_us") is None
        close_all(r)


def test_reader_point_lookup_edges_across_chunks():
    with tempfile.TemporaryDirectory(prefix="dw-point-edges-") as td:
        base = Path(td)
        base, feed = make_chunk_feed(base)
        w = Writer(base, feed)
        base_ts = 3_000_000
        for i in range(10):
            w.write_values(base_ts + i * 10, i)
        w.close()

        r = Reader(base, feed)
        try:
            assert r.first_after(base_ts) == (base_ts, 0)
            assert r.first_after(base_ts + 1) == (base_ts + 10, 1)
            assert r.first_after(base_ts + 30) == (base_ts + 30, 3)
            assert r.first_after(base_ts + 31) == (base_ts + 40, 4)
            assert r.first_after(base_ts + 90) == (base_ts + 90, 9)
            assert r.first_after(base_ts + 91) is None

            assert r.first_before(base_ts - 1) is None
            assert r.first_before(base_ts) == (base_ts, 0)
            assert r.first_before(base_ts + 1) == (base_ts, 0)
            assert r.first_before(base_ts + 39) == (base_ts + 30, 3)
            assert r.first_before(base_ts + 40) == (base_ts + 40, 4)
            assert r.first_before(base_ts + 999) == (base_ts + 90, 9)

            expected = struct.pack(r.format, base_ts + 40, 4)
            assert bytes(r.first_after(base_ts + 31, format="raw")) == expected
            arr = r.first_before(base_ts + 40, format="numpy")
            assert len(arr) == 1
            assert int(arr["ts"][0]) == base_ts + 40
            assert int(arr["v"][0]) == 4
        finally:
            close_all(r)


def test_reader_point_lookups_live_ring_only():
    with tempfile.TemporaryDirectory(prefix="dw-ring-point-") as td:
        base = Path(td)
        base, feed = make_feed("ring1", base)
        w = Writer(base, feed)
        base_ts = 2_000_000
        for i in range(5):
            w.write_values(base_ts + i * 10, i)

        r = Reader(base, feed)
        try:
            assert r.first_after(base_ts) == (base_ts, 0)
            assert r.first_after(base_ts + 15)[1] == 2
            assert r.first_after(base_ts + 41) is None
            assert r.first_before(base_ts - 1) is None
            assert r.first_before(base_ts) == (base_ts, 0)
            assert r.first_before(base_ts + 35)[1] == 3
            assert r.first_before(base_ts + 999) == (base_ts + 40, 4)
            assert bytes(r.first_after(base_ts + 15, format="raw")) == struct.pack(r.format, base_ts + 20, 2)
        finally:
            close_all(r, w)


def run_tests():
    tests = [
        ("range_per_axis_clock3", test_range_per_axis_clock3),
        ("reader_point_lookups_per_axis", test_reader_point_lookups_per_axis),
        ("reader_point_lookup_edges_across_chunks", test_reader_point_lookup_edges_across_chunks),
        ("reader_point_lookups_live_ring_only", test_reader_point_lookups_live_ring_only),
    ]
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
