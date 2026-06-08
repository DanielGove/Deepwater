import struct
import sys
import tempfile
from pathlib import Path

from deepwater import Reader, Writer, create_feed
from deepwater.metadata.feed_registry import FeedRegistry


def _make_feed(base: Path, name: str = "test_bounds") -> None:
    create_feed(base, {
        "feed_name": name,
        "mode": "UF",
        "fields": [
            {"name": "proc_us", "type": "uint64"},
            {"name": "recv_us", "type": "uint64"},
            {"name": "ev_us", "type": "uint64"},
            {"name": "trade_id", "type": "uint64"},
            {"name": "price", "type": "float64"},
        ],
        "clock_level": 3,
        "persist": True,
    })


def _records(base: int = 1_000_000):
    # proc_us, recv_us, ev_us, trade_id, price
    return [
        (base + 20, base + 10, base + 0, 1, 101.0),
        (base + 120, base + 110, base + 100, 2, 102.0),
        (base + 220, base + 210, base + 200, 3, 103.0),
    ]


def test_chunk_metadata_bounds_per_key():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        _make_feed(root)
        w = Writer(root, "test_bounds")
        for rec in _records():
            w.write_values(*rec)
        w.close()

        reg_path = root / "data" / "test_bounds" / "test_bounds.reg"
        reg = FeedRegistry(str(reg_path), mode="r")
        meta = reg.get_latest_chunk()

        assert meta.clock_level == 3
        # primary proc_us
        assert meta.get_qmin(0) == 1_000_020
        assert meta.get_qmax(0) == 1_000_220
        # recv_us
        assert meta.get_qmin(1) == 1_000_010
        assert meta.get_qmax(1) == 1_000_210
        # ev_us (earliest)
        assert meta.get_qmin(2) == 1_000_000
        assert meta.get_qmax(2) == 1_000_200

        meta.release()
        reg.close()


def test_reader_range_uses_per_key_bounds():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        _make_feed(root)
        recs = _records()

        # write_values path
        w = Writer(root, "test_bounds")
        for rec in recs:
            w.write_values(*rec)
        w.close()

        # write path
        w = Writer(root, "test_bounds")
        for rec in recs:
            ts = rec[0]
            buf = struct.pack("<QQQQd", *rec)
            w.write(ts, buf)
        w.close()

        # write_batch_bytes path
        w = Writer(root, "test_bounds")
        batch = b"".join(struct.pack("<QQQQd", *rec) for rec in recs)
        w.write_batch_bytes(batch)
        w.close()

        r = Reader(root, "test_bounds")
        try:
            # Query on earliest timestamp; should include all rows from all write paths.
            ev_start = recs[0][2]  # ev_us of first
            ev_end = recs[-1][2] + 1
            out = r.range(ev_start, ev_end, ts_key="ev_us")
            assert len(out) == len(recs) * 3

            # Query on recv_us; middle window should include last two rows from all write paths.
            recv_start = recs[1][1]
            recv_end = recs[-1][1] + 1
            out_recv = r.range(recv_start, recv_end, ts_key="recv_us")
            assert len(out_recv) == 2 * 3
        finally:
            r.close()


def run_tests():
    tests = [
        ("chunk_metadata_bounds_per_key", test_chunk_metadata_bounds_per_key),
        ("reader_range_uses_per_key_bounds", test_reader_range_uses_per_key_bounds),
    ]
    print("Metadata Bounds Tests")
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
