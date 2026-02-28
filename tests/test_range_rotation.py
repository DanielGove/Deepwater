#!/usr/bin/env python3
"""
Chunk rotation + range correctness under clock_level model.
Large-ish write to force multiple chunks, then tight ranges across boundaries.
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform
from deepwater.metadata.feed_registry import FeedRegistry


def _make_feed(base: Path, chunk_mb: float = 1.0):
    p = Platform(str(base))
    spec = {
        "feed_name": "rot_clock3",
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
        "chunk_size_mb": chunk_mb,
        "index_playback": False,
    }
    p.create_feed(spec)
    return p, spec["feed_name"]


def test_range_spans_multiple_chunks():
    N = 200_000  # large enough to force several rotations with 1MB chunks (~8 chunks)
    with tempfile.TemporaryDirectory(prefix="dw-rotation-") as td:
        base = Path(td)
        p, feed = _make_feed(base, chunk_mb=1.0)
        w = p.create_writer(feed)
        base_ts = 1_000_000
        for i in range(N):
            recv = base_ts + i * 10
            proc = recv + 10
            ev = recv - 10
            trade_id = i + 1
            price = 100.0 + (i % 1000)
            w.write_values(recv, proc, ev, trade_id, price)
        w.close()

        # Ensure multiple chunks were created
        reg_path = base / "data" / feed / f"{feed}.reg"
        reg = FeedRegistry(str(reg_path), mode="r")
        latest = reg.get_latest_chunk_idx()
        assert latest and latest > 2, f"expected >2 chunks, got {latest}"

        # Range spanning middle of data across chunk boundaries (proc_us axis)
        r = p.create_reader(feed)
        start_idx = 50_000
        end_idx = 150_000
        start = base_ts + start_idx * 10 + 10  # proc_us = recv+10
        end = base_ts + end_idx * 10 + 10
        out = r.range(start, end, ts_key="proc_us")
        expected = end_idx - start_idx + 1  # reader.range includes end if ts <= end
        assert len(out) == expected, f"proc_us range count mismatch: {len(out)} vs {expected}"

        # Tight range around a chunk boundary (~every 26k records). Pick boundary near 52k.
        boundary_idx = 52_000
        start_b = base_ts + boundary_idx * 10 + 10
        end_b = start_b + 200  # ~20 records window
        out_b = r.range(start_b, end_b, ts_key="proc_us")
        assert 15 <= len(out_b) <= 25, "boundary window unexpected count"

        # Earliest axis (ev_us) should include all when queried full span
        full = r.range(base_ts - 10, base_ts + N * 10, ts_key="ev_us")
        assert len(full) == N, "ev_us full-span count mismatch"

        r.close(); reg.close(); p.close()


def run_tests():
    tests = [("range_spans_multiple_chunks", test_range_spans_multiple_chunks)]
    print("Range Rotation Tests")
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
