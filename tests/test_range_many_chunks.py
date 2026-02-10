#!/usr/bin/env python3
"""Range across many chunks to catch overlap/iteration bugs."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform
from deepwater.feed_registry import FeedRegistry


def test_range_spans_hundred_chunks():
    N = 200_000  # tuned to yield ~35-40 chunks with 0.25MB chunks
    with tempfile.TemporaryDirectory(prefix="dw-many-") as td:
        base = Path(td)
        p = Platform(str(base))
        spec = {
            "feed_name": "rot100",
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
            "chunk_size_mb": 0.25,  # very small to force many chunks
            "index_playback": False,
        }
        p.create_feed(spec)
        w = p.create_writer("rot100")
        base_ts = 10_000_000
        for i in range(N):
            recv = base_ts + i * 10
            proc = recv + 10
            ev = recv - 10
            trade_id = i + 1
            price = 100.0 + (i % 1000)
            w.write_values(recv, proc, ev, trade_id, price)
        w.close()

        reg = FeedRegistry(str(base / "data" / "rot100" / "rot100.reg"), mode="r")
        latest = reg.get_latest_chunk_idx()
        assert latest and latest > 30, f"expected >30 chunks, got {latest}"

        r = p.create_reader("rot100")
        # Span roughly middle 400k records on proc_us (should cross dozens of chunks)
        start_idx = 30_000
        end_idx = 70_000
        start = base_ts + start_idx * 10 + 10
        end = base_ts + end_idx * 10 + 10
        out = r.range(start, end, ts_key="proc_us")
        expected = end_idx - start_idx + 1  # inclusive end
        assert len(out) == expected, f"proc_us span count mismatch {len(out)} vs {expected}"

        # Small window near a later boundary
        boundary_idx = 75_000
        start_b = base_ts + boundary_idx * 10 + 10
        end_b = start_b + 200  # ~20 records
        out_b = r.range(start_b, end_b, ts_key="proc_us")
        assert 15 <= len(out_b) <= 25, "boundary window unexpected count"

        # Full-span on ev_us returns all rows
        full = r.range(base_ts - 10, base_ts + N * 10, ts_key="ev_us")
        assert len(full) == N, "ev_us full-span mismatch"

        r.close(); reg.close(); p.close()


def run_tests():
    tests = [("range_spans_hundred_chunks", test_range_spans_hundred_chunks)]
    print("Range Many Chunks Tests")
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
