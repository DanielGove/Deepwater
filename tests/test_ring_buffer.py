#!/usr/bin/env python3
"""Ring (persist=False) path smoke tests with wrap-around."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform


def test_ring_wrap_and_range():
    with tempfile.TemporaryDirectory(prefix="dw-ring-") as td:
        base = Path(td)
        p = Platform(str(base))
        spec = {
            "feed_name": "ringtest",
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "v", "type": "uint64"},
            ],
            "clock_level": 1,
            "persist": False,  # SHM ring
            "chunk_size_mb": 0.01,  # ~10KB → small ring to force wrap
        }
        p.create_feed(spec)
        w = p.create_writer("ringtest")
        base_ts = 1_000_000
        N = 2000
        for i in range(N):
            w.write_values(base_ts + i, i)
        w.close()

        r = p.create_reader("ringtest")
        ring_cap = r.ring_bytes // r._rec_size

        # Range over full possible window should return at most capacity (last writes)
        out = r.range(base_ts, base_ts + N + 1)
        assert ring_cap - 5 <= len(out) <= ring_cap, f"expected ~{ring_cap} records after wrap, got {len(out)}"
        # Last record should be one of the most recent writes (may drop a few due to header timing)
        assert out[-1][1] >= N - 5, "ring did not retain newest record"

        # No chunk files should exist for ring feeds
        chunk_path = base / "data" / "ringtest" / "chunk_00000001.bin"
        assert not chunk_path.exists(), "ring feed should not create chunk files"

        r.close(); p.close()


def run_tests():
    tests = [("ring_wrap_and_range", test_ring_wrap_and_range)]
    print("Ring Buffer Tests")
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
