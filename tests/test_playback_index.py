#!/usr/bin/env python3
"""Playback should use index snapshots to include state before start."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform


def test_playback_includes_snapshot_before_start():
    with tempfile.TemporaryDirectory(prefix="dw-playback-") as td:
        base = Path(td)
        p = Platform(str(base))
        spec = {
            "feed_name": "pb",
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "v", "type": "uint64"},
            ],
            "clock_level": 1,
            "persist": True,
            "chunk_size_mb": 0.1,
            "index_playback": True,
        }
        p.create_feed(spec)

        w = p.create_writer("pb")
        base_ts = 1_000_000
        # seed snapshot-worthy data
        for i in range(5):
            w.write_values(base_ts + i * 10, i, create_index=True)
        # more data after the snapshot point
        for i in range(5, 10):
            w.write_values(base_ts + i * 10, i)
        w.close()

        r = p.create_reader("pb")
        start = base_ts + 25  # between records 2 and 3
        end = base_ts + 120
        out_plain = r.range(start, end)
        out_pb = r.range(start, end, playback=True)

        # Without playback, the first two records are skipped
        assert len(out_plain) == 7
        # With playback, we should include the snapshot record at/before start
        assert len(out_pb) == 8
        assert out_pb[0][1] == 2  # snapshot should be the last <= start

        r.close(); p.close()


def run_tests():
    tests = [("playback_includes_snapshot_before_start", test_playback_includes_snapshot_before_start)]
    print("Playback Index Tests")
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
