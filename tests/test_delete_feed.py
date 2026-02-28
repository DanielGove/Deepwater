#!/usr/bin/env python3
"""Tests for Platform.delete_feed()."""
import sys
import tempfile
import time
from multiprocessing import shared_memory
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform


def test_delete_persistent_feed_removes_all_state_and_allows_recreate():
    with tempfile.TemporaryDirectory(prefix="dw-delete-persist-") as td:
        base = Path(td)
        p = Platform(str(base))

        spec = {
            "feed_name": "trades",
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "px", "type": "float64"},
            ],
            "clock_level": 1,
            "persist": True,
            "chunk_size_mb": 1,
        }
        p.create_feed(spec)

        w = p.create_writer("trades")
        for i in range(16):
            w.write_values(1_000_000 + i, float(i))

        # Open reader too; delete_feed should close process-local handles.
        _ = p.create_reader("trades")

        removed = p.delete_feed("trades")
        assert removed is True
        assert not p.feed_exists("trades")
        assert "trades" not in p.list_feeds()
        assert not (base / "data" / "trades").exists()

        try:
            p.create_writer("trades")
            raise AssertionError("expected KeyError for deleted feed")
        except KeyError:
            pass

        # Recreate same feed name and verify it works.
        p.create_feed(spec)
        w2 = p.create_writer("trades")
        ts_now = int(time.time() * 1e6)
        w2.write_values(ts_now, 42.0)
        w2.close()

        r2 = p.create_reader("trades")
        out = r2.range(ts_now - 1_000_000, ts_now + 1_000_000)
        assert len(out) >= 1
        assert out[-1][0] == ts_now
        r2.close()
        p.close()


def test_delete_ring_feed_unlinks_shared_memory_and_registry_entry():
    with tempfile.TemporaryDirectory(prefix="dw-delete-ring-") as td:
        base = Path(td)
        p = Platform(str(base))

        spec = {
            "feed_name": "live",
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "v", "type": "uint64"},
            ],
            "clock_level": 1,
            "persist": False,
            "chunk_size_mb": 0.01,
        }
        p.create_feed(spec)

        w = p.create_writer("live")
        w.write_values(1_000_000, 1)

        shm = shared_memory.SharedMemory(name="live", create=False)
        shm.close()

        removed = p.delete_feed("live")
        assert removed is True
        assert not p.feed_exists("live")
        assert "live" not in p.list_feeds()
        assert not (base / "data" / "live").exists()

        try:
            shared_memory.SharedMemory(name="live", create=False)
            raise AssertionError("ring shared memory still exists after delete")
        except FileNotFoundError:
            pass

        p.close()


def run_tests():
    tests = [
        ("delete_persistent_feed_removes_all_state_and_allows_recreate", test_delete_persistent_feed_removes_all_state_and_allows_recreate),
        ("delete_ring_feed_unlinks_shared_memory_and_registry_entry", test_delete_ring_feed_unlinks_shared_memory_and_registry_entry),
    ]
    print("Delete Feed Tests")
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
