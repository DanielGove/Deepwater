#!/usr/bin/env python3
"""Tests for primitive feed deletion."""
import sys
import tempfile
import time
from multiprocessing import shared_memory
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater import Reader, Writer, create_feed, delete_feed
from deepwater.io.ring import _ring_data_shm_name, ring_buffer_shm_names
from deepwater.metadata.discovery import feed_exists, list_feeds


def _shm_path(name: str) -> Path:
    return Path("/dev/shm") / name.lstrip("/")


def test_delete_persistent_feed_removes_all_state_and_allows_recreate():
    with tempfile.TemporaryDirectory(prefix="dw-delete-persist-") as td:
        base = Path(td)

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
        create_feed(base, spec)

        w = Writer(base, "trades")
        for i in range(16):
            w.write_values(1_000_000 + i, float(i))
        w.close()

        r = Reader(base, "trades")
        r.close()

        removed = delete_feed(base, "trades")
        assert removed is True
        assert not feed_exists(base, "trades")
        assert "trades" not in list_feeds(base)
        assert not (base / "data" / "trades").exists()

        try:
            Writer(base, "trades")
            raise AssertionError("expected KeyError for deleted feed")
        except KeyError:
            pass

        # Recreate same feed name and verify it works.
        create_feed(base, spec)
        w2 = Writer(base, "trades")
        ts_now = int(time.time() * 1e6)
        w2.write_values(ts_now, 42.0)
        w2.close()

        r2 = Reader(base, "trades")
        out = r2.range(ts_now - 1_000_000, ts_now + 1_000_000)
        assert len(out) >= 1
        assert out[-1][0] == ts_now
        r2.close()


def test_delete_ring_feed_unlinks_shared_memory_and_registry_entry():
    with tempfile.TemporaryDirectory(prefix="dw-delete-ring-") as td:
        base = Path(td)

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
        create_feed(base, spec)

        w = Writer(base, "live")
        w.write_values(1_000_000, 1)

        shm_name = ring_buffer_shm_names(base, "live")[0]
        data_shm_name = _ring_data_shm_name(shm_name)
        shm = shared_memory.SharedMemory(name=shm_name, create=False)
        shm.close()
        assert _shm_path(data_shm_name).exists()

        try:
            delete_feed(base, "live")
            raise AssertionError("active writer should block delete")
        except RuntimeError:
            pass

        w.close()
        removed = delete_feed(base, "live")
        assert removed is True
        assert not feed_exists(base, "live")
        assert "live" not in list_feeds(base)
        assert not (base / "data" / "live").exists()

        try:
            shared_memory.SharedMemory(name=shm_name, create=False)
            raise AssertionError("ring shared memory still exists after delete")
        except FileNotFoundError:
            pass
        assert not _shm_path(data_shm_name).exists()


def test_delete_ring_feed_unlinks_shared_memory_with_exported_view():
    with tempfile.TemporaryDirectory(prefix="dw-close-ring-") as td:
        base = Path(td)
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
        create_feed(base, spec)
        w = Writer(base, "live")
        w.write_values(1_000_000, 1)

        r = Reader(base, "live")
        raw_record = next(r.stream(start=0, format="raw"))
        assert bytes(raw_record[:8]) != b""

        shm_name = ring_buffer_shm_names(base, "live")[0]
        data_shm_name = _ring_data_shm_name(shm_name)
        shm = shared_memory.SharedMemory(name=shm_name, create=False)
        shm.close()
        assert _shm_path(data_shm_name).exists()

        w.close()
        delete_feed(base, "live")

        try:
            shared_memory.SharedMemory(name=shm_name, create=False)
            raise AssertionError("ring shared memory still exists after delete")
        except FileNotFoundError:
            pass
        assert not _shm_path(data_shm_name).exists()
        raw_record.release()
        r.close()


def test_reader_close_does_not_unlink_ring_shared_memory():
    with tempfile.TemporaryDirectory(prefix="dw-close-readonly-ring-") as td:
        base = Path(td)
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
        create_feed(base, spec)
        w = Writer(base, "live")
        w.write_values(1_000_000, 1)

        shm_name = ring_buffer_shm_names(base, "live")[0]
        data_shm_name = _ring_data_shm_name(shm_name)
        r2 = Reader(base, "live")
        r2.close()

        shm = shared_memory.SharedMemory(name=shm_name, create=False)
        shm.close()
        assert _shm_path(data_shm_name).exists()

        w.close()
        delete_feed(base, "live")
        try:
            shared_memory.SharedMemory(name=shm_name, create=False)
            raise AssertionError("ring shared memory still exists after delete")
        except FileNotFoundError:
            pass
        assert not _shm_path(data_shm_name).exists()


def run_tests():
    tests = [
        ("delete_persistent_feed_removes_all_state_and_allows_recreate", test_delete_persistent_feed_removes_all_state_and_allows_recreate),
        ("delete_ring_feed_unlinks_shared_memory_and_registry_entry", test_delete_ring_feed_unlinks_shared_memory_and_registry_entry),
        ("delete_ring_feed_unlinks_shared_memory_with_exported_view", test_delete_ring_feed_unlinks_shared_memory_with_exported_view),
        ("reader_close_does_not_unlink_ring_shared_memory", test_reader_close_does_not_unlink_ring_shared_memory),
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
