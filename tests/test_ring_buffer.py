#!/usr/bin/env python3
"""Ring (persist=False) path smoke tests with wrap-around."""
import sys
import tempfile
import struct
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater import Reader, Writer, create_feed
from deepwater.io.ring import PAGE_SIZE, normalize_ring_data_size, ring_buffer_shm_names
from deepwater.metadata.feed_metadata import load_feed_metadata


def test_ring_buffer_shm_name_normalizes_base_path():
    with tempfile.TemporaryDirectory(prefix="dw-ring-name-") as td:
        base = Path(td) / "platform"
        base.mkdir()
        cwd = Path.cwd()
        try:
            os.chdir(base.parent)
            assert ring_buffer_shm_names(base.name, "live")[0] == ring_buffer_shm_names(base.resolve(), "live")[0]
        finally:
            os.chdir(cwd)


def test_ring_size_normalizes_to_page_and_record_multiple():
    with tempfile.TemporaryDirectory(prefix="dw-ring-size-") as td:
        base = Path(td)
        spec = {
            "feed_name": "oddsize",
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "payload", "type": "bytes61"},
            ],
            "clock_level": 1,
            "persist": False,
            "ring_size_mb": 0.25,
            "chunk_size_mb": 0.25,
        }
        create_feed(base, spec)
        metadata = load_feed_metadata(base, "oddsize")
        requested = int(0.25 * 1024 * 1024)
        expected = normalize_ring_data_size(requested, 69)
        assert metadata.ring_size_bytes == expected
        assert metadata.ring_size_bytes % PAGE_SIZE == 0
        assert metadata.ring_size_bytes % 69 == 0
        assert metadata.ring_size_bytes >= requested


def test_ring_wrap_and_range():
    with tempfile.TemporaryDirectory(prefix="dw-ring-") as td:
        base = Path(td)
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
        create_feed(base, spec)
        w = Writer(base, "ringtest")
        base_ts = 1_000_000
        N = 2000
        for i in range(N):
            w.write_values(base_ts + i, i)
        w.close()

        r = Reader(base, "ringtest")
        ring_cap = r._ring.data_size // r._rec_size

        # Range over full possible window should return at most capacity (last writes)
        out = r.range(base_ts, base_ts + N + 1)
        assert ring_cap - 5 <= len(out) <= ring_cap, f"expected ~{ring_cap} records after wrap, got {len(out)}"
        # Last record should be one of the most recent writes (may drop a few due to header timing)
        assert out[-1][1] >= N - 5, "ring did not retain newest record"

        # No chunk files should exist for ring feeds
        chunk_path = base / "data" / "ringtest" / "chunk_00000001.bin"
        assert not chunk_path.exists(), "ring feed should not create chunk files"

        r.close()


def test_live_ring_api_matches_reader_shape():
    with tempfile.TemporaryDirectory(prefix="dw-ring-api-") as td:
        base = Path(td)
        spec = {
            "feed_name": "ringapi",
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
        w = Writer(base, "ringapi")
        base_ts = 2_000_000
        for i in range(6):
            w.write_values(base_ts + i * 10, i)
        w.close()

        r = Reader(base, "ringapi")
        try:
            assert r.range(base_ts + 10, base_ts + 40, ts_key="ts") == [
                (base_ts + 10, 1),
                (base_ts + 20, 2),
                (base_ts + 30, 3),
            ]
            assert [row["v"] for row in r.range(base_ts + 10, base_ts + 40, format="dict", ts_key="ts")] == [1, 2, 3]
            assert list(r.range(base_ts + 10, base_ts + 40, format="numpy", ts_key="ts")["v"]) == [1, 2, 3]
            expected = b"".join(struct.pack("<QQ", base_ts + i * 10, i) for i in range(1, 4))
            assert bytes(r.range(base_ts + 10, base_ts + 40, format="raw", ts_key="ts")) == expected
            assert b"".join(bytes(b) for b in r.range_batches(base_ts + 10, base_ts + 40, format="raw", ts_key="ts", batch_records=2)) == expected
            stream = r.stream(start=base_ts + 20, ts_key="ts")
            assert next(stream) == (base_ts + 20, 2)
        finally:
            r.close()


def run_tests():
    tests = [
        ("ring_buffer_shm_name_normalizes_base_path", test_ring_buffer_shm_name_normalizes_base_path),
        ("ring_size_normalizes_to_page_and_record_multiple", test_ring_size_normalizes_to_page_and_record_multiple),
        ("ring_wrap_and_range", test_ring_wrap_and_range),
        ("live_ring_api_matches_reader_shape", test_live_ring_api_matches_reader_shape),
    ]
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
