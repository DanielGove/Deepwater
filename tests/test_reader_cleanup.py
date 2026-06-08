#!/usr/bin/env python3
"""Reader should skip expired/missing chunks after cleanup."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater import Reader, Writer, create_feed


def test_reader_skips_deleted_chunk_files():
    with tempfile.TemporaryDirectory(prefix="dw-clean-skip-") as td:
        base = Path(td)
        spec = {
            "feed_name": "skip",
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "v", "type": "uint64"},
            ],
            "clock_level": 1,
            "persist": True,
            "chunk_size_mb": 0.01,
            "retention_hours": 1,
        }
        create_feed(base, spec)
        w = Writer(base, "skip")
        for i in range(5):
            w.write_values(1_000_000 + i, i)
        w.close()

        # Delete chunk file manually to simulate cleanup removing it before reader runs
        chunk_path = base / "data" / "skip" / "chunk_00000001.bin"
        assert chunk_path.exists()
        chunk_path.unlink()

        r = Reader(base, "skip")
        out = r.range(1_000_000, 1_000_010)
        assert out == [], "reader should skip missing chunk files instead of crashing"
        r.close()


def test_reader_constructs_directly_from_base_path_and_closes_idempotently():
    with tempfile.TemporaryDirectory(prefix="dw-reader-direct-") as td:
        base = Path(td)
        create_feed(base, {
            "feed_name": "direct",
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "v", "type": "uint64"},
            ],
            "clock_level": 1,
            "persist": True,
            "storage": "chunk",
            "chunk_size_mb": 0.01,
        })
        w = Writer(base, "direct")
        for i in range(3):
            w.write_values(2_000_000 + i, i)
        w.close()

        with Reader(base, "direct") as r:
            out = r.range(2_000_000, 2_000_010)
            assert out == [(2_000_000, 0), (2_000_001, 1), (2_000_002, 2)]

        r.close()


def run_tests():
    tests = [
        ("reader_skips_deleted_chunk_files", test_reader_skips_deleted_chunk_files),
        (
            "reader_constructs_directly_from_base_path_and_closes_idempotently",
            test_reader_constructs_directly_from_base_path_and_closes_idempotently,
        ),
    ]
    print("Reader Cleanup Tests")
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
