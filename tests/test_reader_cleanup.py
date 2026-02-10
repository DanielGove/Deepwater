#!/usr/bin/env python3
"""Reader should skip expired/missing chunks after cleanup."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform


def test_reader_skips_deleted_chunk_files():
    with tempfile.TemporaryDirectory(prefix="dw-clean-skip-") as td:
        base = Path(td)
        p = Platform(str(base))
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
        p.create_feed(spec)
        w = p.create_writer("skip")
        for i in range(5):
            w.write_values(1_000_000 + i, i)
        w.close()

        # Delete chunk file manually to simulate cleanup removing it before reader runs
        chunk_path = base / "data" / "skip" / "chunk_00000001.bin"
        assert chunk_path.exists()
        chunk_path.unlink()

        r = p.create_reader("skip")
        out = r.range(1_000_000, 1_000_010)
        assert out == [], "reader should skip missing chunk files instead of crashing"
        r.close(); p.close()


def run_tests():
    tests = [("reader_skips_deleted_chunk_files", test_reader_skips_deleted_chunk_files)]
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
