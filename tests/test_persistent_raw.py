#!/usr/bin/env python3
"""Persistent ring raw paths should not diverge from tuple/range batch semantics."""
import struct
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform


def _make_platform(base: Path):
    p = Platform(str(base))
    p.create_feed({
        "feed_name": "events",
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "value", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": True,
        "chunk_size_mb": 1,
        "ring_size_mb": 1,
    })
    return p


def test_persistent_raw_range_and_batches_match_tuples():
    with tempfile.TemporaryDirectory(prefix="dw-persist-raw-") as td:
        p = _make_platform(Path(td))
        w = p.create_writer("events")
        start = 1_800_000_000_000_000
        for i in range(8):
            w.write_values(start + i * 10, i)

        r = p.create_reader("events")
        try:
            tuples = r.range(start + 10, start + 70)
            dicts = r.range(start + 10, start + 30, format="dict")
            numpy_records = r.range(start + 10, start + 30, format="numpy")
            raw = bytes(r.range(start + 10, start + 70, format="raw"))
            raw_batches = b"".join(
                bytes(batch)
                for batch in r.range_batches(start + 10, start + 70, format="raw", batch_records=2)
            )
            dict_batches = [
                row
                for batch in r.range_batches(start + 10, start + 50, format="dict", batch_records=2)
                for row in batch
            ]
            numpy_values = [
                int(value)
                for batch in r.range_batches(start + 10, start + 50, format="numpy", batch_records=2)
                for value in batch["value"]
            ]
            raw_iter = b"".join(
                bytes(batch)
                for batch in r.iter_raw_range(start + 10, start + 70, batch_records=2)
            )
            expected = b"".join(struct.pack("<QQ", start + i * 10, i) for i in range(1, 7))

            assert tuples == [(start + i * 10, i) for i in range(1, 7)]
            assert dicts == [
                {"ts": start + 10, "value": 1},
                {"ts": start + 20, "value": 2},
            ]
            assert list(numpy_records["value"]) == [1, 2]
            assert raw == expected
            assert raw_batches == expected
            assert raw_iter == expected
            assert [row["value"] for row in dict_batches] == [1, 2, 3, 4]
            assert numpy_values == [1, 2, 3, 4]
        finally:
            p.close()


def test_persistent_read_available_raw_matches_new_records():
    with tempfile.TemporaryDirectory(prefix="dw-persist-available-") as td:
        p = _make_platform(Path(td))
        w = p.create_writer("events")
        start = 1_800_000_000_000_000
        w.write_values(start, 0)

        r = p.create_reader("events")
        try:
            assert bytes(r.read_available(format="raw")) == b""
            w.write_values(start + 10, 1)
            w.write_values(start + 20, 2)
            assert bytes(r.read_available(max_records=2, format="raw")) == struct.pack(
                "<QQQQ",
                start + 10,
                1,
                start + 20,
                2,
            )
        finally:
            p.close()


def run_tests():
    tests = [
        ("persistent_raw_range_and_batches_match_tuples", test_persistent_raw_range_and_batches_match_tuples),
        ("persistent_read_available_raw_matches_new_records", test_persistent_read_available_raw_matches_new_records),
    ]
    print("Persistent Raw Tests")
    print("=" * 60)
    passed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"✅ {name}")
            passed += 1
        except Exception as e:
            print(f"❌ {name} - {e}")
            raise
    print(f"\nPassed: {passed}/{len(tests)}")
    if passed != len(tests):
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
