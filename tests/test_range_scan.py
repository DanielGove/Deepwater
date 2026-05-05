#!/usr/bin/env python3
"""Parity tests for raw historical range scanning extraction."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform
from deepwater.io.reader import ChunkReader


def _build_feed(base: Path, n: int = 5000):
    p = Platform(str(base))
    p.create_feed({
        "feed_name": "scan",
        "mode": "UF",
        "fields": [
            {"name": "recv_us", "type": "uint64"},
            {"name": "proc_us", "type": "uint64"},
            {"name": "value", "type": "uint64"},
        ],
        "clock_level": 2,
        "persist": True,
        "chunk_size_mb": 0.03,
        "index_playback": False,
    })
    w = p.create_writer("scan")
    base_ts = 1_800_000_000_000_000
    for i in range(n):
        recv = base_ts + i * 10
        w.write_values(recv, recv + 3, i)
    w.close()
    return p, base_ts


def test_chunk_reader_iter_raw_range_matches_range_and_batches():
    with tempfile.TemporaryDirectory(prefix="dw-range-scan-") as td:
        p, base_ts = _build_feed(Path(td))
        r = ChunkReader(p, "scan")
        try:
            start_idx = 137
            end_idx = 4321
            start = base_ts + start_idx * 10 + 3
            end = base_ts + end_idx * 10 + 3

            expected_tuples = r.range(start, end, ts_key="proc_us")
            raw_range = bytes(r.range(start, end, format="raw", ts_key="proc_us"))
            raw_batches = b"".join(
                bytes(batch)
                for batch in r.range_batches(
                    start,
                    end,
                    format="raw",
                    ts_key="proc_us",
                    batch_records=17,
                )
            )
            raw_iter = b"".join(
                bytes(batch)
                for batch in r.iter_raw_range(
                    start,
                    end,
                    ts_key="proc_us",
                    batch_records=17,
                )
            )
            tuple_batches = [
                record
                for batch in r.range_batches(
                    start,
                    end,
                    ts_key="proc_us",
                    batch_records=19,
                )
                for record in batch
            ]

            assert len(expected_tuples) == end_idx - start_idx
            assert len(raw_range) == len(expected_tuples) * r.record_size
            assert raw_iter == raw_range
            assert raw_batches == raw_range
            assert tuple_batches == expected_tuples
        finally:
            r.close()
            p.close()


def test_chunk_reader_iter_raw_range_respects_empty_and_batch_limit():
    with tempfile.TemporaryDirectory(prefix="dw-range-scan-empty-") as td:
        p, base_ts = _build_feed(Path(td), n=100)
        r = ChunkReader(p, "scan")
        try:
            empty = list(r.iter_raw_range(base_ts - 1000, base_ts - 500, batch_records=3))
            assert empty == []

            batches = list(r.iter_raw_range(base_ts, base_ts + 100, batch_records=3))
            assert [len(batch) for batch in batches] == [
                3 * r.record_size,
                3 * r.record_size,
                3 * r.record_size,
                1 * r.record_size,
            ]
        finally:
            r.close()
            p.close()


def run_tests():
    tests = [
        ("chunk_reader_iter_raw_range_matches_range_and_batches", test_chunk_reader_iter_raw_range_matches_range_and_batches),
        ("chunk_reader_iter_raw_range_respects_empty_and_batch_limit", test_chunk_reader_iter_raw_range_respects_empty_and_batch_limit),
    ]
    print("Range Scan Tests")
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
