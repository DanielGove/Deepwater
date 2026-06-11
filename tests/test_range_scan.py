#!/usr/bin/env python3
"""Parity tests for raw historical range scanning extraction."""
import tempfile
from pathlib import Path


from deepwater import Reader, Writer, create_feed


def _build_feed(base: Path, n: int = 5000):
    create_feed(base, {
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
    })
    w = Writer(base, "scan")
    base_ts = 1_800_000_000_000_000
    for i in range(n):
        recv = base_ts + i * 10
        w.write_values(recv, recv + 3, i)
    w.close()
    return base_ts


def test_reader_iter_raw_range_matches_range_and_batches():
    with tempfile.TemporaryDirectory(prefix="dw-range-scan-") as td:
        base = Path(td)
        base_ts = _build_feed(base)
        r = Reader(base, "scan")
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
            assert raw_batches == raw_range
            assert tuple_batches == expected_tuples
        finally:
            r.close()


def test_reader_raw_range_batches_respects_empty_and_batch_limit():
    with tempfile.TemporaryDirectory(prefix="dw-range-scan-empty-") as td:
        base = Path(td)
        base_ts = _build_feed(base, n=100)
        r = Reader(base, "scan")
        try:
            empty = list(r.range_batches(base_ts - 1000, base_ts - 500, format="raw", batch_records=3))
            assert empty == []

            batches = list(r.range_batches(base_ts, base_ts + 100, format="raw", batch_records=3))
            assert [len(batch) for batch in batches] == [
                3 * r.record_size,
                3 * r.record_size,
                3 * r.record_size,
                1 * r.record_size,
            ]
        finally:
            r.close()
