#!/usr/bin/env python3
"""Micro-benchmark the Reader API surfaces.

This is intentionally broader than read_available(). It covers the reader
routes that are easy to regress while refactoring: ring range/point lookups,
durable chunk range/point lookups, raw batches, and timestamp-key selection.

Usage:
  python scripts/bench_reader.py
  python scripts/bench_reader.py --quick
  python scripts/bench_reader.py --records 1000000 --queries 100000 --repeat 9 --json out.json
"""
from __future__ import annotations

import argparse
import gc
import json
import statistics
import tempfile
import time
from pathlib import Path
from typing import Callable

from deepwater import Reader, Writer, create_feed


FIELDS = [
    {"name": "ev_us", "type": "uint64"},
    {"name": "recv_us", "type": "uint64"},
    {"name": "proc_us", "type": "uint64"},
    {"name": "id", "type": "uint64"},
    {"name": "price", "type": "float64"},
]


def _spec(feed_name: str, *, storage: str, chunk_mb: float) -> dict:
    return {
        "feed_name": feed_name,
        "mode": "UF",
        "fields": FIELDS,
        "clock_level": 3,
        "persist": storage == "chunk",
        "storage": storage,
        "chunk_size_mb": chunk_mb,
    }


def _seed(base_path: Path, feed_name: str, n_records: int) -> tuple[int, int]:
    writer = Writer(base_path, feed_name)
    base = time.time_ns() // 1_000
    for i in range(n_records):
        ts = base + i
        writer.write_values(ts, ts + 5, ts + 10, i, 100.0)
    writer.close()
    return base, base + n_records


def _time_ns(fn: Callable[[], int], repeat: int, warmup: int) -> list[float]:
    values = []
    gc.disable()
    try:
        for _ in range(warmup):
            fn()
        for _ in range(repeat):
            t0 = time.perf_counter_ns()
            count = fn()
            elapsed = time.perf_counter_ns() - t0
            values.append(elapsed / max(1, count))
    finally:
        gc.enable()
    return values


def _summary(values: list[float]) -> dict:
    values = sorted(values)
    n = len(values)
    return {
        "p50": statistics.median(values),
        "p90": values[min(int(n * 0.90), n - 1)],
        "p99": values[min(int(n * 0.99), n - 1)],
        "min": values[0],
        "max": values[-1],
        "mean": statistics.mean(values),
        "stdev": statistics.stdev(values) if n > 1 else 0.0,
        "samples": values,
    }


def _line(label: str, summary: dict, unit: str) -> None:
    print(
        f"{label:<34} "
        f"p50={summary['p50']:8.1f} {unit} "
        f"p90={summary['p90']:8.1f} "
        f"p99={summary['p99']:8.1f} "
        f"min={summary['min']:8.1f} "
        f"max={summary['max']:8.1f}"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=1_000_000)
    ap.add_argument("--queries", type=int, default=100_000)
    ap.add_argument("--repeat", type=int, default=9)
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--chunk-mb", type=float, default=256.0)
    ap.add_argument("--window", type=int, default=1024)
    ap.add_argument("--raw-batch-records", type=int, default=65_536)
    ap.add_argument("--quick", action="store_true", help="Use a short smoke-test profile")
    ap.add_argument("--json", type=Path, default=None, help="Write machine-readable benchmark results")
    args = ap.parse_args()

    if args.quick:
        n_records = 50_000
        n_queries = 5_000
        repeat = 3
        warmup = 1
        chunk_mb = 16.0
        window = 256
        raw_batch_records = 4096
    else:
        n_records = int(args.records)
        n_queries = int(args.queries)
        repeat = int(args.repeat)
        warmup = int(args.warmup)
        chunk_mb = float(args.chunk_mb)
        window = int(args.window)
        raw_batch_records = int(args.raw_batch_records)

    with tempfile.TemporaryDirectory(prefix="dw-reader-bench-") as td:
        base_path = Path(td)
        create_feed(base_path, _spec("ring", storage="ring", chunk_mb=chunk_mb))
        create_feed(base_path, _spec("chunk", storage="chunk", chunk_mb=chunk_mb))

        ring_start, ring_end = _seed(base_path, "ring", n_records)
        chunk_start, chunk_end = _seed(base_path, "chunk", n_records)

        ring = Reader(base_path, "ring")
        chunk = Reader(base_path, "chunk")
        ring.read_available()

        mid_ring = ring_start + n_records // 2
        mid_chunk = chunk_start + n_records // 2

        print(
            "Reader benchmark: "
            f"records={n_records} queries={n_queries} repeat={repeat} "
            f"warmup={warmup} window={window} chunk_mb={chunk_mb:g}"
        )
        print()

        def ring_empty_available() -> int:
            for _ in range(n_queries):
                ring.read_available()
            return n_queries

        def ring_range_tuple_default() -> int:
            for i in range(n_queries):
                start = mid_ring + (i % window)
                out = ring.range(start, start + window)
                if len(out) != window:
                    raise RuntimeError("ring range count mismatch")
            return n_queries

        def ring_range_tuple_named() -> int:
            for i in range(n_queries):
                start = mid_ring + (i % window) + 10
                out = ring.range(start, start + window, ts_key="proc_us")
                if len(out) != window:
                    raise RuntimeError("ring named range count mismatch")
            return n_queries

        def ring_first_after_named() -> int:
            for i in range(n_queries):
                start = mid_ring + (i % window) + 10
                if ring.first_after(start, ts_key="proc_us") is None:
                    raise RuntimeError("ring first_after miss")
            return n_queries

        def ring_first_before_named() -> int:
            for i in range(n_queries):
                value = mid_ring + (i % window) + 10
                if ring.first_before(value, ts_key="proc_us") is None:
                    raise RuntimeError("ring first_before miss")
            return n_queries

        def chunk_range_tuple_default() -> int:
            for i in range(n_queries):
                start = mid_chunk + (i % window)
                out = chunk.range(start, start + window)
                if len(out) != window:
                    raise RuntimeError("chunk range count mismatch")
            return n_queries

        def chunk_range_tuple_named() -> int:
            for i in range(n_queries):
                start = mid_chunk + (i % window) + 10
                out = chunk.range(start, start + window, ts_key="proc_us")
                if len(out) != window:
                    raise RuntimeError("chunk named range count mismatch")
            return n_queries

        def chunk_range_raw_full() -> int:
            raw = chunk.range(chunk_start, chunk_end, format="raw")
            expected = n_records * chunk.record_size
            if len(raw) != expected:
                raise RuntimeError("chunk raw full count mismatch")
            return n_records

        def chunk_range_batches_raw_full() -> int:
            total = 0
            for batch in chunk.range_batches(chunk_start, chunk_end, format="raw", batch_records=raw_batch_records):
                total += len(batch) // chunk.record_size
            if total != n_records:
                raise RuntimeError("chunk raw batches count mismatch")
            return n_records

        def chunk_first_after_named() -> int:
            for i in range(n_queries):
                start = mid_chunk + (i % window) + 10
                if chunk.first_after(start, ts_key="proc_us") is None:
                    raise RuntimeError("chunk first_after miss")
            return n_queries

        def chunk_first_before_named() -> int:
            for i in range(n_queries):
                value = mid_chunk + (i % window) + 10
                if chunk.first_before(value, ts_key="proc_us") is None:
                    raise RuntimeError("chunk first_before miss")
            return n_queries

        benches = [
            ("ring.read_available empty", ring_empty_available, "ns/call"),
            ("ring.range tuple default", ring_range_tuple_default, "ns/call"),
            ("ring.range tuple named", ring_range_tuple_named, "ns/call"),
            ("ring.first_after named", ring_first_after_named, "ns/call"),
            ("ring.first_before named", ring_first_before_named, "ns/call"),
            ("chunk.range tuple default", chunk_range_tuple_default, "ns/call"),
            ("chunk.range tuple named", chunk_range_tuple_named, "ns/call"),
            ("chunk.first_after named", chunk_first_after_named, "ns/call"),
            ("chunk.first_before named", chunk_first_before_named, "ns/call"),
            ("chunk.range raw full", chunk_range_raw_full, "ns/record"),
            ("chunk.range_batches raw full", chunk_range_batches_raw_full, "ns/record"),
        ]

        results = {
            "config": {
                "records": n_records,
                "queries": n_queries,
                "repeat": repeat,
                "warmup": warmup,
                "window": window,
                "chunk_mb": chunk_mb,
                "raw_batch_records": raw_batch_records,
            },
            "benchmarks": {},
        }

        for label, fn, unit in benches:
            summary = _summary(_time_ns(fn, repeat, warmup))
            _line(label, summary, unit)
            results["benchmarks"][label] = {"unit": unit, **summary}

        if args.json is not None:
            args.json.parent.mkdir(parents=True, exist_ok=True)
            args.json.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")

        ring.close()
        chunk.close()


if __name__ == "__main__":
    main()
