#!/usr/bin/env python3
"""
Quick micro-benchmark for Writer hot paths.

Measures:
  - write_values loop
  - write_batch_bytes (optional)

Usage:
  python scripts/bench_writer.py --records 1_000_000 --chunk-mb 64
  python scripts/bench_writer.py --records 500_000 --batch 1000
"""
from __future__ import annotations

import argparse
import struct
import tempfile
import time
import statistics
from pathlib import Path

from deepwater import Platform


def make_platform(base: Path, chunk_mb: int) -> Platform:
    p = Platform(str(base))
    spec = {
        "feed_name": "bench",
        "mode": "UF",
        "fields": [
            {"name": "ev_us", "type": "uint64"},
            {"name": "recv_us", "type": "uint64"},
            {"name": "proc_us", "type": "uint64"},
            {"name": "id", "type": "uint64"},
            {"name": "price", "type": "float64"},
        ],
        "clock_level": 3,
        "persist": True,
        "chunk_size_mb": chunk_mb,
        "index_playback": False,
    }
    p.create_feed(spec)
    return p


def bench_write_values(writer, n_records: int) -> tuple[float, int, int]:
    """Return (elapsed_s, start_ts, end_ts)."""
    base = time.time_ns() // 1_000
    start = time.perf_counter()
    for i in range(n_records):
        ts = base + 1234*i
        writer.write_values(ts, ts + 5, ts + 10, i, 100.0)
    writer.close()
    elapsed = time.perf_counter() - start
    end_ts = base + 1234 * n_records
    return elapsed, base, end_ts


def bench_write_batch(writer, batch_size: int, n_records: int) -> tuple[float, int, int]:
    """Return (elapsed_s, start_ts, end_ts) using write_batch_bytes."""
    fmt = f"<QQQQd"
    S = struct.Struct(fmt)
    rec_size = S.size
    base = time.time_ns() // 1_000
    batch_bytes = bytearray(batch_size * rec_size)
    start = time.perf_counter()
    written = 0
    while written < n_records:
        batch_count = min(batch_size, n_records - written)
        for j in range(batch_count):
            idx = written + j
            ts = base + idx
            S.pack_into(batch_bytes, j * rec_size, ts, ts + 5, ts + 10, idx, 100.0)
        writer.write_batch_bytes(batch_bytes[: batch_count * rec_size])
        written += batch_count
    writer.close()
    elapsed = time.perf_counter() - start
    end_ts = base + n_records
    return elapsed, base, end_ts


def bench_read_range(reader, start_us: int, end_us: int, n_records: int, fmt: str) -> float:
    """Return elapsed seconds for reader.range()."""
    start = time.perf_counter()
    data = reader.range(start_us, end_us, format=fmt)
    elapsed = time.perf_counter() - start
    if fmt == "raw":
        count = len(data) // reader.record_size
    else:
        count = len(data)
    if count != n_records:
        raise RuntimeError(f"read count mismatch: expected {n_records}, got {count}")
    return elapsed


def summarize(label: str, values: list[float]) -> None:
    mean = statistics.mean(values)
    median = statistics.median(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    vmin = min(values)
    vmax = max(values)
    print(
        f"{label}: mean {mean:.1f} ns/record (median {median:.1f}, min {vmin:.1f}, max {vmax:.1f}, stdev {stdev:.1f})"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=500_000, help="Total records to write")
    ap.add_argument("--chunk-mb", type=int, default=64, help="Chunk size in MB")
    ap.add_argument("--batch", type=int, default=0, help="Batch size for write_batch_bytes (0 to skip)")
    ap.add_argument("--repeat", type=int, default=5, help="Number of runs for variance stats")
    ap.add_argument("--read-format", type=str, default="tuple", choices=["tuple", "dict", "numpy", "raw"], help="Reader range() format")
    args = ap.parse_args()

    with tempfile.TemporaryDirectory(prefix="dw-bench-") as td:
        base = Path(td)

        write_values_ns = []
        read_values_ns = []
        for i in range(args.repeat):
            p = make_platform(base / f"wv_{i}", chunk_mb=args.chunk_mb)
            writer = p.create_writer("bench")
            elapsed, start_ts, end_ts = bench_write_values(writer, args.records)
            write_values_ns.append((elapsed / args.records) * 1e9)

            reader = p.create_reader("bench")
            read_elapsed = bench_read_range(reader, start_ts, end_ts, args.records, args.read_format)
            read_values_ns.append((read_elapsed / args.records) * 1e9)
            reader.close()
            p.close()

        print(f"write_values over {args.records} records (chunk {args.chunk_mb}MB)")
        summarize("  write_values", write_values_ns)
        print(f"read_range format='{args.read_format}'")
        summarize("  reader.range", read_values_ns)

        if args.batch > 0:
            write_batch_ns = []
            read_batch_ns = []
            for i in range(args.repeat):
                p = make_platform(base / f"wb_{i}", chunk_mb=args.chunk_mb)
                writer = p.create_writer("bench")
                elapsed, start_ts, end_ts = bench_write_batch(writer, args.batch, args.records)
                write_batch_ns.append((elapsed / args.records) * 1e9)

                reader = p.create_reader("bench")
                read_elapsed = bench_read_range(reader, start_ts, end_ts, args.records, args.read_format)
                read_batch_ns.append((read_elapsed / args.records) * 1e9)
                reader.close()
                p.close()

            print(f"write_batch_bytes (batch={args.batch}) over {args.records} records")
            summarize("  write_batch_bytes", write_batch_ns)
            print(f"read_range format='{args.read_format}'")
            summarize("  reader.range", read_batch_ns)


if __name__ == "__main__":
    main()
