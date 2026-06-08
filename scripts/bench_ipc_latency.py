#!/usr/bin/env python3
"""
Measure cross-process publish -> read latency.

Usage:
  python scripts/bench_ipc_latency.py --records 10000 --chunk-mb 64 --sleep-us 0
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import time
import tempfile
import os
from pathlib import Path
from typing import List

from deepwater import Reader, Writer, create_feed
from deepwater.metadata.feed_registry import HEADER_SIZE


def make_feed(base: Path, chunk_mb: int) -> None:
    spec = {
        "feed_name": "bench",
        "mode": "UF",
        "fields": [
            {"name": "recv_ns", "type": "uint64"},
            {"name": "proc_ns", "type": "uint64"},
            {"name": "ev_ns", "type": "uint64"},
            {"name": "id", "type": "uint64"},
            {"name": "price", "type": "float64"},
        ],
        "clock_level": 3,
        "persist": True,
        "chunk_size_mb": chunk_mb,
    }
    create_feed(base, spec)


def now_ns() -> int:
    return time.time_ns()


def _pin_to_cpu(cpu_id: int) -> None:
    try:
        os.sched_setaffinity(0, {cpu_id})
    except AttributeError:
        pass


def _wait_for_registry(base_dir: str, timeout_s: float = 5.0) -> None:
    reg_path = Path(base_dir) / "data" / "bench" / "bench.reg"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if reg_path.exists():
            try:
                if os.path.getsize(reg_path) >= HEADER_SIZE:
                    return
            except OSError:
                pass
        time.sleep(0.01)
    raise RuntimeError("Registry not initialized in time")


def reader_proc(base_dir: str, total: int, sleep_us: int, conn, pin_cpu: int):
    import gc
    if pin_cpu >= 0:
        _pin_to_cpu(pin_cpu)
    _wait_for_registry(base_dir)
    reader = Reader(base_dir, "bench")
    conn.send("ready")

    start_ts = conn.recv()
    conn.send("spinning")
    gc.disable()
    latencies: List[int] = []
    read_count = 0
    done = False
    idle_start = None
    while True:
        records = reader.read_available()
        if records:
            for rec in records:
                sent_ns = rec[1]  # proc_ns
                if sent_ns >= start_ts:
                    latencies.append(now_ns() - sent_ns)
                    read_count += 1
                    if read_count >= total:
                        done = True
                        break
            idle_start = None
        else:
            if sleep_us > 0:
                time.sleep(sleep_us / 1_000_000)
        if conn.poll():
            msg = conn.recv()
            if msg == "done":
                done = True
        if done:
            if idle_start is None:
                idle_start = time.perf_counter()
            elif (time.perf_counter() - idle_start) > 0.25:
                break

    reader.close()
    conn.send(latencies)
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=10000, help="Total records to write")
    ap.add_argument("--chunk-mb", type=int, default=64, help="Chunk size in MB")
    ap.add_argument("--sleep-us", type=int, default=0, help="Reader sleep in microseconds between reads")
    ap.add_argument("--publish-us", type=int, default=0, help="Writer sleep in microseconds between writes")
    ap.add_argument("--pin-writer", type=int, default=-1, help="Pin writer process to CPU core (Linux only)")
    ap.add_argument("--pin-reader", type=int, default=-1, help="Pin reader process to CPU core (Linux only)")
    ap.add_argument("--warmup", type=int, default=1000, help="Drop first N records from stats (warmup)")
    args = ap.parse_args()

    with tempfile.TemporaryDirectory(prefix="dw-ipc-") as td:
        base = Path(td)
        make_feed(base, chunk_mb=args.chunk_mb)
        writer = Writer(base, "bench")

        parent_conn, child_conn = mp.Pipe()
        proc = mp.Process(
            target=reader_proc,
            args=(str(base), args.records, args.sleep_us, child_conn, args.pin_reader),
            daemon=True,
        )
        proc.start()
        if parent_conn.recv() != "ready":
            raise RuntimeError("reader failed to start")

        if args.pin_writer >= 0:
            _pin_to_cpu(args.pin_writer)

        base_ts = now_ns()
        parent_conn.send(base_ts)
        if parent_conn.recv() != "spinning":
            raise RuntimeError("reader not spinning")
        for i in range(args.records):
            ts = now_ns()
            writer.write_values(ts - 5, ts, ts - 10, i, 100.0)
            if args.publish_us > 0:
                time.sleep(args.publish_us / 1_000_000)
        writer.close()
        parent_conn.send("done")

        latencies = parent_conn.recv()
        proc.join(timeout=5)

        # Drop warmup records (first N by insertion order, before sorting)
        warmup = min(args.warmup, len(latencies) // 2)
        latencies = latencies[warmup:]
        latencies.sort()
        n = len(latencies)
        p50 = latencies[int(n * 0.50)]
        p90 = latencies[int(n * 0.90)]
        p99 = latencies[int(n * 0.99)]
        p999 = latencies[min(int(n * 0.999), n - 1)]
        print(
            f"IPC latency over {n} records (warmup={warmup}): "
            f"p50={p50}ns p90={p90}ns p99={p99}ns p99.9={p999}ns "
            f"min={latencies[0]}ns max={latencies[-1]}ns"
        )


if __name__ == "__main__":
    main()
