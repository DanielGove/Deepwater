# --- METRICS HELPERS ---
from time import perf_counter_ns
import math

def mk_latency_meter():
    n = 0
    s = 0
    mx = 0
    buckets = [0] * 32  # ~[0..1us), [1..2us), ..., up to ~2s

    def bucket(ns: int) -> int:
        if ns <= 0: return 0
        return max(0, min(31, math.ceil(math.log2(ns)) - 10))  # 2^10 ~ 1us

    def record(dt_ns: int):
        nonlocal n, s, mx, buckets
        n += 1; s += dt_ns
        if dt_ns > mx: mx = dt_ns
        buckets[bucket(dt_ns)] += 1

    def snapshot(reset: bool = True):
        nonlocal n, s, mx, buckets
        if n == 0:
            stats = {"count": 0, "avg_us": 0.0, "p50_us": 0.0, "p90_us": 0.0, "p99_us": 0.0, "max_us": 0.0}
        else:
            avg_us = (s / n) / 1_000.0
            max_us = mx / 1_000.0
            targets = [0.50, 0.90, 0.99]
            got = [False, False, False]
            acc = 0
            p50 = p90 = p99 = 0.0
            for i, c in enumerate(buckets):
                acc += c
                us_upper = (1 << (i + 10)) / 1_000.0
                for j, t in enumerate(targets):
                    if not got[j] and acc >= t * n:
                        if j == 0: p50 = us_upper
                        elif j == 1: p90 = us_upper
                        else: p99 = us_upper
                        got[j] = True
            stats = {"count": n, "avg_us": avg_us, "p50_us": p50, "p90_us": p90, "p99_us": p99, "max_us": max_us}
        if reset:
            n = 0; s = 0; mx = 0; buckets = [0] * 32
        return stats

    return record, snapshot

def mk_throughput_meter():
    t0 = perf_counter_ns()
    bytes_acc = 0
    recs = 0

    def add(nbytes: int, nrecs: int = 1):
        nonlocal bytes_acc, recs
        bytes_acc += nbytes; recs += nrecs

    def snapshot(reset: bool = True):
        nonlocal t0, bytes_acc, recs
        dt = (perf_counter_ns() - t0) / 1e9
        if dt <= 0: dt = 1e-9
        rps = recs / dt
        mbps = (bytes_acc / 1e6) / dt
        snap = {"rps": rps, "MBps": mbps}
        if reset:
            t0 = perf_counter_ns(); bytes_acc = 0; recs = 0
        return snap

    return add, snapshot
