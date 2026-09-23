#!/usr/bin/env python3
"""Bounded, cross-process latency probe for complete Deepwater records.

Run from the checkout's Python environment. Data is created in a unique temporary
directory; this never attaches to existing feeds. See benchmarks/README.md.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import platform
import struct
import subprocess
import sys
import tempfile
import time
import traceback


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

HEADER = struct.Struct("<QQ")
PROTOCOL = 1


def distribution(values):
    """Nearest-rank percentiles; input ns, output microseconds."""
    import math

    ordered = sorted(values)
    if not ordered:
        raise ValueError("empty latency sample")
    def percentile(p):
        return ordered[max(0, math.ceil(p * len(ordered)) - 1)] / 1_000
    return {
        "count": len(ordered), "mean_us": sum(ordered) / len(ordered) / 1_000,
        "p50_us": percentile(.50), "p95_us": percentile(.95),
        "p99_us": percentile(.99), "max_us": ordered[-1] / 1_000,
    }


def _affinity(cpu):
    if cpu is not None:
        os.sched_setaffinity(0, {cpu})


def _feed_name(base):
    identity = hashlib.sha256(str(Path(base).resolve()).encode()).hexdigest()[:16]
    return f"latency_probe_{identity}"


def _consume(base, mode, count, payload_bytes, cpu, connection):
    from deepwater import Reader

    reader = None
    try:
        _affinity(cpu)
        reader = Reader(base, _feed_name(base))
        reader.read_available()  # Establish a live cursor before the writer starts.
        connection.send({"ready": True})
        received = []
        suffix = b"x" * (payload_bytes - HEADER.size)
        deadline = time.monotonic() + 45
        while len(received) < count:
            if time.monotonic() > deadline:
                raise TimeoutError(f"reader received {len(received)}/{count} records")
            rows = reader.read_available(max_records=64)
            if not rows:
                time.sleep(0)
                continue
            for row in rows:
                payload = reader.blob(row, sidecar="payload") if mode == "blob" else row[1]
                complete_ns = time.perf_counter_ns()
                sequence, start_ns = HEADER.unpack_from(payload)
                if sequence != len(received) or len(payload) != payload_bytes or payload[HEADER.size:] != suffix:
                    raise AssertionError(f"missing, reordered or corrupted payload at {len(received)}")
                received.append((sequence, start_ns, complete_ns))
        connection.send({"received": received})
    except BaseException:
        connection.send({"error": traceback.format_exc()})
    finally:
        if reader is not None:
            reader.close()
        connection.close()


def _produce(base, mode, samples, warmup, rate, payload_bytes, ring_kib, writer_cpu, reader_cpu, connection):
    from deepwater import Writer, create_feed, delete_feed
    from deepwater.io.writer.persistent_ring import run_persister
    from deepwater.metadata.admin import BlobSidecarSpec, FeedSpec, FieldSpec
    from deepwater.metadata.global_registry import GlobalRegistry

    base = Path(base)
    writer = consumer = persister = None
    read_connection = child_connection = None
    ctx = mp.get_context("spawn")
    try:
        fields = (FieldSpec("ts", "uint64"),)
        if mode == "inline":
            fields += (FieldSpec("payload", f"bytes{payload_bytes}"),)
        create_feed(base, FeedSpec(
            feed_name=_feed_name(base), fields=fields, clock_level=1,
            persist=True, uses_ring=True, ring_size_mb=ring_kib / 1024, chunk_size_mb=1,
            sidecars=(BlobSidecarSpec("payload", chunk_size_mb=1),) if mode == "blob" else (),
        ))
        setup_start = time.perf_counter_ns()
        writer = Writer(base, _feed_name(base))
        writer_open_ns = time.perf_counter_ns() - setup_start
        persister = ctx.Process(target=run_persister, args=(base,))
        persister.start()  # Start before pinning the producer, so it can use other CPUs.
        deadline = time.monotonic() + 5
        while True:
            registry = GlobalRegistry(base)
            try:
                owner = registry.get_persistent_ring_owner()
            finally:
                registry.close()
            if owner.get("pid") == persister.pid and owner.get("healthy"):
                break
            if not persister.is_alive() or time.monotonic() > deadline:
                raise TimeoutError("persister did not become ready")
            time.sleep(.01)
        read_connection, child_connection = ctx.Pipe(duplex=False)
        consumer = ctx.Process(target=_consume, args=(
            base, mode, samples + warmup, payload_bytes, reader_cpu, child_connection,
        ))
        consumer.start()
        child_connection.close()
        if not read_connection.poll(5):
            raise TimeoutError("reader did not become ready")
        ready = read_connection.recv()
        if ready != {"ready": True}:
            raise RuntimeError(ready)
        _affinity(writer_cpu)
        suffix = b"x" * (payload_bytes - HEADER.size)
        start_schedule = time.perf_counter_ns()
        writes = []
        for sequence in range(samples + warmup):
            due = start_schedule + round(sequence * 1_000_000_000 / rate)
            remaining = due - time.perf_counter_ns()
            if remaining > 0:
                time.sleep(remaining / 1_000_000_000)
            start = time.perf_counter_ns()
            payload = HEADER.pack(sequence, start) + suffix
            call_start = time.perf_counter_ns()
            if mode == "blob":
                writer.write_blob(start // 1_000, payload, sidecar="payload")
            else:
                writer.write_values(start // 1_000, payload)
            writes.append((due, start, call_start, time.perf_counter_ns()))
        if not read_connection.poll(10):
            raise TimeoutError("reader did not return complete payload measurements")
        result = read_connection.recv()
        if "error" in result:
            raise RuntimeError(result["error"])
        consumer.join(5)
        if consumer.exitcode != 0:
            raise RuntimeError(f"reader exit code: {consumer.exitcode}")
        if len(result["received"]) != len(writes):
            raise AssertionError("reader and writer sample counts differ")
        measurements = []
        for (due, start, call_start, end), (sequence, observed_start, received) in zip(writes, result["received"]):
            if start != observed_start:
                raise AssertionError("payload timestamp differs from producer timestamp")
            measurements.append({
                "sequence": sequence, "write_call_ns": end - call_start,
                "delivery_ns": received - start,
                "scheduled_delivery_ns": received - due,
                "producer_lateness_ns": start - due,
            })
        # The low-volume tail may need an explicit drain. This is separate from
        # publication measurements and is not called a power-loss proof.
        target = int(writer.ring.record_count[0])
        durable_before_final_drain = int(writer.ring.durable_record_count[0])
        drain_start = time.perf_counter_ns()
        writer.ring.request_drain()
        deadline = time.monotonic() + 5
        while int(writer.ring.durable_record_count[0]) < target:
            if not persister.is_alive() or time.monotonic() > deadline:
                raise TimeoutError("persister did not acknowledge the index tail")
            time.sleep(.001)
        index_drain_ns = time.perf_counter_ns() - drain_start
        _require_lossless_index(writer.ring.overrun_count(), int(writer.ring.lost_records[0]))
        measured = measurements[warmup:]
        summary = {key.removesuffix("_ns"): distribution([row[key] for row in measured])
                   for key in ("write_call_ns", "delivery_ns", "scheduled_delivery_ns", "producer_lateness_ns")}
        connection.send({
            "mode": mode, "summary": summary, "samples": measured,
            "first_record": measurements[0], "writer_open_us": writer_open_ns / 1_000,
            "final_index_drain_us": index_drain_ns / 1_000,
            "ring_bytes": writer.ring.data_size,
            "index_durable_before_final_drain": durable_before_final_drain,
            "index_durable_count": int(writer.ring.durable_record_count[0]),
            "overrun_count": writer.ring.overrun_count(),
            "lost_records": int(writer.ring.lost_records[0]),
        })
    except BaseException:
        connection.send({"error": traceback.format_exc()})
    finally:
        for proc in (consumer, persister):
            if proc is not None and proc.is_alive():
                proc.terminate()
                proc.join(2)
                if proc.is_alive():
                    proc.kill()
                    proc.join(2)
        if writer is not None:
            writer.close()
        for conn in (read_connection, child_connection):
            if conn is not None:
                conn.close()
        delete_feed(base, _feed_name(base), missing_ok=True)
        connection.close()


def _run_case(base, mode, args):
    ctx = mp.get_context("spawn")
    receive, send = ctx.Pipe(duplex=False)
    # A separate session lets the watchdog terminate this probe's descendants
    # if the existing engine blocks indefinitely on a full ring.
    process = ctx.Process(target=_case_session, args=(str(base), mode, vars(args), send))
    process.start()
    send.close()
    session_id = None
    try:
        if not receive.poll(5):
            raise TimeoutError("producer session did not become ready")
        session = receive.recv()
        if session != {"session_id": process.pid}:
            raise RuntimeError(f"unexpected producer session: {session}")
        session_id = process.pid
        if not receive.poll(55):
            raise TimeoutError(f"{mode} case exceeded its 55-second watchdog")
        result = receive.recv()
        if "error" in result:
            raise RuntimeError(result["error"])
        process.join(5)
        if process.exitcode != 0:
            raise RuntimeError(f"{mode} producer exit code: {process.exitcode}")
        return result
    finally:
        # Descendants may remain even after their producer has crashed.
        if session_id is not None:
            _kill_session(session_id)
        elif process.is_alive():
            process.kill()
        process.join(2)
        receive.close()
        # Remove only this unique temporary feed's shared memory, including on
        # watchdog failures. Never glob/unlink other applications' dw_* objects.
        from deepwater.io.ring import RingBuffer
        RingBuffer.unlink_for_feed(base, _feed_name(base))


def _kill_session(session_id):
    import signal
    try:
        os.killpg(session_id, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _require_lossless_index(overrun, lost):
    if overrun or lost:
        raise AssertionError(f"persistent index lost data: overrun={overrun}, lost={lost}")


def _case_session(base, mode, args, connection):
    os.setsid()
    connection.send({"session_id": os.getpid()})
    _produce(base, mode, args["samples"], args["warmup"], args["rate"], args["payload_bytes"],
             args["ring_kib"], args["writer_cpu"], args["reader_cpu"], connection)


def _filesystem(path):
    resolved = str(Path(path).resolve())
    matches = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, right = line.split(" - ", 1)
        mount = left.split()[4].replace("\\040", " ")
        if resolved == mount or resolved.startswith(mount.rstrip("/") + "/"):
            fields = right.split()
            matches.append((len(mount), {"mount": mount, "type": fields[0], "device": fields[1]}))
    return max(matches, key=lambda entry: entry[0])[1]


def _code_version():
    import importlib
    import deepwater
    root = Path(deepwater.__file__).resolve().parent.parent
    digest = hashlib.sha256()
    for path in sorted((root / "deepwater").rglob("*")):
        if path.suffix in (".py", ".pyx"):
            digest.update(str(path.relative_to(root)).encode())
            digest.update(path.read_bytes())
    git = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True)
    native = {}
    for name in ("deepwater.io.ring_shadow", "deepwater.io.reader.traversal", "deepwater.io.reader.reader"):
        path = Path(importlib.import_module(name).__file__)
        native[name] = {"file": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    return {"checkout": str(root), "commit": git.stdout.strip() if git.returncode == 0 else None,
            "source_sha256": digest.hexdigest(), "native_modules": native,
            "probe_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}


def compare_reports(current, baseline):
    if current["protocol"] != baseline["protocol"] or current["workload"] != baseline["workload"]:
        raise ValueError("baseline uses a different probe protocol or workload")
    if current["code"]["probe_sha256"] != baseline["code"]["probe_sha256"]:
        raise ValueError("baseline uses a different probe implementation")
    for key in ("python", "machine", "platform", "affinity", "filesystem"):
        if current["environment"][key] != baseline["environment"][key]:
            raise ValueError(f"baseline environment differs: {key}")
    comparisons = []
    old_cases = {case["mode"]: case for case in baseline["cases"]}
    if set(old_cases) != {case["mode"] for case in current["cases"]}:
        raise ValueError("baseline cases differ")
    for case in current["cases"]:
        for metric, stats in case["summary"].items():
            old = old_cases[case["mode"]]["summary"][metric]["p99_us"]
            new = stats["p99_us"]
            comparisons.append({"mode": case["mode"], "metric": metric,
                                "baseline_p99_us": old, "current_p99_us": new,
                                "change_pct": (new / old - 1) * 100 if old > 0 else None})
    return comparisons


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--compare", type=Path)
    parser.add_argument("--samples", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--rate", type=int, default=1000, help="scheduled records/second, without per-record reader ACKs")
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--ring-kib", type=int, default=64, help="requested index-ring budget; actual size includes alignment")
    parser.add_argument("--storage-root", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--writer-cpu", type=int)
    parser.add_argument("--reader-cpu", type=int)
    args = parser.parse_args(argv)
    total = args.samples + args.warmup
    if not (10 <= args.samples <= 10000 and 1 <= args.warmup <= 1000 and 16 <= args.payload_bytes <= 65536):
        parser.error("samples must be 10..10000, warmup 1..1000, payload-bytes 16..65536")
    if args.rate < 1 or total / args.rate > 30 or total * args.payload_bytes > 32 * 1024 * 1024:
        parser.error("each case must schedule <=30 seconds and <=32 MiB of payload")
    from deepwater.io.ring import normalize_ring_data_size
    if not 4 <= args.ring_kib <= 8192:
        parser.error("ring-kib must be 4..8192")
    if normalize_ring_data_size(args.ring_kib * 1024, 8 + args.payload_bytes) > 40 * 1024 * 1024:
        parser.error("payload alignment would allocate a ring larger than 40 MiB; choose a different size")
    allowed_cpus = sorted(os.sched_getaffinity(0))
    if any(cpu is not None and cpu not in allowed_cpus for cpu in (args.writer_cpu, args.reader_cpu)):
        parser.error("requested CPU is outside the process affinity")
    if not args.storage_root.is_dir():
        parser.error("storage-root must be an existing directory")
    workload = {name: getattr(args, name) for name in ("samples", "warmup", "rate", "payload_bytes", "ring_kib", "writer_cpu", "reader_cpu")}
    report = {"protocol": PROTOCOL, "workload": workload, "code": _code_version(),
              "environment": {"python": sys.version, "platform": platform.platform(),
                              "machine": platform.node(), "affinity": allowed_cpus,
                              "filesystem": _filesystem(args.storage_root), "load_average": os.getloadavg()},
              "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "cases": []}
    baseline = json.loads(args.compare.read_text()) if args.compare else None
    if baseline is not None:
        compare_reports(report, {**baseline, "cases": []})  # Reject mismatches before workloads.
    for mode in ("inline", "blob"):
        with tempfile.TemporaryDirectory(prefix=f"dw-latency-{mode}-", dir=args.storage_root) as temporary:
            result = _run_case(Path(temporary) / "feeds", mode, args)
        report["cases"].append(result)
        print(f"{mode}: write p99={result['summary']['write_call']['p99_us']:.1f} us; "
              f"payload delivery p99={result['summary']['delivery']['p99_us']:.1f} us; "
              f"scheduled delivery p99={result['summary']['scheduled_delivery']['p99_us']:.1f} us", flush=True)
    if baseline is not None:
        report["comparison"] = compare_reports(report, baseline)
        for item in report["comparison"]:
            if item["metric"] == "delivery" and item["change_pct"] is not None:
                print(f"{item['mode']}: delivery p99 change {item['change_pct']:+.1f}% versus baseline")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Saved {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
