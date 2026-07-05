#!/usr/bin/env python3
"""Process-level coverage for persistent ring draining."""
from __future__ import annotations

import subprocess
import sys
import tempfile
import time
from pathlib import Path

from deepwater import Reader, Writer, create_feed
from deepwater.metadata.global_registry import GlobalRegistry


ROOT = Path(__file__).resolve().parents[1]


def _make_feed(base: Path, *, ring_size_mb: float = 1.0, chunk_size_mb: float = 1.0) -> None:
    create_feed(base, {
        "feed_name": "events",
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "value", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": True,
        "uses_ring": True,
        "storage": "ring",
        "chunk_size_mb": chunk_size_mb,
        "ring_size_mb": ring_size_mb,
    })


def _start_persister(base: Path) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "deepwater.io.writer.persistent_ring", str(base)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _proc_output(proc: subprocess.Popen) -> str:
    stdout, stderr = proc.communicate(timeout=1)
    return f"stdout={stdout!r} stderr={stderr!r}"


def _wait_for_owner(base: Path, proc: subprocess.Popen, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError(f"persister exited early: {_proc_output(proc)}")
        registry = GlobalRegistry(base)
        try:
            owner = registry.get_persistent_ring_owner()
        finally:
            registry.close()
        if int(owner.get("pid") or 0) == proc.pid and owner.get("healthy"):
            return
        time.sleep(0.02)
    raise AssertionError("persister did not claim persistent ring ownership")


def _wait_for_durable(writer, expected_count: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if int(writer.ring.durable_record_count[0]) >= int(expected_count):
            return
        time.sleep(0.02)
    header = writer.ring.header()
    raise AssertionError(f"durable count did not reach {expected_count}; header={header}")


def test_writer_close_requests_drain_from_live_persister_process():
    with tempfile.TemporaryDirectory(prefix="dw-persister-close-") as td:
        base = Path(td)
        _make_feed(base)
        proc = _start_persister(base)
        writer = None
        try:
            _wait_for_owner(base, proc)
            writer = Writer(base, "events")
            start = 2_200_000_000_000_000
            for i in range(7):
                writer.write_values(start + i, i)

            writer.close()
            writer = None
            proc.wait(timeout=5)
            assert proc.returncode == 0, _proc_output(proc)

            reader = Reader(base, "events")
            try:
                assert reader.range(start, start + 7) == [(start + i, i) for i in range(7)]
            finally:
                reader.close()
        finally:
            if writer is not None:
                writer.close()
            _stop_process(proc)


def test_persister_process_restart_resumes_without_gaps_or_duplicates():
    with tempfile.TemporaryDirectory(prefix="dw-persister-restart-") as td:
        base = Path(td)
        _make_feed(base)
        proc1 = _start_persister(base)
        proc2 = None
        writer = None
        try:
            _wait_for_owner(base, proc1)
            writer = Writer(base, "events")
            start = 2_300_000_000_000_000
            for i in range(3):
                writer.write_values(start + i, i)
            writer.ring.request_drain()
            _wait_for_durable(writer, 3)

            _stop_process(proc1)
            assert proc1.returncode == 0, _proc_output(proc1)

            for i in range(3, 8):
                writer.write_values(start + i, i)

            proc2 = _start_persister(base)
            _wait_for_owner(base, proc2)
            writer.close()
            writer = None
            proc2.wait(timeout=5)
            assert proc2.returncode == 0, _proc_output(proc2)

            reader = Reader(base, "events")
            try:
                assert reader.range(start, start + 8) == [(start + i, i) for i in range(8)]
            finally:
                reader.close()
        finally:
            if writer is not None:
                writer.close()
            _stop_process(proc1)
            if proc2 is not None:
                _stop_process(proc2)


def test_drain_records_overrun_when_durable_cursor_falls_behind_live_window():
    from deepwater.io.writer.persistent_ring import _drain, _FeedState, _FLUSHED

    with tempfile.TemporaryDirectory(prefix="dw-persister-overrun-") as td:
        base = Path(td)
        _make_feed(base, ring_size_mb=0.001)
        writer = Writer(base, "events")
        try:
            start = 2_400_000_000_000_000
            cap = writer._ring_capacity
            for i in range(cap):
                writer.write_values(start + i, i)

            writer.ring.update_durable_header(durable_record_count=1, durable_last_ts=start)
            writer.write_values(start + cap, cap)
            writer.ring.update_durable_header(durable_record_count=0)

            state = _FeedState(base, "events")
            try:
                assert _drain(base, state) == _FLUSHED
            finally:
                state.close()

            header = writer.ring.header()
            assert header[7] == 1
            assert header[8] == 1
        finally:
            writer.close()
