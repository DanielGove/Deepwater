"""Small complete-record checks for shared-memory blob publication and recovery."""
from __future__ import annotations

import builtins
import io
import os
from pathlib import Path
import select
import subprocess
import sys
import time

import pytest

from deepwater import Reader, Writer, create_feed
from deepwater.io.blob_sidecar import PayloadUnavailable
from deepwater.io.writer.persistent_ring import _FeedState, _drain
from deepwater.metadata.admin import BlobSidecarSpec, FeedSpec, FieldSpec


FEED = "blob_live"
SIDECAR = "payload"
PAYLOAD_BYTES = 8192


def _make_feed(base, *, persist=True, uses_ring=True):
    create_feed(base, FeedSpec(
        feed_name=FEED,
        fields=(FieldSpec("ts", "uint64"),),
        clock_level=1,
        persist=persist,
        uses_ring=uses_ring,
        ring_size_mb=0.0625,
        chunk_size_mb=0.0625,
        sidecars=(BlobSidecarSpec(
            SIDECAR, chunk_size_mb=0.01, ring_size_mb=0.03,
        ),),
    ))


def _payload_files(base):
    return list((base / "data" / FEED / "blobs").rglob("*.bin"))


def _forbid_payload_file_io(monkeypatch):
    """Guard opens and already-open descriptors, without blocking shared memory."""
    real_readlink = os.readlink

    def assert_not_payload_file(value):
        if isinstance(value, int):
            try:
                value = real_readlink(f"/proc/self/fd/{value}")
            except OSError:
                return
        try:
            path = os.fsdecode(value)
        except TypeError:
            return
        if "/blobs/" in path and Path(path).name.startswith("blob_"):
            pytest.fail(f"live payload path performed durable file I/O: {path}")

    def guard(function):
        def wrapped(path_or_fd, *args, **kwargs):
            assert_not_payload_file(path_or_fd)
            return function(path_or_fd, *args, **kwargs)
        return wrapped

    for module, name in ((builtins, "open"), (io, "open"), (os, "open")):
        monkeypatch.setattr(module, name, guard(getattr(module, name)))
    for name in ("read", "write", "pread", "pwrite", "fsync", "fdatasync"):
        if hasattr(os, name):
            monkeypatch.setattr(os, name, guard(getattr(os, name)))


def _drain_all(base, writer, state):
    writer.ring.request_drain()
    # These fixtures have fewer than 20 index rows: one drain normally suffices.
    for _ in range(20):
        if int(writer.ring.durable_record_count[0]) == int(writer.ring.record_count[0]):
            return
        _drain(base, state)
    pytest.fail("tiny fixture did not become durable after 20 drain attempts")


@pytest.mark.parametrize("persist", [False, True])
def test_live_blob_round_trip_has_no_durable_payload_io(base_path, monkeypatch, persist):
    _make_feed(base_path, persist=persist)
    with Writer(base_path, FEED) as writer, Reader(base_path, FEED) as reader:
        with monkeypatch.context() as patch:
            _forbid_payload_file_io(patch)
            for ts, payload in ((100, b"first complete payload"), (101, b"second payload")):
                ref = writer.write_blob(ts, payload, sidecar=SIDECAR)
                row = reader.range(ts, ts + 1)[0]
                assert reader.blob_ref(row, sidecar=SIDECAR) == ref
                assert reader.blob(row, sidecar=SIDECAR, memory_only=True) == payload
                assert reader.blob(row, sidecar=SIDECAR) == payload
        assert _payload_files(base_path) == []
        assert int(writer.ring.durable_record_count[0]) == 0


def _read_child_line(proc, timeout=5.0):
    assert proc.stdout is not None
    ready, _, _ = select.select([proc.stdout], [], [], timeout)
    assert ready, "bounded blob producer did not reach its next checkpoint"
    line = proc.stdout.readline().strip()
    assert line, f"blob producer exited early (status={proc.poll()})"
    return line


def test_payload_byte_pressure_drains_a_partial_index_batch(base_path):
    _make_feed(base_path)
    # A child gives the test a hard cleanup boundary if a broken producer blocks.
    # The first three payloads fit, while the fourth exceeds the payload ring;
    # all four index rows fit comfortably in the independent index ring.
    code = """
import sys
from deepwater import Writer
with Writer(sys.argv[1], 'blob_live') as writer:
    for i in range(3):
        writer.write_blob(100 + i, bytes([i]) * 8192, sidecar='payload')
    print('ready', flush=True)
    assert sys.stdin.readline().strip() == 'continue'
    writer.write_blob(103, bytes([3]) * 8192, sidecar='payload')
    print('published', flush=True)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", code, str(base_path)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1,
    )
    state = None
    try:
        assert _read_child_line(proc) == "ready"
        assert _payload_files(base_path) == []
        state = _FeedState(base_path, FEED)
        assert int(state.ring.record_count[0]) == 3
        assert state.target_records > 3
        _drain(base_path, state)
        assert int(state.ring.durable_record_count[0]) == 0
        assert proc.stdin is not None
        proc.stdin.write("continue\n")
        proc.stdin.flush()
        deadline = time.monotonic() + 3.0
        while not state.ring.drain_requested() and time.monotonic() < deadline:
            assert proc.poll() is None, "producer failed before requesting payload pressure drain"
            time.sleep(0.002)
        assert state.ring.drain_requested(), "payload pressure did not request an early index drain"
        assert int(state.ring.record_count[0]) == 3, "index published before payload capacity was safe"
        _drain(base_path, state)
        assert int(state.ring.durable_record_count[0]) >= 3
        assert _read_child_line(proc) == "published"
        assert proc.wait(timeout=5) == 0
        with Reader(base_path, FEED) as reader:
            rows = reader.range(100, 104)
            assert len(rows) == 4
            assert [reader.blob(row, sidecar=SIDECAR) for row in rows] == [
                bytes([i]) * PAYLOAD_BYTES for i in range(4)
            ]
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        if state is not None:
            state.close()
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            if stream is not None:
                stream.close()


def test_evicted_persisted_blob_uses_disk_but_memory_only_never_does(base_path, monkeypatch):
    _make_feed(base_path)
    first_payload = b"A" * PAYLOAD_BYTES
    with Writer(base_path, FEED) as writer, Reader(base_path, FEED) as reader:
        state = _FeedState(base_path, FEED)
        try:
            writer.write_blob(100, first_payload, sidecar=SIDECAR)
            first_row = reader.range(100, 101)[0]
            _drain_all(base_path, writer, state)
            for i in range(1, 9):
                writer.write_blob(100 + i, bytes([i]) * PAYLOAD_BYTES, sidecar=SIDECAR)
                _drain_all(base_path, writer, state)
            with monkeypatch.context() as patch:
                _forbid_payload_file_io(patch)
                with pytest.raises(PayloadUnavailable):
                    reader.blob(first_row, sidecar=SIDECAR, memory_only=True)
                latest = reader.range(108, 109)[0]
                assert reader.blob(latest, sidecar=SIDECAR, memory_only=True) == bytes([8]) * PAYLOAD_BYTES
            assert reader.blob(first_row, sidecar=SIDECAR) == first_payload
            assert _payload_files(base_path)
            assert int(writer.ring.durable_record_count[0]) == 9
        finally:
            state.close()


def test_volatile_payload_expiration_is_explicit_even_while_index_is_live(base_path, monkeypatch):
    _make_feed(base_path, persist=False)
    with Writer(base_path, FEED) as writer, Reader(base_path, FEED) as reader:
        writer.write_blob(100, b"A" * PAYLOAD_BYTES, sidecar=SIDECAR)
        first_row = reader.range(100, 101)[0]
        for i in range(1, 9):
            writer.write_blob(100 + i, bytes([i]) * PAYLOAD_BYTES, sidecar=SIDECAR)
        assert len(reader.range(100, 109)) == 9, "expiration must be caused by payload bytes, not index eviction"
        with monkeypatch.context() as patch:
            _forbid_payload_file_io(patch)
            for memory_only in (False, True):
                with pytest.raises(PayloadUnavailable):
                    reader.blob(first_row, sidecar=SIDECAR, memory_only=memory_only)
            latest = reader.range(108, 109)[0]
            assert reader.blob(latest, sidecar=SIDECAR) == bytes([8]) * PAYLOAD_BYTES
        assert _payload_files(base_path) == []


@pytest.mark.parametrize("uses_ring", [True, False])
def test_sidecar_rollover_preserves_each_reference(base_path, uses_ring):
    _make_feed(base_path, uses_ring=uses_ring)
    payloads = [bytes([i]) * 4096 for i in range(3)]
    with Writer(base_path, FEED) as writer:
        refs = [writer.write_blob(100 + i, payload, sidecar=SIDECAR) for i, payload in enumerate(payloads)]
    assert [(ref.chunk_id, ref.offset) for ref in refs] == [(1, 0), (1, 4096), (2, 0)]
    with Reader(base_path, FEED) as reader:
        rows = reader.range(100, 103)
        assert len(rows) == 3
        assert [reader.blob(row, sidecar=SIDECAR) for row in rows] == payloads
        if uses_ring:
            assert [reader.blob(row, sidecar=SIDECAR, memory_only=True) for row in rows] == payloads
            assert _payload_files(base_path) == []
        else:
            assert len(_payload_files(base_path)) == 2
            with pytest.raises(PayloadUnavailable):
                reader.blob(rows[0], sidecar=SIDECAR, memory_only=True)


@pytest.mark.parametrize("evict", [False, True])
def test_blob_crc_mismatch_is_rejected_in_memory_and_on_disk(base_path, evict):
    _make_feed(base_path)
    with Writer(base_path, FEED) as writer, Reader(base_path, FEED) as reader:
        writer.write_blob(100, b"checked payload" * 500, sidecar=SIDECAR)
        row = dict(reader.range(100, 101, format="dict")[0])
        state = _FeedState(base_path, FEED)
        try:
            if evict:
                _drain_all(base_path, writer, state)
                for i in range(1, 9):
                    writer.write_blob(100 + i, bytes([i]) * PAYLOAD_BYTES, sidecar=SIDECAR)
                    _drain_all(base_path, writer, state)
                with pytest.raises(PayloadUnavailable):
                    reader.blob(row, sidecar=SIDECAR, memory_only=True)
            row["payload_crc"] ^= 1
            with pytest.raises(ValueError, match="(?i)crc|checksum"):
                reader.blob(row, sidecar=SIDECAR, memory_only=not evict)
        finally:
            state.close()


def test_writer_restart_preserves_undurable_payloads_and_reference_positions(base_path):
    _make_feed(base_path)
    payloads = [bytes([i]) * 4096 for i in range(3)]
    with Writer(base_path, FEED) as writer:
        refs = [writer.write_blob(100 + i, payloads[i], sidecar=SIDECAR) for i in range(2)]
        assert int(writer.ring.durable_record_count[0]) == 0
    assert _payload_files(base_path) == []
    with Writer(base_path, FEED) as writer:
        assert int(writer.ring.record_count[0]) == 2
        assert int(writer.ring.durable_record_count[0]) == 0
        refs.append(writer.write_blob(102, payloads[2], sidecar=SIDECAR))
        assert len({(ref.chunk_id, ref.offset) for ref in refs}) == 3
        with Reader(base_path, FEED) as reader:
            rows = reader.range(100, 103)
            assert len(rows) == 3
            assert [reader.blob(row, sidecar=SIDECAR, memory_only=True) for row in rows] == payloads
        state = _FeedState(base_path, FEED)
        try:
            _drain_all(base_path, writer, state)
            assert int(writer.ring.durable_record_count[0]) == 3
        finally:
            state.close()
