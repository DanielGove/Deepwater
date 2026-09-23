"""Bounded persister fault injection for payload/index commit atomicity."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from deepwater import Reader, Writer, create_feed
from deepwater.io.writer.commit_journal import CommitJournal
from deepwater.io.writer.persistent_ring import _FeedState, _drain
from deepwater.io.writer.writer import ChunkWriter
from deepwater.metadata.admin import BlobSidecarSpec, FeedSpec, FieldSpec


FEED = "blob_commit_failures"
PAYLOADS = (b"first record", b"second record", b"third record")


def _make_feed(base):
    create_feed(base, FeedSpec(
        feed_name=FEED,
        fields=(FieldSpec("ts", "uint64"),),
        clock_level=1,
        persist=True,
        uses_ring=True,
        chunk_size_mb=0.0625,
        ring_size_mb=0.0625,
        sidecars=(BlobSidecarSpec(
            "payload", chunk_size_mb=0.01, ring_size_mb=0.03,
        ),),
    ))


def _write_initial_batch(writer):
    # Repeated timestamps are legal; durable identity must use record sequence.
    for payload in PAYLOADS[:2]:
        writer.write_blob(100, payload, sidecar="payload")
    writer.ring.request_drain()


def _assert_rows(base, expected):
    with Reader(base, FEED) as reader:
        rows = reader.range(100, 101)
        assert len(rows) == len(expected), "failed commit was either lost or replayed twice"
        assert [reader.blob(row, sidecar="payload") for row in rows] == list(expected)


def _finish_recovery_and_append(base, writer):
    recovered = _FeedState(base, FEED)
    try:
        writer.ring.request_drain()
        _drain(base, recovered)
        assert int(writer.ring.durable_record_count[0]) == 2
        _assert_rows(base, PAYLOADS[:2])
        writer.write_blob(100, PAYLOADS[2], sidecar="payload")
        writer.ring.request_drain()
        _drain(base, recovered)
        assert int(writer.ring.durable_record_count[0]) == 3
        _assert_rows(base, PAYLOADS)
    finally:
        recovered.close()


@pytest.mark.parametrize("boundary", ["payload_fsync", "index_fsync"])
def test_failed_data_sync_is_not_acknowledged_and_recovers_once(base_path, monkeypatch, boundary):
    _make_feed(base_path)
    real_fsync = os.fsync
    failures = []

    def interrupted_fsync(fd):
        path = os.readlink(f"/proc/self/fd/{fd}")
        is_payload = "/blobs/payload/blob_" in path
        is_index = Path(path).name.startswith("chunk_") and path.endswith(".bin")
        selected = is_payload if boundary == "payload_fsync" else is_index
        if selected and not failures:
            failures.append(path)
            raise OSError(f"injected {boundary}")
        return real_fsync(fd)

    with Writer(base_path, FEED) as writer:
        _write_initial_batch(writer)
        state = _FeedState(base_path, FEED)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(os, "fsync", interrupted_fsync)
                with pytest.raises(OSError, match=f"injected {boundary}"):
                    _drain(base_path, state)
            assert len(failures) == 1, f"{boundary} was never exercised"
            assert int(writer.ring.durable_record_count[0]) == 0
            _assert_rows(base_path, PAYLOADS[:2])
        finally:
            state.close()
        _finish_recovery_and_append(base_path, writer)


@pytest.mark.parametrize("commit_finished", [False, True])
def test_interrupted_journal_commit_recovers_without_duplicate_index(base_path, monkeypatch, commit_finished):
    _make_feed(base_path)
    real_commit = CommitJournal.commit
    injected = []

    def interrupted_commit(journal):
        if commit_finished:
            real_commit(journal)
        injected.append(commit_finished)
        raise OSError("injected journal commit interruption")

    with Writer(base_path, FEED) as writer:
        _write_initial_batch(writer)
        state = _FeedState(base_path, FEED)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(CommitJournal, "commit", interrupted_commit)
                with pytest.raises(OSError, match="injected journal commit"):
                    _drain(base_path, state)
            assert injected == [commit_finished]
            assert int(writer.ring.durable_record_count[0]) == 0
        finally:
            state.close()

        if commit_finished:
            # Durable commit exists despite the failed return. Recovery must
            # acknowledge it, rather than append its bytes a second time.
            def forbid_index_rewrite(*args, **kwargs):
                pytest.fail("recovery rewrote an already committed index batch")

            with monkeypatch.context() as patch:
                patch.setattr(ChunkWriter, "write_batch_bytes", forbid_index_rewrite)
                recovered = _FeedState(base_path, FEED)
                try:
                    _drain(base_path, recovered)
                    assert int(writer.ring.durable_record_count[0]) == 2
                finally:
                    recovered.close()
        _finish_recovery_and_append(base_path, writer)


def test_interruption_after_ram_ack_is_safe_to_recover_repeatedly(base_path, monkeypatch):
    _make_feed(base_path)
    with Writer(base_path, FEED) as writer:
        _write_initial_batch(writer)
        state = _FeedState(base_path, FEED)
        real_ack = state.ring.update_durable_header
        injected = []

        def interrupted_ack(**kwargs):
            real_ack(**kwargs)
            injected.append(kwargs)
            raise RuntimeError("injected interruption after RAM acknowledgement")

        try:
            with monkeypatch.context() as patch:
                patch.setattr(state.ring, "update_durable_header", interrupted_ack)
                with pytest.raises(RuntimeError, match="after RAM acknowledgement"):
                    _drain(base_path, state)
            assert len(injected) == 1
            assert int(writer.ring.durable_record_count[0]) == 2
        finally:
            state.close()

        def forbid_index_rewrite(*args, **kwargs):
            pytest.fail("acknowledged index batch was rewritten on persister restart")

        with monkeypatch.context() as patch:
            patch.setattr(ChunkWriter, "write_batch_bytes", forbid_index_rewrite)
            for _ in range(2):
                recovered = _FeedState(base_path, FEED)
                try:
                    _drain(base_path, recovered)
                    assert int(writer.ring.durable_record_count[0]) == 2
                    _assert_rows(base_path, PAYLOADS[:2])
                finally:
                    recovered.close()
        _finish_recovery_and_append(base_path, writer)
