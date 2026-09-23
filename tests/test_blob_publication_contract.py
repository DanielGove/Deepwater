"""Required complete-record guarantees, exercised only in temporary feeds."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from deepwater import Reader, Writer, create_feed
from deepwater.metadata.admin import BlobSidecarSpec, FeedSpec, FieldSpec
from deepwater.io.writer.persistent_ring import _FeedState, _drain


def make_feed(base, *, persist=True):
    create_feed(base, FeedSpec(
        feed_name="blob_contract", fields=(FieldSpec("ts", "uint64"),),
        clock_level=1, persist=persist, uses_ring=True,
        ring_size_mb=0.0625, chunk_size_mb=0.0625,
        sidecars=(BlobSidecarSpec("payload", chunk_size_mb=0.0625),),
    ))


def test_volatile_blob_feed_publishes_complete_payload_without_payload_files(base_path):
    make_feed(base_path, persist=False)
    with Writer(base_path, "blob_contract") as writer:
        ref = writer.write_blob(100, b"complete payload", sidecar="payload")
        with Reader(base_path, "blob_contract") as reader:
            row = reader.range(100, 101)[0]
            assert reader.blob_ref(row, sidecar="payload") == ref
            assert reader.blob(row, sidecar="payload") == b"complete payload"
        files = list((base_path / "data" / "blob_contract" / "blobs").rglob("*.bin"))
        assert files == [], "A nonpersistent ring must not persist its payload through an implicit sidecar path"


def test_payload_is_synced_before_its_index_is_acknowledged(base_path, monkeypatch):
    make_feed(base_path)
    synced = []
    real_fsync = os.fsync

    def record_fsync(fd):
        synced.append(os.readlink(f"/proc/self/fd/{fd}"))
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", record_fsync)
    with Writer(base_path, "blob_contract") as writer:
        writer.write_blob(100, b"durable together", sidecar="payload")
        state = _FeedState(base_path, "blob_contract")
        try:
            writer.ring.request_drain()
            _drain(base_path, state)
            assert int(writer.ring.durable_record_count[0]) == 1
            payload_syncs = [i for i, name in enumerate(synced) if "/blobs/payload/blob_" in name]
            index_syncs = [i for i, name in enumerate(synced) if "/chunk_" in name]
            assert payload_syncs, "Durable index acknowledgement did not establish payload durability"
            assert index_syncs
            assert max(payload_syncs) < min(index_syncs)
        finally:
            state.close()


def test_retry_after_index_commit_before_ack_does_not_duplicate_records(base_path, monkeypatch):
    make_feed(base_path)
    with Writer(base_path, "blob_contract") as writer:
        writer.write_blob(100, b"one committed record", sidecar="payload")
        writer.ring.request_drain()
        state = _FeedState(base_path, "blob_contract")
        try:
            # Inject at the boundary after the index chunk is committed, before
            # the shared-memory acknowledgement is advanced.
            def fail_ack(**kwargs):
                raise RuntimeError("injected interruption before acknowledgement")

            with monkeypatch.context() as patch:
                patch.setattr(state.ring, "update_durable_header", fail_ack)
                with pytest.raises(RuntimeError, match="injected interruption"):
                    _drain(base_path, state)
            assert int(writer.ring.durable_record_count[0]) == 0
        finally:
            state.close()
        recovered = _FeedState(base_path, "blob_contract")
        try:
            _drain(base_path, recovered)
        finally:
            recovered.close()
        with Reader(base_path, "blob_contract") as reader:
            rows = reader.range(100, 101)
            assert len(rows) == 1, "Retry duplicated an index chunk that was already committed"
            assert reader.blob(rows[0], sidecar="payload") == b"one committed record"
