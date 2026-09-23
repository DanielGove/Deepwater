"""Bounded recovery integrity checks using temporary feeds and tiny payloads."""
from __future__ import annotations

import os

import pytest

from deepwater import Reader, Writer, create_feed
from deepwater.io.ring import RingBuffer
from deepwater.io.writer.commit_journal import CommitJournal, JournalError
from deepwater.io.writer.persistent_ring import _FeedState, _drain
from deepwater.metadata.admin import BlobSidecarSpec, FeedSpec, FieldSpec


FEED = "blob_recovery_integrity"
PAYLOADS = (b"first durable payload", b"second durable payload", b"third durable payload")


def _make_feed(base):
    create_feed(base, FeedSpec(
        feed_name=FEED, fields=(FieldSpec("ts", "uint64"),),
        clock_level=1, persist=True, uses_ring=True,
        chunk_size_mb=0.0625, ring_size_mb=0.0625,
        sidecars=(BlobSidecarSpec("payload", chunk_size_mb=0.01, ring_size_mb=0.03),),
    ))


def _drain_requested(base, writer, state):
    writer.ring.request_drain()
    _drain(base, state)


def _assert_payloads(base, expected):
    with Reader(base, FEED) as reader:
        # All fixture records deliberately share their timestamp.
        rows = reader.range(100, 101)
        assert len(rows) == len(expected), "recovery lost or duplicated an index record"
        assert [reader.blob(row, sidecar="payload") for row in rows] == list(expected)


def _write_byte(path, offset, value):
    with path.open("r+b") as fh:
        fh.seek(offset)
        fh.write(value)
        fh.flush()
        os.fsync(fh.fileno())


@pytest.mark.parametrize("damaged", ["index", "payload"])
def test_committed_corruption_prevents_recovery_ack_until_repaired(base_path, monkeypatch, damaged):
    _make_feed(base_path)
    with Writer(base_path, FEED) as writer:
        ref = writer.write_blob(100, PAYLOADS[0], sidecar="payload")
        state = _FeedState(base_path, FEED)

        def interrupt_before_ack(**kwargs):
            raise RuntimeError("injected interruption before durable acknowledgement")

        try:
            with monkeypatch.context() as patch:
                patch.setattr(state.ring, "update_durable_header", interrupt_before_ack)
                with pytest.raises(RuntimeError, match="before durable acknowledgement"):
                    _drain_requested(base_path, writer, state)
            assert int(writer.ring.durable_record_count[0]) == 0
        finally:
            state.close()

        feed_dir = base_path / "data" / FEED
        committed = CommitJournal(feed_dir).committed
        assert committed is not None
        if damaged == "index":
            path = feed_dir / f"chunk_{committed.chunk_id:08d}.bin"
            offset = 0
            expected_error, message = JournalError, "index.*corrupt"
        else:
            path = feed_dir / "blobs" / "payload" / f"blob_{ref.chunk_id:08d}.bin"
            offset = ref.offset
            expected_error, message = ValueError, "CRC mismatch"
        with path.open("rb") as fh:
            fh.seek(offset)
            original = fh.read(1)
        assert len(original) == 1
        _write_byte(path, offset, bytes((original[0] ^ 1,)))

        for _ in range(2):
            with pytest.raises(expected_error, match=message):
                _FeedState(base_path, FEED)
            assert int(writer.ring.durable_record_count[0]) == 0
            assert CommitJournal(feed_dir).committed == committed

        # A failed integrity check must preserve enough state for a legitimate
        # repair to recover the same committed batch, rather than append it twice.
        _write_byte(path, offset, original)
        recovered = _FeedState(base_path, FEED)
        try:
            assert int(writer.ring.durable_record_count[0]) == 1
            _assert_payloads(base_path, PAYLOADS[:1])
        finally:
            recovered.close()


@pytest.mark.parametrize("discard_finished", [False, True])
def test_interrupted_pending_rollback_preserves_previous_commit(base_path, monkeypatch, discard_finished):
    _make_feed(base_path)
    with Writer(base_path, FEED) as writer:
        state = _FeedState(base_path, FEED)
        try:
            writer.write_blob(100, PAYLOADS[0], sidecar="payload")
            _drain_requested(base_path, writer, state)
            assert int(writer.ring.durable_record_count[0]) == 1
            first_commit = state.journal.committed
            writer.write_blob(100, PAYLOADS[1], sidecar="payload")

            def interrupt_commit(journal):
                raise OSError("injected interruption before journal commit")

            with monkeypatch.context() as patch:
                patch.setattr(CommitJournal, "commit", interrupt_commit)
                with pytest.raises(OSError, match="before journal commit"):
                    _drain_requested(base_path, writer, state)
            assert int(writer.ring.durable_record_count[0]) == 1
        finally:
            state.close()

        feed_dir = base_path / "data" / FEED
        pending = CommitJournal(feed_dir).pending
        assert pending is not None
        real_discard = CommitJournal.discard_pending
        interrupted = []

        def interrupt_rollback(journal):
            if discard_finished:
                real_discard(journal)
            interrupted.append(discard_finished)
            raise OSError("injected interruption during rollback journal update")

        with monkeypatch.context() as patch:
            patch.setattr(CommitJournal, "discard_pending", interrupt_rollback)
            # Before discard, the same pending descriptor must tolerate repeated
            # recovery after its physical index has already been removed.
            for _ in range(1 if discard_finished else 2):
                with pytest.raises(OSError, match="during rollback journal update"):
                    _FeedState(base_path, FEED)
                assert int(writer.ring.durable_record_count[0]) == 1
                assert not (feed_dir / f"chunk_{pending.chunk_id:08d}.bin").exists()
        assert len(interrupted) == (1 if discard_finished else 2)
        assert CommitJournal(feed_dir).committed == first_commit

        recovered = _FeedState(base_path, FEED)
        try:
            _drain_requested(base_path, writer, recovered)
            assert int(writer.ring.durable_record_count[0]) == 2
            _assert_payloads(base_path, PAYLOADS[:2])
            writer.write_blob(100, PAYLOADS[2], sidecar="payload")
            _drain_requested(base_path, writer, recovered)
            assert int(writer.ring.durable_record_count[0]) == 3
            _assert_payloads(base_path, PAYLOADS)
        finally:
            recovered.close()


def test_new_ram_epoch_appends_without_replacing_old_durable_history(base_path):
    _make_feed(base_path)
    with Writer(base_path, FEED) as writer:
        old_epoch = writer.ring.epoch
        state = _FeedState(base_path, FEED)
        try:
            for payload in PAYLOADS[:2]:
                writer.write_blob(100, payload, sidecar="payload")
            _drain_requested(base_path, writer, state)
            assert int(writer.ring.durable_record_count[0]) == 2
            old_commit = state.journal.committed
        finally:
            state.close()

    # Simulate reboot loss of shared memory only, after closing every owner.
    RingBuffer.unlink_for_feed(base_path, FEED)

    with Writer(base_path, FEED) as writer:
        assert writer.ring.epoch != old_epoch
        assert int(writer.ring.record_count[0]) == 0
        assert int(writer.ring.durable_record_count[0]) == 0
        state = _FeedState(base_path, FEED)
        try:
            _assert_payloads(base_path, PAYLOADS[:2])
            ref = writer.write_blob(100, PAYLOADS[2], sidecar="payload")
            # Historical and new live data must coexist before the new commit.
            _assert_payloads(base_path, PAYLOADS)
            _drain_requested(base_path, writer, state)
            assert int(writer.ring.durable_record_count[0]) == 1
            assert state.journal.committed.epoch == writer.ring.epoch
            assert state.journal.committed.chunk_id > old_commit.chunk_id
            assert state.journal.committed.start_seq == 0
            assert state.journal.committed.end_seq == 1
            assert ref.offset > 0 or ref.chunk_id > 1
            _assert_payloads(base_path, PAYLOADS)
        finally:
            state.close()

    # A second persister recovery in the new epoch must not append the last row.
    with Writer(base_path, FEED) as writer:
        state = _FeedState(base_path, FEED)
        try:
            _drain_requested(base_path, writer, state)
            assert int(writer.ring.durable_record_count[0]) == 1
            _assert_payloads(base_path, PAYLOADS)
        finally:
            state.close()


def test_many_small_payload_chunks_keep_persistence_descriptors_bounded(base_path, monkeypatch):
    from deepwater.metadata.admin import BlobSidecarSpec, FeedSpec, FieldSpec
    from deepwater.io.writer.commit_journal import CommitJournal

    name = "many_payload_chunks"
    create_feed(base_path, FeedSpec(
        feed_name=name, fields=(FieldSpec("ts", "uint64"),), clock_level=1,
        persist=True, uses_ring=True, ring_size_mb=.0625, chunk_size_mb=.0625,
        sidecars=(BlobSidecarSpec("payload", chunk_size_mb=1 / (1024 * 1024), ring_size_mb=.01),),
    ))
    real_open, real_close = os.open, os.close
    live, peak = set(), []

    def opened(path, *args, **kwargs):
        fd = real_open(path, *args, **kwargs)
        if "/blobs/payload/blob_" in os.fsdecode(path):
            live.add(fd)
            peak.append(len(live))
        return fd

    def closed(fd):
        live.discard(fd)
        return real_close(fd)

    with Writer(base_path, name) as writer:
        for i in range(80):
            writer.write_blob(100 + i, bytes([i]), sidecar="payload")
        state = _FeedState(base_path, name)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(os, "open", opened)
                patch.setattr(os, "close", closed)
                writer.ring.request_drain()
                _drain(base_path, state)
            assert max(peak) <= 64
            assert not live
            descriptor = CommitJournal(base_path / "data" / name).committed
            assert descriptor.payload_highwaters == (("payload", 80, 1),)
        finally:
            state.close()
    with Reader(base_path, name) as reader:
        assert reader.blobs(reader.range(100, 180), sidecar="payload") == [bytes([i]) for i in range(80)]
