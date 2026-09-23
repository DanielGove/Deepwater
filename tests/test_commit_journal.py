"""Small fault-injection tests for persistence metadata, without shared memory."""
from dataclasses import asdict, replace
import hashlib
import os
import stat
from uuid import UUID

import msgspec
import pytest

from deepwater.io.writer.commit_journal import BatchDescriptor, CommitJournal, JournalError


EPOCH = UUID("458f08d5-2890-41fa-a73d-c02b469b1d08")
OTHER_EPOCH = UUID("124a3aa7-2890-41fa-a73d-c02b469b1d08")


def batch(**changes):
    values = dict(
        epoch=EPOCH.bytes,
        start_seq=0,
        end_seq=2,
        chunk_id=1,
        size=32,
        digest=hashlib.sha256(b"two fixed records").hexdigest(),
        last_ts=100,
        payload_highwaters=(("payload", 1, 25),),
    )
    values.update(changes)
    return BatchDescriptor(**values)


def committed_journal(tmp_path):
    journal = CommitJournal(tmp_path)
    journal.prepare(batch())
    journal.commit()
    return journal


def test_prepare_reload_commit_and_ack_recovery(tmp_path):
    journal = CommitJournal(tmp_path)
    assert journal.recover(EPOCH, 0).action == "ready"
    descriptor = batch()
    assert journal.prepare(descriptor) == "prepared"
    assert descriptor.epoch == str(EPOCH)
    assert journal.prepare(descriptor) == "already_pending"
    assert journal.committed is None

    journal = CommitJournal(tmp_path)
    decision = journal.recover(EPOCH.bytes, 0)
    assert decision.action == "rollback_pending"
    assert decision.descriptor == descriptor
    # Caller validates and synchronizes payload/index/registry before commit.
    assert journal.commit() == descriptor

    journal = CommitJournal(tmp_path)
    assert journal.pending is None
    decision = journal.recover(str(EPOCH), 0)
    assert decision.action == "ack_committed"
    assert decision.next_seq == 2
    assert journal.prepare(descriptor) == "already_committed"
    assert journal.commit() == descriptor
    assert journal.recover(EPOCH, 2).action == "ready"


def test_pending_preserves_prior_commit_and_can_be_rolled_back(tmp_path):
    journal = committed_journal(tmp_path)
    later = batch(start_seq=2, end_seq=4, chunk_id=2, last_ts=100)
    journal.prepare(later)
    journal = CommitJournal(tmp_path)
    assert journal.committed == batch()
    assert journal.pending == later
    assert journal.recover(EPOCH, 2).action == "rollback_pending"
    assert journal.discard_pending() == later
    assert journal.discard_pending() is None
    journal = CommitJournal(tmp_path)
    assert journal.committed == batch()
    assert journal.pending is None
    assert journal.prepare(later) == "prepared"
    journal.commit()
    assert CommitJournal(tmp_path).committed == later


def test_timestamp_does_not_identify_batch_and_new_epoch_restarts_sequence(tmp_path):
    journal = committed_journal(tmp_path)
    later = batch(start_seq=2, end_seq=4, chunk_id=2)  # Same timestamp and digest.
    journal.prepare(later)
    journal.commit()
    assert journal.recover(EPOCH, 2).action == "ack_committed"
    assert journal.recover(OTHER_EPOCH, 0).action == "new_epoch"
    restarted = batch(epoch=OTHER_EPOCH, chunk_id=3)
    journal.prepare(restarted)
    journal.commit()
    assert CommitJournal(tmp_path).recover(OTHER_EPOCH, 0).next_seq == 2


def test_conflicting_or_noncontiguous_retry_fails_closed(tmp_path):
    journal = committed_journal(tmp_path)
    for descriptor in (
        batch(digest="b" * 64),
        batch(start_seq=3, end_seq=5, chunk_id=2),
        batch(start_seq=1, end_seq=3, chunk_id=2),
        batch(epoch=OTHER_EPOCH),
    ):
        with pytest.raises(JournalError):
            journal.prepare(descriptor)
    later = batch(start_seq=2, end_seq=4, chunk_id=2)
    journal.prepare(later)
    with pytest.raises(JournalError, match="another batch"):
        journal.prepare(replace(later, last_ts=101))
    assert journal.pending == later


@pytest.mark.parametrize("changes", [
    {"epoch": b"short"}, {"epoch": "not a UUID"},
    {"start_seq": -1}, {"end_seq": 0}, {"end_seq": 1 << 64},
    {"start_seq": True}, {"chunk_id": 0}, {"size": 0}, {"size": 31},
    {"digest": "b" * 63}, {"digest": "A" * 64}, {"last_ts": -1},
    {"payload_highwaters": (("../escape", 1, 1),)},
    {"payload_highwaters": (("payload", 0, 1),)},
    {"payload_highwaters": (("payload", 1, -1),)},
    {"payload_highwaters": (("payload", 1, 1), ("payload", 1, 2))},
])
def test_invalid_descriptors_are_rejected(changes):
    with pytest.raises(JournalError):
        batch(**changes)


def test_commit_without_batch_and_impossible_ack_are_rejected(tmp_path):
    journal = CommitJournal(tmp_path)
    with pytest.raises(JournalError, match="no batch"):
        journal.commit()
    descriptor = batch(start_seq=8, end_seq=10)
    journal.prepare(descriptor)
    journal.commit()
    for ack in (0, 11, -1, True):
        with pytest.raises(JournalError):
            journal.recover(EPOCH, ack)


@pytest.mark.parametrize("payload", [
    b"", b"\xc1", b"a" * (1024 * 1024 + 1),
    msgspec.msgpack.encode({}),
    msgspec.msgpack.encode({"magic": "other", "version": 1, "committed": None, "pending": None}),
    msgspec.msgpack.encode({"magic": "deepwater-fixed-index-commit", "version": 2, "committed": None, "pending": None}),
    msgspec.msgpack.encode({"magic": "deepwater-fixed-index-commit", "version": True, "committed": None, "pending": None}),
])
def test_corrupt_or_unknown_journal_fails_closed(tmp_path, payload):
    path = tmp_path / ".commit-journal.msgpack"
    path.write_bytes(payload)
    with pytest.raises(JournalError):
        CommitJournal(tmp_path)
    assert path.read_bytes() == payload


def test_loaded_transition_and_descriptor_are_validated(tmp_path):
    journal = committed_journal(tmp_path)
    state = msgspec.msgpack.decode(journal.path.read_bytes())
    state.pop("checksum")
    state["pending"] = asdict(batch(start_seq=3, end_seq=5, chunk_id=2))
    state["checksum"] = hashlib.sha256(msgspec.msgpack.encode(state)).hexdigest()
    journal.path.write_bytes(msgspec.msgpack.encode(state))
    with pytest.raises(JournalError, match="contiguous"):
        CommitJournal(tmp_path)
    state["pending"] = None
    state["committed"]["digest"] = "bad"
    state.pop("checksum")
    state["checksum"] = hashlib.sha256(msgspec.msgpack.encode(state)).hexdigest()
    journal.path.write_bytes(msgspec.msgpack.encode(state))
    with pytest.raises(JournalError, match="digest"):
        CommitJournal(tmp_path)


def test_atomic_replace_is_bracketed_by_file_and_directory_fsync(tmp_path, monkeypatch):
    calls = []
    real_fsync, real_replace = os.fsync, os.replace

    def fsync(fd):
        calls.append("directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
        return real_fsync(fd)

    def replace_file(source, destination):
        calls.append("replace")
        return real_replace(source, destination)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "replace", replace_file)
    journal = CommitJournal(tmp_path)
    journal.prepare(batch())
    journal.commit()
    assert calls == ["file", "replace", "directory"] * 2


@pytest.mark.parametrize("operation", ["prepare", "commit", "discard"])
@pytest.mark.parametrize("boundary", ["file_fsync", "before_replace", "after_replace", "directory_fsync"])
def test_interrupted_update_reload_and_retry(tmp_path, monkeypatch, operation, boundary):
    journal = committed_journal(tmp_path)
    old = batch()
    later = batch(start_seq=2, end_seq=4, chunk_id=2)
    if operation != "prepare":
        journal.prepare(later)
    real_fsync, real_replace = os.fsync, os.replace

    def fsync(fd):
        is_dir = stat.S_ISDIR(os.fstat(fd).st_mode)
        if (boundary == "file_fsync" and not is_dir) or (boundary == "directory_fsync" and is_dir):
            raise OSError("injected sync interruption")
        return real_fsync(fd)

    def replace_file(source, destination):
        if boundary == "before_replace":
            raise OSError("injected replace interruption")
        result = real_replace(source, destination)
        if boundary == "after_replace":
            raise OSError("injected interruption following replace")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", fsync)
        patch.setattr(os, "replace", replace_file)
        with pytest.raises(OSError, match="injected"):
            if operation == "prepare":
                journal.prepare(later)
            elif operation == "commit":
                journal.commit()
            else:
                journal.discard_pending()
    with pytest.raises(JournalError, match="reload"):
        journal.recover(EPOCH, 2)
    assert not list(tmp_path.glob("*.tmp"))

    recovered = CommitJournal(tmp_path)
    replaced = boundary in {"after_replace", "directory_fsync"}
    if operation == "prepare":
        assert recovered.committed == old
        assert recovered.pending == (later if replaced else None)
        recovered.prepare(later)
        recovered.commit()
    elif operation == "commit":
        assert recovered.committed == (later if replaced else old)
        assert recovered.pending == (None if replaced else later)
        recovered.commit()
        assert recovered.recover(EPOCH, 2).action == "ack_committed"
    else:
        assert recovered.committed == old
        assert recovered.pending == (None if replaced else later)
        recovered.discard_pending()
        assert recovered.recover(EPOCH, 2).action == "ready"


def test_orphan_temporary_file_does_not_replace_committed_journal(tmp_path):
    journal = committed_journal(tmp_path)
    (tmp_path / ".commit-journal.msgpack.orphan.tmp").write_bytes(b"interrupted write")
    assert CommitJournal(tmp_path).committed == journal.committed


def test_valid_msgpack_corruption_is_caught_by_checksum(tmp_path):
    journal = committed_journal(tmp_path)
    state = msgspec.msgpack.decode(journal.path.read_bytes())
    state["committed"]["last_ts"] += 1
    journal.path.write_bytes(msgspec.msgpack.encode(state))
    with pytest.raises(JournalError, match="checksum"):
        CommitJournal(tmp_path)


def test_reloaded_state_is_synced_before_recovery_can_ack(tmp_path, monkeypatch):
    committed_journal(tmp_path)
    calls = []
    real_fsync = os.fsync

    def fsync(fd):
        calls.append("directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fsync)
    journal = CommitJournal(tmp_path)
    assert calls == ["file", "directory"]
    assert journal.recover(EPOCH, 0).action == "ack_committed"


def test_reload_sync_error_prevents_exposing_recovered_state(tmp_path, monkeypatch):
    committed_journal(tmp_path)

    def fail_sync(fd):
        raise OSError("injected recovery sync failure")

    monkeypatch.setattr(os, "fsync", fail_sync)
    with pytest.raises(OSError, match="recovery sync failure"):
        CommitJournal(tmp_path)
