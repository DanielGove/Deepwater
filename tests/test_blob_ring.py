from __future__ import annotations

import multiprocessing as mp

import pytest

from deepwater.io import blob_ring as module
from deepwater.io.blob_ring import BlobRing, BufferFull, PayloadUnavailable
from deepwater.io.ring_shadow import atomic_load_u64, atomic_store_u64, unlink_shm


@pytest.fixture
def rings(tmp_path):
    opened = []
    identities = set()

    def make(*, feed="test", sidecar="payload", chunk_bytes=1024,
             capacity_bytes=4096, create=True, initial_address=0, parent_epoch="epoch-1"):
        identities.add((tmp_path, feed, sidecar))
        ring = BlobRing(tmp_path, feed, sidecar, chunk_bytes=chunk_bytes,
                        capacity_bytes=capacity_bytes, create=create,
                        initial_address=initial_address, parent_epoch=parent_epoch)
        opened.append(ring)
        return ring

    make.base = tmp_path
    try:
        yield make
    finally:
        for ring in reversed(opened):
            ring.close()
        for base, feed, sidecar in identities:
            BlobRing.unlink(base, feed, sidecar)


def _append(ring, payload, sequence):
    chunk, offset = ring.reserve_and_write(payload, sequence)
    ring.publish(sequence)
    return chunk, offset, len(payload)


def test_variable_payloads_preserve_coordinates_and_pending_readability(rings):
    ring = rings()
    assert _append(ring, b"a", 1) == (1, 0, 1)
    assert _append(ring, b"bc", 2) == (1, 1, 2)
    assert _append(ring, b"d" * 1020, 3) == (1, 3, 1020)
    chunk, offset = ring.reserve_and_write(b"abcdef", 4)
    assert (chunk, offset) == (2, 0)
    assert ring.pending_record_seq == 4
    # Another process can see the parent index before publish bookkeeping.
    attached = rings(create=False)
    assert attached.read(chunk, offset, 6) == b"abcdef"
    assert ring.allocated_end == 1030
    ring.acknowledge(1030)  # Parent durable commit may also beat bookkeeping.
    ring.publish(4)
    ring.publish(4)
    assert ring.pending_record_seq == 0


def test_payload_crosses_shadow_mapping_boundary(rings):
    ring = rings(chunk_bytes=3000, capacity_bytes=8192, initial_address=7000)
    payload = bytes(range(250)) * 8
    ref = _append(ring, payload, 1)
    assert ref == (3, 1000, 2000)
    assert ring.allocated_end == 9000
    assert ring.read(*ref) == payload


def test_byte_fullness_acknowledgement_and_stale_reference(rings):
    ring = rings(chunk_bytes=2048)
    first = _append(ring, b"a" * 2048, 1)
    second = _append(ring, b"b" * 2048, 2)
    owned = ring.read(*first)
    assert not ring.can_reserve(1)
    with pytest.raises(BufferFull):
        ring.reserve_and_write(b"c", 3)
    ring.acknowledge(2048)
    third = _append(ring, b"c" * 2048, 3)
    assert third == (3, 0, 2048)
    with pytest.raises(PayloadUnavailable):
        ring.read(*first)
    assert ring.read(*second) == b"b" * 2048
    assert ring.read(*third) == b"c" * 2048
    assert owned == b"a" * 2048
    with pytest.raises(ValueError):
        ring.acknowledge(2047)
    with pytest.raises(ValueError):
        ring.acknowledge(ring.allocated_end + 1)


def test_abort_does_not_resurrect_bytes_overwritten_by_reservation(rings):
    ring = rings(chunk_bytes=2048)
    old = _append(ring, b"a" * 2048, 1)
    recent = _append(ring, b"b" * 2048, 2)
    ring.acknowledge(2048)
    ring.reserve_and_write(b"c" * 2048, 3)
    ring.abort(3)
    assert ring.allocated_end == 4096
    assert ring.pending_record_seq == 0
    with pytest.raises(PayloadUnavailable):
        ring.read(*old)
    assert ring.read(*recent) == b"b" * 2048
    replacement = _append(ring, b"d" * 2048, 3)
    assert ring.read(*replacement) == b"d" * 2048


@pytest.mark.parametrize("parent_count,expected_end", [(1, 3), (2, 7)])
def test_reopen_recovers_against_parent_publication(rings, parent_count, expected_end):
    ring = rings()
    first = _append(ring, b"one", 1)
    chunk, offset = ring.reserve_and_write(b"next", 2)
    ring.close()
    reopened = rings(create=False)
    assert reopened.pending_record_seq == 2
    reopened.recover(parent_count)
    assert reopened.allocated_end == expected_end
    assert reopened.pending_record_seq == 0
    assert reopened.read(*first) == b"one"
    if parent_count == 2:
        assert reopened.read(chunk, offset, 4) == b"next"
        reopened.publish(2)
    else:
        with pytest.raises(PayloadUnavailable):
            reopened.read(chunk, offset, 4)
        assert _append(reopened, b"redo", 2) == (1, 3, 4)


def test_interrupted_copy_is_rolled_back_or_rejected_when_parent_claims_it(rings):
    ring = rings()
    _append(ring, b"one", 1)
    ring.reserve_and_write(b"next", 2)
    # Simulate interruption with a valid journal but unfinished payload copy.
    ring._store(module._PENDING_READY, 0)
    ring._store(module._MUTATION, ring._load(module._MUTATION) + 1)
    with pytest.raises(RuntimeError, match="incomplete"):
        ring.recover(2)
    ring.recover(1)
    assert ring.allocated_end == 3
    assert ring.pending_record_seq == 0
    assert _append(ring, b"redo", 2) == (1, 3, 4)
    with pytest.raises(RuntimeError, match="regressed"):
        ring.recover(1)


def test_reuse_between_read_check_and_copy_is_rejected(rings, monkeypatch):
    writer = rings(chunk_bytes=2048)
    ref = _append(writer, b"a" * 2048, 1)
    _append(writer, b"b" * 2048, 2)
    writer.acknowledge(2048)
    reader = rings(chunk_bytes=2048, create=False)
    real_fence = module.atomic_fence
    injected = False

    def race_at_fence():
        nonlocal injected
        if not injected:
            injected = True
            _append(writer, b"c" * 2048, 3)
        real_fence()

    monkeypatch.setattr(module, "atomic_fence", race_at_fence)
    with pytest.raises(PayloadUnavailable):
        reader.read(*ref)
    assert injected


def test_epoch_size_and_missing_payload_mapping_fail_closed(rings):
    original = rings()
    with pytest.raises(RuntimeError, match="epoch"):
        rings(parent_epoch="different")
    with pytest.raises(ValueError, match="differs"):
        rings(chunk_bytes=2048)
    with pytest.raises(ValueError, match="differs"):
        rings(capacity_bytes=8192)
    unlink_shm(original.data_name)
    with pytest.raises(FileNotFoundError):
        rings(create=True)


def test_steady_state_write_read_ack_does_not_use_files(rings, monkeypatch):
    ring = rings()
    import builtins

    def forbidden(*args, **kwargs):
        raise AssertionError("steady-state blob ring performed filesystem I/O")

    monkeypatch.setattr(builtins, "open", forbidden)
    monkeypatch.setattr(module.os, "open", forbidden)
    ref = _append(ring, b"in memory", 1)
    assert ring.read(*ref) == b"in memory"
    ring.acknowledge(ring.allocated_end)


def _child_read(base, reference, sender):
    try:
        with BlobRing(base, "test", "payload", chunk_bytes=1024,
                      capacity_bytes=4096, parent_epoch="epoch-1") as ring:
            sender.send(ring.read(*reference))
    finally:
        sender.close()


def test_spawned_process_can_read_unfinalized_reservation(rings):
    ring = rings()
    payload = b"another process sees complete bytes"
    chunk, offset = ring.reserve_and_write(payload, 1)
    ctx = mp.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    child = ctx.Process(target=_child_read,
                        args=(rings.base, (chunk, offset, len(payload)), sender))
    child.start()
    sender.close()
    try:
        assert receiver.poll(5), "child did not return the payload"
        assert receiver.recv() == payload
        child.join(5)
        assert child.exitcode == 0
    finally:
        if child.is_alive():
            child.kill()
            child.join(5)
        receiver.close()
    ring.publish(1)


def test_atomic_helpers_validate_control_access():
    storage = bytearray(16)
    atomic_store_u64(storage, 0, (1 << 64) - 1)
    assert atomic_load_u64(storage, 0) == (1 << 64) - 1
    for invalid_offset in (-1, 1, 9):
        with pytest.raises(ValueError):
            atomic_load_u64(storage, invalid_offset)
