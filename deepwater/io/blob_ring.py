"""Live blob bytes, addressed by stable append-only file coordinates.

The parent owns the single producer and persister. reserve_and_write makes bytes
readable, but does not publish an index. publish finalizes bookkeeping AFTER that
index is visible; abort discards an unpublished reservation. Restart calls recover
under exclusive producer ownership. Acknowledgement requires a durable parent
commit, including its payload, and may precede publish bookkeeping.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

from .ring_shadow import (
    atomic_fence, atomic_load_u64, atomic_store_u64,
    ensure_shm, map_shadow, unlink_shm,
)


PAGE_SIZE = int(os.sysconf("SC_PAGE_SIZE"))
_MAGIC_VALUE = 0x4457424C4F423031
_VERSION_VALUE = 1
_MAX_U64 = (1 << 64) - 1
(_MAGIC, _VERSION, _CHUNK_BYTES, _CAPACITY, _MUTATION,
 _ALLOCATED, _ACKNOWLEDGED, _RETAINED, _PENDING_SEQ, _PENDING_OLD,
 _PENDING_END, _PENDING_READY, _PUBLISHED_SEQ, _EPOCH_LO, _EPOCH_HI) = range(0, 15 * 8, 8)


class PayloadUnavailable(LookupError):
    """The reference is not in this live buffer; consult durable storage."""

    def __init__(self, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable


class BufferFull(BufferError):
    """The parent must drain persistence before reserving more bytes."""


def _names(base, feed: str, sidecar_name: str) -> tuple[str, str]:
    key = "\0".join((str(Path(base).resolve()), str(feed), str(sidecar_name)))
    digest = hashlib.sha256(key.encode("utf-8", "surrogatepass")).hexdigest()[:32]
    return f"dw_blob_{digest}", f"dw_blob_{digest}_data"


class BlobRing:
    """Single-producer byte ring. Readers get owned copies, never live views.

    Logical addresses are (chunk_id - 1) * chunk_bytes + offset. Chunk-tail holes
    consume capacity. Reclamation is monotonic, even after abort: tentative writes
    may already have overwritten acknowledged bytes.

    Methods do not acquire the parent's writer lock. Concurrent producers or
    recover() while another producer is alive are invalid. unlink() requires a
    stopped feed and invalidates all old references/mappings.
    """

    def __init__(self, base, feed: str, sidecar_name: str, *, chunk_bytes: int,
                 capacity_bytes: int, create: bool = False, initial_address: int = 0,
                 parent_epoch: str | None = None):
        chunk_bytes, capacity_bytes = int(chunk_bytes), int(capacity_bytes)
        initial_address = int(initial_address)
        if chunk_bytes <= 0 or capacity_bytes < 2 * chunk_bytes:
            raise ValueError("capacity_bytes must be at least twice positive chunk_bytes")
        if capacity_bytes % PAGE_SIZE:
            raise ValueError("capacity_bytes must be page aligned")
        if not 0 <= initial_address <= _MAX_U64:
            raise ValueError("initial_address must be an unsigned 64-bit address")
        epoch = bytes(16) if parent_epoch is None else hashlib.sha256(str(parent_epoch).encode()).digest()[:16]
        epoch_lo, epoch_hi = int.from_bytes(epoch[:8], "little"), int.from_bytes(epoch[8:], "little")
        self.chunk_bytes = chunk_bytes
        self._capacity = capacity_bytes
        self._control = self._data = None
        self._control_map = self._data_map = None
        self._closed = False
        self.control_name, self.data_name = _names(base, feed, sidecar_name)
        control_created = False
        try:
            control_created = ensure_shm(self.control_name, PAGE_SIZE, create)
            self._control_map = map_shadow(self.control_name, 0, PAGE_SIZE)
            self._control = self._control_map.view[:PAGE_SIZE]
            if not control_created:
                if self._load(_MAGIC) != _MAGIC_VALUE or self._load(_VERSION) != _VERSION_VALUE:
                    raise RuntimeError("uninitialized or unsupported blob ring control version")
                if self._load(_CHUNK_BYTES) != chunk_bytes or self._load(_CAPACITY) != capacity_bytes:
                    raise ValueError("existing blob ring chunk size or capacity differs")
                if (self._load(_EPOCH_LO), self._load(_EPOCH_HI)) != (epoch_lo, epoch_hi):
                    raise RuntimeError("blob ring belongs to a different parent epoch; explicit cleanup required")
            # Existing control must NEVER silently recreate missing live data.
            ensure_shm(self.data_name, capacity_bytes, bool(control_created))
            self._data_map = map_shadow(self.data_name, 0, capacity_bytes)
            self._data = self._data_map.view
            if control_created:
                self._store(_VERSION, _VERSION_VALUE)
                self._store(_CHUNK_BYTES, chunk_bytes)
                self._store(_CAPACITY, capacity_bytes)
                self._store(_ALLOCATED, initial_address)
                self._store(_ACKNOWLEDGED, initial_address)
                self._store(_RETAINED, initial_address)
                self._store(_EPOCH_LO, epoch_lo)
                self._store(_EPOCH_HI, epoch_hi)
                self._store(_MAGIC, _MAGIC_VALUE)
        except BaseException:
            self.close()
            if control_created:
                try:
                    unlink_shm(self.control_name)
                except FileNotFoundError:
                    pass
            raise

    def _load(self, offset: int) -> int:
        if self._closed:
            raise RuntimeError("blob ring is closed")
        return atomic_load_u64(self._control, offset)

    def _store(self, offset: int, value: int) -> None:
        atomic_store_u64(self._control, offset, value)

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def allocated_end(self) -> int:
        return self._load(_ALLOCATED)

    @property
    def acknowledged_end(self) -> int:
        return self._load(_ACKNOWLEDGED)

    @property
    def pending_record_seq(self) -> int:
        return self._load(_PENDING_SEQ)

    def _reservation(self, size: int) -> tuple[int, int]:
        if not 0 < size <= self.chunk_bytes:
            raise ValueError("payload size must be positive and no larger than chunk_bytes")
        start = self.allocated_end
        offset = start % self.chunk_bytes
        if offset + size > self.chunk_bytes:
            start += self.chunk_bytes - offset
        end = start + size
        if end > _MAX_U64:
            raise OverflowError("blob logical address space exhausted")
        return start, end

    def can_reserve(self, size: int) -> bool:
        _, end = self._reservation(int(size))
        return (not self.pending_record_seq and not self._load(_MUTATION) % 2
                and end - self.acknowledged_end <= self.capacity)

    def _begin_mutation(self, *, recovery: bool = False) -> int:
        sequence = self._load(_MUTATION)
        if sequence % 2:
            if recovery:
                return sequence
            raise RuntimeError("interrupted blob mutation; recover before writing")
        if sequence >= _MAX_U64 - 1:
            raise OverflowError("blob control sequence exhausted")
        sequence += 1
        self._store(_MUTATION, sequence)
        return sequence

    def _finish_mutation(self, sequence: int) -> None:
        self._store(_MUTATION, sequence + 1)

    def reserve_and_write(self, payload: bytes, record_seq: int) -> tuple[int, int]:
        payload = bytes(payload)
        record_seq = int(record_seq)
        if not 0 < record_seq <= _MAX_U64 or record_seq <= self._load(_PUBLISHED_SEQ):
            raise ValueError("record_seq must advance the parent's one-based record count")
        if self.pending_record_seq:
            raise RuntimeError("publish or abort the existing reservation first")
        old_end = self.allocated_end
        start, end = self._reservation(len(payload))
        if end - self.acknowledged_end > self.capacity:
            raise BufferFull("blob bytes await parent persistence acknowledgement")
        sequence = self._begin_mutation()
        try:
            self._store(_PENDING_OLD, old_end)
            self._store(_PENDING_END, end)
            self._store(_PENDING_READY, 0)
            self._store(_PENDING_SEQ, record_seq)
            self._store(_RETAINED, max(self._load(_RETAINED), end - self.capacity))
            # Invalidate reused addresses BEFORE overwriting them. Readers fence
            # around owned copies and reject ranges invalidated during the copy.
            atomic_fence()
            position = start % self.capacity
            self._data[position:position + len(payload)] = payload
            atomic_fence()
            self._store(_PENDING_READY, 1)
            self._store(_ALLOCATED, end)
            self._finish_mutation(sequence)
        except BaseException:
            self.recover(record_seq - 1)
            raise
        return start // self.chunk_bytes + 1, start % self.chunk_bytes

    def publish(self, record_seq: int) -> None:
        """Finish bookkeeping AFTER the parent index row is visible."""
        record_seq = int(record_seq)
        if not self.pending_record_seq and record_seq == self._load(_PUBLISHED_SEQ):
            return
        if record_seq != self.pending_record_seq or not self._load(_PENDING_READY):
            raise RuntimeError("no complete reservation for this parent record")
        sequence = self._begin_mutation()
        self._store(_PUBLISHED_SEQ, record_seq)
        self._store(_PENDING_SEQ, 0)
        self._finish_mutation(sequence)

    def abort(self, record_seq: int) -> None:
        """Discard a reservation whose parent index row was never published."""
        if int(record_seq) != self.pending_record_seq or not self.pending_record_seq:
            raise RuntimeError("no pending reservation for this parent record")
        if self.acknowledged_end > self._load(_PENDING_OLD):
            raise RuntimeError("cannot abort bytes already acknowledged by the parent")
        sequence = self._begin_mutation()
        self._store(_ALLOCATED, self._load(_PENDING_OLD))
        self._store(_PENDING_SEQ, 0)
        self._finish_mutation(sequence)

    def recover(self, parent_record_count: int) -> None:
        """Resolve an interrupted reservation under exclusive producer ownership."""
        parent_record_count = int(parent_record_count)
        if not 0 <= parent_record_count <= _MAX_U64:
            raise ValueError("invalid parent_record_count")
        if self._load(_PUBLISHED_SEQ) > parent_record_count:
            raise RuntimeError("parent index regressed behind published blob state")
        sequence = self._begin_mutation(recovery=True)
        pending = self.pending_record_seq
        if pending:
            if pending <= parent_record_count:
                if not self._load(_PENDING_READY):
                    raise RuntimeError("parent published a reservation with incomplete bytes")
                self._store(_ALLOCATED, self._load(_PENDING_END))
                self._store(_PUBLISHED_SEQ, pending)
            else:
                old_end = self._load(_PENDING_OLD)
                if self.acknowledged_end > old_end:
                    raise RuntimeError("unpublished reservation was acknowledged")
                self._store(_ALLOCATED, old_end)
            self._store(_PENDING_SEQ, 0)
        self._finish_mutation(sequence)

    def read(self, chunk_id: int, offset: int, size: int) -> bytes:
        chunk_id, offset, size = int(chunk_id), int(offset), int(size)
        if chunk_id <= 0 or offset < 0 or size <= 0 or offset + size > self.chunk_bytes:
            raise ValueError("invalid blob coordinates")
        start = (chunk_id - 1) * self.chunk_bytes + offset
        end = start + size
        if start < self._load(_RETAINED) or end > self.allocated_end:
            raise PayloadUnavailable("payload is outside the retained live byte range")
        atomic_fence()
        position = start % self.capacity
        payload = bytes(self._data[position:position + size])
        atomic_fence()
        # Already-copied bytes stay safe even if reclamation follows this check.
        if start < self._load(_RETAINED) or end > self.allocated_end:
            raise PayloadUnavailable("payload was reclaimed while it was being copied")
        return payload

    def acknowledge(self, logical_end: int) -> None:
        """Persister only: acknowledge bytes after the durable parent commit."""
        logical_end = int(logical_end)
        if not self.acknowledged_end <= logical_end <= self.allocated_end:
            raise ValueError("acknowledgement must advance within allocated complete bytes")
        self._store(_ACKNOWLEDGED, logical_end)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._data = None
        if self._data_map is not None:
            self._data_map.close()
            self._data_map = None
        if self._control is not None:
            self._control.release()
            self._control = None
        if self._control_map is not None:
            self._control_map.close()
            self._control_map = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    @classmethod
    def unlink(cls, base, feed: str, sidecar_name: str) -> None:
        """Unlink only this stopped feed's sidecar shared-memory objects."""
        for name in _names(base, feed, sidecar_name):
            try:
                unlink_shm(name)
            except FileNotFoundError:
                pass
