"""Single-owner, crash-recoverable fixed-index batch commit bookkeeping.

The caller prepares before writing an index chunk, then commits only after the
payloads, index, registry and directory entries are durable. A committed batch
can be acknowledged in RAM repeatedly without writing its index again. Pending
batches require caller-owned rollback (or complete validation and synchronization)
before discarding (or committing) the descriptor. This module does no data IO.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import os
from pathlib import Path
import re
import tempfile
from typing import Literal
from uuid import UUID

import msgspec


_MAGIC = "deepwater-fixed-index-commit"
_VERSION = 1
_MAX_BYTES = 1024 * 1024
_U64_MAX = (1 << 64) - 1
_FIELDS = {"epoch", "start_seq", "end_seq", "chunk_id", "size", "digest", "last_ts", "payload_highwaters"}


class JournalError(RuntimeError):
    """Invalid journal state; persistence must stop rather than guess."""


def _uint(value: object, name: str, *, positive: bool = False) -> int:
    if type(value) is not int or not (int(positive) <= value <= _U64_MAX):
        raise JournalError(f"{name} must be a {'positive ' if positive else ''}uint64")
    return value


def _epoch(value: str | bytes | UUID) -> str:
    try:
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, bytes):
            return str(UUID(bytes=value))
        if isinstance(value, str):
            return str(UUID(value))
    except (ValueError, AttributeError) as exc:
        raise JournalError("epoch must be a UUID string or 16 bytes") from exc
    raise JournalError("epoch must be a UUID string or 16 bytes")


@dataclass(frozen=True)
class BatchDescriptor:
    epoch: str | bytes | UUID
    start_seq: int
    end_seq: int
    chunk_id: int
    size: int
    digest: str
    last_ts: int
    payload_highwaters: tuple[tuple[str, int, int], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "epoch", _epoch(self.epoch))
        for name in ("start_seq", "end_seq", "last_ts"):
            _uint(getattr(self, name), name)
        for name in ("chunk_id", "size"):
            _uint(getattr(self, name), name, positive=True)
        count = self.end_seq - self.start_seq
        if count <= 0 or self.size % count:
            raise JournalError("batch range must be nonempty and size a multiple of its record count")
        if not isinstance(self.digest, str) or re.fullmatch(r"[0-9a-f]{64}", self.digest) is None:
            raise JournalError("digest must be a lowercase SHA256 hex digest")
        if not isinstance(self.payload_highwaters, (tuple, list)) or len(self.payload_highwaters) > 4096:
            raise JournalError("invalid payload highwaters")
        highwaters = []
        seen = set()
        for entry in self.payload_highwaters:
            if not isinstance(entry, (tuple, list)) or len(entry) != 3:
                raise JournalError("payload highwater must contain sidecar, chunk_id, end_offset")
            name, chunk_id, end_offset = entry
            if not isinstance(name, str) or not name or len(name) > 255 or name in {".", ".."} or any(c in name for c in "/\\\0"):
                raise JournalError("invalid payload sidecar name")
            _uint(chunk_id, "payload chunk_id", positive=True)
            _uint(end_offset, "payload end_offset", positive=True)
            if (name, chunk_id) in seen:
                raise JournalError("duplicate payload highwater")
            seen.add((name, chunk_id))
            highwaters.append((name, chunk_id, end_offset))
        object.__setattr__(self, "payload_highwaters", tuple(sorted(highwaters)))


@dataclass(frozen=True)
class RecoveryDecision:
    action: Literal["rollback_pending", "ack_committed", "ready", "new_epoch"]
    descriptor: BatchDescriptor | None
    next_seq: int


def _decode_descriptor(value: object) -> BatchDescriptor | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != _FIELDS:
        raise JournalError("invalid batch descriptor fields")
    return BatchDescriptor(**value)


def _validate_successor(committed: BatchDescriptor | None, pending: BatchDescriptor) -> None:
    if committed is None:
        return
    if pending.chunk_id <= committed.chunk_id:
        raise JournalError("new batch must use a later chunk ID")
    if pending.epoch == committed.epoch and pending.start_seq != committed.end_seq:
        raise JournalError("same-epoch batch sequence must be contiguous")


class CommitJournal:
    """One journal per feed; its caller must hold the persistence owner lock.

    After any write/fsync exception this instance is unusable. Reload the journal
    and recover physical storage before continuing: a failed directory fsync can
    leave either the old or new journal durable even when replace succeeded.
    """

    def __init__(self, feed_dir: str | Path):
        self.path = Path(feed_dir) / ".commit-journal.msgpack"
        self._committed: BatchDescriptor | None = None
        self._pending: BatchDescriptor | None = None
        self._failed = False
        try:
            with self.path.open("rb") as fh:
                payload = fh.read(_MAX_BYTES + 1)
        except FileNotFoundError:
            return
        if len(payload) > _MAX_BYTES:
            raise JournalError("journal exceeds size limit")
        try:
            state = msgspec.msgpack.decode(payload)
        except msgspec.DecodeError as exc:
            raise JournalError("corrupt commit journal") from exc
        if not isinstance(state, dict) or set(state) != {"magic", "version", "committed", "pending", "checksum"}:
            raise JournalError("invalid commit journal fields")
        checksum = state.pop("checksum")
        if not isinstance(checksum, str) or checksum != hashlib.sha256(msgspec.msgpack.encode(state)).hexdigest():
            raise JournalError("commit journal checksum mismatch")
        if state["magic"] != _MAGIC or type(state["version"]) is not int or state["version"] != _VERSION:
            raise JournalError("unsupported commit journal format")
        self._committed = _decode_descriptor(state["committed"])
        self._pending = _decode_descriptor(state["pending"])
        if self._pending is not None:
            _validate_successor(self._committed, self._pending)
        # Establish a replaced journal durably if its previous directory sync failed.
        with self.path.open("rb") as fh:
            os.fsync(fh.fileno())
        _fsync_directory(self.path.parent)

    def _check(self) -> None:
        if self._failed:
            raise JournalError("journal write was interrupted; reload and recover before continuing")

    @property
    def committed(self) -> BatchDescriptor | None:
        self._check()
        return self._committed

    @property
    def pending(self) -> BatchDescriptor | None:
        self._check()
        return self._pending

    def prepare(self, descriptor: BatchDescriptor) -> Literal["prepared", "already_pending", "already_committed"]:
        self._check()
        if not isinstance(descriptor, BatchDescriptor):
            raise JournalError("prepare requires a BatchDescriptor")
        if descriptor == self._pending:
            return "already_pending"
        if self._pending is not None:
            raise JournalError("another batch is pending; recover it before preparing")
        if descriptor == self._committed:
            return "already_committed"
        _validate_successor(self._committed, descriptor)
        self._store(self._committed, descriptor)
        return "prepared"

    def commit(self) -> BatchDescriptor:
        self._check()
        if self._pending is None:
            if self._committed is None:
                raise JournalError("no batch to commit")
            return self._committed
        descriptor = self._pending
        self._store(descriptor, None)
        return descriptor

    def discard_pending(self) -> BatchDescriptor | None:
        """Call only after pending physical index/registry state is rolled back."""
        self._check()
        descriptor = self._pending
        if descriptor is not None:
            self._store(self._committed, None)
        return descriptor

    def recover(self, epoch: str | bytes | UUID, durable_seq: int) -> RecoveryDecision:
        """Recommend recovery without mutating storage or inferring identity from time."""
        self._check()
        epoch = _epoch(epoch)
        _uint(durable_seq, "durable_seq")
        if self._pending is not None:
            return RecoveryDecision("rollback_pending", self._pending, durable_seq)
        if self._committed is None:
            return RecoveryDecision("ready", None, durable_seq)
        committed = self._committed
        if committed.epoch != epoch:
            return RecoveryDecision("new_epoch", committed, durable_seq)
        if not committed.start_seq <= durable_seq <= committed.end_seq:
            raise JournalError("RAM acknowledgement is inconsistent with the committed batch")
        action = "ack_committed" if durable_seq < committed.end_seq else "ready"
        return RecoveryDecision(action, committed, committed.end_seq)

    def _store(self, committed: BatchDescriptor | None, pending: BatchDescriptor | None) -> None:
        state = {
            "magic": _MAGIC,
            "version": _VERSION,
            "committed": asdict(committed) if committed is not None else None,
            "pending": asdict(pending) if pending is not None else None,
        }
        state["checksum"] = hashlib.sha256(msgspec.msgpack.encode(state)).hexdigest()
        payload = msgspec.msgpack.encode(state)
        if len(payload) > _MAX_BYTES:
            raise JournalError("journal exceeds size limit")
        temporary: str | None = None
        try:
            fd, temporary = tempfile.mkstemp(prefix=self.path.name + ".", suffix=".tmp", dir=self.path.parent)
            with os.fdopen(fd, "wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(temporary, self.path)
            _fsync_directory(self.path.parent)
        except BaseException:
            self._failed = True
            raise
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except FileNotFoundError:
                    pass
        self._committed, self._pending = committed, pending


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
