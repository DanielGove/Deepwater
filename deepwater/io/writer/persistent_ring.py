from __future__ import annotations

import fcntl
import hashlib
import logging
import os
import signal
import struct
import sys
import time
from pathlib import Path
from uuid import UUID

from ..ring import RingBuffer, _yield_cpu, ring_buffer_shm_names
from .writer import ChunkWriter
from .commit_journal import BatchDescriptor, CommitJournal, JournalError
from ..blob_ring import BlobRing, PayloadUnavailable
from ..blob_sidecar import BlobRef, chunk_path, codec_name, ring_capacity, validate_payload
from ...metadata.sidecar import load_sidecars, sidecar_dir
from ...metadata.feed_registry import FeedRegistry, ON_DISK
from ...metadata.feed_metadata import load_feed_metadata
from ...metadata.feed_schema import load_record_schema_for_feed


log = logging.getLogger("dw.persistent_ring")

_HEARTBEAT_INTERVAL_S = 0.25
_DISCOVER_INTERVAL_S = 0.25
_DEFAULT_RING_BYTES = 64 * 1024 * 1024
_U64 = struct.Struct("<Q")

_IDLE = 0
_WAITING = 1
_FLUSHED = 2


class _FeedState:
    def __init__(self, base: Path, feed_name: str):
        self.base = Path(base)
        self.name = feed_name
        self.feed_dir = self.base / "data" / feed_name
        self.writer = None
        self.ring = None
        self.payload_rings = {}
        self.failed = False
        self.closed = False
        metadata = load_feed_metadata(base, feed_name)
        if metadata is None:
            raise KeyError(feed_name)
        layout = load_record_schema_for_feed(base, feed_name)
        self.record_size = int(layout.record_size)
        self.sidecars = load_sidecars(base, feed_name)
        fields = {field.name: field for field in layout.fields}
        self.ref_layouts = []
        for sidecar in self.sidecars:
            names = sidecar.ref_fields
            offsets = tuple(fields[getattr(names, name)].offset for name in
                            ("chunk_id", "offset", "size", "codec", "schema_id", "flags", "crc"))
            self.ref_layouts.append((sidecar, offsets))
        ring_bytes = int(metadata.ring_size_bytes or _DEFAULT_RING_BYTES)
        shm_names = ring_buffer_shm_names(base, feed_name)
        try:
            self.ring = RingBuffer(feed_name, data_size=ring_bytes, create=False,
                                   shm_name=shm_names[0], fallback_names=shm_names[1:])
            self._recover()
            self.writer = ChunkWriter(base, feed_name, commit_mode=True)
            self.capacity_records = max(1, self.ring.data_size // self.record_size)
            self.max_records = min(self.capacity_records, max(1, int(metadata.chunk_size_bytes) // self.record_size))
            self.target_records = min(self.max_records, max(1, self.capacity_records // 2))
        except BaseException:
            self.failed = True
            self.close()
            raise

    def refs(self, index_bytes: bytes):
        for offset in range(0, len(index_bytes), self.record_size):
            for sidecar, offsets in self.ref_layouts:
                values = tuple(_U64.unpack_from(index_bytes, offset + field_offset)[0] for field_offset in offsets)
                chunk_id, position, size, codec, schema_id, flags, crc = values
                if not any(values):
                    continue
                if chunk_id <= 0 or size <= 0 or position + size > sidecar.chunk_size_bytes:
                    raise ValueError(f"invalid persisted blob reference: {sidecar.name}")
                yield sidecar, BlobRef(sidecar.name, chunk_id, position, size, codec_name(codec),
                                       codec, schema_id, flags, crc)

    def payload_ring(self, sidecar):
        ring = self.payload_rings.get(sidecar.name)
        if ring is None:
            ring = BlobRing(self.base, self.name, sidecar.name,
                            chunk_bytes=sidecar.chunk_size_bytes,
                            capacity_bytes=ring_capacity(sidecar),
                            parent_epoch=self.ring.epoch)
            self.payload_rings[sidecar.name] = ring
        return ring

    def _validate_committed(self, registry, descriptor):
        if descriptor.size % self.record_size or descriptor.size // self.record_size != descriptor.end_seq - descriptor.start_seq:
            raise JournalError("committed batch record size differs from its schema")
        idx = registry.find_chunk_by_id(descriptor.chunk_id)
        if idx is None:
            raise JournalError("committed index chunk is missing from registry")
        meta = registry.get_chunk_metadata(idx)
        try:
            if int(meta.write_pos) != descriptor.size or int(meta.num_records) != descriptor.end_seq - descriptor.start_seq:
                raise JournalError("committed index registry bounds disagree with journal")
            if int(meta.get_qmax(0)) != descriptor.last_ts:
                raise JournalError("committed index timestamp disagrees with journal")
            epoch_tag = int.from_bytes(hashlib.blake2b(UUID(descriptor.epoch).bytes, digest_size=8).digest(), "little")
            if int(meta.ring_end_seq) != descriptor.end_seq or int(meta.ring_epoch_tag) != epoch_tag:
                raise JournalError("committed index ring identity disagrees with journal")
        finally:
            meta.release()
        path = self.feed_dir / f"chunk_{descriptor.chunk_id:08d}.bin"
        with path.open("rb") as fh:
            index_bytes = fh.read(descriptor.size)
        if len(index_bytes) != descriptor.size or hashlib.sha256(index_bytes).hexdigest() != descriptor.digest:
            raise JournalError("committed index chunk is missing or corrupt")
        observed = {}
        for sidecar, ref in self.refs(index_bytes):
            _disk_payload(self.feed_dir, sidecar, ref)
            observed[sidecar.name] = max(observed.get(sidecar.name, (0, 0)), (ref.chunk_id, ref.offset + ref.size))
        highwaters = tuple(sorted((name, cid, end) for name, (cid, end) in observed.items()))
        if highwaters != descriptor.payload_highwaters:
            raise JournalError("committed payload highwaters disagree with index references")
        return idx

    def _recover(self):
        registry = FeedRegistry(self.feed_dir / f"{self.name}.reg", mode="w")
        try:
            self.journal = CommitJournal(self.feed_dir)
            committed = self.journal.committed
            pending = self.journal.pending
            committed_idx = self._validate_committed(registry, committed) if committed is not None else None
            # Only the recorded pending batch and unused constructor chunks may
            # be discarded. An unexpected nonempty chunk is never guessed away.
            while True:
                latest = registry.get_latest_chunk_idx()
                if latest is None or latest == committed_idx:
                    break
                meta = registry.get_chunk_metadata(latest)
                try:
                    chunk_id = int(meta.chunk_id)
                    count = int(meta.num_records)
                    status = int(meta.status)
                finally:
                    meta.release()
                is_pending = pending is not None and chunk_id == pending.chunk_id
                if status == ON_DISK and count:
                    if committed is not None or pending is not None and chunk_id >= pending.chunk_id:
                        raise JournalError("published index chunk has no committed descriptor")
                    break  # Existing pre-journal durable history.
                if count and not is_pending:
                    raise JournalError("nonempty index chunk has no commit descriptor")
                try:
                    (self.feed_dir / f"chunk_{chunk_id:08d}.bin").unlink()
                except FileNotFoundError:
                    if not is_pending and count:
                        raise
                registry.pop_latest_chunk()
            _sync_registry(registry, self.feed_dir)
            if pending is not None:
                # A failed write can have unlinked its chunk before crashing.
                if registry.find_chunk_by_id(pending.chunk_id) is not None:
                    raise JournalError("pending index rollback did not complete")
                try:
                    (self.feed_dir / f"chunk_{pending.chunk_id:08d}.bin").unlink()
                except FileNotFoundError:
                    pass
                _sync_registry(registry, self.feed_dir)
                self.journal.discard_pending()
            if committed is not None:
                meta = registry.get_chunk_metadata(committed_idx)
                try:
                    meta.status = ON_DISK
                finally:
                    meta.release()
                _sync_registry(registry, self.feed_dir)
            decision = self.journal.recover(self.ring.epoch, int(self.ring.durable_record_count[0]))
            if committed is not None and committed.epoch == self.ring.epoch and decision.action in {"ack_committed", "ready"}:
                if decision.next_seq > int(self.ring.record_count[0]):
                    raise JournalError("committed batch is beyond the live ring sequence")
                # Restore both fields even if the count already reached end_seq:
                # a prior interruption may have left the timestamp incomplete.
                self.ring.update_durable_header(durable_record_count=decision.next_seq,
                                                durable_last_ts=committed.last_ts)
            if committed is not None and committed.epoch == self.ring.epoch:
                self.ack_payloads(committed.payload_highwaters, missing_ok=True)
        finally:
            registry.close()

    def ack_payloads(self, highwaters, *, missing_ok=False):
        for name, chunk_id, end in highwaters:
            sidecar = self.sidecars[name]
            try:
                ring = self.payload_ring(sidecar)
            except FileNotFoundError:
                if missing_ok:
                    continue
                raise
            address = (chunk_id - 1) * sidecar.chunk_size_bytes + end
            if address > ring.acknowledged_end:
                ring.acknowledge(address)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            if self.writer is not None:
                try:
                    if self.failed:
                        self.writer.abort_chunk_commit()
                finally:
                    self.writer.close()
        finally:
            for ring in self.payload_rings.values():
                ring.close()
            self.payload_rings.clear()
            if self.ring is not None:
                self.ring.close(unlink=False)


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _sync_registry(registry, directory):
    registry.flush()
    os.fsync(registry.fd)
    _fsync_directory(directory)


def _disk_payload(feed_dir, sidecar, ref):
    with chunk_path(sidecar_dir(feed_dir, sidecar.name), ref.chunk_id).open("rb") as fh:
        fh.seek(ref.offset)
        return validate_payload(ref, fh.read(ref.size))


def _persist_payloads(state, index_bytes):
    handles = {}
    highwaters = {}
    synced_directories = set()
    try:
        for sidecar, ref in state.refs(index_bytes):
            try:
                arena = state.payload_ring(sidecar)
                payload = validate_payload(ref, arena.read(ref.chunk_id, ref.offset, ref.size))
            except (FileNotFoundError, PayloadUnavailable) as exc:
                # A recovered previously acknowledged reference can already be
                # reclaimed from its arena. Unacknowledged bytes cannot fall back.
                arena = state.payload_rings.get(sidecar.name)
                address = (ref.chunk_id - 1) * sidecar.chunk_size_bytes + ref.offset + ref.size
                if isinstance(exc, PayloadUnavailable) and (getattr(exc, "retryable", False) or
                        arena is not None and address > arena.acknowledged_end):
                    raise
                payload = _disk_payload(state.feed_dir, sidecar, ref)
            key = (sidecar.name, ref.chunk_id)
            if key not in handles:
                # Bound descriptors independently of payload and index capacity.
                if len(handles) >= 64:
                    oldest = next(iter(handles))
                    fd = handles.pop(oldest)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                root = sidecar_dir(state.feed_dir, sidecar.name)
                root.mkdir(parents=True, exist_ok=True)
                handles[key] = os.open(chunk_path(root, ref.chunk_id), os.O_RDWR | os.O_CREAT, 0o644)
                synced_directories.add(sidecar.name)
            fd = handles[key]
            position = 0
            while position < len(payload):
                written = os.pwrite(fd, payload[position:], ref.offset + position)
                if written <= 0:
                    raise OSError("short payload write")
                position += written
            highwaters[sidecar.name] = max(highwaters.get(sidecar.name, (0, 0)), (ref.chunk_id, ref.offset + ref.size))
        for fd in handles.values():
            os.fsync(fd)
        for name in synced_directories:
            _fsync_directory(sidecar_dir(state.feed_dir, name))
        if handles:
            _fsync_directory(state.feed_dir / "blobs")
            _fsync_directory(state.feed_dir)
    finally:
        for fd in handles.values():
            os.close(fd)
    return tuple(sorted((name, cid, end) for name, (cid, end) in highwaters.items()))

def _raise_nofile_limit(target: int = 65536) -> None:
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        desired = min(max(int(soft), int(target)), int(hard))
        if desired > soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (desired, hard))
    except Exception:
        pass


def _configure_logging(base_path: Path) -> None:
    path = base_path / "deepwater.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s pid=%(process)d: %(message)s"))
    log.handlers[:] = [handler]
    log.setLevel(logging.INFO)
    log.propagate = False


def _writer_active(base: Path, feed_name: str) -> bool:
    path = base / "data" / feed_name / f"{feed_name}.writer.lock"
    if not path.exists():
        return False
    try:
        fd = os.open(path, os.O_RDWR)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def _copy_ring_window(state: _FeedState, start_seq: int, end_seq: int, earliest_live: int, start_pos: int) -> int:
    ring = state.ring
    writer = state.writer
    record_size = state.record_size
    n_records = end_seq - start_seq
    if n_records <= 0:
        return _IDLE
    pos = (start_seq * record_size) % ring.data_size
    byte_len = n_records * record_size
    # Own these bytes throughout slow IO; no mmap view outlives the copy.
    index_bytes = bytes(ring.data[pos:pos + byte_len])
    last_ts = _U64.unpack_from(index_bytes, byte_len - record_size)[0]
    highwaters = _persist_payloads(state, index_bytes)
    writer.begin_chunk_commit()
    descriptor = BatchDescriptor(
        epoch=ring.epoch, start_seq=start_seq, end_seq=end_seq,
        chunk_id=int(writer.current_chunk_id), size=byte_len,
        digest=hashlib.sha256(index_bytes).hexdigest(), last_ts=last_ts,
        payload_highwaters=highwaters,
    )
    result = state.journal.prepare(descriptor)
    if result != "prepared":
        raise JournalError("batch requires recovery before writing its index")
    writer.write_batch_bytes(index_bytes)
    writer.current_chunk_metadata.ring_end_seq = end_seq
    writer.current_chunk_metadata.ring_epoch_tag = ring.epoch_tag
    writer.finish_chunk_commit()
    _sync_registry(writer.registry, state.feed_dir)
    state.journal.commit()
    writer.publish_chunk_commit(descriptor.chunk_id)
    # Parent acknowledgement precedes payload reclamation; both follow commit.
    ring.update_durable_header(durable_record_count=end_seq, durable_last_ts=last_ts)
    state.ack_payloads(highwaters)
    return _FLUSHED


def _drain(base: Path, state: _FeedState) -> int:
    if state.failed or state.closed:
        raise RuntimeError("failed persister state must be reconstructed")
    try:
        return _drain_once(base, state)
    except BaseException:
        state.failed = True
        raise


def _drain_once(base: Path, state: _FeedState) -> int:
    ring = state.ring
    _, start_pos, _, _, record_count, durable_count, _, overrun_count, lost_records = ring.header()
    record_count = int(record_count)
    durable_count = int(durable_count)
    earliest_live = max(0, record_count - state.capacity_records)

    if durable_count < earliest_live:
        lost = earliest_live - durable_count
        # Recovery retries see the same unrecoverable interval. Count it once;
        # the durable cursor deliberately remains at the last real commit.
        if lost > int(lost_records):
            ring.update_durable_header(
                overrun_count=int(overrun_count) + 1,
                lost_records=lost,
            )
        raise RuntimeError(f"unpersisted ring records were overwritten: lost={lost}")

    pending = record_count - durable_count
    drain_requested = ring.drain_requested()
    if pending <= 0:
        if drain_requested:
            ring.clear_drain_request()
        return _IDLE
    if pending < state.target_records and not drain_requested and _writer_active(base, state.name):
        return _WAITING

    end_seq = min(record_count, durable_count + state.max_records)
    result = _copy_ring_window(state, durable_count, end_seq, earliest_live, int(start_pos))
    if end_seq >= record_count and drain_requested:
        ring.clear_drain_request()
    return result


def run_persister(base_path: str) -> None:
    from deepwater.metadata.global_registry import GlobalRegistry

    _raise_nofile_limit()
    base = Path(base_path)
    _configure_logging(base)

    registry = GlobalRegistry(base)
    pid = os.getpid()
    if not registry.claim_persistent_ring_owner(pid):
        log.info("persister exit: owner already claimed base_path=%s pid=%d", base_path, pid)
        registry.close()
        return

    states: dict[str, _FeedState] = {}
    stop_requested = False
    last_heartbeat = 0.0
    last_discover = 0.0

    def _stop(_signum, _frame) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    log.info("persister start base_path=%s pid=%d", base_path, pid)

    try:
        while not stop_requested:
            now = time.monotonic()
            if now - last_heartbeat >= _HEARTBEAT_INTERVAL_S:
                if not registry.heartbeat_persistent_ring_owner(pid):
                    break
                last_heartbeat = now

            if now - last_discover >= _DISCOVER_INTERVAL_S:
                for feed_name in registry.list_feeds():
                    if feed_name in states:
                        continue
                    metadata = registry.get_feed(feed_name)
                    if metadata is None or not metadata.uses_ring or not metadata.persist:
                        continue
                    try:
                        states[feed_name] = _FeedState(base, feed_name)
                    except FileNotFoundError:
                        continue
                    except Exception:
                        log.exception("persister recovery failed feed=%s", feed_name)
                last_discover = now

            did_work = False
            pending = False
            for feed_name, state in list(states.items()):
                try:
                    status = _drain(base, state)
                except Exception:
                    log.exception("persister failed feed=%s", feed_name)
                    states.pop(feed_name, None)
                    try:
                        state.close()
                    except Exception:
                        log.exception("persister failed-state close failed feed=%s", feed_name)
                    pending = True
                    continue
                did_work = did_work or status == _FLUSHED
                pending = pending or status != _IDLE

            if states and not pending:
                if not any(_writer_active(base, state.name) for state in states.values()):
                    break
            if not did_work:
                _yield_cpu()
    finally:
        for state in states.values():
            try:
                state.close()
            except Exception:
                pass
        try:
            registry.release_persistent_ring_owner(pid)
        finally:
            registry.close()
        log.info("persister stop base_path=%s pid=%d", base_path, pid)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 1:
        print("usage: python -m deepwater.io.writer.persistent_ring <base_path>", file=sys.stderr)
        return 2
    run_persister(argv[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
