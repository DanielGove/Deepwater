from __future__ import annotations

import fcntl
import logging
import os
import signal
import struct
import sys
import time
from pathlib import Path

from .ring import RingBuffer, _yield_cpu, ring_buffer_shm_names
from .writer import ChunkWriter
from ..metadata.feed_metadata import load_feed_metadata
from ..metadata.feed_schema import load_record_schema_for_feed


log = logging.getLogger("dw.persistent_ring")

_HEARTBEAT_INTERVAL_S = 0.25
_DISCOVER_INTERVAL_S = 0.25
_DEFAULT_RING_BYTES = 64 * 1024 * 1024
_U64 = struct.Struct("<Q")

_IDLE = 0
_WAITING = 1
_FLUSHED = 2


class _FeedState:
    __slots__ = (
        "name",
        "ring",
        "writer",
        "record_size",
        "capacity_records",
        "target_records",
        "max_records",
    )

    def __init__(self, base: Path, feed_name: str):
        metadata = load_feed_metadata(base, feed_name)
        if metadata is None:
            raise KeyError(feed_name)
        layout = load_record_schema_for_feed(base, feed_name)
        ring_bytes = int(metadata.ring_size_bytes or _DEFAULT_RING_BYTES)
        shm_names = ring_buffer_shm_names(base, feed_name)
        ring = RingBuffer(
            feed_name,
            data_size=ring_bytes,
            create=False,
            shm_name=shm_names[0],
            fallback_names=shm_names[1:],
        )
        writer = ChunkWriter(base, feed_name, segment_tracking=False)
        record_size = int(layout.record_size)
        capacity_records = max(1, ring.data_size // record_size)
        chunk_records = max(1, int(metadata.chunk_size_bytes) // record_size)
        max_records = min(capacity_records, chunk_records)

        self.name = feed_name
        self.ring = ring
        self.writer = writer
        self.record_size = record_size
        self.capacity_records = capacity_records
        self.max_records = max_records
        self.target_records = min(max_records, max(1, capacity_records // 2))

    def close(self) -> None:
        try:
            self.writer.close()
        finally:
            self.ring.close(unlink=False)


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
    data = ring.data
    n_records = end_seq - start_seq
    if n_records <= 0:
        return _IDLE
    pos = (start_pos + ((start_seq - earliest_live) * record_size)) % ring.data_size
    byte_len = n_records * record_size
    blob = data[pos:pos + byte_len]
    last_ts = _U64.unpack_from(blob, byte_len - record_size)[0]

    writer.begin_chunk_commit()
    try:
        writer.write_batch_bytes(blob)
        writer.finish_chunk_commit()
    except Exception:
        writer.finish_chunk_commit()
        raise

    ring.update_durable_header(durable_record_count=end_seq, durable_last_ts=last_ts)
    return _FLUSHED


def _drain(base: Path, state: _FeedState) -> int:
    ring = state.ring
    _, start_pos, _, _, record_count, durable_count, _, overrun_count, lost_records = ring.header()
    record_count = int(record_count)
    durable_count = int(durable_count)
    earliest_live = max(0, record_count - state.capacity_records)

    if durable_count < earliest_live:
        lost = earliest_live - durable_count
        durable_count = earliest_live
        ring.update_durable_header(
            durable_record_count=durable_count,
            overrun_count=int(overrun_count) + 1,
            lost_records=int(lost_records) + lost,
        )

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
                last_discover = now

            did_work = False
            pending = False
            for feed_name, state in list(states.items()):
                try:
                    status = _drain(base, state)
                except Exception:
                    log.exception("persister failed feed=%s", feed_name)
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
        print("usage: python -m deepwater.io.persistent_ring <base_path>", file=sys.stderr)
        return 2
    run_persister(argv[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
