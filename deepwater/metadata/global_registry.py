# core/global_registry.py  (drop-in)
import struct, mmap, os, fcntl
from pathlib import Path
from typing import Optional
import time

from ..utils.process import ProcessUtils


FEED_NAME_LEN = 32
PERSISTER_STALE_US = 5_000_000

# name:32 chunk_sz_bytes:u32 ring_sz_bytes:u32 retain_h:u32 flags:u16 clock_level:u16 created_us:u64 reserved:72
_ENTRY_CORE = struct.Struct("<32sIIIHHQ72x")
_HEADER_CORE = struct.Struct('<QQQQ96x')  # feed_count, persister_pid, started_us, heartbeat_us
ENTRY_SIZE = _ENTRY_CORE.size
HEADER_SIZE = _HEADER_CORE.size
CHUNK_SIZE_OFFSET = 32
RING_SIZE_OFFSET = 36
RETAIN_HOURS_OFFSET = 40
FLAGS_OFFSET = 44
CLOCK_LEVEL_OFFSET = 46
PERSISTER_PID_OFFSET = 8
PERSISTER_STARTED_US_OFFSET = 16
PERSISTER_HEARTBEAT_US_OFFSET = 24

FEED_FLAG_PERSIST = 1 << 0
FEED_FLAG_SEGMENT_TRACKING = 1 << 1
FEED_FLAG_PREFAULT_RING = 1 << 2
FEED_FLAG_USES_RING = 1 << 3


class FeedMetadata:
    """Immutable copied snapshot of one global registry entry."""

    __slots__ = (
        "feed_name",
        "chunk_size_bytes",
        "ring_size_bytes",
        "retention_hours",
        "flags",
        "clock_level",
        "created_us",
    )

    def __init__(
        self,
        feed_name: str,
        chunk_size_bytes: int,
        ring_size_bytes: int,
        retention_hours: int,
        flags: int,
        clock_level: int,
        created_us: int,
    ):
        self.feed_name = str(feed_name)
        self.chunk_size_bytes = int(chunk_size_bytes)
        self.ring_size_bytes = int(ring_size_bytes)
        self.retention_hours = int(retention_hours)
        self.flags = int(flags)
        self.clock_level = int(clock_level)
        self.created_us = int(created_us)

    @classmethod
    def from_registry(cls, buffer, offset: int) -> "FeedMetadata":
        name, chunk_size_bytes, ring_size_bytes, retention_hours, flags, clock_level, created_us = (
            _ENTRY_CORE.unpack_from(buffer, int(offset))
        )
        return cls(
            name.rstrip(b"\0").decode(),
            chunk_size_bytes,
            ring_size_bytes,
            retention_hours,
            flags,
            clock_level,
            created_us,
        )

    @property
    def persist(self) -> bool:
        return bool(self.flags & FEED_FLAG_PERSIST)

    @property
    def segment_tracking(self) -> bool:
        return bool(self.flags & FEED_FLAG_SEGMENT_TRACKING)

    @property
    def prefault_ring(self) -> bool:
        return bool(self.flags & FEED_FLAG_PREFAULT_RING)

    @property
    def uses_ring(self) -> bool:
        return bool(self.flags & FEED_FLAG_USES_RING)

    def to_dict(self) -> dict:
        return {
            "feed_name": self.feed_name,
            "chunk_size_bytes": self.chunk_size_bytes,
            "ring_size_bytes": self.ring_size_bytes,
            "retention_hours": self.retention_hours,
            "persist": self.persist,
            "segment_tracking": self.segment_tracking,
            "prefault_ring": self.prefault_ring,
            "uses_ring": self.uses_ring,
            "clock_level": self.clock_level,
            "created_us": self.created_us,
        }


class GlobalRegistry:
    """
    Zero-copy global feed registry with interprocess locking.
    Each entry is 128 bytes: [feed name + fixed metadata + reserved].
    Formatting (fmt/fields/ts_off) lives in data/<feed>/layout.json, not here.
    """

    def __init__(self, base_path: Path):
        self.registry_path = base_path / "registry"
        self.registry_path.mkdir(parents=True, exist_ok=True)
        self.registry_file = self.registry_path / "global_registry.bin"
        self.max_feeds = 16383
        self.size = HEADER_SIZE + self.max_feeds * ENTRY_SIZE

        if not self.registry_file.exists():
            with open(self.registry_file, 'wb') as f:
                f.write(_HEADER_CORE.pack(0, 0, 0, 0))
                f.write(b'\x00' * (self.size - HEADER_SIZE))

        self.fd = os.open(self.registry_file, os.O_RDWR)
        self.lock_fd = open(self.registry_file, 'rb+')
        self.mmap = mmap.mmap(self.fd, self.size)

    # ---------- locking ----------
    def _lock(self):
        fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX)
    def _unlock(self):
        fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)

    # ---------- header ----------
    @property
    def feed_count(self) -> int:
        return int.from_bytes(self.mmap[0:8], "little")
    @feed_count.setter
    def feed_count(self, v: int) -> None:
        self.mmap[0:8] = int(v).to_bytes(8, "little")

    def _read_qword(self, offset: int) -> int:
        return int.from_bytes(self.mmap[offset:offset + 8], "little")

    def _write_qword(self, offset: int, value: int) -> None:
        self.mmap[offset:offset + 8] = int(value).to_bytes(8, "little")

    # ---------- locate ----------
    def _find_insert_offset(self, name: str) -> int:
        key = name.ljust(FEED_NAME_LEN, '\0').encode('utf-8')
        lo, hi = HEADER_SIZE, HEADER_SIZE + self.feed_count * ENTRY_SIZE
        while lo < hi:
            mid = (lo + hi) // 2 // ENTRY_SIZE * ENTRY_SIZE
            mid_name = self.mmap[mid:mid+FEED_NAME_LEN]
            lo, hi = (mid + ENTRY_SIZE, hi) if mid_name < key else (lo, mid)
        return hi

    def _find_feed_offset(self, name: str) -> Optional[int]:
        key = name.ljust(FEED_NAME_LEN, '\0').encode('utf-8')
        lo, hi = HEADER_SIZE, HEADER_SIZE + self.feed_count * ENTRY_SIZE
        while lo <= hi:
            mid = (lo + hi) // 2 // ENTRY_SIZE * ENTRY_SIZE
            mid_name = self.mmap[mid:mid+FEED_NAME_LEN]
            if mid_name == key: return mid
            lo, hi = (mid + ENTRY_SIZE, hi) if mid_name < key else (lo, mid - ENTRY_SIZE)
        return 0

    def feed_exists(self, name: str) -> bool:
        return self._find_feed_offset(name) > 0
    def list_feeds(self):
        return [self.mmap[offset:offset+FEED_NAME_LEN].rstrip(b'\x00').decode() for offset in range(HEADER_SIZE, HEADER_SIZE+self.feed_count*ENTRY_SIZE,ENTRY_SIZE)]
    # ---------- public API ----------
    def register_feed(self, name: str, metadata: dict, clock_level: int = 1) -> bool:
        self._lock()
        try:
            if self.feed_exists(name):
                return False
            if self.feed_count >= self.max_feeds:
                raise RuntimeError("Registry full")
            offset = self._find_insert_offset(name)
            self.mmap[offset+ENTRY_SIZE:HEADER_SIZE+(self.feed_count*ENTRY_SIZE)+ENTRY_SIZE] = self.mmap[offset:HEADER_SIZE+(self.feed_count*ENTRY_SIZE)]
            flags = 0
            if bool(metadata.get("persist", True)):
                flags |= FEED_FLAG_PERSIST
            if bool(metadata.get("segment_tracking", True)):
                flags |= FEED_FLAG_SEGMENT_TRACKING
            if bool(metadata.get("prefault_ring", True)):
                flags |= FEED_FLAG_PREFAULT_RING
            if bool(metadata.get("uses_ring", True)):
                flags |= FEED_FLAG_USES_RING
            _ENTRY_CORE.pack_into(self.mmap, offset,
                                       name.ljust(FEED_NAME_LEN, '\0').encode(),
                                       int(metadata.get("chunk_size_bytes", 64 * 1024 * 1024)),
                                       int(metadata.get("ring_size_bytes", 64 * 1024 * 1024)),
                                       int(metadata.get("retention_hours", 0)),
                                       int(flags),
                                       int(clock_level),
                                       int(time.time_ns() // 1_000))
            self.feed_count = self.feed_count+1
            self.mmap.flush()
            return True
        finally:
            self._unlock()

    def unregister_feed(self, name: str) -> bool:
        """
        Remove a feed entry from the global registry.
        Returns True if removed, False if it did not exist.
        """
        self._lock()
        try:
            offset = self._find_feed_offset(name)
            if offset == 0:
                return False

            count = self.feed_count
            end = HEADER_SIZE + (count * ENTRY_SIZE)
            next_offset = offset + ENTRY_SIZE

            # Shift subsequent entries left to keep sorted dense layout.
            if next_offset < end:
                self.mmap[offset:end - ENTRY_SIZE] = self.mmap[next_offset:end]

            # Zero the last slot and update header count.
            self.mmap[end - ENTRY_SIZE:end] = b"\x00" * ENTRY_SIZE
            self.feed_count = count - 1
            self.mmap.flush()
            return True
        finally:
            self._unlock()

    def get_feed(self, name: str) -> Optional[FeedMetadata]:
        offset = self._find_feed_offset(name)
        if offset == 0:
            return None
        return FeedMetadata.from_registry(self.mmap, offset)
    
    def update_metadata(self, name:str, **kwargs) -> Optional[dict]:
            self._lock()
            try:
                offset = self._find_feed_offset(name)
                if offset == 0: return False
                if kwargs.get("chunk_size_bytes") is not None:
                    self.mmap[offset+CHUNK_SIZE_OFFSET:offset+CHUNK_SIZE_OFFSET+4] = int(kwargs.get("chunk_size_bytes")).to_bytes(4, "little")
                if kwargs.get("ring_size_bytes") is not None:
                    self.mmap[offset+RING_SIZE_OFFSET:offset+RING_SIZE_OFFSET+4] = int(kwargs.get("ring_size_bytes")).to_bytes(4, "little")
                if kwargs.get("retention_hours") is not None:
                    self.mmap[offset+RETAIN_HOURS_OFFSET:offset+RETAIN_HOURS_OFFSET+4] = int(kwargs.get("retention_hours")).to_bytes(4, "little")
                flag_updates = {
                    "persist": FEED_FLAG_PERSIST,
                    "segment_tracking": FEED_FLAG_SEGMENT_TRACKING,
                    "prefault_ring": FEED_FLAG_PREFAULT_RING,
                    "uses_ring": FEED_FLAG_USES_RING,
                }
                if any(kwargs.get(key) is not None for key in flag_updates):
                    flags = int.from_bytes(self.mmap[offset+FLAGS_OFFSET:offset+FLAGS_OFFSET+2], "little")
                    for key, bit in flag_updates.items():
                        value = kwargs.get(key)
                        if value is None:
                            continue
                        if bool(value):
                            flags |= bit
                        else:
                            flags &= ~bit
                    self.mmap[offset+FLAGS_OFFSET:offset+FLAGS_OFFSET+2] = int(flags).to_bytes(2, "little")
                self.mmap.flush()
                return True
            finally:
                self._unlock()

    def get_persistent_ring_owner(self) -> dict:
        pid = self._read_qword(PERSISTER_PID_OFFSET)
        started_us = self._read_qword(PERSISTER_STARTED_US_OFFSET)
        heartbeat_us = self._read_qword(PERSISTER_HEARTBEAT_US_OFFSET)
        now_us = time.time_ns() // 1_000
        alive = ProcessUtils.is_process_alive(pid)
        stale = bool(pid and heartbeat_us and (now_us - heartbeat_us) > PERSISTER_STALE_US)
        healthy = bool(pid and alive)
        return {
            "pid": pid,
            "started_us": started_us,
            "heartbeat_us": heartbeat_us,
            "alive": alive,
            "healthy": healthy,
            "stale": stale,
        }

    def claim_persistent_ring_owner(self, pid: int) -> bool:
        if pid <= 0:
            return False
        now_us = time.time_ns() // 1_000
        self._lock()
        try:
            current_pid = self._read_qword(PERSISTER_PID_OFFSET)
            current_started_us = self._read_qword(PERSISTER_STARTED_US_OFFSET)
            current_heartbeat_us = self._read_qword(PERSISTER_HEARTBEAT_US_OFFSET)
            current_alive = ProcessUtils.is_process_alive(current_pid)
            current_stale = bool(
                current_pid
                and current_heartbeat_us
                and (now_us - current_heartbeat_us) > PERSISTER_STALE_US
            )
            if current_pid and current_alive and current_pid != pid:
                return False

            self._write_qword(PERSISTER_PID_OFFSET, pid)
            self._write_qword(
                PERSISTER_STARTED_US_OFFSET,
                current_started_us if current_pid == pid and current_started_us and not current_stale else now_us,
            )
            self._write_qword(PERSISTER_HEARTBEAT_US_OFFSET, now_us)
            self.mmap.flush()
            return True
        finally:
            self._unlock()

    def heartbeat_persistent_ring_owner(self, pid: int) -> bool:
        if pid <= 0:
            return False
        now_us = time.time_ns() // 1_000
        self._lock()
        try:
            current_pid = self._read_qword(PERSISTER_PID_OFFSET)
            if current_pid != pid:
                return False
            self._write_qword(PERSISTER_HEARTBEAT_US_OFFSET, now_us)
            self.mmap.flush()
            return True
        finally:
            self._unlock()

    def release_persistent_ring_owner(self, pid: int) -> bool:
        if pid <= 0:
            return False
        self._lock()
        try:
            current_pid = self._read_qword(PERSISTER_PID_OFFSET)
            if current_pid != pid:
                return False
            self._write_qword(PERSISTER_PID_OFFSET, 0)
            self._write_qword(PERSISTER_STARTED_US_OFFSET, 0)
            self._write_qword(PERSISTER_HEARTBEAT_US_OFFSET, 0)
            self.mmap.flush()
            return True
        finally:
            self._unlock()

    def close(self):
        self.mmap.flush()
        self.mmap.close()
        self.lock_fd.close()
        os.close(self.fd)


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        reg = GlobalRegistry(Path(td))
        try:
            assert reg.register_feed(
                "events",
                {
                    "chunk_size_bytes": 1024,
                    "ring_size_bytes": 4096,
                    "retention_hours": 24,
                    "persist": True,
                    "segment_tracking": True,
                    "prefault_ring": False,
                    "uses_ring": True,
                },
                clock_level=2,
            )
            md = reg.get_feed("events")
            assert md is not None
            assert md.feed_name == "events"
            assert md.chunk_size_bytes == 1024
            assert md.ring_size_bytes == 4096
            assert md.retention_hours == 24
            assert md.persist is True
            assert md.segment_tracking is True
            assert md.prefault_ring is False
            assert md.uses_ring is True
            assert md.clock_level == 2
            assert reg.update_metadata("events", persist=False, uses_ring=False, ring_size_bytes=8192)
            md = reg.get_feed("events")
            assert md is not None
            assert md.persist is False
            assert md.uses_ring is False
            assert md.ring_size_bytes == 8192
        finally:
            reg.close()
    print("OK: GlobalRegistry smoke test passed")
