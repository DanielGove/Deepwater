# core/global_registry.py  (drop-in)
import struct, mmap, os, fcntl
from pathlib import Path
from typing import Optional
import time


FEED_NAME_LEN = 32
# name:32  chunk_sz_bytes:I  retain_h:I  persist:bool index_callback:bool  clock_level:Q created_us:Q
_ENTRY_CORE   = struct.Struct('<32sII??6xQQ64x') # Pad to 128
ENTRY_SIZE = _ENTRY_CORE.size
HEADER_SIZE = _ENTRY_CORE.size
CHUNK_SIZE_OFFSET = 32
RETAIN_HOURS_OFFSET = 36
PERSIST_OFFSET = 40
INDEX_PLAYBACK_OFFSET = 41
CLOCK_LEVEL_OFFSET = 48


class GlobalRegistry:
    """
    Zero-copy global feed registry with interprocess locking.
    Each entry is 256 bytes: [core meta + lifecycle defaults + reserved].
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
                f.write(struct.pack('<Q120x', 0))  # feed_count = 0
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
        return int.from_bytes(self.mmap[0:8])
    @feed_count.setter
    def feed_count(self, v: int) -> None:
        self.mmap[0:8] = int.to_bytes(v,8)

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
    def register_feed(self, name: str, lifecycle: dict, clock_level) -> bool:
        self._lock()
        try:
            if self.feed_exists(name):
                return False
            if self.feed_count >= self.max_feeds:
                raise RuntimeError("Registry full")
            offset = self._find_insert_offset(name)
            self.mmap[offset+ENTRY_SIZE:HEADER_SIZE+(self.feed_count*ENTRY_SIZE)+ENTRY_SIZE] = self.mmap[offset:HEADER_SIZE+(self.feed_count*ENTRY_SIZE)]
            _ENTRY_CORE.pack_into(self.mmap, offset,
                                       name.ljust(FEED_NAME_LEN, '\0').encode(),
                                       lifecycle.get("chunk_size_bytes", 64 * 1024 * 1024),
                                       lifecycle.get("retention_hours", 0),
                                       lifecycle.get("persist", True),
                                       lifecycle.get("index_playback", False),
                                       clock_level,
                                       time.time_ns() // 1_000)
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

    def get_metadata(self, name: str) -> Optional[dict]:
        offset = self._find_feed_offset(name)
        if offset == 0:
            return None
        nm, chunk_sz_bytes, retain_h, persist, index_playback, clock_level, created_us = _ENTRY_CORE.unpack(self.mmap[offset:offset+ENTRY_SIZE])
        name = nm.rstrip(b'\0').decode()
        return {
            "feed_name": name,
            "chunk_size_bytes": chunk_sz_bytes,
            "retention_hours": retain_h,
            "persist": persist,
            "index_playback": index_playback,
            "clock_level": clock_level,
            "created_us": created_us,
        }
    
    def update_metadata(self, name:str, **kwargs) -> Optional[dict]:
            self._lock()
            try:
                offset = self._find_feed_offset(name)
                if offset == 0: return False
                if kwargs.get("chunk_size_bytes") is not None:
                    self.mmap[offset+CHUNK_SIZE_OFFSET:offset+CHUNK_SIZE_OFFSET+4] = int.to_bytes(kwargs.get("chunk_size_bytes"),4,'little')
                if kwargs.get("retention_hours") is not None:
                    self.mmap[offset+RETAIN_HOURS_OFFSET:offset+RETAIN_HOURS_OFFSET+4] = int.to_bytes(kwargs.get("retention_hours"),4,'little')
                if kwargs.get("persist") is not None:
                    self.mmap[offset+PERSIST_OFFSET:offset+PERSIST_OFFSET+1] = kwargs.get("persist").to_bytes(1)
                if kwargs.get("index_playback") is not None:
                    self.mmap[offset+INDEX_PLAYBACK_OFFSET:offset+INDEX_PLAYBACK_OFFSET+1] = kwargs.get("index_playback").to_bytes(1)
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
    # Minimal unit tests for GlobalRegistry v512 entries + lifecycle defaults
    import tempfile, time
    from pathlib import Path

    def assert_eq(a, b, msg=""):
        if a != b:
            raise AssertionError(f"{msg} (got {a!r}, expected {b!r})")

    def test_init_and_sizes():
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            reg = GlobalRegistry(base)
            try:
                # file exists and is correctly sized
                assert reg.registry_file.exists()
                assert reg.feed_count == 0
                assert_eq(ENTRY_SIZE, 128, "ENTRY_SIZE should be 128")
                # header present
                assert reg.size >= HEADER_SIZE + ENTRY_SIZE
            finally:
                reg.close()

    def test_register_and_get():
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            reg = GlobalRegistry(base)
            try:
                lc = {
                    "chunk_size_bytes": 1_048_576,
                    "rotate_s": 60,
                    "persist": True,
                    "index_playback": True,
                }
                ok = reg.register_feed("CB-TRADES-BTC-USD", lc, now_us=123)
                assert ok is True
                md = reg.get_metadata("CB-TRADES-BTC-USD")
                assert md is not None
                assert_eq(md["feed_name"], "CB-TRADES-BTC-USD")
                assert_eq(md["chunk_size_bytes"], 1_048_576)
                assert_eq(md["rotate_s"], 60)
                assert md["persist"] is True
                assert md["index_playback"] is True
                # second register should be a no-op / False
                assert reg.register_feed("CB-TRADES-BTC-USD", lc) is False
            finally:
                reg.close()

    def test_sorted_insert_and_iteration():
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            reg = GlobalRegistry(base)
            try:
                for name in ["E-FEED", "C-FEED", "D-FEED", "B-FEED", "A-FEED"]:
                    assert reg.register_feed(name, {}) is True
                assert_eq(reg.feed_count, 5)
                # internal order should be sorted by name
                names = []
                for name in ["A-FEED", "B-FEED", "C-FEED", "D-FEED", "E-FEED"]:
                    entry = reg.get_metadata(name)
                    names.append(entry["feed_name"])
                assert_eq(names, ["A-FEED", "B-FEED", "C-FEED", "D-FEED", "E-FEED"], "registry order")
            finally:
                reg.close()

    def test_update_lifecycle_and_metadata_persist():
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            reg = GlobalRegistry(base)
            try:
                reg.register_feed("CB-L2-BTC-USD", {
                    "chunk_size_bytes": 16 * 1024,  # tiny for test
                    "rotate_s": 3600,
                    "persist": True,
                    "index_playback": False,
                }, now_us=111)
                assert reg.update_metadata("CB-L2-BTC-USD", index_playback=True) is True
                md = reg.get_metadata("CB-L2-BTC-USD")
                assert md["index_playback"] is True

                # close + reopen → persistence
                reg.close()
                reg = GlobalRegistry(base)
                md3 = reg.get_metadata("CB-L2-BTC-USD")
                assert md3["index_playback"] is True
            finally:
                reg.close()

    def test_concurrent_register_guard():
        # best-effort: two instances against same file; second register should fail
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            reg1 = GlobalRegistry(base)
            reg2 = GlobalRegistry(base)
            try:
                assert reg1.register_feed("DUP-FEED", {}) is True
                # reg2 sees it and refuses
                assert reg2.register_feed("DUP-FEED", {}) is False
            finally:
                reg1.close(); reg2.close()

    # Run all tests
    tests = [
        ("init_and_sizes", test_init_and_sizes),
        ("register_and_get", test_register_and_get),
        ("sorted_insert", test_sorted_insert_and_iteration),
        ("update_lifecycle_and_metadata_persist", test_update_lifecycle_and_metadata_persist),
        ("concurrent_register_guard", test_concurrent_register_guard),
    ]

    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS {name}")
        except Exception as e:
            failed += 1
            print(f"FAIL {name}: {e}")

    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print("OK: GlobalRegistry tests passed")
