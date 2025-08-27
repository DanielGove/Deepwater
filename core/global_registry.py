# core/global_registry.py  (drop-in)
import struct, mmap, os, fcntl
from pathlib import Path
from typing import Optional

class GlobalRegistry:
    """
    Zero-copy global feed registry with interprocess locking.
    Each entry is 512 bytes: [core meta + lifecycle defaults + reserved].
    Formatting (fmt/fields/ts_off) lives in data/<feed>/layout.json, not here.
    """

    FEED_NAME_LEN = 32
    HEADER_SIZE   = 512                    # [feed_count:8][reserved:504]
    ENTRY_SIZE    = 512                   # per-feed slot (was 64)
    # core meta + lifecycle block (total 80 bytes) + pad to 512
    #  name:32  first:Q  last:Q  chunks:I  chunk_sz_bytes:I  rotate_s:I  retain_h:I  flags:I  created_ns:Q  updated_ns:Q
    _ENTRY_CORE   = struct.Struct('<32sQQIIIIIQQ')
    _PAD_LEN      = ENTRY_SIZE - _ENTRY_CORE.size

    # flags
    FLAG_PERSIST         = 1 << 0
    FLAG_INDEX_PLAYBACK  = 1 << 1
    FLAG_TAILMETA        = 1 << 2   # (optional future)

    def __init__(self, base_path: Path):
        self.registry_path = base_path / "registry"
        self.registry_path.mkdir(parents=True, exist_ok=True)
        self.registry_file = self.registry_path / "global_registry.bin"
        self.max_feeds = 16383
        self.size = self.HEADER_SIZE + self.max_feeds * self.ENTRY_SIZE

        if not self.registry_file.exists():
            with open(self.registry_file, 'wb') as f:
                f.write(struct.pack('<Q504x', 0))  # feed_count = 0
                f.write(b'\x00' * (self.size - self.HEADER_SIZE))

        self.fd = os.open(self.registry_file, os.O_RDWR)
        self.lock_fd = open(self.registry_file, 'rb+')
        self.mm = mmap.mmap(self.fd, self.size)
        self.header = memoryview(self.mm)[:self.HEADER_SIZE]
        self.entries = memoryview(self.mm)[self.HEADER_SIZE:]

    # ---------- locking ----------
    def _lock(self):
        fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX)
    def _unlock(self):
        fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)

    # ---------- header ----------
    def feed_count(self) -> int:
        return struct.unpack('<Q', self.header[:8])[0]
    def _set_feed_count(self, n: int):
        struct.pack_into('<Q', self.header, 0, n)

    # ---------- locate ----------
    def _off(self, idx: int) -> int:
        return idx * self.ENTRY_SIZE
    def _read_raw(self, idx: int) -> memoryview:
        o = self._off(idx)
        return self.entries[o:o + self.ENTRY_SIZE]
    def _write_raw(self, idx: int, b: bytes):
        o = self._off(idx)
        self.entries[o:o + self.ENTRY_SIZE] = b

    def _find_insert_pos(self, name: str) -> int:
        key = name.ljust(self.FEED_NAME_LEN, '\0').encode()
        lo, hi = 0, self.feed_count()
        while lo < hi:
            mid = (lo + hi) // 2
            mid_name = bytes(self._read_raw(mid)[:self.FEED_NAME_LEN])
            lo, hi = (mid + 1, hi) if mid_name < key else (lo, mid)
        return lo

    def _find_feed_idx(self, name: str) -> Optional[int]:
        key = name.ljust(self.FEED_NAME_LEN, '\0').encode()
        lo, hi = 0, self.feed_count() - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            mv = self._read_raw(mid)
            mid_name = bytes(mv[:self.FEED_NAME_LEN])
            if mid_name == key: return mid
            lo, hi = (mid + 1, hi) if mid_name < key else (lo, mid - 1)
        return None

    def feed_exists(self, name: str) -> bool:
        return self._find_feed_idx(name) is not None

    # ---------- pack/unpack ----------
    def _pack_entry(self, name: str, first: int, last: int, chunks: int, lc: dict, created_ns: int, updated_ns: int) -> bytes:
        nm = name.ljust(self.FEED_NAME_LEN, '\0').encode()
        chunk_sz_bytes = int(lc.get("chunk_size_bytes", 64 * 1024 * 1024))
        rotate_s       = int(lc.get("rotate_s",        3600))
        retain_h       = int(lc.get("retention_hours", 72))
        flags          = 0
        if lc.get("persist", True):           flags |= self.FLAG_PERSIST
        if lc.get("index_playback", False):   flags |= self.FLAG_INDEX_PLAYBACK
        core = self._ENTRY_CORE.pack(nm, first, last, chunks, chunk_sz_bytes, rotate_s, retain_h, flags, created_ns, updated_ns)
        return core + (b'\x00' * self._PAD_LEN)

    def _unpack_entry(self, mv: memoryview) -> dict:
        nm, first, last, chunks, chunk_sz_bytes, rotate_s, retain_h, flags, created_ns, updated_ns = \
            self._ENTRY_CORE.unpack(bytes(mv[:self._ENTRY_CORE.size]))
        name = nm.rstrip(b'\0').decode()
        return {
            "feed_name": name,
            "first_time": first,
            "last_time": last,
            "chunk_count": chunks,
            "lifecycle": {
                "chunk_size_bytes": chunk_sz_bytes,
                "rotate_s": rotate_s,
                "retention_hours": retain_h,
                "persist": bool(flags & self.FLAG_PERSIST),
                "index_playback": bool(flags & self.FLAG_INDEX_PLAYBACK),
                "flags": flags,
            },
            "created_ns": created_ns,
            "updated_ns": updated_ns,
        }

    # ---------- public API ----------
    def register_feed(self, name: str, lifecycle: dict | None = None, now_ns: int = 0) -> bool:
        lifecycle = lifecycle or {}
        self._lock()
        try:
            if self.feed_exists(name):
                return False
            n = self.feed_count()
            if n >= self.max_feeds:
                raise RuntimeError("Registry full")

            pos = self._find_insert_pos(name)
            if pos < n:
                src = self._off(pos); dst = self._off(pos + 1)
                size = (n - pos) * self.ENTRY_SIZE
                self.entries[dst:dst + size] = self.entries[src:src + size]

            blob = self._pack_entry(name, 0, 0, 0, lifecycle, now_ns, now_ns)
            self._write_raw(pos, blob)
            self._set_feed_count(n + 1)
            self.mm.flush()
            return True
        finally:
            self._unlock()

    def get_metadata(self, name: str) -> Optional[dict]:
        idx = self._find_feed_idx(name)
        if idx is None: return None
        return self._unpack_entry(self._read_raw(idx))

    def update_metadata(self, name: str, first: int | None = None, last: int | None = None, chunks: int | None = None, updated_ns: int = 0) -> bool:
        self._lock()
        try:
            idx = self._find_feed_idx(name)
            if idx is None: return False
            cur = self._unpack_entry(self._read_raw(idx))
            first  = cur["first_time"] if first  is None else first
            last   = cur["last_time"]  if last   is None else last
            chunks = cur["chunk_count"] if chunks is None else chunks
            blob = self._pack_entry(name, first, last, chunks, cur["lifecycle"], cur["created_ns"], updated_ns or cur["updated_ns"])
            self._write_raw(idx, blob); self.mm.flush()
            return True
        finally:
            self._unlock()

    def get_lifecycle(self, name: str) -> Optional[dict]:
        md = self.get_metadata(name)
        return md["lifecycle"] if md else None

    def update_lifecycle(self, name: str, **kwargs) -> bool:
        """Partial update of lifecycle defaults (e.g., retention_hours=168)."""
        self._lock()
        try:
            idx = self._find_feed_idx(name)
            if idx is None: return False
            cur = self._unpack_entry(self._read_raw(idx))
            lc  = cur["lifecycle"]; lc.update(kwargs)
            blob = self._pack_entry(name, cur["first_time"], cur["last_time"], cur["chunk_count"], lc, cur["created_ns"], cur["updated_ns"])
            self._write_raw(idx, blob); self.mm.flush()
            return True
        finally:
            self._unlock()

    def close(self):
        try:
            self.mm.close(); os.close(self.fd)
        except: pass


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
                assert reg.feed_count() == 0
                assert_eq(reg.ENTRY_SIZE, 512, "ENTRY_SIZE should be 512")
                # header present
                assert reg.size >= reg.HEADER_SIZE + reg.ENTRY_SIZE
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
                    "retention_hours": 2,
                    "persist": True,
                    "index_playback": True,
                }
                ok = reg.register_feed("CB-TRADES-BTC-USD", lc, now_ns=123)
                assert ok is True
                md = reg.get_metadata("CB-TRADES-BTC-USD")
                assert md is not None
                assert_eq(md["feed_name"], "CB-TRADES-BTC-USD")
                assert_eq(md["lifecycle"]["chunk_size_bytes"], 1_048_576)
                assert_eq(md["lifecycle"]["rotate_s"], 60)
                assert_eq(md["lifecycle"]["retention_hours"], 2)
                assert md["lifecycle"]["persist"] is True
                assert md["lifecycle"]["index_playback"] is True
                # second register should be a no-op / False
                assert reg.register_feed("CB-TRADES-BTC-USD", lc) is False
            finally:
                reg.close()

    def test_sorted_insert_and_iteration():
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            reg = GlobalRegistry(base)
            try:
                for name in ["B-FEED", "A-FEED", "C-FEED"]:
                    assert reg.register_feed(name, {}) is True
                assert_eq(reg.feed_count(), 3)
                # internal order should be sorted by name
                names = []
                for i in range(reg.feed_count()):
                    entry = reg._unpack_entry(reg._read_raw(i))
                    names.append(entry["feed_name"])
                assert_eq(names, ["A-FEED", "B-FEED", "C-FEED"], "registry order")
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
                    "retention_hours": 24,
                    "persist": True,
                    "index_playback": False,
                }, now_ns=111)
                # update lifecycle (partial)
                assert reg.update_lifecycle("CB-L2-BTC-USD", retention_hours=168, index_playback=True) is True
                md = reg.get_metadata("CB-L2-BTC-USD")
                assert_eq(md["lifecycle"]["retention_hours"], 168)
                assert md["lifecycle"]["index_playback"] is True

                # update metadata (first/last/chunks)
                now = int(time.time_ns())
                assert reg.update_metadata("CB-L2-BTC-USD", first=10, last=99, chunks=2, updated_ns=now) is True
                md2 = reg.get_metadata("CB-L2-BTC-USD")
                assert_eq(md2["first_time"], 10)
                assert_eq(md2["last_time"], 99)
                assert_eq(md2["chunk_count"], 2)
                assert_eq(md2["updated_ns"], now)

                # close + reopen → persistence
                reg.close()
                reg = GlobalRegistry(base)
                md3 = reg.get_metadata("CB-L2-BTC-USD")
                assert_eq(md3["lifecycle"]["retention_hours"], 168)
                assert_eq(md3["first_time"], 10)
                assert_eq(md3["last_time"], 99)
                assert_eq(md3["chunk_count"], 2)
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
