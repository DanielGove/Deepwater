import struct
import mmap
import os
import fcntl
from pathlib import Path
from typing import Optional


class GlobalRegistry:
    """Zero-copy global feed registry with mmap and interprocess locking"""

    FEED_NAME_LEN = 32
    ENTRY_SIZE = 64  # [name:32][first:8][last:8][chunks:4][reserved:12]
    HEADER_SIZE = 64  # [feed_count:8][reserved:56]
    FORMAT = '<32sQQI12x'

    def __init__(self, base_path: Path):
        self.registry_path = base_path / "registry"
        self.registry_path.mkdir(parents=True, exist_ok=True)
        self.registry_file = self.registry_path / "global_registry.bin"
        self.max_feeds = 8191
        self.size = self.HEADER_SIZE + self.max_feeds * self.ENTRY_SIZE

        if not self.registry_file.exists():
            with open(self.registry_file, 'wb') as f:
                f.write(struct.pack('<Q56x', 0))  # feed_count = 0
                f.write(b'\x00' * (self.size - self.HEADER_SIZE))

        self.fd = os.open(self.registry_file, os.O_RDWR)
        self.lock_fd = open(self.registry_file, 'rb+')
        self.mm = mmap.mmap(self.fd, self.size)
        self.header = memoryview(self.mm)[:self.HEADER_SIZE]
        self.entries = memoryview(self.mm)[self.HEADER_SIZE:]

    def _lock(self):
        fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX)

    def _unlock(self):
        fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)

    def feed_count(self) -> int:
        return struct.unpack('<Q', self.header[:8])[0]

    def _get_offset(self, idx: int) -> int:
        return idx * self.ENTRY_SIZE

    def _read_entry(self, idx: int):
        off = self._get_offset(idx)
        return self.entries[off:off + self.ENTRY_SIZE]

    def _write_entry(self, idx: int, entry: bytes):
        off = self._get_offset(idx)
        self.entries[off:off + self.ENTRY_SIZE] = entry

    def _unpack_entry(self, b: bytes):
        return struct.unpack(self.FORMAT, b)

    def _pack_entry(self, name: str, first: int, last: int, chunks: int):
        return struct.pack(self.FORMAT, name.ljust(self.FEED_NAME_LEN, '\0').encode(), first, last, chunks)

    def _get_name_from_entry(self, b: bytes):
        return b[:self.FEED_NAME_LEN].rstrip(b'\0').decode()

    def _find_insert_pos(self, name: str) -> int:
        name_bytes = name.ljust(self.FEED_NAME_LEN, '\0').encode()
        left, right = 0, self.feed_count()
        while left < right:
            mid = (left + right) // 2
            mid_name = self._read_entry(mid)[:self.FEED_NAME_LEN]
            if mid_name < name_bytes:
                left = mid + 1
            else:
                right = mid
        return left

    def _find_feed_idx(self, name: str) -> Optional[int]:
        name_bytes = name.ljust(self.FEED_NAME_LEN, '\0').encode()
        left, right = 0, self.feed_count() - 1
        while left <= right:
            mid = (left + right) // 2
            mid_entry = self._read_entry(mid)
            if mid_entry[:self.FEED_NAME_LEN] == name_bytes:
                return mid
            elif mid_entry[:self.FEED_NAME_LEN] < name_bytes:
                left = mid + 1
            else:
                right = mid - 1
        return None

    def feed_exists(self, name: str) -> bool:
        return self._find_feed_idx(name) is not None

    def register_feed(self, name: str) -> bool:
        self._lock()
        try:
            if self.feed_exists(name):
                return False

            count = self.feed_count()
            if count >= self.max_feeds:
                raise RuntimeError("Registry full")

            pos = self._find_insert_pos(name)
            if pos < count:
                src = self._get_offset(pos)
                dst = self._get_offset(pos + 1)
                size = (count - pos) * self.ENTRY_SIZE
                self.entries[dst:dst + size] = self.entries[src:src + size]

            self._write_entry(pos, self._pack_entry(name, 0, 0, 0))
            struct.pack_into('<Q', self.header, 0, count+1)
            self.mm.flush()
            return True
        finally:
            self._unlock()

    def get_metadata(self, name: str):
        idx = self._find_feed_idx(name)
        if idx is None:
            return None
        raw = self._read_entry(idx)
        name, first, last, chunks = self._unpack_entry(raw)
        return {
            'feed_name': name.decode().rstrip('\0'),
            'first_time': first,
            'last_time': last,
            'chunk_count': chunks
        }

    def update_metadata(self, name: str, first: int, last: int, chunks: int):
        self._lock()
        try:
            idx = self._find_feed_idx(name)
            if idx is None:
                return False
            self._write_entry(idx, self._pack_entry(name, first, last, chunks))
            self.mm.flush()
            return True
        finally:
            self._unlock()

    def close(self):
        try:
            self.mm.close()
            os.close(self.fd)
        except:
            pass