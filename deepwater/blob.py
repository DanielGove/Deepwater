from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from .io import Writer
from .metadata.discovery import create_feed


@dataclass(frozen=True)
class BlobRef:
    digest: str
    size: int
    flags: int

    @property
    def hex(self) -> str:
        return self.digest


class BlobStore:
    def __init__(self, base_path, feed_name: str):
        self.base_path = Path(base_path)
        self.feed_name = feed_name
        self.root = self.base_path / "data" / feed_name / "blobs"
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, digest: str) -> Path:
        return self.root / digest

    def write(self, payload: bytes) -> BlobRef:
        digest = hashlib.sha256(payload).hexdigest()
        path = self.path(digest)
        if not path.exists():
            path.write_bytes(payload)
        return BlobRef(digest=digest, size=len(payload), flags=0)

    def read(self, digest: str) -> bytes:
        return self.path(digest).read_bytes()


class BlobReferenceWriter:
    def __init__(self, base_path, feed_name: str):
        self.base_path = Path(base_path)
        self.feed_name = feed_name
        self.store = BlobStore(self.base_path, feed_name)
        self.writer = Writer(self.base_path, feed_name)

    def write_blob(self, ts: int, payload: bytes, *, flags: int = 0) -> BlobRef:
        ref = self.store.write(payload)
        self.writer.write_values(int(ts), ref.digest.encode("ascii"), int(ref.size), int(flags))
        return BlobRef(digest=ref.digest, size=ref.size, flags=int(flags))

    def close(self) -> None:
        self.writer.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


def create_blob_feed(base_path, feed_name: str, *, chunk_size_mb: float = 64.0, persist: bool = True) -> None:
    create_feed(
        base_path,
        {
            "feed_name": feed_name,
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "blob_sha256", "type": "bytes64"},
                {"name": "size", "type": "uint64"},
                {"name": "flags", "type": "uint64"},
            ],
            "clock_level": 1,
            "persist": bool(persist),
            "chunk_size_mb": float(chunk_size_mb),
        },
    )


def read_blob_ref(base_path, feed_name: str, row) -> bytes:
    if hasattr(row, "dtype") and getattr(row.dtype, "names", None):
        digest = row["blob_sha256"]
        if isinstance(digest, (bytes, bytearray, memoryview)):
            digest = bytes(digest).decode("ascii").rstrip("\x00")
        else:
            digest = str(digest)
    elif isinstance(row, dict):
        digest = row["blob_sha256"]
        if isinstance(digest, (bytes, bytearray, memoryview)):
            digest = bytes(digest).decode("ascii").rstrip("\x00")
    else:
        digest = row[1]
        if isinstance(digest, (bytes, bytearray, memoryview)):
            digest = bytes(digest).decode("ascii").rstrip("\x00")
    return BlobStore(base_path, feed_name).read(digest)
