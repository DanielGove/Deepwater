"""Append-only payload chunks for blob sidecars."""

from __future__ import annotations

import os
import zlib
from dataclasses import dataclass
from pathlib import Path

from ..metadata.sidecar import BlobSidecar, Sidecars, load_sidecars, sidecar_dir

_CODEC_IDS = {
    "raw": 0,
    "msgpack": 1,
    "json": 2,
    "zstd": 3,
    "npy": 4,
}
_CODEC_NAMES = {value: key for key, value in _CODEC_IDS.items()}


@dataclass(frozen=True, slots=True)
class BlobRef:
    sidecar: str
    chunk_id: int
    offset: int
    size: int
    codec: str
    codec_id: int
    schema_id: int
    flags: int
    crc: int


def codec_id(codec: str) -> int:
    try:
        return _CODEC_IDS[codec]
    except KeyError:
        raise ValueError(f"unknown blob codec: {codec!r}") from None


def codec_name(value: int) -> str:
    return _CODEC_NAMES.get(int(value), f"codec:{int(value)}")


def chunk_path(root: Path, chunk_id: int) -> Path:
    return root / f"blob_{int(chunk_id):08d}.bin"


class BlobSidecarWriter:
    __slots__ = ("sidecar", "root", "_chunk_id", "_file", "_offset")

    def __init__(self, feed_dir: Path, sidecar: BlobSidecar):
        self.sidecar = sidecar
        self.root = sidecar_dir(feed_dir, sidecar.name)
        self.root.mkdir(parents=True, exist_ok=True)
        self._chunk_id = self._latest_chunk_id()
        self._file = None
        self._offset = 0
        self._open_current()

    def _latest_chunk_id(self) -> int:
        latest = 0
        for path in self.root.glob("blob_*.bin"):
            try:
                latest = max(latest, int(path.stem.rsplit("_", 1)[1]))
            except (IndexError, ValueError):
                continue
        return latest or 1

    def _open_current(self) -> None:
        path = chunk_path(self.root, self._chunk_id)
        self._file = path.open("a+b")
        self._file.seek(0, os.SEEK_END)
        self._offset = self._file.tell()

    def _roll_chunk(self) -> None:
        if self._file is not None:
            self._file.close()
        self._chunk_id += 1
        self._file = None
        self._offset = 0
        self._open_current()

    def write(self, payload: bytes | bytearray | memoryview, *, codec: str, schema_id: int, flags: int) -> BlobRef:
        payload = bytes(payload)
        size = len(payload)
        if size <= 0:
            raise ValueError("blob payload must not be empty")
        if size > int(self.sidecar.chunk_size_bytes):
            raise ValueError("blob payload exceeds sidecar chunk size")
        if self._offset and self._offset + size > int(self.sidecar.chunk_size_bytes):
            self._roll_chunk()

        cid = codec_id(codec)
        offset = self._offset
        self._file.write(payload)
        self._file.flush()
        self._offset += size
        return BlobRef(
            sidecar=self.sidecar.name,
            chunk_id=self._chunk_id,
            offset=offset,
            size=size,
            codec=codec,
            codec_id=cid,
            schema_id=int(schema_id),
            flags=int(flags),
            crc=zlib.crc32(payload) & 0xFFFFFFFF,
        )

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


class BlobSidecarWriters:
    __slots__ = ("feed_dir", "sidecars", "_writers")

    def __init__(self, base_path: Path, feed_name: str):
        self.feed_dir = Path(base_path) / "data" / feed_name
        self.sidecars = load_sidecars(base_path, feed_name)
        self._writers: dict[str, BlobSidecarWriter] = {}

    def __getitem__(self, name: str) -> BlobSidecarWriter:
        writer = self._writers.get(name)
        if writer is None:
            writer = BlobSidecarWriter(self.feed_dir, self.sidecars[name])
            self._writers[name] = writer
        return writer

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()


class BlobSidecarReader:
    __slots__ = ("root", "sidecar")

    def __init__(self, feed_dir: Path, sidecar: BlobSidecar):
        self.root = sidecar_dir(feed_dir, sidecar.name)
        self.sidecar = sidecar

    def read(self, ref: BlobRef) -> bytes:
        path = chunk_path(self.root, ref.chunk_id)
        with path.open("rb") as f:
            f.seek(ref.offset)
            data = f.read(ref.size)
        if len(data) != ref.size:
            raise FileNotFoundError(f"incomplete blob payload: {path}:{ref.offset}+{ref.size}")
        return data


class BlobSidecarReaders:
    __slots__ = ("feed_dir", "sidecars", "_readers")

    def __init__(self, base_path: Path, feed_name: str):
        self.feed_dir = Path(base_path) / "data" / feed_name
        self.sidecars = load_sidecars(base_path, feed_name)
        self._readers: dict[str, BlobSidecarReader] = {}

    def __getitem__(self, name: str) -> BlobSidecarReader:
        reader = self._readers.get(name)
        if reader is None:
            reader = BlobSidecarReader(self.feed_dir, self.sidecars[name])
            self._readers[name] = reader
        return reader


def ref_values(ref: BlobRef) -> tuple[int, int, int, int, int, int, int]:
    return (
        int(ref.chunk_id),
        int(ref.offset),
        int(ref.size),
        int(ref.codec_id),
        int(ref.schema_id),
        int(ref.flags),
        int(ref.crc),
    )
