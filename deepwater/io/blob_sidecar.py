"""Blob storage adapters: shared live bytes and durable payload chunks."""

from __future__ import annotations

import os
import zlib
from dataclasses import dataclass
from pathlib import Path

from ..metadata.sidecar import BlobSidecar, load_sidecars, sidecar_dir
from .blob_ring import BlobRing, BufferFull, PayloadUnavailable
from .ring import _yield_cpu

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
        cid = codec_id(codec)
        payload = bytes(payload)
        size = len(payload)
        if size <= 0:
            raise ValueError("blob payload must not be empty")
        if size > int(self.sidecar.chunk_size_bytes):
            raise ValueError("blob payload exceeds sidecar chunk size")
        if self._offset and self._offset + size > int(self.sidecar.chunk_size_bytes):
            self._roll_chunk()

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

    def publish(self) -> None:
        pass

    def abort(self) -> None:
        pass

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


def ring_capacity(sidecar: BlobSidecar) -> int:
    page = os.sysconf("SC_PAGE_SIZE")
    requested = sidecar.ring_size_bytes or 2 * sidecar.chunk_size_bytes
    return ((requested + page - 1) // page) * page


def _initial_address(feed_dir: Path, sidecar: BlobSidecar) -> int:
    # Only startup consults disk. Continuing an existing arena preserves its tail.
    root = sidecar_dir(feed_dir, sidecar.name)
    chunks = sorted(root.glob("blob_*.bin"))
    if not chunks:
        return 0
    last = chunks[-1]
    chunk_id = int(last.stem.rsplit("_", 1)[1])
    size = last.stat().st_size
    if size > sidecar.chunk_size_bytes:
        raise ValueError(f"oversized blob chunk: {last}")
    return (chunk_id - 1) * sidecar.chunk_size_bytes + size


class RingBlobSidecarWriter:
    """One payload arena, owned by the parent feed's sole writer."""

    def __init__(self, parent, sidecar: BlobSidecar):
        self.parent = parent
        self.sidecar = sidecar
        self.ring = BlobRing(
            parent.base_path, parent.feed_name, sidecar.name,
            chunk_bytes=sidecar.chunk_size_bytes,
            capacity_bytes=ring_capacity(sidecar), create=True,
            initial_address=_initial_address(parent.base_path / "data" / parent.feed_name, sidecar),
            parent_epoch=parent.ring.epoch,
        )
        self.ring.recover(int(parent.ring.record_count[0]))
        self._seq = None

    def write(self, payload, *, codec: str, schema_id: int, flags: int) -> BlobRef:
        self.parent._ensure_open()
        cid = codec_id(codec)
        payload = bytes(payload)
        if not payload or len(payload) > self.sidecar.chunk_size_bytes:
            raise ValueError("blob payload must be nonempty and fit a sidecar chunk")
        self.parent._wait_for_persisted_space(1)
        if not self.parent._persist:
            # Volatile feeds may reclaim old payloads independently of index rows.
            self.ring.acknowledge(self.ring.allocated_end)
        while not self.ring.can_reserve(len(payload)):
            self.parent.ring.request_drain()
            self.parent._ensure_open()
            _yield_cpu()
        seq = int(self.parent.ring.record_count[0]) + 1
        chunk_id, offset = self.ring.reserve_and_write(payload, seq)
        self._seq = seq
        return BlobRef(self.sidecar.name, chunk_id, offset, len(payload), codec,
                       cid, int(schema_id), int(flags), zlib.crc32(payload) & 0xFFFFFFFF)

    def publish(self) -> None:
        self.ring.publish(self._seq)
        self._seq = None

    def abort(self) -> None:
        if self._seq is not None:
            # A failure after parent publication must never roll bytes back.
            self.ring.recover(int(self.parent.ring.record_count[0]))
            self._seq = None

    def close(self) -> None:
        self.ring.close()


class BlobSidecarWriters:
    def __init__(self, base_path: Path, feed_name: str, *, parent=None):
        self.feed_dir = Path(base_path) / "data" / feed_name
        self.sidecars = load_sidecars(base_path, feed_name)
        self._writers = {}
        try:
            for sidecar in self.sidecars:
                self._writers[sidecar.name] = (
                    RingBlobSidecarWriter(parent, sidecar) if parent is not None
                    else BlobSidecarWriter(self.feed_dir, sidecar)
                )
        except BaseException:
            self.close()
            raise

    def __getitem__(self, name: str):
        return self._writers[name]

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()


def validate_payload(ref: BlobRef, data: bytes) -> bytes:
    if len(data) != ref.size:
        raise PayloadUnavailable(f"incomplete blob: {ref.sidecar}:{ref.chunk_id}:{ref.offset}")
    if zlib.crc32(data) & 0xFFFFFFFF != ref.crc:
        raise ValueError(f"blob CRC mismatch: {ref.sidecar}:{ref.chunk_id}:{ref.offset}")
    return data


class BlobSidecarReader:
    def __init__(self, feed_dir: Path, sidecar: BlobSidecar, *, ring=None, persistent=True, attach=None):
        self.root = sidecar_dir(feed_dir, sidecar.name)
        self.sidecar = sidecar
        self.ring = ring
        self.persistent = persistent
        self._attach = attach

    def read(self, ref: BlobRef, *, memory_only: bool = False) -> bytes:
        if ref.chunk_id <= 0 or ref.offset < 0 or ref.size <= 0 or ref.offset + ref.size > self.sidecar.chunk_size_bytes:
            raise ValueError("invalid blob reference bounds")
        if self.ring is None and self._attach is not None:
            try:
                self.ring = self._attach()
            except FileNotFoundError:
                pass
        if self.ring is not None:
            try:
                return validate_payload(ref, self.ring.read(ref.chunk_id, ref.offset, ref.size))
            except PayloadUnavailable as exc:
                end = (ref.chunk_id - 1) * self.sidecar.chunk_size_bytes + ref.offset + ref.size
                if memory_only or not self.persistent or getattr(exc, "retryable", False) or end > self.ring.acknowledged_end:
                    raise
        if memory_only or not self.persistent:
            raise PayloadUnavailable("blob payload is outside the live memory window")
        path = chunk_path(self.root, ref.chunk_id)
        try:
            with path.open("rb") as f:
                f.seek(ref.offset)
                data = f.read(ref.size)
        except FileNotFoundError as exc:
            raise PayloadUnavailable(f"blob payload is unavailable: {path}") from exc
        return validate_payload(ref, data)

    def close(self) -> None:
        if self.ring is not None:
            self.ring.close()
            self.ring = None


class BlobSidecarReaders:
    def __init__(self, base_path: Path, feed_name: str, *, parent_ring=None, persistent=True):
        self.feed_dir = Path(base_path) / "data" / feed_name
        self.sidecars = load_sidecars(base_path, feed_name)
        self._readers = {}
        try:
            for sidecar in self.sidecars:
                ring = None
                attach = None
                if parent_ring is not None:
                    epoch = parent_ring.epoch
                    def attach(meta=sidecar, epoch=epoch):
                        return BlobRing(base_path, feed_name, meta.name,
                                        chunk_bytes=meta.chunk_size_bytes,
                                        capacity_bytes=ring_capacity(meta), parent_epoch=epoch)
                    try:
                        ring = attach()
                    except FileNotFoundError:
                        pass
                self._readers[sidecar.name] = BlobSidecarReader(
                    self.feed_dir, sidecar, ring=ring, persistent=persistent, attach=attach)
        except BaseException:
            self.close()
            raise

    def __getitem__(self, name: str) -> BlobSidecarReader:
        return self._readers[name]

    def close(self) -> None:
        for reader in self._readers.values():
            reader.close()
        self._readers.clear()


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
