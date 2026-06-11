"""Typed metadata for blob sidecars attached to fixed-row feeds."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import msgspec

SIDECARS_FILENAME = "sidecars.msgpack"


class BlobSidecarSpec(msgspec.Struct, frozen=True, array_like=True):
    name: str
    codec: str = "raw"
    chunk_size_mb: float = 64.0
    retention: str = "follows_parent"
    storage: str = "blob_chunks"


class SidecarRefFields(msgspec.Struct, frozen=True, array_like=True):
    chunk_id: str
    offset: str
    size: str
    codec: str
    schema_id: str
    flags: str
    crc: str


class BlobSidecar(msgspec.Struct, frozen=True, array_like=True):
    name: str
    codec: str
    chunk_size_mb: float
    chunk_size_bytes: int
    retention: str
    storage: str
    ref_fields: SidecarRefFields


class _SidecarFile(msgspec.Struct, frozen=True, array_like=True):
    version: int
    sidecars: tuple[BlobSidecar, ...]


class Sidecars:
    __slots__ = ("_items", "_by_name", "names")

    def __init__(self, items: tuple[BlobSidecar, ...] = ()):
        self._items = items
        self._by_name = {item.name: item for item in items}
        self.names = tuple(item.name for item in items)

    def __bool__(self) -> bool:
        return bool(self._items)

    def __iter__(self) -> Iterator[BlobSidecar]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, name: str) -> BlobSidecar:
        return self._by_name[name]


def _base(base_path) -> Path:
    return Path(base_path)


def feed_dir(base_path, feed_name: str) -> Path:
    return _base(base_path) / "data" / feed_name


def sidecar_dir(feed_directory: Path | str, sidecar_name: str) -> Path:
    return Path(feed_directory) / "blobs" / sidecar_name


def _chunk_size_bytes(chunk_size_mb: float) -> int:
    if chunk_size_mb <= 0:
        raise ValueError("blob sidecar chunk_size_mb must be > 0")
    return max(1, int(float(chunk_size_mb) * 1024 * 1024))


def _ref_fields(name: str) -> SidecarRefFields:
    prefix = f"{name}_"
    return SidecarRefFields(
        chunk_id=f"{prefix}chunk_id",
        offset=f"{prefix}offset",
        size=f"{prefix}size",
        codec=f"{prefix}codec",
        schema_id=f"{prefix}schema_id",
        flags=f"{prefix}flags",
        crc=f"{prefix}crc",
    )


def build_sidecars(specs: tuple[BlobSidecarSpec, ...]) -> Sidecars:
    names: set[str] = set()
    items: list[BlobSidecar] = []
    for spec in specs:
        if not spec.name:
            raise ValueError("blob sidecar name is required")
        if spec.name in names:
            raise ValueError(f"duplicate blob sidecar: {spec.name!r}")
        if spec.storage != "blob_chunks":
            raise ValueError("blob sidecar storage must be 'blob_chunks'")
        names.add(spec.name)
        items.append(
            BlobSidecar(
                name=spec.name,
                codec=spec.codec,
                chunk_size_mb=float(spec.chunk_size_mb),
                chunk_size_bytes=_chunk_size_bytes(spec.chunk_size_mb),
                retention=spec.retention,
                storage=spec.storage,
                ref_fields=_ref_fields(spec.name),
            )
        )
    return Sidecars(tuple(items))


def save_sidecars(feed_directory: Path | str, sidecars: Sidecars) -> None:
    feed_directory = Path(feed_directory)
    if not sidecars:
        return
    for sidecar in sidecars:
        sidecar_dir(feed_directory, sidecar.name).mkdir(parents=True, exist_ok=True)
    path = feed_directory / SIDECARS_FILENAME
    tmp = path.with_suffix(".msgpack.tmp")
    tmp.write_bytes(msgspec.msgpack.encode(_SidecarFile(version=1, sidecars=tuple(sidecars))))
    tmp.replace(path)


def load_sidecars(base_path: Path | str, feed_name: str) -> Sidecars:
    path = feed_dir(base_path, feed_name) / SIDECARS_FILENAME
    if not path.exists():
        return Sidecars()
    data = msgspec.msgpack.decode(path.read_bytes(), type=_SidecarFile)
    return Sidecars(data.sidecars)
