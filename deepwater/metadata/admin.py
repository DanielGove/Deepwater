from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import msgspec
import orjson

from .global_registry import GlobalRegistry
from .feed_schema import build_schema, save_schema
from ..io.ring import normalize_ring_data_size
from .sidecar import BlobSidecarSpec, build_sidecars, save_sidecars

__all__ = ["BlobSidecarSpec", "FieldSpec", "FeedSpec", "create_feed", "delete_feed"]


def _base(base_path) -> Path:
    return Path(base_path)


def feed_dir(base, feed_name: str) -> Path:
    return _base(base) / "data" / feed_name


class FieldSpec(msgspec.Struct, frozen=True, array_like=True):
    name: str
    type: str


class FeedSpec(msgspec.Struct, frozen=True, array_like=True):
    feed_name: str
    fields: tuple[FieldSpec, ...]
    clock_level: int
    mode: str = "UF"
    persist: bool = True
    storage: str | None = None
    uses_ring: bool | None = None
    chunk_size_mb: float = 64.0
    ring_size_mb: float | None = None
    retention_hours: int = 0
    segment_tracking: bool = True
    prefault_ring: bool = True
    aligned: bool = False
    sidecars: tuple[BlobSidecarSpec, ...] = ()


def _field_spec(value) -> FieldSpec:
    if isinstance(value, FieldSpec):
        return value
    if isinstance(value, Mapping):
        return FieldSpec(str(value["name"]), str(value["type"]))
    if isinstance(value, (tuple, list)) and len(value) == 2:
        name, typ = value
        return FieldSpec(str(name), str(typ))
    raise TypeError(f"invalid field spec: {value!r}")


def _sidecar_spec(value) -> BlobSidecarSpec:
    if isinstance(value, BlobSidecarSpec):
        return value
    if isinstance(value, Mapping):
        return BlobSidecarSpec(
            name=str(value["name"]),
            codec=str(value.get("codec", "raw")),
            chunk_size_mb=float(value.get("chunk_size_mb", 64.0)),
            retention=str(value.get("retention", "follows_parent")),
            storage=str(value.get("storage", "blob_chunks")),
        )
    raise TypeError(f"invalid blob sidecar spec: {value!r}")


def feed_spec_from_mapping(spec: FeedSpec | Mapping[str, Any]) -> FeedSpec:
    if isinstance(spec, FeedSpec):
        return spec
    if not isinstance(spec, Mapping):
        raise TypeError("feed spec must be FeedSpec or mapping")
    return FeedSpec(
        feed_name=str(spec["feed_name"]),
        fields=tuple(_field_spec(field) for field in spec["fields"]),
        clock_level=int(spec["clock_level"]),
        mode=str(spec.get("mode", "UF")),
        persist=bool(spec.get("persist", True)),
        storage=None if spec.get("storage") is None else str(spec["storage"]),
        uses_ring=None if "uses_ring" not in spec else bool(spec["uses_ring"]),
        chunk_size_mb=float(spec.get("chunk_size_mb", 64.0)),
        ring_size_mb=None if spec.get("ring_size_mb") is None else float(spec["ring_size_mb"]),
        retention_hours=int(spec.get("retention_hours", 0)),
        segment_tracking=bool(spec.get("segment_tracking", True)),
        prefault_ring=bool(spec.get("prefault_ring", True)),
        aligned=bool(spec.get("aligned", False)),
        sidecars=tuple(_sidecar_spec(sidecar) for sidecar in spec.get("sidecars", ())),
    )


def _resolve_uses_ring(spec: FeedSpec) -> bool:
    storage = spec.storage
    if storage is not None and storage not in ("ring", "chunk"):
        raise ValueError("storage must be 'ring' or 'chunk'")
    if spec.uses_ring is not None:
        uses_ring = bool(spec.uses_ring)
        if storage is not None and uses_ring != (storage == "ring"):
            raise ValueError("uses_ring contradicts storage")
    elif storage is not None:
        uses_ring = storage == "ring"
    else:
        uses_ring = not spec.persist
    if not uses_ring and not spec.persist:
        raise ValueError("feed must be persistent when uses_ring=false")
    return uses_ring


def _mb_to_bytes(value, *, default: float) -> int:
    mb = float(default if value is None else value)
    if mb <= 0:
        raise ValueError("size in MB must be > 0")
    return max(1, int(mb * 1024 * 1024))


def ensure_base(base_path) -> Path:
    base = _base(base_path)
    data_path = base / "data"
    data_path.mkdir(parents=True, exist_ok=True)
    return base


def create_feed(base_path, spec: FeedSpec | Mapping[str, Any]) -> None:
    spec = feed_spec_from_mapping(spec)
    if spec.mode != "UF":
        raise ValueError("only UF feeds are supported")

    base = ensure_base(base_path)
    registry = GlobalRegistry(base)
    try:
        name = spec.feed_name
        if registry.feed_exists(name):
            return

        fdir = feed_dir(base, name)
        fdir.mkdir(parents=True, exist_ok=False)

        persist = spec.persist
        uses_ring = _resolve_uses_ring(spec)

        sidecars = build_sidecars(spec.sidecars)
        fields = list(spec.fields)
        for sidecar in sidecars:
            ref = sidecar.ref_fields
            fields.extend(
                (
                    FieldSpec(ref.chunk_id, "uint64"),
                    FieldSpec(ref.offset, "uint64"),
                    FieldSpec(ref.size, "uint64"),
                    FieldSpec(ref.codec, "uint64"),
                    FieldSpec(ref.schema_id, "uint64"),
                    FieldSpec(ref.flags, "uint64"),
                    FieldSpec(ref.crc, "uint64"),
                )
            )

        schema = build_schema(
            tuple((field.name, field.type) for field in fields),
            clock_level=spec.clock_level,
            aligned=spec.aligned,
        )

        chunk_size_mb = float(spec.chunk_size_mb)
        chunk_size_bytes = _mb_to_bytes(chunk_size_mb, default=64)
        if not uses_ring:
            ring_size_mb = 0.0
            ring_size_bytes = 0
        elif spec.ring_size_mb is not None:
            ring_size_mb = float(spec.ring_size_mb)
            ring_size_bytes = _mb_to_bytes(ring_size_mb, default=ring_size_mb)
        elif persist:
            ring_size_mb = max(8.0, chunk_size_mb * 8.0)
            ring_size_bytes = _mb_to_bytes(ring_size_mb, default=ring_size_mb)
        else:
            ring_size_mb = chunk_size_mb
            ring_size_bytes = _mb_to_bytes(ring_size_mb, default=ring_size_mb)
        if uses_ring:
            ring_size_bytes = normalize_ring_data_size(ring_size_bytes, schema.record_size)
            ring_size_mb = ring_size_bytes / (1024 * 1024)
        save_schema(fdir, schema)
        save_sidecars(fdir, sidecars)

        feed_metadata = {
            "chunk_size_bytes": chunk_size_bytes,
            "retention_hours": spec.retention_hours,
            "persist": persist,
            "ring_size_bytes": ring_size_bytes,
            "segment_tracking": spec.segment_tracking,
            "prefault_ring": spec.prefault_ring,
            "uses_ring": uses_ring,
        }

        config = {
            "feed_name": name,
            "mode": spec.mode,
            "fields": [{"name": field.name, "type": field.type} for field in fields],
            "clock_level": spec.clock_level,
            "persist": persist,
            "chunk_size_mb": chunk_size_mb,
            "ring_size_mb": ring_size_mb,
            "ring_size_bytes": ring_size_bytes,
            "retention_hours": spec.retention_hours,
            "storage": "ring" if uses_ring else "chunk",
            "uses_ring": uses_ring,
            "segment_tracking": spec.segment_tracking,
            "prefault_ring": spec.prefault_ring,
            "aligned": spec.aligned,
            "sidecars": [
                {
                    "name": sidecar.name,
                    "codec": sidecar.codec,
                    "chunk_size_mb": sidecar.chunk_size_mb,
                    "retention": sidecar.retention,
                    "storage": sidecar.storage,
                }
                for sidecar in sidecars
            ],
        }
        (fdir / "config.json").write_bytes(orjson.dumps(config))

        registry.register_feed(name, feed_metadata, clock_level=schema.clock_level)
    finally:
        registry.close()


def delete_feed(base_path, feed_name: str, *, missing_ok: bool = False) -> bool:
    import fcntl
    import os
    import shutil

    from ..io.ring import RingBuffer

    base = _base(base_path)
    registry = GlobalRegistry(base)
    try:
        exists = registry.feed_exists(feed_name)
        fdir = feed_dir(base, feed_name)
        on_disk = fdir.exists()
        if not exists and not on_disk:
            if missing_ok:
                return False
            raise KeyError(feed_name)

        lock_path = fdir / f"{feed_name}.writer.lock"
        if lock_path.exists():
            fd = os.open(lock_path, os.O_RDWR)
            try:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise RuntimeError(f"active writer for '{feed_name}'")
                else:
                    fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

        if fdir.exists():
            shutil.rmtree(fdir)

        RingBuffer.unlink_for_feed(base, feed_name)

        removed = registry.unregister_feed(feed_name)
        return bool(on_disk or removed)
    finally:
        registry.close()
