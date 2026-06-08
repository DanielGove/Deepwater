from __future__ import annotations

from pathlib import Path

import orjson

from .global_registry import GlobalRegistry
from .feed_schema import build_schema, save_schema


def _base(base_path) -> Path:
    return Path(base_path)


def feed_dir(base, feed_name: str) -> Path:
    return _base(base) / "data" / feed_name


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


def create_feed(base_path, spec: dict) -> None:
    base = ensure_base(base_path)
    registry = GlobalRegistry(base)
    try:
        name = spec["feed_name"]
        if registry.feed_exists(name):
            return

        fdir = feed_dir(base, name)
        fdir.mkdir(parents=True, exist_ok=False)

        persist = bool(spec.get("persist", True))
        storage = spec.get("storage")
        if storage is not None:
            storage = str(storage)
            if storage not in ("ring", "chunk"):
                raise ValueError("storage must be 'ring' or 'chunk'")
        if "uses_ring" in spec:
            uses_ring = bool(spec["uses_ring"])
            if storage is not None and uses_ring != (storage == "ring"):
                raise ValueError("uses_ring contradicts storage")
        elif storage is not None:
            uses_ring = storage == "ring"
        else:
            uses_ring = not persist
        if not uses_ring and not persist:
            raise ValueError("feed must be persistent when uses_ring=false")

        chunk_size_mb = float(spec.get("chunk_size_mb", 64))
        chunk_size_bytes = _mb_to_bytes(chunk_size_mb, default=64)
        if not uses_ring:
            ring_size_mb = 0.0
            ring_size_bytes = 0
        elif "ring_size_mb" in spec:
            ring_size_mb = float(spec["ring_size_mb"])
            ring_size_bytes = _mb_to_bytes(ring_size_mb, default=ring_size_mb)
        elif persist:
            ring_size_mb = max(8.0, chunk_size_mb * 8.0)
            ring_size_bytes = _mb_to_bytes(ring_size_mb, default=ring_size_mb)
        else:
            ring_size_mb = chunk_size_mb
            ring_size_bytes = _mb_to_bytes(ring_size_mb, default=ring_size_mb)

        schema = build_schema(
            tuple((field["name"], field["type"]) for field in spec["fields"]),
            clock_level=spec.get("clock_level"),
        )
        save_schema(fdir, schema)

        feed_metadata = {
            "chunk_size_bytes": chunk_size_bytes,
            "ring_size_bytes": ring_size_bytes,
            "retention_hours": int(spec.get("retention_hours", 0)),
            "persist": persist,
            "segment_tracking": bool(spec.get("segment_tracking", True)),
            "prefault_ring": bool(spec.get("prefault_ring", True)),
            "uses_ring": uses_ring,
        }

        config = dict(spec)
        config["persist"] = persist
        config["chunk_size_mb"] = chunk_size_mb
        config["ring_size_mb"] = ring_size_mb
        config["storage"] = "ring" if uses_ring else "chunk"
        config["uses_ring"] = uses_ring
        config["segment_tracking"] = bool(spec.get("segment_tracking", True))
        config["prefault_ring"] = bool(spec.get("prefault_ring", True))
        (fdir / "config.json").write_bytes(orjson.dumps(config))

        registry.register_feed(name, feed_metadata, clock_level=schema.clock_level)
    finally:
        registry.close()


def delete_feed(base_path, feed_name: str, *, missing_ok: bool = False) -> bool:
    import fcntl
    import os
    import shutil
    from multiprocessing import shared_memory

    from ..io.ring import ring_buffer_shm_names

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

        for shm_name in ring_buffer_shm_names(base, feed_name):
            try:
                shm = shared_memory.SharedMemory(name=shm_name, create=False)
            except FileNotFoundError:
                continue
            try:
                shm.unlink()
            except FileNotFoundError:
                pass
            finally:
                shm.close()

        removed = registry.unregister_feed(feed_name)
        return bool(on_disk or removed)
    finally:
        registry.close()
