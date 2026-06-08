from __future__ import annotations

from pathlib import Path

from ..metadata.feed_metadata import load_feed_metadata
from .reader import Reader
from .writer import ChunkWriter, RingWriter


def _base_path(base_path) -> Path:
    return Path(base_path)


def Writer(base_path, feed_name: str):
    """
    Unified public writer constructor.

    Ring-backed feeds write into shared memory first. If those feeds are also
    persistent, the background persister drains the ring to chunks. Non-ring
    persistent feeds write chunks directly.
    """
    base = _base_path(base_path)
    feed_metadata = load_feed_metadata(base, feed_name)
    if feed_metadata is None:
        raise KeyError(feed_name)
    if bool(feed_metadata.uses_ring):
        return RingWriter(base, feed_name)
    return ChunkWriter(base, feed_name)


def ensure_base_path(base_path):
    """Public helper for modules that need a normalized base path."""
    return _base_path(base_path)


__all__ = [
    "Writer",
    "Reader",
    "ensure_base_path",
    "ChunkWriter",
    "RingWriter",
]
