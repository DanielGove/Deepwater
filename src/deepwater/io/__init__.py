from __future__ import annotations

from .persistent_ring import PersistentRingReader
from .reader import ChunkReader
from .ring import RingReader, RingWriter
from .writer import ChunkWriter


def Writer(platform, feed_name: str):
    """
    Unified public writer constructor.

    Deepwater writes always ingress through the ring path; persistence is handled
    by the background persister when the feed lifecycle has persist=true.
    """
    return platform.create_writer(feed_name)


def Reader(platform, feed_name: str):
    """
    Unified public reader constructor.

    For persist=true feeds this fronts the durable chunk reader with ring-aware
    semantics; for persist=false feeds it tails the ring directly.
    """
    return platform.create_reader(feed_name)


__all__ = [
    "Writer",
    "Reader",
    "ChunkWriter",
    "ChunkReader",
    "RingWriter",
    "RingReader",
    "PersistentRingReader",
]
