from ...metadata.feed_metadata import load_feed_metadata
from .writer import ChunkWriter, RingWriter


def Writer(base_path, feed_name: str):
    """
    Unified public writer constructor.

    Ring-backed feeds write into shared memory first. If those feeds are also
    persistent, the background persister drains the ring to chunks. Non-ring
    persistent feeds write chunks directly.
    """
    from pathlib import Path

    base = Path(base_path)
    feed_metadata = load_feed_metadata(base, feed_name)
    if feed_metadata is None:
        raise KeyError(feed_name)
    if bool(feed_metadata.uses_ring):
        return RingWriter(base, feed_name)
    return ChunkWriter(base, feed_name)


__all__ = ["Writer", "ChunkWriter", "RingWriter"]
