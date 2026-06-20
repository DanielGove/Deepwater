from __future__ import annotations

from .reader import Reader
from .writer import ChunkWriter, RingWriter, Writer


def ensure_base_path(base_path):
    """Public helper for modules that need a normalized base path."""
    from pathlib import Path

    return Path(base_path)


__all__ = [
    "Writer",
    "Reader",
    "ensure_base_path",
    "ChunkWriter",
    "RingWriter",
]
