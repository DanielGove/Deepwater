"""Deepwater public API."""

from __future__ import annotations

__version__ = "0.0.1"

from .blob import BlobReferenceWriter, BlobStore, create_blob_feed, read_blob_ref
from .io import Reader, Writer
from .metadata.catalog import catalog, feed_coverage
from .metadata.discovery import (
    codec,
    common_time_windows,
    create_feed,
    delete_feed,
    describe_feed,
    feed_metadata,
    feed_dir,
    feed_exists,
    get_record_format,
    list_feeds,
    list_segments,
    recommended_train_validation,
    suggested_reader_range,
)


__all__ = [
    "BlobReferenceWriter",
    "BlobStore",
    "Reader",
    "Writer",
    "catalog",
    "codec",
    "common_time_windows",
    "create_blob_feed",
    "create_feed",
    "delete_feed",
    "describe_feed",
    "feed_metadata",
    "feed_dir",
    "feed_exists",
    "feed_coverage",
    "get_record_format",
    "list_feeds",
    "list_segments",
    "recommended_train_validation",
    "read_blob_ref",
    "suggested_reader_range",
]
