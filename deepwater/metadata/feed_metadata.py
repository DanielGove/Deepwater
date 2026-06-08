from __future__ import annotations

from pathlib import Path

from .global_registry import FeedMetadata, GlobalRegistry
from .feed_schema import load_record_schema_for_feed, schema_to_dict


def base_path(path) -> Path:
    return Path(path)


def feed_dir(base, feed_name: str) -> Path:
    return base_path(base) / "data" / feed_name


def feed_exists(base, feed_name: str) -> bool:
    registry = GlobalRegistry(base_path(base))
    try:
        return registry.feed_exists(feed_name)
    finally:
        registry.close()


def list_feeds(base) -> list[str]:
    registry = GlobalRegistry(base_path(base))
    try:
        return registry.list_feeds()
    finally:
        registry.close()


def load_feed_metadata(base, feed_name: str) -> FeedMetadata | None:
    registry = GlobalRegistry(base_path(base))
    try:
        return registry.get_feed(feed_name)
    finally:
        registry.close()


def load_feed_metadata_dict(base, feed_name: str) -> dict | None:
    metadata = load_feed_metadata(base, feed_name)
    return None if metadata is None else metadata.to_dict()


def load_record_format_dict(base, feed_name: str) -> dict:
    return schema_to_dict(load_record_schema_for_feed(base, feed_name))
