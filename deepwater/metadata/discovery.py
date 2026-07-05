from __future__ import annotations

from pathlib import Path

from .admin import create_feed as _create_feed
from .admin import delete_feed as _delete_feed
from .datasets import build_manifest, common_intervals, recommend_train_validation, with_duration
from .feed_registry import FeedRegistry, UINT64_MAX
from .feed_metadata import (
    feed_exists as _feed_exists,
    list_feeds as _list_feeds,
    load_feed_metadata_dict,
    load_record_format_dict,
)
from .feed_schema import load_schema
from .global_registry import GlobalRegistry

from ..network.path import parse_target
from ..network.protocol import Op


def _with_remote(base_path, fn):
    target = parse_target(str(base_path))
    if not target.is_remote:
        return None
    from ..network.client import with_remote_metadata

    return with_remote_metadata(target, fn)


def _base(base_path):
    return Path(base_path)


def ensure_base(base_path) -> Path:
    base = _base(base_path)
    data_path = base / "data"
    data_path.mkdir(parents=True, exist_ok=True)
    return base


def feed_dir(base_path, feed_name: str) -> Path:
    return _base(base_path) / "data" / feed_name


def _local_feed_exists(base_path, feed_name: str) -> bool:
    return _feed_exists(_base(base_path), feed_name)


def _local_list_feeds(base_path):
    return _list_feeds(_base(base_path))


def _local_metadata(base_path, feed_name: str) -> dict | None:
    return load_feed_metadata_dict(_base(base_path), feed_name)


def _local_get_record_format(base_path, feed_name: str) -> dict:
    return load_record_format_dict(_base(base_path), feed_name)


def _local_describe_feed(base_path, feed_name: str) -> dict:
    reg = GlobalRegistry(_base(base_path))
    try:
        metadata = reg.get_feed(feed_name)
        if metadata is None:
            raise KeyError(feed_name)
    finally:
        reg.close()
    lay = _local_get_record_format(base_path, feed_name)
    clock_level = int(lay.get("clock_level") or 1)
    fields = list(lay.get("fields", []))
    ts_fields = fields[:clock_level] if fields else []
    ts_offset = ts_fields[0]["offset"] if ts_fields else 0
    return {
        "feed_name": feed_name,
        "metadata": {
            "chunk_size_bytes": metadata.chunk_size_bytes,
            "retention_hours": metadata.retention_hours,
            "persist": metadata.persist,
            "ring_size_bytes": metadata.ring_size_bytes,
            "prefault_ring": metadata.prefault_ring,
            "uses_ring": metadata.uses_ring,
            "storage": "ring" if metadata.uses_ring else "chunk",
        },
        "clock_level": clock_level,
        "timestamp_fields": ts_fields,
        "record_fmt": lay["fmt"],
        "record_size": lay["record_size"],
        "ts_offset": ts_offset,
        "fields": fields,
        "created_us": metadata.created_us,
    }


def _local_feed_intervals(base_path, feed_name: str) -> list[tuple[int, int]]:
    if not _local_feed_exists(base_path, feed_name):
        raise KeyError(feed_name)
    reg_path = feed_dir(base_path, feed_name) / f"{feed_name}.reg"
    if not reg_path.exists():
        return []
    registry = FeedRegistry(reg_path, mode="r")
    try:
        intervals: list[tuple[int, int]] = []
        latest = int(registry.get_latest_chunk_idx() or 0)
        for chunk_id in range(1, latest + 1):
            meta = registry.get_chunk_metadata(chunk_id)
            if meta is None:
                continue
            try:
                qmin = int(meta.get_qmin(0))
                qmax = int(meta.get_qmax(0))
                records = int(meta.num_records)
            finally:
                meta.release()
            if records > 0 and qmin != UINT64_MAX and qmax >= qmin:
                intervals.append((qmin, qmax))
        return intervals
    finally:
        registry.close()


def _local_common_time_windows(
    base_path,
    feed_names: list[str],
    *,
    min_duration_us: int = 0,
    merge_touching: bool = True,
):
    if not feed_names:
        raise ValueError("feed_names cannot be empty")
    interval_sets = []
    for feed_name in feed_names:
        if not _local_feed_exists(base_path, feed_name):
            raise KeyError(feed_name)
        interval_sets.append(_local_feed_intervals(base_path, feed_name))
    return with_duration(
        common_intervals(interval_sets, merge_touching=merge_touching),
        min_duration_us=min_duration_us,
    )


def _local_recommended_train_validation(
    base_path,
    feed_names: list[str],
    *,
    min_duration_us: int = 0,
    train_ratio: float = 0.8,
):
    feeds = list(feed_names)
    intervals = _local_common_time_windows(base_path, feeds, min_duration_us=min_duration_us)
    recommendation = recommend_train_validation(intervals, train_ratio=train_ratio)
    return build_manifest(
        base_path=str(_base(base_path)),
        feeds=feeds,
        source="chunks",
        intervals=intervals,
        recommendation=recommendation,
    )


def codec(base_path, feed_name: str):
    import struct

    lay = load_schema(feed_dir(base_path, feed_name))
    return struct.Struct(lay.fmt), int(lay.primary_ts_offset)


def create_feed(base_path, spec) -> None:
    return _create_feed(_base(base_path), spec)


def delete_feed(base_path, feed_name: str, *, missing_ok: bool = False) -> bool:
    return _delete_feed(_base(base_path), feed_name, missing_ok=missing_ok)


def feed_exists(base_path, feed_name: str) -> bool:
    remote_value = _with_remote(base_path, lambda remote: bool(remote(Op.FEED_EXISTS, feed_name)))
    if remote_value is not None:
        return bool(remote_value)
    return _local_feed_exists(_base(base_path), feed_name)


def list_feeds(base_path):
    remote_value = _with_remote(base_path, lambda remote: remote(Op.LIST_FEEDS))
    if remote_value is not None:
        return remote_value
    return _local_list_feeds(_base(base_path))


def feed_metadata(base_path, feed_name: str) -> dict:
    remote_value = _with_remote(base_path, lambda remote: remote(Op.FEED_METADATA, feed_name))
    if remote_value is not None:
        return remote_value
    return _local_metadata(_base(base_path), feed_name)


def get_record_format(base_path, feed_name: str) -> dict:
    remote_value = _with_remote(base_path, lambda remote: remote(Op.RECORD_FORMAT, feed_name))
    if remote_value is not None:
        return remote_value
    return _local_get_record_format(_base(base_path), feed_name)


def describe_feed(base_path, feed_name: str) -> dict:
    remote_value = _with_remote(base_path, lambda remote: remote(Op.DESCRIBE_FEED, feed_name))
    if remote_value is not None:
        return remote_value
    return _local_describe_feed(_base(base_path), feed_name)


def feed_intervals(base_path, feed_name: str):
    remote_value = _with_remote(base_path, lambda remote: remote(Op.FEED_INTERVALS, feed_name))
    if remote_value is not None:
        return [tuple(interval) for interval in remote_value]
    return _local_feed_intervals(_base(base_path), feed_name)


def common_time_windows(base_path, feed_names, *, min_duration_us: int = 0, merge_touching: bool = True):
    remote_value = _with_remote(
        base_path,
        lambda remote: _remote_common_time_windows(
            remote,
            list(feed_names),
            min_duration_us=min_duration_us,
            merge_touching=merge_touching,
        )
    )
    if remote_value is not None:
        return remote_value
    return _local_common_time_windows(
        _base(base_path),
        list(feed_names),
        min_duration_us=min_duration_us,
        merge_touching=merge_touching,
    )


def recommended_train_validation(base_path, feed_names, *, min_duration_us: int = 0, train_ratio: float = 0.8):
    remote_value = _with_remote(
        base_path,
        lambda remote: _remote_recommended_train_validation(
            str(base_path),
            remote,
            list(feed_names),
            min_duration_us=min_duration_us,
            train_ratio=train_ratio,
        ),
    )
    if remote_value is not None:
        return remote_value
    return _local_recommended_train_validation(
        _base(base_path),
        list(feed_names),
        min_duration_us=min_duration_us,
        train_ratio=train_ratio,
    )


def _remote_common_time_windows(
    remote,
    feed_names: list[str],
    *,
    min_duration_us: int = 0,
    merge_touching: bool = True,
):
    if not feed_names:
        raise ValueError("feed_names cannot be empty")
    interval_sets = []
    for feed_name in feed_names:
        if not bool(remote(Op.FEED_EXISTS, feed_name)):
            raise KeyError(feed_name)
        interval_sets.append([tuple(interval) for interval in remote(Op.FEED_INTERVALS, feed_name)])
    return with_duration(
        common_intervals(interval_sets, merge_touching=merge_touching),
        min_duration_us=int(min_duration_us),
    )


def _remote_recommended_train_validation(
    base_path: str,
    remote,
    feed_names: list[str],
    *,
    min_duration_us: int = 0,
    train_ratio: float = 0.8,
):
    intervals = _remote_common_time_windows(
        remote,
        feed_names,
        min_duration_us=min_duration_us,
    )
    recommendation = recommend_train_validation(intervals, train_ratio=train_ratio)
    return build_manifest(
        base_path=base_path,
        feeds=list(feed_names),
        source="chunks",
        intervals=intervals,
        recommendation=recommendation,
    )
