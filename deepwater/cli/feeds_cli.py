#!/usr/bin/env python3
"""CLI for querying Deepwater feed metadata."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import orjson

from ..metadata.discovery import describe_feed, list_feeds
from .timefmt import TimestampFormat, add_ts_fmt_arg, format_timestamp_us, parse_ts_fmt


def _unique(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        name = item.strip()
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _print_feed(desc: dict, ts_fmt: TimestampFormat) -> None:
    metadata = desc.get("metadata", {})
    print(f"feed={desc.get('feed_name')}")
    print(
        "  "
        f"persist={metadata.get('persist')} "
        f"chunk_size_bytes={metadata.get('chunk_size_bytes')} "
        f"ring_size_bytes={metadata.get('ring_size_bytes')} "
        f"retention_hours={metadata.get('retention_hours')} "
        f"segment_tracking={metadata.get('segment_tracking')}"
    )
    print(
        "  "
        f"record_fmt={desc.get('record_fmt')} "
        f"record_size={desc.get('record_size')} "
        f"ts_offset={desc.get('ts_offset')}"
    )
    if ts_fmt.epoch:
        print(f"  created_us={desc.get('created_us')}")
    else:
        print(f"  created={format_timestamp_us(desc.get('created_us'), ts_fmt)}")
    print("  fields:")
    for f in desc.get("fields", []):
        print(
            "    "
            f"{f.get('name')}:{f.get('type')} "
            f"offset={f.get('offset')} size={f.get('size')}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Query Deepwater feed metadata")
    parser.add_argument("--base-path", required=True, help="Deepwater base path")
    parser.add_argument(
        "--feed",
        action="append",
        default=[],
        help="Describe one feed (repeatable)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Describe all feeds (full metadata)",
    )
    add_ts_fmt_arg(parser)
    parser.add_argument("--json", action="store_true", help="Output JSON")
    args = parser.parse_args(argv)

    try:
        ts_fmt = parse_ts_fmt(args.ts_fmt)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    feed_names = _unique(args.feed)
    if args.all and feed_names:
        print("ERROR: use either --all or --feed (not both)", file=sys.stderr)
        return 2

    base_path = str(Path(args.base_path))

    if not args.all and not feed_names:
        names = list_feeds(args.base_path)
        if args.json:
            payload = {"base_path": base_path, "feeds": names}
            print(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode("utf-8"))
        else:
            print(f"base_path={base_path} feeds={len(names)}")
            for name in names:
                print(f"  {name}")
        return 0

    if args.all:
        feed_names = list_feeds(args.base_path)

    described: list[dict] = []
    for name in feed_names:
        try:
            described.append(describe_feed(args.base_path, name))
        except KeyError:
            print(f"ERROR: feed '{name}' not found", file=sys.stderr)
            return 1

    if args.json:
        if len(described) == 1:
            print(orjson.dumps(described[0], option=orjson.OPT_INDENT_2).decode("utf-8"))
        else:
            payload = {"base_path": base_path, "feeds": described}
            print(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode("utf-8"))
        return 0

    print(f"base_path={base_path} feeds={len(described)}")
    for i, desc in enumerate(described):
        if i:
            print()
        _print_feed(desc, ts_fmt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
