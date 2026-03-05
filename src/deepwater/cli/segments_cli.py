#!/usr/bin/env python3
"""CLI for querying Deepwater segment metadata."""
from __future__ import annotations

import argparse
import sys

import orjson

from ..platform import Platform
from .timefmt import (
    add_ts_fmt_arg,
    field_label,
    format_timestamp_us,
    parse_ts_fmt,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Query Deepwater feed segments")
    parser.add_argument("--base-path", required=True, help="Deepwater base path")
    parser.add_argument("--feed", required=True, help="Feed name")
    parser.add_argument(
        "--status",
        default="all",
        help="Segment status filter: all/open/closed/crash_closed/invalid_empty/usable",
    )
    parser.add_argument(
        "--suggest-range",
        action="store_true",
        help="Print suggested reader range from usable segments",
    )
    add_ts_fmt_arg(parser)
    parser.add_argument("--json", action="store_true", help="Output JSON")
    args = parser.parse_args(argv)

    try:
        ts_fmt = parse_ts_fmt(args.ts_fmt)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    p = Platform(args.base_path)
    try:
        try:
            segments = p.list_segments(args.feed, status=args.status)
        except KeyError:
            print(f"ERROR: feed '{args.feed}' not found", file=sys.stderr)
            return 1

        suggested = p.suggested_reader_range(args.feed)

        if args.json:
            payload = {
                "feed": args.feed,
                "status_filter": args.status,
                "segments": segments,
                "suggested_range": (
                    {"start_us": suggested[0], "end_us": suggested[1]}
                    if suggested is not None
                    else None
                ),
            }
            print(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode("utf-8"))
            return 0

        start_label = field_label("start", ts_fmt)
        end_label = field_label("end", ts_fmt)
        print(f"feed={args.feed} status_filter={args.status} segments={len(segments)}")
        for s in segments:
            print(
                "  "
                f"id={s.get('id')} "
                f"status={s.get('status')} "
                f"{start_label}={format_timestamp_us(s.get('start_us'), ts_fmt)} "
                f"{end_label}={format_timestamp_us(s.get('end_us'), ts_fmt)} "
                f"records={s.get('records')} "
                f"reason={s.get('close_reason')}"
            )

        if args.suggest_range:
            if suggested is None:
                print("suggested_range=None")
            else:
                suggested_start_label = field_label("suggested_range_start", ts_fmt)
                suggested_end_label = field_label("suggested_range_end", ts_fmt)
                print(
                    f"{suggested_start_label}={format_timestamp_us(suggested[0], ts_fmt)} "
                    f"{suggested_end_label}={format_timestamp_us(suggested[1], ts_fmt)}"
                )
        return 0
    finally:
        p.close()


if __name__ == "__main__":
    raise SystemExit(main())
