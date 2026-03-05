#!/usr/bin/env python3
"""CLI for querying Deepwater segment metadata."""
from __future__ import annotations

import argparse
import sys

import orjson

from ..platform import Platform
from .timefmt import add_timestamp_format_arg, field_label, format_timestamp_us


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
    add_timestamp_format_arg(parser)
    parser.add_argument("--json", action="store_true", help="Output JSON")
    args = parser.parse_args(argv)

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

        start_label = field_label("start", args.timestamp_format)
        end_label = field_label("end", args.timestamp_format)
        print(f"feed={args.feed} status_filter={args.status} segments={len(segments)}")
        for s in segments:
            print(
                "  "
                f"id={s.get('id')} "
                f"status={s.get('status')} "
                f"{start_label}={format_timestamp_us(s.get('start_us'), args.timestamp_format)} "
                f"{end_label}={format_timestamp_us(s.get('end_us'), args.timestamp_format)} "
                f"records={s.get('records')} "
                f"reason={s.get('close_reason')}"
            )

        if args.suggest_range:
            if suggested is None:
                print("suggested_range=None")
            else:
                suggested_start_label = field_label("suggested_range_start", args.timestamp_format)
                suggested_end_label = field_label("suggested_range_end", args.timestamp_format)
                print(
                    f"{suggested_start_label}={format_timestamp_us(suggested[0], args.timestamp_format)} "
                    f"{suggested_end_label}={format_timestamp_us(suggested[1], args.timestamp_format)}"
                )
        return 0
    finally:
        p.close()


if __name__ == "__main__":
    raise SystemExit(main())
