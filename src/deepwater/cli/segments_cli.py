#!/usr/bin/env python3
"""CLI for querying Deepwater segment metadata."""
from __future__ import annotations

import argparse
import sys

import orjson

from ..platform import Platform


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

        print(f"feed={args.feed} status_filter={args.status} segments={len(segments)}")
        for s in segments:
            print(
                "  "
                f"id={s.get('id')} "
                f"status={s.get('status')} "
                f"start_us={s.get('start_us')} "
                f"end_us={s.get('end_us')} "
                f"records={s.get('records')} "
                f"reason={s.get('close_reason')}"
            )

        if args.suggest_range:
            if suggested is None:
                print("suggested_range=None")
            else:
                print(f"suggested_range_start_us={suggested[0]} suggested_range_end_us={suggested[1]}")
        return 0
    finally:
        p.close()


if __name__ == "__main__":
    raise SystemExit(main())
