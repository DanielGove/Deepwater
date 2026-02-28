#!/usr/bin/env python3
"""CLI for multi-feed contiguous dataset planning."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import orjson

from .platform import Platform
from .datasets import common_intervals, with_duration, recommend_train_validation, build_multi_manifest


def _collect_feeds(feed_args: list[str], feeds_arg: str | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for name in feed_args:
        nm = name.strip()
        if nm and nm not in seen:
            seen.add(nm)
            out.append(nm)

    if feeds_arg:
        for name in feeds_arg.split(","):
            nm = name.strip()
            if nm and nm not in seen:
                seen.add(nm)
                out.append(nm)

    return out


def _collect_feed_refs(feed_ref_args: list[str], feed_refs_arg: str | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for ref in feed_ref_args:
        r = ref.strip()
        if r and r not in seen:
            seen.add(r)
            out.append(r)

    if feed_refs_arg:
        for ref in feed_refs_arg.split(","):
            r = ref.strip()
            if r and r not in seen:
                seen.add(r)
                out.append(r)
    return out


def _parse_sources(source_args: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in source_args:
        raw = item.strip()
        if not raw:
            continue
        if "=" not in raw:
            raise ValueError(f"Invalid --source '{raw}', expected alias=/path/to/base")
        alias, path = raw.split("=", 1)
        alias = alias.strip()
        path = path.strip()
        if not alias or not path:
            raise ValueError(f"Invalid --source '{raw}', expected alias=/path/to/base")
        if alias in out and out[alias] != path:
            raise ValueError(f"Duplicate alias '{alias}' with different paths")
        out[alias] = path
    return out


def _write_manifest(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compute contiguous common windows across feeds")
    parser.add_argument("--base-path", help="Deepwater base path (single-source mode)")
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Multi-source mode: alias=/path/to/base (repeatable)",
    )
    parser.add_argument("--feed", action="append", default=[], help="Feed name (repeatable)")
    parser.add_argument("--feeds", help="Comma-separated feed names")
    parser.add_argument(
        "--feed-ref",
        action="append",
        default=[],
        help="Multi-source mode feed ref alias:feed (repeatable)",
    )
    parser.add_argument(
        "--feed-refs",
        help="Multi-source mode comma-separated alias:feed refs",
    )
    parser.add_argument(
        "--status",
        default="usable",
        help="Segment status filter passed to platform (default: usable)",
    )
    parser.add_argument(
        "--min-duration-seconds",
        type=float,
        default=0.0,
        help="Filter out windows shorter than this duration",
    )
    parser.add_argument(
        "--train-ratio",
        type=float,
        default=0.8,
        help="Train ratio for recommended split on longest common window",
    )
    parser.add_argument(
        "--show-top",
        type=int,
        default=20,
        help="How many intervals to print in text mode",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON manifest")
    parser.add_argument("--out", help="Write JSON manifest to file path")
    args = parser.parse_args(argv)

    min_duration_us = int(args.min_duration_seconds * 1_000_000)
    try:
        sources = _parse_sources(args.source)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    manifest: dict
    display_name: str
    display_count: int

    if sources:
        feed_refs = _collect_feed_refs(args.feed_ref, args.feed_refs)
        if not feed_refs:
            print("ERROR: multi-source mode requires --feed-ref and/or --feed-refs", file=sys.stderr)
            return 2

        interval_sets: list[list[tuple[int, int]]] = []
        platforms: dict[str, Platform] = {}
        try:
            for alias, path in sources.items():
                platforms[alias] = Platform(path)

            for ref in feed_refs:
                if ":" not in ref:
                    print(f"ERROR: invalid feed ref '{ref}', expected alias:feed", file=sys.stderr)
                    return 2
                alias, feed = ref.split(":", 1)
                alias = alias.strip()
                feed = feed.strip()
                if not alias or not feed:
                    print(f"ERROR: invalid feed ref '{ref}', expected alias:feed", file=sys.stderr)
                    return 2
                if alias not in platforms:
                    print(f"ERROR: unknown source alias '{alias}' for feed ref '{ref}'", file=sys.stderr)
                    return 2

                p = platforms[alias]
                if not p.feed_exists(feed):
                    print(f"ERROR: feed not found: {alias}:{feed}", file=sys.stderr)
                    return 1
                segs = p.list_segments(feed, status=args.status)
                intervals = []
                for seg in segs:
                    s = seg.get("start_us")
                    e = seg.get("end_us")
                    if s is None or e is None:
                        continue
                    s_i = int(s)
                    e_i = int(e)
                    if s_i <= e_i:
                        intervals.append((s_i, e_i))
                interval_sets.append(intervals)

            commons = common_intervals(interval_sets, merge_touching=True)
            intervals = with_duration(commons, min_duration_us=min_duration_us)
            recommendation = recommend_train_validation(intervals, train_ratio=float(args.train_ratio))
            manifest = build_multi_manifest(
                sources=sources,
                feed_refs=feed_refs,
                status_filter=args.status,
                intervals=intervals,
                recommendation=recommendation,
            )
            display_name = "feed_refs"
            display_count = len(feed_refs)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 2
        finally:
            for p in platforms.values():
                p.close()
    else:
        if not args.base_path:
            print("ERROR: provide --base-path for single-source mode", file=sys.stderr)
            return 2
        feeds = _collect_feeds(args.feed, args.feeds)
        if not feeds:
            print("ERROR: provide --feed and/or --feeds", file=sys.stderr)
            return 2

        p = Platform(args.base_path)
        try:
            try:
                manifest = p.recommended_train_validation(
                    feeds,
                    status=args.status,
                    train_ratio=float(args.train_ratio),
                    min_duration_us=min_duration_us,
                )
            except KeyError as e:
                print(f"ERROR: feed not found: {e}", file=sys.stderr)
                return 1
            except ValueError as e:
                print(f"ERROR: {e}", file=sys.stderr)
                return 2
        finally:
            p.close()
        display_name = "feeds"
        display_count = len(feeds)

    if args.out:
        _write_manifest(Path(args.out), manifest)

    if args.json:
        print(orjson.dumps(manifest, option=orjson.OPT_INDENT_2).decode("utf-8"))
        return 0

    intervals = manifest.get("intervals", [])
    rec = manifest.get("recommended")
    print(
        f"{display_name}={display_count} "
        f"status_filter={manifest.get('status_filter')} "
        f"common_windows={len(intervals)}"
    )

    top = max(0, int(args.show_top))
    for i, seg in enumerate(intervals[:top], start=1):
        print(
            f"  {i}. start_us={seg['start_us']} end_us={seg['end_us']} duration_us={seg['duration_us']}"
        )

    if rec is None:
        print("recommended_split=None")
    else:
        t = rec["train"]
        v = rec["validation"]
        print(
            "recommended_split "
            f"train=[{t['start_us']},{t['end_us']}] "
            f"validation=[{v['start_us']},{v['end_us']}] "
            f"ratio={rec['train_ratio']}"
        )

    if args.out:
        print(f"manifest_written={args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
