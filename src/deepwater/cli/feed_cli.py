#!/usr/bin/env python3
"""
CLI tools for feed lifecycle operations.

Provides:
    - deepwater-create-feed
    - deepwater-delete-feed
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

import orjson

from ..platform import Platform


def _load_specs_from_file(path: Path) -> list[dict]:
    """Load one or more feed specs from a JSON config file."""
    try:
        raw = orjson.loads(path.read_bytes())
    except FileNotFoundError as e:
        raise ValueError(f"Config file not found: {path}") from e
    except Exception as e:
        raise ValueError(f"Invalid JSON in {path}: {e}") from e

    if isinstance(raw, dict):
        specs_raw = raw.get("feeds", [raw])
    elif isinstance(raw, list):
        specs_raw = raw
    else:
        raise ValueError(
            f"Config {path} must be an object, list, or object with 'feeds' list"
        )

    specs: list[dict] = []
    for idx, spec in enumerate(specs_raw):
        if not isinstance(spec, dict):
            raise ValueError(f"{path}: feed spec #{idx} is not an object")
        name = spec.get("feed_name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"{path}: feed spec #{idx} missing valid 'feed_name'")
        specs.append(spec)
    return specs


def _config_paths(configs: Iterable[str], config_dir: Optional[str]) -> list[Path]:
    paths: list[Path] = []
    seen: set[Path] = set()

    for config in configs:
        p = Path(config)
        if p not in seen:
            seen.add(p)
            paths.append(p)

    if config_dir:
        d = Path(config_dir)
        if not d.exists() or not d.is_dir():
            raise ValueError(f"--config-dir is not a directory: {d}")
        for p in sorted(d.iterdir()):
            if p.is_file() and not p.name.startswith(".") and p not in seen:
                seen.add(p)
                paths.append(p)

    return paths


def _print(msg: str, quiet: bool) -> None:
    if not quiet:
        print(msg)


def create_main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Create Deepwater feeds from config files")
    parser.add_argument("--base-path", required=True, help="Deepwater base path")
    parser.add_argument(
        "--config",
        action="append",
        default=[],
        help="Feed config JSON file path (repeatable)",
    )
    parser.add_argument(
        "--config-dir",
        help="Directory containing feed config JSON files",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress non-error output")
    args = parser.parse_args(argv)

    try:
        paths = _config_paths(args.config, args.config_dir)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    if not paths:
        print("ERROR: provide at least one --config or --config-dir", file=sys.stderr)
        return 2

    specs: list[dict] = []
    for path in paths:
        try:
            specs.extend(_load_specs_from_file(path))
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 2

    created = 0
    existed = 0
    failed = 0
    platform = Platform(args.base_path)
    try:
        for spec in specs:
            name = spec["feed_name"]
            was_present = platform.feed_exists(name)
            try:
                platform.create_feed(spec)
            except Exception as e:
                failed += 1
                print(f"ERROR creating feed '{name}': {e}", file=sys.stderr)
                continue

            if was_present:
                existed += 1
                _print(f"UNCHANGED {name} (already exists)", args.quiet)
            else:
                created += 1
                _print(f"CREATED   {name}", args.quiet)
    finally:
        platform.close()

    _print(
        f"Create summary: created={created} unchanged={existed} failed={failed}",
        args.quiet,
    )
    return 1 if failed else 0


def _extract_targets(feed_names: list[str], paths: list[Path]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    for name in feed_names:
        nm = name.strip()
        if not nm:
            continue
        if nm not in seen:
            seen.add(nm)
            names.append(nm)

    for path in paths:
        specs = _load_specs_from_file(path)
        for spec in specs:
            name = spec["feed_name"]
            if name not in seen:
                seen.add(name)
                names.append(name)

    return names


def delete_main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Delete Deepwater feeds")
    parser.add_argument("--base-path", required=True, help="Deepwater base path")
    parser.add_argument(
        "--feed",
        action="append",
        default=[],
        help="Feed name to delete (repeatable)",
    )
    parser.add_argument(
        "--config",
        action="append",
        default=[],
        help="Feed config JSON file path (feed_name used for delete)",
    )
    parser.add_argument(
        "--config-dir",
        help="Directory containing feed config JSON files (feed_name used for delete)",
    )
    parser.add_argument(
        "--strict-missing",
        action="store_true",
        help="Treat missing feeds as errors",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress non-error output")
    args = parser.parse_args(argv)

    try:
        paths = _config_paths(args.config, args.config_dir)
        targets = _extract_targets(args.feed, paths)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    if not targets:
        print("ERROR: provide at least one --feed, --config, or --config-dir", file=sys.stderr)
        return 2

    deleted = 0
    missing = 0
    failed = 0
    platform = Platform(args.base_path)
    try:
        for name in targets:
            try:
                removed = platform.delete_feed(name, missing_ok=not args.strict_missing)
            except KeyError:
                # strict mode path - Platform raises for missing feed.
                missing += 1
                failed += 1
                print(f"ERROR deleting '{name}': feed does not exist", file=sys.stderr)
                continue
            except Exception as e:
                failed += 1
                print(f"ERROR deleting '{name}': {e}", file=sys.stderr)
                continue

            if removed:
                deleted += 1
                _print(f"DELETED   {name}", args.quiet)
            else:
                missing += 1
                _print(f"MISSING   {name}", args.quiet)
    finally:
        platform.close()

    _print(
        f"Delete summary: deleted={deleted} missing={missing} failed={failed}",
        args.quiet,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    print("Use 'deepwater-create-feed' or 'deepwater-delete-feed'.", file=sys.stderr)
    sys.exit(2)
