#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time

import deepwater as dw


def main() -> None:
    parser = argparse.ArgumentParser(description="Read a local or remote Deepwater feed")
    parser.add_argument("target", help="Local path, host:/path, or dw://host/path")
    parser.add_argument("feed", help="Feed name to read")
    parser.add_argument("--start", type=int, help="Start timestamp in microseconds")
    parser.add_argument("--end", type=int, help="End timestamp in microseconds")
    parser.add_argument("--seconds", type=float, default=60.0, help="Latest window when start/end are omitted")
    parser.add_argument("--format", choices=("tuple", "dict", "numpy", "raw"), default="tuple")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--port", type=int, default=7447)
    args = parser.parse_args()

    with dw.reader(args.target, stream=args.feed, port=args.port) as reader:
        if args.start is not None or args.end is not None:
            if args.start is None or args.end is None:
                raise SystemExit("--start and --end must be passed together")
            records = reader.range(args.start, args.end, format=args.format)
        else:
            records = reader.latest(args.seconds, format=args.format)

        if args.format == "raw":
            count = len(records) // reader.record_size
            print(f"{count} records ({len(records)} bytes)")
            return

        count = len(records)
        print(f"{count} records")
        for record in list(records[: args.limit]):
            print(record)


if __name__ == "__main__":
    main()
