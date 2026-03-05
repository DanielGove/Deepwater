#!/usr/bin/env python3
"""Shared timestamp formatting helpers for Deepwater CLIs."""
from __future__ import annotations

import argparse
import datetime as _dt
from dataclasses import dataclass
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..utils.timestamps import us_to_iso


@dataclass(frozen=True)
class TimestampFormat:
    value: str
    epoch: bool
    tz: _dt.tzinfo | None
    utc: bool


def add_ts_fmt_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--ts-fmt",
        default="local",
        metavar="FMT",
        help=(
            "Timestamp display for text output: "
            "epoch|local|utc|<IANA timezone> (default: local)"
        ),
    )


def parse_ts_fmt(value: str) -> TimestampFormat:
    """Parse ts-fmt option into a rendering plan."""
    raw = (value or "").strip()
    if not raw:
        raise ValueError("Invalid --ts-fmt ''. Use epoch, local, utc, or an IANA timezone.")

    key = raw.lower()
    if key == "epoch":
        return TimestampFormat(value="epoch", epoch=True, tz=None, utc=False)
    if key == "local":
        return TimestampFormat(value="local", epoch=False, tz=None, utc=False)
    if key == "utc":
        return TimestampFormat(value="utc", epoch=False, tz=_dt.timezone.utc, utc=True)

    try:
        tz = ZoneInfo(raw)
    except ZoneInfoNotFoundError as e:
        raise ValueError(
            f"Invalid --ts-fmt '{raw}'. Use epoch, local, utc, or an IANA timezone like America/New_York."
        ) from e
    return TimestampFormat(value=raw, epoch=False, tz=tz, utc=False)


def format_timestamp_us(ts_us: object, fmt: TimestampFormat) -> str:
    """Format a microsecond timestamp for text output."""
    if ts_us is None:
        return "None"
    try:
        value = int(ts_us)
    except (TypeError, ValueError):
        return str(ts_us)

    if fmt.epoch:
        return str(value)
    if fmt.utc:
        return us_to_iso(value)

    seconds, micros = divmod(value, 1_000_000)
    dt = _dt.datetime.fromtimestamp(seconds, tz=_dt.timezone.utc)
    dt = dt.replace(microsecond=int(micros))
    if fmt.tz is None:
        dt = dt.astimezone()
    else:
        dt = dt.astimezone(fmt.tz)
    return dt.isoformat(timespec="microseconds")


def field_label(base: str, fmt: TimestampFormat) -> str:
    """Return label suffixing for timestamp fields in text output."""
    return f"{base}_us" if fmt.epoch else base
