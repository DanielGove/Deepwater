#!/usr/bin/env python3
"""Shared timestamp formatting helpers for Deepwater CLIs."""
from __future__ import annotations

import argparse
from typing import Literal

from ..utils.timestamps import us_to_iso

TimestampFormat = Literal["human", "us"]


def add_timestamp_format_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--timestamp-format",
        choices=("human", "us"),
        default="human",
        help="Timestamp display format for text output (default: human)",
    )


def format_timestamp_us(ts_us: object, fmt: TimestampFormat) -> str:
    """Format a microsecond timestamp for text output."""
    if ts_us is None:
        return "None"
    try:
        value = int(ts_us)
    except (TypeError, ValueError):
        return str(ts_us)

    if fmt == "us":
        return str(value)
    return f"{us_to_iso(value)} ({value} us)"


def field_label(base: str, fmt: TimestampFormat) -> str:
    """Return label suffixing for timestamp fields in text output."""
    return f"{base}_us" if fmt == "us" else base
