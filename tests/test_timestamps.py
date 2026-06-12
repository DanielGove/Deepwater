from __future__ import annotations

import datetime as dt

import pytest

from deepwater.utils.timestamps import parse_us_timestamp, us_to_iso


def _epoch_us(value: str) -> int:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1_000_000)


def test_parse_us_timestamp_accepts_ascii_inputs():
    text = "2026-06-11T12:34:56.123456Z"
    expected = _epoch_us(text)

    assert parse_us_timestamp(text) == expected
    assert parse_us_timestamp(text.encode("ascii")) == expected
    assert parse_us_timestamp(bytearray(text, "ascii")) == expected


def test_parse_us_timestamp_pads_and_truncates_fraction():
    assert parse_us_timestamp("2026-06-11T12:34:56.1Z") == _epoch_us("2026-06-11T12:34:56.100000Z")
    assert parse_us_timestamp("2026-06-11T12:34:56.123456789Z") == _epoch_us("2026-06-11T12:34:56.123456Z")
    assert parse_us_timestamp("2026-06-11T12:34:56Z") == _epoch_us("2026-06-11T12:34:56.000000Z")


def test_parse_timestamp_rejects_non_ascii_containers():
    with pytest.raises(TypeError):
        parse_us_timestamp(object())

    with pytest.raises(ValueError):
        parse_us_timestamp("short")


def test_us_to_iso_formats_microsecond_timestamp():
    value = parse_us_timestamp("2026-06-11T12:34:56.123456Z")
    assert us_to_iso(value) == "2026-06-11T12:34:56.123456Z"
