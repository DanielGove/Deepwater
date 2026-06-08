#!/usr/bin/env python3
"""Tests for deepwater-feeds metadata CLI."""
import io
import sys
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater import create_feed
from deepwater.cli.feeds_cli import main as feeds_main


def _spec(name: str, persist: bool = True) -> dict:
    return {
        "feed_name": name,
        "mode": "UF",
        "fields": [
            {"name": "recv_us", "type": "uint64"},
            {"name": "price", "type": "float64"},
            {"name": "size", "type": "float64"},
        ],
        "clock_level": 1,
        "persist": persist,
        "chunk_size_mb": 1,
        "retention_hours": 24,
    }


def test_list_feeds_text():
    with tempfile.TemporaryDirectory(prefix="dw-feeds-list-") as td:
        base = Path(td) / "platform"
        create_feed(base, _spec("trades"))
        create_feed(base, _spec("quotes", persist=False))

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = feeds_main(["--base-path", str(base)])
        out = buf.getvalue()

        assert rc == 0
        assert "feeds=2" in out
        assert "trades" in out
        assert "quotes" in out


def test_describe_single_feed_json():
    with tempfile.TemporaryDirectory(prefix="dw-feeds-one-") as td:
        base = Path(td) / "platform"
        create_feed(base, _spec("trades"))

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = feeds_main([
                "--base-path", str(base),
                "--feed", "trades",
                "--json",
            ])
        payload = orjson.loads(buf.getvalue().encode("utf-8"))

        assert rc == 0
        assert payload["feed_name"] == "trades"
        assert payload["metadata"]["persist"] is True
        assert isinstance(payload["record_fmt"], str) and payload["record_fmt"], "missing fmt"
        assert payload["record_size"] > 0
        assert payload["fields"][0]["name"] == "recv_us"


def test_describe_all_json():
    with tempfile.TemporaryDirectory(prefix="dw-feeds-all-") as td:
        base = Path(td) / "platform"
        create_feed(base, _spec("a"))
        create_feed(base, _spec("b", persist=False))

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = feeds_main(["--base-path", str(base), "--all", "--json"])
        payload = orjson.loads(buf.getvalue().encode("utf-8"))

        assert rc == 0
        described = payload["feeds"]
        names = {x["feed_name"] for x in described}
        assert names == {"a", "b"}, f"unexpected feed set: {names}"


def test_describe_missing_feed_fails():
    with tempfile.TemporaryDirectory(prefix="dw-feeds-missing-") as td:
        base = Path(td) / "platform"
        (base / "data").mkdir(parents=True, exist_ok=True)

        err = io.StringIO()
        with redirect_stderr(err):
            rc = feeds_main(["--base-path", str(base), "--feed", "missing"])

        assert rc == 1
        assert "not found" in err.getvalue()


def test_describe_single_feed_text_timestamp_formats():
    with tempfile.TemporaryDirectory(prefix="dw-feeds-text-ts-") as td:
        base = Path(td) / "platform"
        create_feed(base, _spec("trades"))

        human_buf = io.StringIO()
        with redirect_stdout(human_buf):
            rc_human = feeds_main([
                "--base-path", str(base),
                "--feed", "trades",
                "--ts-fmt", "utc",
            ])
        assert rc_human == 0
        human_out = human_buf.getvalue()
        assert "feed=trades" in human_out
        assert "created=" in human_out
        assert "created_us=" not in human_out
        assert "created=20" in human_out

        us_buf = io.StringIO()
        with redirect_stdout(us_buf):
            rc_us = feeds_main([
                "--base-path", str(base),
                "--feed", "trades",
                "--ts-fmt", "epoch",
            ])
        assert rc_us == 0
        us_out = us_buf.getvalue()
        assert "created_us=" in us_out


def run_tests():
    tests = [
        ("list_feeds_text", test_list_feeds_text),
        ("describe_single_feed_json", test_describe_single_feed_json),
        ("describe_all_json", test_describe_all_json),
        ("describe_missing_feed_fails", test_describe_missing_feed_fails),
        ("describe_single_feed_text_timestamp_formats", test_describe_single_feed_text_timestamp_formats),
    ]
    print("Feeds CLI Tests")
    print("=" * 60)
    passed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"✅ {name}")
            passed += 1
        except Exception as e:
            print(f"❌ {name} - {e}")
    print(f"\nPassed: {passed}/{len(tests)}")
    if passed != len(tests):
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
