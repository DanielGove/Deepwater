#!/usr/bin/env python3
"""Tests for multi-feed contiguous dataset planning."""
import io
import json
import sys
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform
from deepwater.cli.datasets_cli import main as datasets_cli_main


def _spec(name: str) -> dict:
    return {
        "feed_name": name,
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "v", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": True,
        "chunk_size_mb": 1,
    }


def _write_segment(p: Platform, feed: str, start_us: int, end_us: int) -> None:
    w = p.create_writer(feed)
    if end_us < start_us:
        raise ValueError("end_us must be >= start_us")
    w.write_values(start_us, 1)
    if end_us != start_us:
        w.write_values(end_us, 2)
    p.close_writer(feed)


def test_common_windows_across_two_feeds():
    with tempfile.TemporaryDirectory(prefix="dw-datasets-") as td:
        base = Path(td)
        p = Platform(str(base))
        p.create_feed(_spec("f1"))
        p.create_feed(_spec("f2"))

        _write_segment(p, "f1", 100, 200)
        _write_segment(p, "f1", 300, 500)

        _write_segment(p, "f2", 150, 250)
        _write_segment(p, "f2", 260, 480)

        windows = p.common_time_windows(["f1", "f2"], status="usable")
        got = [(w["start_us"], w["end_us"]) for w in windows]
        assert got == [(150, 200), (300, 480)], f"unexpected windows: {got}"

        manifest = p.recommended_train_validation(["f1", "f2"], train_ratio=0.8)
        rec = manifest["recommended"]
        assert rec is not None
        assert rec["interval"]["start_us"] == 300
        assert rec["interval"]["end_us"] == 480
        # duration 181, train floor(144) -> 300..443
        assert rec["train"]["start_us"] == 300
        assert rec["train"]["end_us"] == 443
        assert rec["validation"]["start_us"] == 444
        assert rec["validation"]["end_us"] == 480
        p.close()


def test_datasets_cli_json_and_manifest_output():
    with tempfile.TemporaryDirectory(prefix="dw-datasets-cli-") as td:
        base = Path(td) / "base"
        out_file = Path(td) / "manifest.json"

        p = Platform(str(base))
        p.create_feed(_spec("f1"))
        p.create_feed(_spec("f2"))
        _write_segment(p, "f1", 1_000_000, 1_000_200)
        _write_segment(p, "f2", 1_000_100, 1_000_300)
        p.close()

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = datasets_cli_main([
                "--base-path", str(base),
                "--feed", "f1",
                "--feed", "f2",
                "--json",
                "--out", str(out_file),
            ])
        assert rc == 0

        payload = json.loads(buf.getvalue())
        assert payload["feeds"] == ["f1", "f2"]
        assert len(payload["intervals"]) == 1
        assert payload["intervals"][0]["start_us"] == 1_000_100
        assert payload["intervals"][0]["end_us"] == 1_000_200
        assert out_file.exists(), "manifest file not written"


def test_datasets_cli_multi_source_two_base_paths():
    with tempfile.TemporaryDirectory(prefix="dw-datasets-multi-") as td:
        root = Path(td)
        base_a = root / "base_a"
        base_b = root / "base_b"

        pa = Platform(str(base_a))
        pa.create_feed(_spec("a_feed"))
        _write_segment(pa, "a_feed", 10_000, 20_000)
        pa.close()

        pb = Platform(str(base_b))
        pb.create_feed(_spec("b_feed"))
        _write_segment(pb, "b_feed", 15_000, 25_000)
        pb.close()

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = datasets_cli_main([
                "--source", f"A={base_a}",
                "--source", f"B={base_b}",
                "--feed-ref", "A:a_feed",
                "--feed-ref", "B:b_feed",
                "--json",
            ])
        assert rc == 0

        payload = json.loads(buf.getvalue())
        assert payload["mode"] == "multi_source"
        assert payload["sources"]["A"] == str(base_a)
        assert payload["sources"]["B"] == str(base_b)
        assert payload["feed_refs"] == ["A:a_feed", "B:b_feed"]
        assert len(payload["intervals"]) == 1
        assert payload["intervals"][0]["start_us"] == 15_000
        assert payload["intervals"][0]["end_us"] == 20_000


def run_tests():
    tests = [
        ("common_windows_across_two_feeds", test_common_windows_across_two_feeds),
        ("datasets_cli_json_and_manifest_output", test_datasets_cli_json_and_manifest_output),
        ("datasets_cli_multi_source_two_base_paths", test_datasets_cli_multi_source_two_base_paths),
    ]
    print("Datasets Tests")
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
