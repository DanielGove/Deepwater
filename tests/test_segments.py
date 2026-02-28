#!/usr/bin/env python3
"""Tests for automatic writer-driven segmentation metadata."""
import io
import os
import subprocess
import sys
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform
from deepwater.segments_cli import main as segments_cli_main


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


def test_writer_auto_segment_closed_and_suggested_range():
    with tempfile.TemporaryDirectory(prefix="dw-seg-basic-") as td:
        base = Path(td)
        p = Platform(str(base))
        p.create_feed(_spec("segfeed"))

        w = p.create_writer("segfeed")
        for i in range(5):
            w.write_values(1_000_000 + i, i)
        w.close()

        segs = p.list_segments("segfeed")
        assert len(segs) == 1, f"expected one segment, got {len(segs)}"
        seg = segs[0]
        assert seg["status"] == "closed"
        assert seg["start_us"] == 1_000_000
        assert seg["end_us"] == 1_000_004

        suggested = p.suggested_reader_range("segfeed")
        assert suggested == (1_000_000, 1_000_004)
        p.close()


def test_writer_recovery_crash_closes_previous_open_segment_at_last_ts():
    with tempfile.TemporaryDirectory(prefix="dw-seg-crash-") as td:
        base = Path(td)
        src = Path(__file__).parent.parent / "src"

        # Crashy writer process: writes records and exits abruptly without close().
        child_code = f"""
import os
import sys
sys.path.insert(0, {str(src)!r})
from deepwater import Platform

base = {str(base)!r}
p = Platform(base)
p.create_feed({{'feed_name':'segfeed','mode':'UF','fields':[{{'name':'ts','type':'uint64'}},{{'name':'v','type':'uint64'}}],'clock_level':1,'persist':True,'chunk_size_mb':1}})
w = p.create_writer('segfeed')
for i in range(4):
    w.write_values(2_000_000 + i, i)
os._exit(0)
"""
        proc = subprocess.run([sys.executable, "-c", child_code], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr

        # Restart writer in parent. This should crash-close previous open segment.
        p = Platform(str(base))
        w = p.create_writer("segfeed")
        w.write_values(3_000_000, 999)
        w.close()

        segs = p.list_segments("segfeed")
        assert len(segs) >= 2, f"expected at least two segments, got {len(segs)}"

        first = segs[0]
        assert first["status"] == "crash_closed", f"unexpected first status: {first}"
        assert first["start_us"] == 2_000_000
        assert first["end_us"] == 2_000_003

        last = segs[-1]
        assert last["status"] == "closed"
        assert last["start_us"] == 3_000_000
        assert last["end_us"] == 3_000_000
        p.close()


def test_segments_cli_lists_and_suggests_range():
    with tempfile.TemporaryDirectory(prefix="dw-seg-cli-") as td:
        base = Path(td)
        p = Platform(str(base))
        p.create_feed(_spec("segfeed"))
        w = p.create_writer("segfeed")
        for i in range(3):
            w.write_values(4_000_000 + i, i)
        w.close()
        p.close()

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = segments_cli_main([
                "--base-path", str(base),
                "--feed", "segfeed",
                "--suggest-range",
            ])
        out = buf.getvalue()
        assert rc == 0
        assert "segments=1" in out
        assert "suggested_range_start_us=4000000" in out
        assert "suggested_range_end_us=4000002" in out


def test_ring_writer_auto_segments_closed():
    with tempfile.TemporaryDirectory(prefix="dw-seg-ring-") as td:
        base = Path(td)
        p = Platform(str(base))
        p.create_feed({
            "feed_name": "ringseg",
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "v", "type": "uint64"},
            ],
            "clock_level": 1,
            "persist": False,
            "chunk_size_mb": 0.01,
        })
        w = p.create_writer("ringseg")
        w.write_values(5_000_000, 1)
        w.write_values(5_000_010, 2)
        w.close()

        segs = p.list_segments("ringseg")
        assert len(segs) == 1
        seg = segs[0]
        assert seg["status"] == "closed"
        assert seg["start_us"] == 5_000_000
        assert seg["end_us"] == 5_000_010
        p.close()


def run_tests():
    tests = [
        ("writer_auto_segment_closed_and_suggested_range", test_writer_auto_segment_closed_and_suggested_range),
        ("writer_recovery_crash_closes_previous_open_segment_at_last_ts", test_writer_recovery_crash_closes_previous_open_segment_at_last_ts),
        ("segments_cli_lists_and_suggests_range", test_segments_cli_lists_and_suggests_range),
        ("ring_writer_auto_segments_closed", test_ring_writer_auto_segments_closed),
    ]
    print("Segments Tests")
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
