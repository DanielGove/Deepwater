#!/usr/bin/env python3
"""Tests for automatic writer-driven segmentation metadata."""
import io
import os
import subprocess
import sys
import tempfile
from contextlib import redirect_stdout
from pathlib import Path


from deepwater import Writer, create_feed
from deepwater.cli.segments_cli import main as segments_cli_main
from deepwater.metadata.feed_registry import FeedRegistry, UINT64_MAX
from deepwater.metadata.discovery import list_segments, suggested_reader_range
from deepwater.ops import repair
from deepwater.metadata.segments import SegmentStore


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
        "storage": "chunk",
        "chunk_size_mb": 1,
    }


def test_writer_auto_segment_closed_and_suggested_range():
    with tempfile.TemporaryDirectory(prefix="dw-seg-basic-") as td:
        base = Path(td)
        create_feed(base, _spec("segfeed"))

        w = Writer(base, "segfeed")
        for i in range(5):
            w.write_values(1_000_000 + i, i)
        w.close()

        segs = list_segments(base, "segfeed")
        assert len(segs) == 1, f"expected one segment, got {len(segs)}"
        seg = segs[0]
        assert seg["status"] == "closed"
        assert seg["start_us"] == 1_000_000
        assert seg["end_us"] == 1_000_004

        suggested = suggested_reader_range(base, "segfeed")
        assert suggested == (1_000_000, 1_000_004)


def test_writer_recovery_crash_closes_previous_open_segment_at_last_ts():
    with tempfile.TemporaryDirectory(prefix="dw-seg-crash-") as td:
        base = Path(td)
        repo_root = Path(__file__).parent.parent

        # Crashy writer process: writes records and exits abruptly without close().
        child_code = f"""
import os
import sys
sys.path.insert(0, {str(repo_root)!r})
from deepwater import Writer, create_feed

base = {str(base)!r}
create_feed(base, {{'feed_name':'segfeed','mode':'UF','fields':[{{'name':'ts','type':'uint64'}},{{'name':'v','type':'uint64'}}],'clock_level':1,'persist':True,'storage':'chunk','chunk_size_mb':1}})
w = Writer(base, 'segfeed')
for i in range(4):
    w.write_values(2_000_000 + i, i)
os._exit(0)
"""
        proc = subprocess.run([sys.executable, "-c", child_code], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr

        # Restart writer in parent. This should crash-close previous open segment.
        w = Writer(base, "segfeed")
        w.write_values(3_000_000, 999)
        w.close()

        segs = list_segments(base, "segfeed")
        assert len(segs) >= 2, f"expected at least two segments, got {len(segs)}"

        first = segs[0]
        assert first["status"] == "crash_closed", f"unexpected first status: {first}"
        assert first["start_us"] == 2_000_000
        assert first["end_us"] == 2_000_003
        assert first["records"] == 4

        last = segs[-1]
        assert last["status"] == "closed"
        assert last["start_us"] == 3_000_000
        assert last["end_us"] == 3_000_000


def test_writer_recovery_repairs_chunk_metadata_before_segment_close():
    with tempfile.TemporaryDirectory(prefix="dw-seg-repair-meta-") as td:
        base = Path(td)
        repo_root = Path(__file__).parent.parent

        child_code = f"""
import os
import sys
sys.path.insert(0, {str(repo_root)!r})
from deepwater import Writer, create_feed

base = {str(base)!r}
create_feed(base, {{'feed_name':'segfeed','mode':'UF','fields':[{{'name':'ts','type':'uint64'}},{{'name':'v','type':'uint64'}}],'clock_level':1,'persist':True,'storage':'chunk','chunk_size_mb':1}})
w = Writer(base, 'segfeed')
for i in range(5):
    w.write_values(2_100_000 + i, i)
os._exit(0)
"""
        proc = subprocess.run([sys.executable, "-c", child_code], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr

        feed_dir = base / "data" / "segfeed"
        reg = FeedRegistry(str(feed_dir / "segfeed.reg"), mode="w")
        try:
            idx = reg.get_latest_chunk_idx()
            assert idx is not None
            meta = reg.get_chunk_metadata(int(idx))
            try:
                # Simulate stale/invalid metadata from abrupt crash.
                meta.num_records = 0
                meta.set_qbounds(0, UINT64_MAX, 0)
            finally:
                meta.release()
        finally:
            reg.close()

        w = Writer(base, "segfeed")
        w.write_values(3_100_000, 999)
        w.close()

        segs = list_segments(base, "segfeed")
        assert len(segs) >= 2, f"expected at least two segments, got {len(segs)}"
        first = segs[0]
        assert first["status"] == "crash_closed", f"unexpected first status: {first}"
        assert first["start_us"] == 2_100_000
        assert first["end_us"] == 2_100_004
        assert first["records"] == 5


def test_repair_utility_backfills_legacy_segment_zero_records():
    with tempfile.TemporaryDirectory(prefix="dw-seg-repair-utility-") as td:
        base = Path(td)
        create_feed(base, _spec("segfeed"))
        w = Writer(base, "segfeed")
        for i in range(5):
            w.write_values(7_000_000 + i, i)
        w.close()

        store = SegmentStore(base / "data" / "segfeed", "segfeed")
        fd, mm = store._open_mmap()
        try:
            header = store._read_header(mm)
            assert int(header["segment_count"]) >= 1
            seg = store._read_entry(mm, 1)
            seg["records"] = 0
            store._write_entry(mm, 1, seg)
            mm.flush()
        finally:
            mm.close()
            os.close(fd)

        checked, repaired = repair.repair_feed(base, "segfeed", dry_run=False)
        assert checked >= 2  # at least one chunk + one segment checked
        assert repaired >= 1

        segs = list_segments(base, "segfeed")
        assert segs[0]["status"] == "closed"
        assert segs[0]["records"] == 5


def test_repair_utility_crash_closes_open_segment():
    with tempfile.TemporaryDirectory(prefix="dw-seg-repair-open-") as td:
        base = Path(td)
        repo_root = Path(__file__).parent.parent

        child_code = f"""
import os
import sys
sys.path.insert(0, {str(repo_root)!r})
from deepwater import Writer, create_feed

base = {str(base)!r}
create_feed(base, {{'feed_name':'segfeed','mode':'UF','fields':[{{'name':'ts','type':'uint64'}},{{'name':'v','type':'uint64'}}],'clock_level':1,'persist':True,'storage':'chunk','chunk_size_mb':1}})
w = Writer(base, 'segfeed')
for i in range(3):
    w.write_values(8_000_000 + i, i)
os._exit(0)
"""
        proc = subprocess.run([sys.executable, "-c", child_code], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr

        checked, repaired = repair.repair_feed(base, "segfeed", dry_run=False)
        assert checked >= 2  # at least one chunk + one segment checked
        assert repaired >= 1

        segs = list_segments(base, "segfeed")
        assert len(segs) >= 1
        seg = segs[0]
        assert seg["status"] == "crash_closed"
        assert seg["start_us"] == 8_000_000
        assert seg["end_us"] == 8_000_002
        assert seg["records"] == 3


def test_segments_cli_lists_and_suggests_range():
    with tempfile.TemporaryDirectory(prefix="dw-seg-cli-") as td:
        base = Path(td)
        create_feed(base, _spec("segfeed"))
        w = Writer(base, "segfeed")
        for i in range(3):
            w.write_values(4_000_000 + i, i)
        w.close()

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = segments_cli_main([
                "--base-path", str(base),
                "--feed", "segfeed",
                "--suggest-range",
                "--ts-fmt", "utc",
            ])
        out = buf.getvalue()
        assert rc == 0
        assert "segments=1" in out
        assert "start=1970-01-01T00:00:04.000000Z" in out
        assert "end=1970-01-01T00:00:04.000002Z" in out
        assert "suggested_range_start=1970-01-01T00:00:04.000000Z" in out
        assert "suggested_range_end=1970-01-01T00:00:04.000002Z" in out


def test_segments_cli_timestamp_format_us():
    with tempfile.TemporaryDirectory(prefix="dw-seg-cli-us-") as td:
        base = Path(td)
        create_feed(base, _spec("segfeed"))
        w = Writer(base, "segfeed")
        for i in range(3):
            w.write_values(4_000_000 + i, i)
        w.close()

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = segments_cli_main([
                "--base-path", str(base),
                "--feed", "segfeed",
                "--suggest-range",
                "--ts-fmt", "epoch",
            ])
        out = buf.getvalue()
        assert rc == 0
        assert "start_us=4000000" in out
        assert "end_us=4000002" in out
        assert "suggested_range_start_us=4000000" in out
        assert "suggested_range_end_us=4000002" in out


def test_segments_cli_timestamp_format_timezone_name():
    with tempfile.TemporaryDirectory(prefix="dw-seg-cli-tzname-") as td:
        base = Path(td)
        create_feed(base, _spec("segfeed"))
        w = Writer(base, "segfeed")
        for i in range(3):
            w.write_values(4_000_000 + i, i)
        w.close()

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = segments_cli_main([
                "--base-path", str(base),
                "--feed", "segfeed",
                "--suggest-range",
                "--ts-fmt", "America/New_York",
            ])
        out = buf.getvalue()
        assert rc == 0
        assert "start=1969-12-31T19:00:04.000000-05:00" in out
        assert "end=1969-12-31T19:00:04.000002-05:00" in out
        assert "suggested_range_start=1969-12-31T19:00:04.000000-05:00" in out
        assert "suggested_range_end=1969-12-31T19:00:04.000002-05:00" in out


def test_ring_writer_auto_segments_closed():
    with tempfile.TemporaryDirectory(prefix="dw-seg-ring-") as td:
        base = Path(td)
        create_feed(base, {
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
        w = Writer(base, "ringseg")
        w.write_values(5_000_000, 1)
        w.write_values(5_000_010, 2)
        w.close()

        segs = list_segments(base, "ringseg")
        assert len(segs) == 1
        seg = segs[0]
        assert seg["status"] == "closed"
        assert seg["start_us"] == 5_000_000
        assert seg["end_us"] == 5_000_010


def test_writer_manual_segment_boundary_without_close():
    with tempfile.TemporaryDirectory(prefix="dw-seg-boundary-") as td:
        base = Path(td)
        create_feed(base, _spec("segmanual"))
        w = Writer(base, "segmanual")
        w.write_values(6_000_000, 1)
        w.write_values(6_000_001, 2)
        assert w.mark_segment_boundary("disconnect") is True
        w.write_values(6_000_100, 3)
        w.write_values(6_000_101, 4)
        w.close()

        segs = list_segments(base, "segmanual")
        assert len(segs) == 2, f"expected two segments, got {len(segs)}"

        first = segs[0]
        assert first["status"] == "closed"
        assert first["close_reason"] == "disconnect"
        assert first["start_us"] == 6_000_000
        assert first["end_us"] == 6_000_001
        assert first["records"] == 2

        second = segs[1]
        assert second["status"] == "closed"
        assert second["close_reason"] == "writer_close"
        assert second["start_us"] == 6_000_100
        assert second["end_us"] == 6_000_101
        assert second["records"] == 2
