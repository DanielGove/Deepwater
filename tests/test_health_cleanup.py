#!/usr/bin/env python3
"""Tests for health_check and cleanup utilities."""
import tempfile
from pathlib import Path
import subprocess
import sys
import time


from deepwater import Writer, create_feed
from deepwater.ops.health_check import check_global_registry, check_feeds
from deepwater.ops.cleanup import cleanup_all_feeds


def _make_feed(base: Path, name: str, retention_hours: int = 0, clock_level: int = 1, persist=True):
    spec = {
        "feed_name": name,
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "v", "type": "uint64"},
        ],
        "clock_level": clock_level,
        "persist": persist,
        "chunk_size_mb": 0.5,
        "retention_hours": retention_hours,
    }
    create_feed(base, spec)


def test_health_checks_pass_on_fresh_platform():
    with tempfile.TemporaryDirectory(prefix="dw-health-") as td:
        base = Path(td)
        _make_feed(base, "hfeed", retention_hours=0)
        ok, msg = check_global_registry(base)
        assert ok, msg
        ok, msg = check_feeds(base)
        assert ok, msg


def test_cleanup_deletes_expired_chunks():
    with tempfile.TemporaryDirectory(prefix="dw-clean-") as td:
        base = Path(td)
        _make_feed(base, "cfeed", retention_hours=1)
        w = Writer(base, "cfeed")
        base_ts = int(time.time() * 1e6) - 3_600_000_000  # 1 hour ago
        # Write a couple records to ensure chunk sealed
        for i in range(10):
            w.write_values(base_ts + i, i)
        w.close()

        reg_path = base / "data" / "cfeed" / "cfeed.reg"
        assert reg_path.exists()
        chunk_file = base / "data" / "cfeed" / "chunk_00000001.bin"
        assert chunk_file.exists()
        size_before = chunk_file.stat().st_size

        # Force metadata end_time/last_update to old timestamp to trigger retention
        from deepwater.metadata.feed_registry import FeedRegistry, ON_DISK
        reg = FeedRegistry(str(reg_path), mode="w")
        meta = reg.get_latest_chunk()
        meta.end_time = base_ts
        meta.last_update = base_ts
        meta.status = ON_DISK
        meta.release()
        reg.close()

        # Run cleanup; should delete the expired chunk
        cleanup_all_feeds(base, dry_run=False)

        assert not chunk_file.exists(), "cleanup did not delete expired chunk"


def test_ops_cli_error_paths_exit_cleanly():
    missing = "/tmp/deepwater-missing-cli-smoke"
    commands = [
        [sys.executable, "-m", "deepwater.ops.cleanup", "--base-path", missing],
        [sys.executable, "-m", "deepwater.ops.repair", "--base-path", missing, "--all"],
    ]
    for cmd in commands:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        assert proc.returncode == 1
        combined = proc.stdout + proc.stderr
        assert "NameError" not in combined
        assert "base path does not exist" in combined.lower()
