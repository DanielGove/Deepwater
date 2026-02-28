#!/usr/bin/env python3
"""Tests for the top-level `deepwater` bootstrap command."""
import io
import os
import sys
import tempfile
from contextlib import redirect_stderr
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater.cli.bootstrap import main as bootstrap_main


def test_bootstrap_creates_starter_folder_and_files():
    with tempfile.TemporaryDirectory(prefix="dw-bootstrap-") as td:
        out = Path(td) / "starter"
        rc = bootstrap_main(["--path", str(out), "--quiet"])
        assert rc == 0

        start_here = out / "START_HERE.md"
        guide = out / "GUIDE.md"
        runbook = out / "OPS_RUNBOOK.md"
        trades = out / "configs" / "trades.json"
        quotes = out / "configs" / "quotes.json"
        feeds = out / "configs" / "feeds.json"
        quickstart_app = out / "apps" / "quickstart_app.py"

        assert start_here.exists(), "START_HERE.md not created"
        assert guide.exists(), "GUIDE.md not created"
        assert runbook.exists(), "OPS_RUNBOOK.md not created"
        assert trades.exists(), "trades.json not created"
        assert quotes.exists(), "quotes.json not created"
        assert feeds.exists(), "feeds.json not created"
        assert quickstart_app.exists(), "apps/quickstart_app.py not created"

        text = start_here.read_text(encoding="utf-8")
        assert "deepwater-create-feed" in text
        assert "deepwater-feeds" in text
        assert "deepwater-health" in text
        assert "deepwater-segments" in text
        assert "deepwater-datasets" in text

        runbook_text = runbook.read_text(encoding="utf-8")
        assert "Common Incident Playbooks" in runbook_text
        assert "deepwater-cleanup" in runbook_text


def test_bootstrap_defaults_to_local_deepwater_starter_folder():
    with tempfile.TemporaryDirectory(prefix="dw-bootstrap-default-") as td:
        old_cwd = os.getcwd()
        try:
            os.chdir(td)
            rc = bootstrap_main(["--quiet"])
            assert rc == 0

            starter = Path(td) / "deepwater-starter"
            assert (starter / "START_HERE.md").exists(), "START_HERE.md not created in default folder"
            assert (starter / "OPS_RUNBOOK.md").exists(), "OPS_RUNBOOK.md not created in default folder"
            assert (starter / "configs" / "trades.json").exists(), "trades config missing in default folder"
        finally:
            os.chdir(old_cwd)


def test_bootstrap_refuses_overwrite_without_force():
    with tempfile.TemporaryDirectory(prefix="dw-bootstrap-force-") as td:
        out = Path(td) / "starter"
        rc1 = bootstrap_main(["--path", str(out), "--quiet"])
        assert rc1 == 0

        buf = io.StringIO()
        with redirect_stderr(buf):
            rc2 = bootstrap_main(["--path", str(out)])

        assert rc2 == 1
        assert "already exists" in buf.getvalue()


def test_bootstrap_force_overwrites_generated_files():
    with tempfile.TemporaryDirectory(prefix="dw-bootstrap-overwrite-") as td:
        out = Path(td) / "starter"
        rc1 = bootstrap_main(["--path", str(out), "--quiet"])
        assert rc1 == 0

        start_here = out / "START_HERE.md"
        start_here.write_text("custom", encoding="utf-8")

        rc2 = bootstrap_main(["--path", str(out), "--force", "--quiet"])
        assert rc2 == 0
        assert "Deepwater: Start Here" in start_here.read_text(encoding="utf-8")


def run_tests():
    tests = [
        ("bootstrap_creates_starter_folder_and_files", test_bootstrap_creates_starter_folder_and_files),
        ("bootstrap_defaults_to_local_deepwater_starter_folder", test_bootstrap_defaults_to_local_deepwater_starter_folder),
        ("bootstrap_refuses_overwrite_without_force", test_bootstrap_refuses_overwrite_without_force),
        ("bootstrap_force_overwrites_generated_files", test_bootstrap_force_overwrites_generated_files),
    ]
    print("Bootstrap CLI Tests")
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
