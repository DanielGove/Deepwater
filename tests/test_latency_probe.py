"""Tests for benchmark accounting; storage correctness has its own suite."""
import copy
import importlib.util
from pathlib import Path

import pytest


def _probe():
    path = Path(__file__).resolve().parents[1] / "benchmarks" / "live_latency.py"
    spec = importlib.util.spec_from_file_location("live_latency_probe", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_percentiles_convert_nanoseconds_and_use_nearest_rank():
    stats = _probe().distribution([10_000, 1_000, 3_000, 2_000])
    assert stats == {"count": 4, "mean_us": 4.0, "p50_us": 2.0,
                     "p95_us": 10.0, "p99_us": 10.0, "max_us": 10.0}
    with pytest.raises(ValueError, match="empty"):
        _probe().distribution([])


def _report():
    return {"protocol": 1, "workload": {"rate": 1000},
            "code": {"probe_sha256": "same"},
            "environment": {key: "same" for key in
                            ("python", "machine", "platform", "affinity", "filesystem")},
            "cases": [{"mode": "blob", "summary": {"delivery": {"p99_us": 10}}}]}


def test_comparison_reports_change_and_rejects_incomparable_runs():
    probe = _probe()
    previous = _report()
    current = copy.deepcopy(previous)
    current["cases"][0]["summary"]["delivery"]["p99_us"] = 15
    assert probe.compare_reports(current, previous)[0]["change_pct"] == 50
    for section, key in (("workload", "rate"), ("environment", "filesystem"),
                         ("environment", "python"), ("code", "probe_sha256")):
        mismatch = copy.deepcopy(current)
        mismatch[section][key] = "different"
        with pytest.raises(ValueError):
            probe.compare_reports(mismatch, previous)
    mismatch = copy.deepcopy(current)
    mismatch["cases"] = []
    with pytest.raises(ValueError, match="cases"):
        probe.compare_reports(mismatch, previous)


def test_temporary_feed_names_are_isolated_by_base_path(tmp_path):
    probe = _probe()
    first = probe._feed_name(tmp_path / "first")
    second = probe._feed_name(tmp_path / "second")
    assert first != second
    assert first.startswith("latency_probe_")
    assert first == probe._feed_name(tmp_path / "first" / ".." / "first")

def test_persistence_loss_cannot_be_compared_as_a_successful_run():
    probe = _probe()
    probe._require_lossless_index(0, 0)
    for overrun, lost in ((1, 0), (0, 1), (1, 10)):
        with pytest.raises(AssertionError, match="lost data"):
            probe._require_lossless_index(overrun, lost)


@pytest.mark.skipif(not Path("/proc").exists(), reason="Linux process lifecycle probe")
def test_cleanup_reaches_child_after_session_leader_crashes():
    import subprocess
    import sys
    import time

    probe = _probe()
    # The child closes stdout so the crashed leader cannot leave a pipe read open.
    code = (
        "import os, subprocess, sys; "
        "p=subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], "
        "stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL); "
        "print(p.pid, flush=True); os._exit(7)"
    )
    leader = subprocess.Popen([sys.executable, "-c", code], start_new_session=True,
                              stdout=subprocess.PIPE, text=True)
    try:
        output, _ = leader.communicate(timeout=3)
        assert leader.returncode == 7
        child_pid = int(output.strip())
        probe._kill_session(leader.pid)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            stat = Path(f"/proc/{child_pid}/stat")
            if not stat.exists():
                break
            try:
                state = stat.read_text().split(") ", 1)[1].split()[0]
            except FileNotFoundError:
                break
            if state == "Z":
                break
            time.sleep(.01)
        else:
            pytest.fail("descendant survived cleanup after leader crash")
    finally:
        probe._kill_session(leader.pid)
        leader.wait(timeout=3)
