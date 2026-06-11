from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_COINBASE = PROJECT_ROOT / "examples" / "coinbase"


if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(EXAMPLES_COINBASE) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_COINBASE))


def _deepwater_shm_files() -> set[Path]:
    shm_dir = Path("/dev/shm")
    if not shm_dir.exists():
        return set()
    return set(shm_dir.glob("dw_*"))


def _cleanup_new_shm(before: set[Path]) -> None:
    for path in _deepwater_shm_files() - before:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


@pytest.fixture(autouse=True)
def cleanup_deepwater_shm():
    before = _deepwater_shm_files()
    try:
        yield
    finally:
        _cleanup_new_shm(before)


@pytest.fixture
def base_path(tmp_path: Path) -> Path:
    return tmp_path / "platform"


@pytest.fixture
def repo_root() -> Path:
    return PROJECT_ROOT


@pytest.fixture(autouse=True)
def subprocess_pythonpath(monkeypatch: pytest.MonkeyPatch) -> None:
    current = os.environ.get("PYTHONPATH")
    value = str(PROJECT_ROOT) if not current else f"{PROJECT_ROOT}:{current}"
    monkeypatch.setenv("PYTHONPATH", value)


@pytest.fixture
def trade_feed_spec() -> dict:
    return {
        "feed_name": "trades",
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "price", "type": "float64"},
            {"name": "size", "type": "float64"},
        ],
        "clock_level": 1,
        "persist": True,
        "chunk_size_mb": 1,
    }
