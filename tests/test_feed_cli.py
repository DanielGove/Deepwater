#!/usr/bin/env python3
"""Tests for deepwater-create-feed and deepwater-delete-feed CLIs."""
import sys
import tempfile
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform
from deepwater.cli.feed_cli import create_main, delete_main


def _spec(name: str, persist: bool = True) -> dict:
    return {
        "feed_name": name,
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "v", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": persist,
        "chunk_size_mb": 1,
    }


def test_create_cli_idempotent_from_single_config():
    with tempfile.TemporaryDirectory(prefix="dw-cli-create-") as td:
        root = Path(td)
        base = root / "platform"
        cfg = root / "trades.json"
        cfg.write_bytes(orjson.dumps(_spec("trades")))

        rc1 = create_main(["--base-path", str(base), "--config", str(cfg)])
        rc2 = create_main(["--base-path", str(base), "--config", str(cfg)])
        assert rc1 == 0
        assert rc2 == 0

        p = Platform(str(base))
        feeds = p.list_feeds()
        assert feeds == ["trades"], f"expected one feed, got {feeds}"
        p.close()


def test_create_cli_supports_bundle_and_config_dir():
    with tempfile.TemporaryDirectory(prefix="dw-cli-bundle-") as td:
        root = Path(td)
        base = root / "platform"
        cfg_dir = root / "configs"
        cfg_dir.mkdir(parents=True, exist_ok=True)

        (cfg_dir / "a.json").write_bytes(orjson.dumps(_spec("a")))
        (cfg_dir / "bundle.json").write_bytes(
            orjson.dumps({"feeds": [_spec("b"), _spec("c", persist=False)]})
        )

        rc = create_main(["--base-path", str(base), "--config-dir", str(cfg_dir)])
        assert rc == 0

        p = Platform(str(base))
        feeds = set(p.list_feeds())
        assert feeds == {"a", "b", "c"}, f"unexpected feeds: {feeds}"
        p.close()


def test_delete_cli_from_feed_names_and_config():
    with tempfile.TemporaryDirectory(prefix="dw-cli-delete-") as td:
        root = Path(td)
        base = root / "platform"
        cfg = root / "delete.json"
        cfg.write_bytes(orjson.dumps(_spec("b")))

        p = Platform(str(base))
        p.create_feed(_spec("a"))
        p.create_feed(_spec("b"))
        p.close()

        rc = delete_main([
            "--base-path", str(base),
            "--feed", "a",
            "--config", str(cfg),
        ])
        assert rc == 0

        p = Platform(str(base))
        assert not p.feed_exists("a")
        assert not p.feed_exists("b")
        p.close()


def test_delete_cli_strict_missing_fails():
    with tempfile.TemporaryDirectory(prefix="dw-cli-missing-") as td:
        base = Path(td) / "platform"
        rc = delete_main([
            "--base-path", str(base),
            "--feed", "missing",
            "--strict-missing",
        ])
        assert rc == 1


def run_tests():
    tests = [
        ("create_cli_idempotent_from_single_config", test_create_cli_idempotent_from_single_config),
        ("create_cli_supports_bundle_and_config_dir", test_create_cli_supports_bundle_and_config_dir),
        ("delete_cli_from_feed_names_and_config", test_delete_cli_from_feed_names_and_config),
        ("delete_cli_strict_missing_fails", test_delete_cli_strict_missing_fails),
    ]
    print("Feed CLI Tests")
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
