#!/usr/bin/env python3
"""Regression: websocket disconnect should close writers and segments."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "tests"))

from deepwater import Platform
import websocket_client as ws_client
from websocket_client import MarketDataEngine


def _test_trade_spec(pid: str) -> dict:
    return {
        "feed_name": f"CB-TRADES-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "recv_us", "type": "uint64"},
            {"name": "price", "type": "float64"},
            {"name": "size", "type": "float64"},
        ],
        "clock_level": 1,
        "persist": True,
        "chunk_size_mb": 1,
    }


def _test_l2_spec(pid: str) -> dict:
    return {
        "feed_name": f"CB-L2-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "recv_us", "type": "uint64"},
            {"name": "price", "type": "float64"},
            {"name": "qty", "type": "float64"},
        ],
        "clock_level": 1,
        "persist": True,
        "chunk_size_mb": 1,
    }


def _write_trade(writer, ts: int) -> int:
    return writer.write_values(
        ts,         # recv_us
        100.0,      # price
        1.0,        # size
    )


def test_disconnect_closes_segments_and_reopens_writers():
    with tempfile.TemporaryDirectory(prefix="dw-ws-disconnect-") as td:
        base = Path(td) / "platform"
        pid = "BTC-USD"
        feed_name = f"CB-TRADES-{pid}"

        engine = MarketDataEngine(sample_size=4)
        old_trade_spec = ws_client.trades_spec
        old_l2_spec = ws_client.l2_spec
        # Replace default platform path and feed specs with test-local config.
        engine.platform.close()
        engine.platform = Platform(str(base))
        engine.trade_writers.clear()
        engine.book_writers.clear()
        engine.product_ids.clear()
        ws_client.trades_spec = _test_trade_spec
        ws_client.l2_spec = _test_l2_spec

        try:
            engine.subscribe(pid)
            w1 = engine.trade_writers[pid]
            assert _write_trade(w1, 1_000_000) > 0

            # Simulate websocket disconnect finalizer path.
            engine._close_all_writers()

            segs = engine.platform.list_segments(feed_name)
            assert segs, "expected at least one segment after write"
            assert segs[-1]["status"] == "closed", f"expected closed segment, got {segs[-1]}"

            # Re-subscribe should create a fresh writer that can ingest again.
            engine.subscribe(pid)
            w2 = engine.trade_writers[pid]
            assert w2 is not w1
            assert _write_trade(w2, 2_000_000) > 0
        finally:
            ws_client.trades_spec = old_trade_spec
            ws_client.l2_spec = old_l2_spec
            engine.stop()


def run_tests():
    tests = [
        ("disconnect_closes_segments_and_reopens_writers", test_disconnect_closes_segments_and_reopens_writers),
    ]
    print("Websocket Disconnect Segment Tests")
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
