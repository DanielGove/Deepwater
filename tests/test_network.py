#!/usr/bin/env python3
"""Tests for Deepwater networking v0."""
from __future__ import annotations

import socket
import sys
import tempfile
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import deepwater as dw
from deepwater import Platform
from deepwater.network.agent import make_server
from deepwater.network.client import RemoteReader
from deepwater.network.path import parse_target, resolve_agent_path
from deepwater.network.protocol import decode_frame, encode_frame, read_frame, write_frame


def test_path_parser():
    local = parse_target("/deepwater/data/local")
    assert not local.is_remote
    assert local.path == "/deepwater/data/local"

    remote = parse_target("deepwater-pioneer:/deepwater/data/hyperliquid-node")
    assert remote.is_remote
    assert remote.host == "deepwater-pioneer"
    assert remote.port == 7447
    assert remote.path == "/deepwater/data/hyperliquid-node"

    url = parse_target("dw://deepwater-pioneer:7448/deepwater/data/hyperliquid-node")
    assert url.is_remote
    assert url.host == "deepwater-pioneer"
    assert url.port == 7448
    assert url.path == "/deepwater/data/hyperliquid-node"


def test_agent_path_guard():
    with tempfile.TemporaryDirectory(prefix="dw-net-root-") as td:
        root = Path(td)
        inside = root / "node"
        inside.mkdir()
        assert resolve_agent_path(root, inside) == inside.resolve()
        assert resolve_agent_path(root, "node") == inside.resolve()
        try:
            resolve_agent_path(root, root.parent)
        except ValueError:
            pass
        else:
            raise AssertionError("outside path was not rejected")


def test_protocol_round_trip():
    payload = b"\x00\x01payload"
    frame = encode_frame({"op": "PING", "id": 7}, payload)
    header, decoded_payload = decode_frame(frame)
    assert header == {"op": "PING", "id": 7}
    assert decoded_payload == payload

    left, right = socket.socketpair()
    try:
        write_frame(left, {"op": "DATA", "id": 8, "count": 1}, payload)
        header, decoded_payload = read_frame(right)
        assert header["op"] == "DATA"
        assert header["id"] == 8
        assert decoded_payload == payload
    finally:
        left.close()
        right.close()


def test_remote_reader_range_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-") as td:
        base = Path(td) / "node"
        p = Platform(str(base))
        p.create_feed({
            "feed_name": "events",
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "value", "type": "uint64"},
            ],
            "clock_level": 1,
            "persist": False,
            "ring_size_mb": 1,
        })
        writer = p.create_writer("events")
        start = 1_700_000_000_000_000
        for i in range(5):
            writer.write_values(start + i * 10, i)

        server = make_server(Path(td), ("127.0.0.1", 0))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = int(server.server_address[1])
            rr = RemoteReader("127.0.0.1", str(base), "events", port=port, timeout=2.0)
            try:
                records = rr.range(start, start + 50)
                assert records == [(start + i * 10, i) for i in range(5)]
                as_dict = rr.range(start, start + 20, format="dict")
                assert as_dict == [
                    {"ts": start, "value": 0},
                    {"ts": start + 10, "value": 1},
                ]
                raw = rr.range(start, start + 10, format="raw")
                assert len(raw) == rr.record_size
            finally:
                rr.close()

            with dw.reader(f"dw://127.0.0.1:{port}{base}", stream="events", timeout=2.0) as helper:
                records = helper.range(start, start + 20)
                assert records == [(start, 0), (start + 10, 1)]
        finally:
            server.shutdown()
            server.server_close()
            p.close()


def run_tests():
    tests = [
        ("path_parser", test_path_parser),
        ("agent_path_guard", test_agent_path_guard),
        ("protocol_round_trip", test_protocol_round_trip),
        ("remote_reader_range_loopback", test_remote_reader_range_loopback),
    ]
    print("Network Tests")
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
