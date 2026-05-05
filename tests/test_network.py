#!/usr/bin/env python3
"""Tests for Deepwater networking v0."""
from __future__ import annotations

import socket
import sys
import tempfile
import threading
import io
import struct
import time
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import deepwater as dw
import deepwater.network.protocol as protocol
import orjson
from deepwater import Platform
from deepwater.cli.datasets_cli import main as datasets_cli_main
from deepwater.cli.feeds_cli import main as feeds_cli_main
from deepwater.cli.segments_cli import main as segments_cli_main
from deepwater.network.agent import make_server
from deepwater.network.client import RemoteReader, RemoteReaderError
from deepwater.network.path import parse_target, resolve_agent_path
from deepwater.network.protocol import ProtocolError, decode_frame, encode_frame, read_frame, write_frame


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
        root = Path(td) / "root"
        root.mkdir()
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

        outside = Path(td) / "outside"
        outside.mkdir()
        symlink = root / "escape"
        try:
            symlink.symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError):
            return
        try:
            resolve_agent_path(root, symlink)
        except ValueError:
            pass
        else:
            raise AssertionError("symlink escape path was not rejected")


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


def test_protocol_rejects_oversized_frames_without_payload_alloc():
    old_max_frame = protocol.MAX_FRAME_BYTES
    old_max_header = protocol.MAX_HEADER_BYTES
    try:
        protocol.MAX_FRAME_BYTES = 32
        try:
            encode_frame({"op": "DATA"}, b"x" * 64)
            raise AssertionError("oversized encoded frame was not rejected")
        except ProtocolError:
            pass

        too_large_prefix = (33).to_bytes(8, "big") + (2).to_bytes(4, "big") + b"{}" + b"x" * 31
        try:
            decode_frame(too_large_prefix)
            raise AssertionError("oversized decoded frame was not rejected")
        except ProtocolError:
            pass

        protocol.MAX_FRAME_BYTES = old_max_frame
        protocol.MAX_HEADER_BYTES = 8
        try:
            encode_frame({"operation": "header-too-large"})
            raise AssertionError("oversized header was not rejected")
        except ProtocolError:
            pass
    finally:
        protocol.MAX_FRAME_BYTES = old_max_frame
        protocol.MAX_HEADER_BYTES = old_max_header


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
                expected_raw = struct.pack("<QQ", start, 0)
            finally:
                rr.close()
            assert bytes(raw) == expected_raw

            with dw.reader(f"dw://127.0.0.1:{port}{base}", stream="events", timeout=2.0) as helper:
                records = helper.range(start, start + 20)
                assert records == [(start, 0), (start + 10, 1)]
        finally:
            server.shutdown()
            server.server_close()
            p.close()


def test_remote_reader_ts_key_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-ts-key-") as td:
        base = Path(td) / "node"
        p = Platform(str(base))
        p.create_feed({
            "feed_name": "events",
            "mode": "UF",
            "fields": [
                {"name": "event_ts", "type": "uint64"},
                {"name": "recv_ts", "type": "uint64"},
                {"name": "value", "type": "uint64"},
            ],
            "clock_level": 2,
            "persist": True,
            "chunk_size_mb": 1,
        })
        writer = p.create_writer("events")
        start = int(time.time() * 1_000_000) - 10_000
        for i in range(5):
            writer.write_values(start + i * 10, start + 1_000 + i * 100, i)
        writer.close()

        server = make_server(Path(td), ("127.0.0.1", 0))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = int(server.server_address[1])
            rr = RemoteReader("127.0.0.1", str(base), "events", port=port, timeout=2.0)
            try:
                by_recv = rr.range(start + 1_100, start + 1_400, ts_key="recv_ts")
                assert by_recv == [
                    (start + 10, start + 1_100, 1),
                    (start + 20, start + 1_200, 2),
                    (start + 30, start + 1_300, 3),
                ]
                batches = list(rr.range_batches(start + 1_100, start + 1_500, ts_key="recv_ts", batch_records=2))
                assert [len(batch) for batch in batches] == [2, 2]
                assert [record[2] for batch in batches for record in batch] == [1, 2, 3, 4]
                latest = rr.latest(1_000_000, ts_key="recv_ts")
                assert latest[-1] == (start + 40, start + 1_400, 4)
                try:
                    rr.range(start, start + 1, ts_key="missing_ts")
                    raise AssertionError("invalid remote ts_key was not rejected")
                except RemoteReaderError:
                    pass
            finally:
                rr.close()
        finally:
            server.shutdown()
            server.server_close()
            p.close()


def test_remote_reader_range_batches_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-paged-") as td:
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
            "persist": True,
            "chunk_size_mb": 1,
        })
        writer = p.create_writer("events")
        start = 1_700_000_000_000_000
        for i in range(7):
            writer.write_values(start + i * 10, i)
        writer.close()

        direct = p.create_reader("events")
        direct_batches = list(direct.range_batches(start, start + 70, batch_records=4))
        assert [len(batch) for batch in direct_batches] == [4, 3]
        boundary_batches = list(direct.range_batches(start, start + 20, batch_records=10))
        assert boundary_batches == [[(start, 0), (start + 10, 1)]]
        p.close_reader("events")

        with dw.reader(str(base), stream="events") as local:
            local_batches = list(local.range_batches(start, start + 70, batch_records=4))
            assert [len(batch) for batch in local_batches] == [4, 3]

        server = make_server(Path(td), ("127.0.0.1", 0))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = int(server.server_address[1])
            rr = RemoteReader("127.0.0.1", str(base), "events", port=port, timeout=2.0)
            try:
                assert rr.protocol_version == 1
                assert rr.capabilities["paged_range"] is True
                assert rr.format == "<QQ"
                assert rr.field_names == ("ts", "value")
                assert rr.record_format["record_size"] == rr.record_size
                batches = list(rr.range_batches(start, start + 70, batch_records=3))
                assert [len(batch) for batch in batches] == [3, 3, 1]
                assert [record for batch in batches for record in batch] == [
                    (start + i * 10, i) for i in range(7)
                ]
                raw_batches = list(rr.range_batches(start, start + 19, format="raw", batch_records=1))
                assert [len(batch) for batch in raw_batches] == [rr.record_size, rr.record_size]
            finally:
                rr.close()
        finally:
            server.shutdown()
            server.server_close()
            p.close()


def test_remote_reader_read_available_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-available-") as td:
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
        writer.write_values(start, 0)

        server = make_server(Path(td), ("127.0.0.1", 0))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = int(server.server_address[1])
            rr = RemoteReader("127.0.0.1", str(base), "events", port=port, timeout=2.0)
            try:
                assert rr.read_available() == []
                writer.write_values(start + 10, 1)
                writer.write_values(start + 20, 2)
                assert rr.read_available(max_records=1) == [(start + 10, 1)]
                assert rr.read_available() == [(start + 20, 2)]
            finally:
                rr.close()
        finally:
            server.shutdown()
            server.server_close()
            p.close()


def test_remote_stream_heartbeat_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-heartbeat-") as td:
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
        writer.write_values(1_700_000_000_000_000, 0)

        server = make_server(
            Path(td),
            ("127.0.0.1", 0),
            heartbeat_interval=0.05,
            stream_poll_interval=0.005,
            idle_timeout=2.0,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = int(server.server_address[1])
            sock = socket.create_connection(("127.0.0.1", port), timeout=1.0)
            sock.settimeout(1.0)
            try:
                write_frame(sock, {"op": "HELLO", "id": 1, "version": 1})
                header, _payload = read_frame(sock)
                assert header["op"] == "HELLO"
                assert header["capabilities"]["heartbeat"] is True

                write_frame(sock, {
                    "op": "OPEN_READER",
                    "id": 2,
                    "path": str(base),
                    "feed_name": "events",
                })
                header, _payload = read_frame(sock)
                assert header["op"] == "READER_OPEN"

                write_frame(sock, {
                    "op": "SUBSCRIBE_LIVE",
                    "id": 3,
                    "start": None,
                    "playback": False,
                })
                header, _payload = read_frame(sock)
                assert header["op"] == "SUBSCRIBED"
                assert header["heartbeat_interval_s"] == 0.05

                header, payload = read_frame(sock)
                assert header["op"] == "HEARTBEAT"
                assert header["id"] == 3
                assert payload == b""
                assert "server_time_us" in header
            finally:
                sock.close()
        finally:
            server.shutdown()
            server.server_close()
            p.close()


def test_local_ring_reader_range_batches_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-ring-batches-") as td:
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

        try:
            reader = p.create_reader("events")
            batches = list(reader.range_batches(start, start + 50, batch_records=2))
            assert [len(batch) for batch in batches] == [2, 2, 1]
            assert [record for batch in batches for record in batch] == [
                (start + i * 10, i) for i in range(5)
            ]
            raw_batches = list(reader.range_batches(start, start + 20, format="raw", batch_records=1))
            assert [len(batch) for batch in raw_batches] == [reader._rec_size, reader._rec_size]
            assert b"".join(bytes(batch) for batch in raw_batches) == struct.pack(
                "<QQQQ",
                start,
                0,
                start + 10,
                1,
            )
        finally:
            p.close()


def test_remote_platform_metadata_and_reader_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-platform-") as td:
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
            "persist": True,
            "chunk_size_mb": 1,
        })
        writer = p.create_writer("events")
        start = 1_700_000_000_000_000
        for i in range(3):
            writer.write_values(start + i * 10, i)
        writer.close()

        server = make_server(Path(td), ("127.0.0.1", 0))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = int(server.server_address[1])
            remote = Platform(f"dw://127.0.0.1:{port}{base}")
            try:
                assert remote.feed_exists("events") is True
                assert "events" in remote.list_feeds()
                assert remote.lifecycle("events")["persist"] is True
                assert remote.get_record_format("events")["fmt"] == "<QQ"
                assert remote.describe_feed("events")["record_size"] == 16
                assert isinstance(remote.list_segments("events"), list)
                assert remote.suggested_reader_range("events") is not None
                reader = remote.create_reader("events")
                try:
                    assert reader.range(start, start + 30) == [
                        (start, 0),
                        (start + 10, 1),
                        (start + 20, 2),
                    ]
                finally:
                    reader.close()
            finally:
                remote.close()
        finally:
            server.shutdown()
            server.server_close()
            p.close()


def test_remote_platform_close_closes_created_readers():
    with tempfile.TemporaryDirectory(prefix="dw-net-platform-close-") as td:
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
            "persist": True,
            "chunk_size_mb": 1,
        })
        writer = p.create_writer("events")
        start = 1_700_000_000_000_000
        writer.write_values(start, 1)
        writer.close()

        server = make_server(Path(td), ("127.0.0.1", 0))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = int(server.server_address[1])
            remote = Platform(f"dw://127.0.0.1:{port}{base}")
            reader = remote.create_reader("events")
            assert reader.range(start, start + 10) == [(start, 1)]
            remote.close()
            try:
                reader.range(start, start + 10)
                raise AssertionError("reader should be closed by remote platform close")
            except RemoteReaderError:
                pass
        finally:
            server.shutdown()
            server.server_close()
            p.close()


def test_remote_metadata_clis_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-cli-") as td:
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
            "persist": True,
            "chunk_size_mb": 1,
        })
        writer = p.create_writer("events")
        start = 1_700_000_000_000_000
        for i in range(5):
            writer.write_values(start + i * 10, i)
        writer.close()

        server = make_server(Path(td), ("127.0.0.1", 0))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = int(server.server_address[1])
            remote_base = f"dw://127.0.0.1:{port}{base}"

            out = io.StringIO()
            with redirect_stdout(out):
                rc = feeds_cli_main(["--base-path", remote_base, "--feed", "events", "--json"])
            assert rc == 0
            feed_payload = orjson.loads(out.getvalue().encode("utf-8"))
            assert feed_payload["feed_name"] == "events"
            assert feed_payload["record_fmt"] == "<QQ"

            out = io.StringIO()
            with redirect_stdout(out):
                rc = segments_cli_main([
                    "--base-path", remote_base,
                    "--feed", "events",
                    "--status", "usable",
                    "--json",
                ])
            assert rc == 0
            segments_payload = orjson.loads(out.getvalue().encode("utf-8"))
            assert segments_payload["feed"] == "events"
            assert segments_payload["segments"]

            out = io.StringIO()
            with redirect_stdout(out):
                rc = datasets_cli_main([
                    "--base-path", remote_base,
                    "--feed", "events",
                    "--json",
                ])
            assert rc == 0
            datasets_payload = orjson.loads(out.getvalue().encode("utf-8"))
            assert datasets_payload["base_path"] == remote_base
            assert datasets_payload["feeds"] == ["events"]
            assert datasets_payload["intervals"]
        finally:
            server.shutdown()
            server.server_close()
            p.close()


def run_tests():
    tests = [
        ("path_parser", test_path_parser),
        ("agent_path_guard", test_agent_path_guard),
        ("protocol_round_trip", test_protocol_round_trip),
        ("protocol_rejects_oversized_frames_without_payload_alloc", test_protocol_rejects_oversized_frames_without_payload_alloc),
        ("remote_reader_range_loopback", test_remote_reader_range_loopback),
        ("remote_reader_ts_key_loopback", test_remote_reader_ts_key_loopback),
        ("remote_reader_range_batches_loopback", test_remote_reader_range_batches_loopback),
        ("remote_reader_read_available_loopback", test_remote_reader_read_available_loopback),
        ("remote_stream_heartbeat_loopback", test_remote_stream_heartbeat_loopback),
        ("local_ring_reader_range_batches_loopback", test_local_ring_reader_range_batches_loopback),
        ("remote_platform_metadata_and_reader_loopback", test_remote_platform_metadata_and_reader_loopback),
        ("remote_platform_close_closes_created_readers", test_remote_platform_close_closes_created_readers),
        ("remote_metadata_clis_loopback", test_remote_metadata_clis_loopback),
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
