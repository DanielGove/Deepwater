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

sys.path.insert(0, str(Path(__file__).parent.parent))

import deepwater.network.protocol as protocol
import orjson
import deepwater as dw
from deepwater import Reader, Writer, create_feed, delete_feed
from deepwater.cli.datasets_cli import main as datasets_cli_main
from deepwater.cli.feeds_cli import main as feeds_cli_main
from deepwater.cli.segments_cli import main as segments_cli_main
from deepwater.metadata.feed_schema import FeedSchema
from deepwater.network.agent import make_server
from deepwater.network.client import RemoteReader, RemoteReaderError, remote_metadata
from deepwater.network.path import parse_target, resolve_agent_path
from deepwater.network.protocol import (
    Op,
    ProtocolError,
    decode_frame,
    encode_frame,
    frame,
    read_frame,
    write_frame,
)


def _create_events_feed(
    base: Path,
    *,
    fields: list[dict] | None = None,
    clock_level: int = 1,
    persist: bool = True,
    ring_size_mb: float | None = None,
    chunk_size_mb: float = 1,
) -> None:
    spec = {
        "feed_name": "events",
        "mode": "UF",
        "fields": fields or [
            {"name": "ts", "type": "uint64"},
            {"name": "value", "type": "uint64"},
        ],
        "clock_level": clock_level,
        "persist": persist,
        "chunk_size_mb": chunk_size_mb,
    }
    if ring_size_mb is not None:
        spec["ring_size_mb"] = ring_size_mb
    create_feed(base, spec)


def _close_all(*objects) -> None:
    for obj in objects:
        try:
            if obj is not None:
                obj.close()
        except Exception:
            pass


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
    encoded = encode_frame(frame(Op.PING, 7), payload)
    header, decoded_payload = decode_frame(encoded)
    assert header.op == Op.PING
    assert header.id == 7
    assert header.args is None
    assert decoded_payload == payload

    left, right = socket.socketpair()
    try:
        write_frame(left, frame(Op.DATA, 8, (16, 1, 0, False)), payload)
        header, decoded_payload = read_frame(right)
        assert header.op == Op.DATA
        assert header.id == 8
        assert header.args[1] == 1
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
            encode_frame(frame(Op.DATA, 1, (1, 64, 0, False)), b"x" * 64)
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
            encode_frame(frame(Op.DATA, 1, (1, 1, 123456789, False)))
            raise AssertionError("oversized header was not rejected")
        except ProtocolError:
            pass
    finally:
        protocol.MAX_FRAME_BYTES = old_max_frame
        protocol.MAX_HEADER_BYTES = old_max_header


def test_remote_reader_range_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-") as td:
        base = Path(td) / "node"
        _create_events_feed(base, persist=False, ring_size_mb=1)
        writer = Writer(base, "events")
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
                assert isinstance(rr.layout, FeedSchema)
                assert isinstance(rr.record_format, FeedSchema)
                assert rr.layout.record_size == 16
                description = rr.describe()
                assert description["record_size"] == 16
                assert description["record_fmt"] == "<QQ"
                assert rr.fields == (
                    {"name": "ts", "type": "uint64", "offset": 0, "size": 8},
                    {"name": "value", "type": "uint64", "offset": 8, "size": 8},
                )
                assert rr.timestamp_fields == (
                    {"name": "ts", "type": "uint64", "offset": 0, "size": 8},
                )
                assert rr.is_persistent is False
                state = rr.state()
                assert state["record_count"] >= 5
                assert rr.first_after(start + 1) == (start + 10, 1)
                assert rr.first_before(start + 25) == (start + 20, 2)
                assert rr.first_before(start - 1) is None
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
                assert bytes(rr.range(start, start + 10, format="raw")) == expected_raw
                arr = rr.range(start, start + 20, format="numpy")
                assert arr.shape == (2,)
                assert arr["value"].tolist() == [0, 1]
                cols = rr.range_columns(start, start + 30, ["value"])
                assert cols["value"].tolist() == [0, 1, 2]
            finally:
                rr.close()
            assert bytes(raw) == expected_raw

            remote_url = f"dw://127.0.0.1:{port}{base}"
            with Reader(remote_url, "events", timeout=2.0) as helper:
                records = helper.range(start, start + 20)
                assert records == [(start, 0), (start + 10, 1)]
        finally:
            server.shutdown()
            server.server_close()
            _close_all(writer)
            delete_feed(base, "events", missing_ok=True)


def test_remote_reader_ts_key_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-ts-key-") as td:
        base = Path(td) / "node"
        _create_events_feed(
            base,
            fields=[
                {"name": "event_ts", "type": "uint64"},
                {"name": "recv_ts", "type": "uint64"},
                {"name": "value", "type": "uint64"},
            ],
            clock_level=2,
            persist=True,
            chunk_size_mb=1,
        )
        writer = Writer(base, "events")
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
                tail = rr.range(start + 1_300, start + 1_500, ts_key="recv_ts")
                assert tail[-1] == (start + 40, start + 1_400, 4)
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
            _close_all(writer)
            delete_feed(base, "events", missing_ok=True)


def test_remote_reader_range_batches_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-paged-") as td:
        base = Path(td) / "node"
        _create_events_feed(base, persist=True, chunk_size_mb=1)
        writer = Writer(base, "events")
        start = 1_700_000_000_000_000
        for i in range(7):
            writer.write_values(start + i * 10, i)
        writer.close()

        direct = Reader(base, "events")
        direct_batches = list(direct.range_batches(start, start + 70, batch_records=4))
        assert [len(batch) for batch in direct_batches] == [4, 3]
        boundary_batches = list(direct.range_batches(start, start + 20, batch_records=10))
        assert boundary_batches == [[(start, 0), (start + 10, 1)]]
        direct.close()

        with Reader(str(base), "events") as local:
            local_batches = list(local.range_batches(start, start + 70, batch_records=4))
            assert [len(batch) for batch in local_batches] == [4, 3]
            assert local.first_after(start + 1) == (start + 10, 1)
            assert local.first_before(start + 25) == (start + 20, 2)

        server = make_server(Path(td), ("127.0.0.1", 0))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = int(server.server_address[1])
            rr = RemoteReader("127.0.0.1", str(base), "events", port=port, timeout=2.0)
            try:
                assert rr.protocol_version == protocol.PROTOCOL_VERSION
                assert rr.capabilities["paged_range"] is True
                assert rr.format == "<QQ"
                assert rr.field_names == ("ts", "value")
                assert rr.record_format.record_size == rr.record_size
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
            delete_feed(base, "events", missing_ok=True)


def test_remote_reader_read_available_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-available-") as td:
        base = Path(td) / "node"
        _create_events_feed(base, persist=False, ring_size_mb=1)
        writer = Writer(base, "events")
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
            _close_all(writer)
            delete_feed(base, "events", missing_ok=True)


def test_remote_stream_with_start_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-stream-start-") as td:
        base = Path(td) / "node"
        _create_events_feed(base, persist=True)
        writer = Writer(base, "events")
        start = 1_700_000_000_000_000
        for i in range(3):
            writer.write_values(start + i * 10, i)
        writer.close()

        server = make_server(Path(td), ("127.0.0.1", 0), heartbeat_interval=0.1)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = int(server.server_address[1])
            rr = RemoteReader("127.0.0.1", str(base), "events", port=port, timeout=2.0)
            stream = rr.stream(start=start + 10)
            try:
                assert next(stream) == (start + 10, 1)
                assert next(stream) == (start + 20, 2)
            finally:
                stream.close()
                rr.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)


def test_remote_stream_heartbeat_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-heartbeat-") as td:
        base = Path(td) / "node"
        _create_events_feed(base, persist=False, ring_size_mb=1)
        writer = Writer(base, "events")
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
                write_frame(sock, frame(Op.HELLO, 1, (1,)))
                header, _payload = read_frame(sock)
                assert header.op == Op.HELLO
                assert header.args[1]["heartbeat"] is True

                write_frame(sock, frame(Op.OPEN_READER, 2, (str(base), "events")))
                header, _payload = read_frame(sock)
                assert header.op == Op.READER_OPEN

                write_frame(sock, frame(Op.SUBSCRIBE_LIVE, 3))
                header, _payload = read_frame(sock)
                assert header.op == Op.SUBSCRIBED
                assert header.args[2] == 0.05

                header, payload = read_frame(sock)
                assert header.op == Op.HEARTBEAT
                assert header.id == 3
                assert payload == b""
                assert header.args[1] > 0
            finally:
                sock.close()
        finally:
            server.shutdown()
            server.server_close()
            _close_all(writer)
            delete_feed(base, "events", missing_ok=True)


def test_local_live_ring_range_batches_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-ring-batches-") as td:
        base = Path(td) / "node"
        _create_events_feed(base, persist=False, ring_size_mb=1)
        writer = Writer(base, "events")
        start = 1_700_000_000_000_000
        for i in range(5):
            writer.write_values(start + i * 10, i)

        reader = None
        try:
            reader = Reader(base, "events")
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
            _close_all(reader, writer)
            delete_feed(base, "events", missing_ok=True)


def test_remote_metadata_and_reader_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-platform-") as td:
        base = Path(td) / "node"
        _create_events_feed(base, persist=True, chunk_size_mb=1)
        writer = Writer(base, "events")
        start = 1_700_000_000_000_000
        for i in range(3):
            writer.write_values(start + i * 10, i)
        writer.close()

        server = make_server(Path(td), ("127.0.0.1", 0))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = int(server.server_address[1])
            remote_base = f"dw://127.0.0.1:{port}{base}"
            assert remote_metadata(remote_base, Op.FEED_EXISTS, "events") is True
            assert "events" in dw.list_feeds(remote_base)
            assert dw.feed_metadata(remote_base, "events")["persist"] is True
            assert dw.get_record_format(remote_base, "events")["fmt"] == "<QQ"
            assert dw.describe_feed(remote_base, "events")["record_size"] == 16
            assert isinstance(dw.list_segments(remote_base, "events"), list)
            assert dw.suggested_reader_range(remote_base, "events") is not None
            with Reader(remote_base, "events", timeout=2.0) as reader:
                assert reader.range(start, start + 30) == [
                    (start, 0),
                    (start + 10, 1),
                    (start + 20, 2),
                ]
        finally:
            server.shutdown()
            server.server_close()
            delete_feed(base, "events", missing_ok=True)


def test_remote_metadata_clis_loopback():
    with tempfile.TemporaryDirectory(prefix="dw-net-cli-") as td:
        base = Path(td) / "node"
        _create_events_feed(base, persist=True, chunk_size_mb=1)
        writer = Writer(base, "events")
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
            delete_feed(base, "events", missing_ok=True)


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
        ("remote_stream_with_start_loopback", test_remote_stream_with_start_loopback),
        ("remote_stream_heartbeat_loopback", test_remote_stream_heartbeat_loopback),
        ("local_live_ring_range_batches_loopback", test_local_live_ring_range_batches_loopback),
        ("remote_metadata_and_reader_loopback", test_remote_metadata_and_reader_loopback),
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
