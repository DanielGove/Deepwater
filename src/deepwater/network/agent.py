from __future__ import annotations

import argparse
import itertools
import logging
import socket
import socketserver
import time
from pathlib import Path
from typing import Any, Optional

from deepwater import Platform

from .path import DEFAULT_PORT, resolve_agent_path
from .protocol import (
    DEFAULT_MAX_BATCH_RECORDS,
    DEFAULT_HEARTBEAT_INTERVAL_S,
    DEFAULT_IDLE_TIMEOUT_S,
    DEFAULT_STREAM_POLL_INTERVAL_S,
    MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    SERVER_CAPABILITIES,
    SERVER_LIMITS,
    ProtocolError,
    read_frame,
    write_frame,
)


log = logging.getLogger("dw.network.agent")

DW_REMOTE_AGENT_STARTED = "DW_REMOTE_AGENT_STARTED"
DW_REMOTE_AGENT_CONNECTED = "DW_REMOTE_AGENT_CONNECTED"
DW_REMOTE_AGENT_DISCONNECTED = "DW_REMOTE_AGENT_DISCONNECTED"
DW_REMOTE_AGENT_HEARTBEAT = "DW_REMOTE_AGENT_HEARTBEAT"
DW_REMOTE_AGENT_REJECTED_PATH = "DW_REMOTE_AGENT_REJECTED_PATH"

_SESSION_IDS = itertools.count(1)


def _configure_agent_socket(sock: socket.socket, *, idle_timeout: float) -> None:
    sock.settimeout(float(idle_timeout))
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if hasattr(socket, "TCP_KEEPIDLE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
    if hasattr(socket, "TCP_KEEPINTVL"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
    if hasattr(socket, "TCP_KEEPCNT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)


def _raw_payload(value) -> bytes:
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if not value:
        return b""
    return b"".join(bytes(item) for item in value)


def _reader_range(reader, start: int, end: int, *, playback: bool, ts_key: str | None):
    try:
        return reader.range(start, end, format="raw", playback=playback, ts_key=ts_key)
    except TypeError:
        if playback or ts_key:
            raise
        return reader.range(start, end, format="raw")


def _reader_latest(reader, seconds: float, *, ts_key: str | None):
    try:
        return reader.latest(seconds, format="raw", ts_key=ts_key)
    except TypeError:
        if ts_key:
            raise
        return reader.latest(seconds, format="raw")


def _reader_read_available(reader, *, max_records: int | None):
    try:
        return reader.read_available(max_records=max_records, format="raw")
    except TypeError:
        if max_records is not None:
            raise
        return reader.read_available(format="raw")


def _reader_range_batches(
    reader,
    start: int,
    end: int,
    *,
    playback: bool,
    ts_key: str | None,
    batch_records: int,
):
    if hasattr(reader, "iter_raw_range"):
        try:
            yield from reader.iter_raw_range(
                start,
                end,
                playback=playback,
                ts_key=ts_key,
                batch_records=batch_records,
            )
        except TypeError:
            if playback or ts_key:
                raise
            yield from reader.iter_raw_range(
                start,
                end,
                batch_records=batch_records,
            )
        return

    if hasattr(reader, "range_batches"):
        try:
            yield from reader.range_batches(
                start,
                end,
                format="raw",
                playback=playback,
                ts_key=ts_key,
                batch_records=batch_records,
            )
        except TypeError:
            if playback or ts_key:
                raise
            yield from reader.range_batches(
                start,
                end,
                format="raw",
                batch_records=batch_records,
            )
        return

    raw = memoryview(_reader_range(reader, start, end, playback=playback, ts_key=ts_key)).cast("B")
    record_size = int(getattr(reader, "record_size", 0) or reader.record_format["record_size"])
    page_bytes = int(batch_records) * record_size
    for pos in range(0, raw.nbytes, page_bytes):
        yield raw[pos:pos + page_bytes]


def _reader_stream(reader, *, start: Optional[int], playback: bool, ts_key: str | None):
    try:
        return reader.stream(start=start, format="raw", playback=playback, ts_key=ts_key)
    except TypeError:
        if start is not None or playback or ts_key:
            raise
        return reader.stream(format="raw")


def _bounded_batch_size(requested: Any) -> int:
    batch_records = int(requested or DEFAULT_MAX_BATCH_RECORDS)
    if batch_records <= 0:
        raise ValueError("batch_records must be positive")
    return min(batch_records, DEFAULT_MAX_BATCH_RECORDS)


class DeepwaterAgentServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address,
        handler_class,
        *,
        root: str | Path,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL_S,
        stream_poll_interval: float = DEFAULT_STREAM_POLL_INTERVAL_S,
        idle_timeout: float = DEFAULT_IDLE_TIMEOUT_S,
    ):
        self.root_path = Path(root).expanduser().resolve()
        self.heartbeat_interval_s = float(heartbeat_interval)
        self.stream_poll_interval_s = float(stream_poll_interval)
        self.idle_timeout_s = float(idle_timeout)
        super().__init__(server_address, handler_class)


class DeepwaterAgentHandler(socketserver.BaseRequestHandler):
    platform: Platform | None
    reader: Any
    feed_name: str | None
    base_path: Path | None

    def setup(self) -> None:
        self.session_id = next(_SESSION_IDS)
        self.connected_at = time.monotonic()
        self.platform = None
        self.reader = None
        self.feed_name = None
        self.base_path = None
        _configure_agent_socket(self.request, idle_timeout=self.server.idle_timeout_s)
        log.info(
            "%s session=%s peer=%s",
            DW_REMOTE_AGENT_CONNECTED,
            self.session_id,
            self.client_address,
        )

    def handle(self) -> None:
        while True:
            try:
                header, payload = read_frame(self.request)
            except EOFError:
                break
            except TimeoutError:
                log.info("DW_REMOTE_AGENT_IDLE_TIMEOUT session=%s peer=%s", self.session_id, self.client_address)
                break
            except socket.timeout:
                log.info("DW_REMOTE_AGENT_IDLE_TIMEOUT session=%s peer=%s", self.session_id, self.client_address)
                break
            except ProtocolError as exc:
                self._send_error(None, "PROTOCOL_ERROR", str(exc))
                break

            request_id = header.get("id")
            op = header.get("op")
            try:
                if op == "PING":
                    self._send({"op": "PONG", "id": request_id})
                elif op == "HELLO":
                    self._hello(request_id, header)
                elif op == "OPEN_READER":
                    self._open_reader(request_id, header)
                elif op == "LIST_FEEDS":
                    self._list_feeds(request_id, header)
                elif op == "FEED_EXISTS":
                    self._feed_exists(request_id, header)
                elif op == "DESCRIBE_FEED":
                    self._describe_feed(request_id, header)
                elif op == "LIFECYCLE":
                    self._lifecycle(request_id, header)
                elif op == "RECORD_FORMAT":
                    self._record_format(request_id, header)
                elif op == "LIST_SEGMENTS":
                    self._list_segments(request_id, header)
                elif op == "SUGGESTED_READER_RANGE":
                    self._suggested_reader_range(request_id, header)
                elif op == "READ_RANGE":
                    self._read_range(request_id, header)
                elif op == "READ_RANGE_PAGE":
                    self._read_range_page(request_id, header)
                elif op == "LATEST":
                    self._latest(request_id, header)
                elif op == "READ_AVAILABLE":
                    self._read_available(request_id, header)
                elif op == "SUBSCRIBE_LIVE":
                    self._subscribe_live(request_id, header)
                    break
                elif op == "CLOSE":
                    self._close_reader()
                    self._send({"op": "CLOSED", "id": request_id})
                    break
                else:
                    self._send_error(request_id, "UNKNOWN_OP", f"unknown op: {op!r}")
            except Exception as exc:
                self._send_error(request_id, "REQUEST_FAILED", str(exc))

    def finish(self) -> None:
        try:
            self._close_reader()
        finally:
            elapsed_s = time.monotonic() - self.connected_at
            log.info(
                "%s session=%s peer=%s elapsed_s=%.3f",
                DW_REMOTE_AGENT_DISCONNECTED,
                self.session_id,
                self.client_address,
                elapsed_s,
            )

    @property
    def root_path(self) -> Path:
        return self.server.root_path

    def _send(self, header: dict[str, Any], payload: bytes | bytearray | memoryview | None = None) -> None:
        write_frame(self.request, header, payload)

    def _send_error(self, request_id: Any, code: str, message: str) -> None:
        try:
            self._send({"op": "ERROR", "id": request_id, "code": code, "message": message})
        except Exception:
            pass

    def _close_reader(self) -> None:
        if self.reader is not None:
            try:
                self.reader.close()
            except Exception:
                pass
            self.reader = None
        if self.platform is not None:
            try:
                self.platform.close()
            except Exception:
                pass
            self.platform = None

    def _open_reader(self, request_id: Any, header: dict[str, Any]) -> None:
        requested_path = header.get("path")
        feed_name = header.get("feed_name")
        if not requested_path or not isinstance(requested_path, str):
            raise ValueError("OPEN_READER requires path")
        if not feed_name or not isinstance(feed_name, str):
            raise ValueError("OPEN_READER requires feed_name")

        try:
            base_path = resolve_agent_path(self.root_path, requested_path)
        except ValueError:
            log.info("%s path=%s root=%s", DW_REMOTE_AGENT_REJECTED_PATH, requested_path, self.root_path)
            raise

        if not base_path.exists() or not base_path.is_dir():
            raise FileNotFoundError(f"Deepwater base path not found: {base_path}")
        if not (base_path / "data").exists():
            raise FileNotFoundError(f"not a Deepwater base path: {base_path}")

        self._close_reader()
        platform = Platform(str(base_path))
        try:
            if not platform.feed_exists(feed_name):
                raise KeyError(feed_name)
            reader = platform.create_reader(feed_name)
        except Exception:
            platform.close()
            raise

        self.platform = platform
        self.reader = reader
        self.feed_name = feed_name
        self.base_path = base_path
        self._send({
            "op": "READER_OPEN",
            "id": request_id,
            "feed_name": feed_name,
            "path": str(base_path),
            "layout": platform.get_record_format(feed_name),
            "lifecycle": platform.lifecycle(feed_name) or {},
        })

    def _open_platform(self, requested_path: Any) -> Platform:
        if not requested_path or not isinstance(requested_path, str):
            raise ValueError("metadata request requires path")
        base_path = resolve_agent_path(self.root_path, requested_path)
        if not base_path.exists() or not base_path.is_dir():
            raise FileNotFoundError(f"Deepwater base path not found: {base_path}")
        if not (base_path / "data").exists():
            raise FileNotFoundError(f"not a Deepwater base path: {base_path}")
        return Platform(str(base_path))

    def _metadata(self, request_id: Any, value: Any) -> None:
        self._send({"op": "METADATA", "id": request_id, "value": value})

    def _send_heartbeat(self, request_id: Any, *, idle_s: float) -> None:
        self._send({
            "op": "HEARTBEAT",
            "id": request_id,
            "session_id": self.session_id,
            "server_time_us": time.time_ns() // 1_000,
            "idle_s": round(float(idle_s), 6),
        })
        log.debug(
            "%s session=%s peer=%s idle_s=%.6f",
            DW_REMOTE_AGENT_HEARTBEAT,
            self.session_id,
            self.client_address,
            idle_s,
        )

    def _send_data(self, request_id: Any, payload: bytes, record_size: int, **fields: Any) -> None:
        self._send({
            "op": "DATA",
            "id": request_id,
            "format": "raw",
            "record_size": record_size,
            "count": len(payload) // record_size if record_size else 0,
            **fields,
        }, payload)

    def _list_feeds(self, request_id: Any, header: dict[str, Any]) -> None:
        platform = self._open_platform(header.get("path"))
        try:
            self._metadata(request_id, platform.list_feeds())
        finally:
            platform.close()

    def _feed_exists(self, request_id: Any, header: dict[str, Any]) -> None:
        platform = self._open_platform(header.get("path"))
        try:
            self._metadata(request_id, bool(platform.feed_exists(str(header.get("feed_name")))))
        finally:
            platform.close()

    def _describe_feed(self, request_id: Any, header: dict[str, Any]) -> None:
        platform = self._open_platform(header.get("path"))
        try:
            self._metadata(request_id, platform.describe_feed(str(header.get("feed_name"))))
        finally:
            platform.close()

    def _lifecycle(self, request_id: Any, header: dict[str, Any]) -> None:
        platform = self._open_platform(header.get("path"))
        try:
            self._metadata(request_id, platform.lifecycle(str(header.get("feed_name"))))
        finally:
            platform.close()

    def _record_format(self, request_id: Any, header: dict[str, Any]) -> None:
        platform = self._open_platform(header.get("path"))
        try:
            self._metadata(request_id, platform.get_record_format(str(header.get("feed_name"))))
        finally:
            platform.close()

    def _list_segments(self, request_id: Any, header: dict[str, Any]) -> None:
        platform = self._open_platform(header.get("path"))
        try:
            self._metadata(request_id, platform.list_segments(
                str(header.get("feed_name")),
                status=header.get("status"),
            ))
        finally:
            platform.close()

    def _suggested_reader_range(self, request_id: Any, header: dict[str, Any]) -> None:
        platform = self._open_platform(header.get("path"))
        try:
            suggested = platform.suggested_reader_range(str(header.get("feed_name")))
            self._metadata(request_id, list(suggested) if suggested is not None else None)
        finally:
            platform.close()

    def _require_reader(self):
        if self.reader is None or self.platform is None or self.feed_name is None:
            raise RuntimeError("reader is not open")
        return self.reader

    def _hello(self, request_id: Any, header: dict[str, Any]) -> None:
        requested_version = int(header.get("version", PROTOCOL_VERSION))
        if requested_version > PROTOCOL_VERSION:
            self._send_error(
                request_id,
                "PROTOCOL_MISMATCH",
                f"agent supports protocol v{PROTOCOL_VERSION}, client requested v{requested_version}",
            )
            return
        self._send({
            "op": "HELLO",
            "id": request_id,
            "version": PROTOCOL_VERSION,
            "capabilities": SERVER_CAPABILITIES,
            "limits": {
                **SERVER_LIMITS,
                "heartbeat_interval_s": self.server.heartbeat_interval_s,
                "idle_timeout_s": self.server.idle_timeout_s,
            },
        })

    def _read_range(self, request_id: Any, header: dict[str, Any]) -> None:
        reader = self._require_reader()
        start = int(header["start"])
        end = int(header["end"])
        playback = bool(header.get("playback", False))
        ts_key = header.get("ts_key")
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")
        raw = _reader_range(reader, start, end, playback=playback, ts_key=ts_key)
        payload = _raw_payload(raw)
        record_size = int(self.platform.get_record_format(self.feed_name)["record_size"])
        self._send_data(request_id, payload, record_size)

    def _read_range_page(self, request_id: Any, header: dict[str, Any]) -> None:
        reader = self._require_reader()
        start = int(header["start"])
        end = int(header["end"])
        playback = bool(header.get("playback", False))
        ts_key = header.get("ts_key")
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")

        batch_records = _bounded_batch_size(header.get("batch_records"))
        layout = self.platform.get_record_format(self.feed_name)
        record_size = int(layout["record_size"])
        max_records_by_frame = max(1, (MAX_FRAME_BYTES - 4096) // max(record_size, 1))
        batch_records = min(batch_records, max_records_by_frame)
        page_id = 0
        for raw in _reader_range_batches(
            reader,
            start,
            end,
            playback=playback,
            ts_key=ts_key,
            batch_records=batch_records,
        ):
            payload = memoryview(raw).cast("B")
            if not payload.nbytes:
                continue
            page_id += 1
            self._send_data(request_id, payload.tobytes(), record_size, page_id=page_id, range_end=False)
        self._send({
            "op": "RANGE_END",
            "id": request_id,
            "pages": page_id,
        })

    def _latest(self, request_id: Any, header: dict[str, Any]) -> None:
        reader = self._require_reader()
        seconds = float(header.get("seconds", 60.0))
        ts_key = header.get("ts_key")
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")
        raw = _reader_latest(reader, seconds, ts_key=ts_key)
        payload = _raw_payload(raw)
        record_size = int(self.platform.get_record_format(self.feed_name)["record_size"])
        self._send_data(request_id, payload, record_size)

    def _read_available(self, request_id: Any, header: dict[str, Any]) -> None:
        reader = self._require_reader()
        max_records = header.get("max_records")
        if max_records is not None:
            max_records = int(max_records)
        raw = _reader_read_available(reader, max_records=max_records)
        payload = _raw_payload(raw)
        record_size = int(self.platform.get_record_format(self.feed_name)["record_size"])
        self._send_data(request_id, payload, record_size)

    def _subscribe_live(self, request_id: Any, header: dict[str, Any]) -> None:
        reader = self._require_reader()
        start = header.get("start")
        if start is not None:
            start = int(start)
        playback = bool(header.get("playback", False))
        ts_key = header.get("ts_key")
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")

        record_size = int(self.platform.get_record_format(self.feed_name)["record_size"])
        heartbeat_interval = max(0.01, float(self.server.heartbeat_interval_s))
        self._send({
            "op": "SUBSCRIBED",
            "id": request_id,
            "record_size": record_size,
            "session_id": self.session_id,
            "heartbeat_interval_s": heartbeat_interval,
        })

        if start is not None or playback:
            for raw in _reader_stream(reader, start=start, playback=playback, ts_key=ts_key):
                payload = _raw_payload(raw)
                if payload:
                    self._send_data(request_id, payload, record_size)
            return

        # Live-only streams use non-blocking reads so the agent can emit
        # heartbeats while idle and notice dead peers on failed writes.
        _reader_read_available(reader, max_records=1)
        next_heartbeat = time.monotonic() + heartbeat_interval
        last_data = time.monotonic()
        poll_interval = max(0.001, float(self.server.stream_poll_interval_s))
        while True:
            raw = _reader_read_available(reader, max_records=1)
            payload = _raw_payload(raw)
            now = time.monotonic()
            if payload:
                self._send_data(request_id, payload, record_size)
                last_data = now
                next_heartbeat = now + heartbeat_interval
                continue
            if now >= next_heartbeat:
                self._send_heartbeat(request_id, idle_s=now - last_data)
                next_heartbeat = now + heartbeat_interval
            time.sleep(poll_interval)


def parse_bind(value: str) -> tuple[str, int]:
    if ":" not in value:
        return value, DEFAULT_PORT
    host, port_text = value.rsplit(":", 1)
    return host or "0.0.0.0", int(port_text)


def make_server(
    root: str | Path,
    bind: tuple[str, int],
    *,
    heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL_S,
    stream_poll_interval: float = DEFAULT_STREAM_POLL_INTERVAL_S,
    idle_timeout: float = DEFAULT_IDLE_TIMEOUT_S,
) -> DeepwaterAgentServer:
    return DeepwaterAgentServer(
        bind,
        DeepwaterAgentHandler,
        root=root,
        heartbeat_interval=heartbeat_interval,
        stream_poll_interval=stream_poll_interval,
        idle_timeout=idle_timeout,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Deepwater Tailscale-first remote reader agent")
    parser.add_argument("--root", required=True, help="Only open Deepwater base paths under this root")
    parser.add_argument("--bind", default=f"0.0.0.0:{DEFAULT_PORT}", help="Bind address, e.g. 0.0.0.0:7447")
    parser.add_argument("--heartbeat-interval", type=float, default=DEFAULT_HEARTBEAT_INTERVAL_S)
    parser.add_argument("--stream-poll-interval", type=float, default=DEFAULT_STREAM_POLL_INTERVAL_S)
    parser.add_argument("--idle-timeout", type=float, default=DEFAULT_IDLE_TIMEOUT_S)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    bind = parse_bind(args.bind)
    with make_server(
        args.root,
        bind,
        heartbeat_interval=args.heartbeat_interval,
        stream_poll_interval=args.stream_poll_interval,
        idle_timeout=args.idle_timeout,
    ) as server:
        host, port = server.server_address
        log.info(
            "%s root=%s bind=%s:%s heartbeat_interval_s=%.3f idle_timeout_s=%.3f",
            DW_REMOTE_AGENT_STARTED,
            Path(args.root).resolve(),
            host,
            port,
            args.heartbeat_interval,
            args.idle_timeout,
        )
        server.serve_forever()


if __name__ == "__main__":
    main()
