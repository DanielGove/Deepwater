from __future__ import annotations

import argparse
import itertools
import logging
import socket
import socketserver
import time
from pathlib import Path
from typing import Any

from deepwater import Reader
from deepwater.metadata.feed_metadata import (
    feed_exists,
    list_feeds,
    load_feed_metadata,
    load_feed_metadata_dict,
    load_record_format_dict,
)
from deepwater.metadata.feed_schema import load_record_schema_for_feed
from deepwater.metadata.segments import SegmentStore

from .path import DEFAULT_PORT, resolve_agent_path
from .protocol import (
    Frame,
    DEFAULT_MAX_BATCH_RECORDS,
    DEFAULT_HEARTBEAT_INTERVAL_S,
    DEFAULT_IDLE_TIMEOUT_S,
    DEFAULT_STREAM_POLL_INTERVAL_S,
    MAX_FRAME_BYTES,
    Op,
    PROTOCOL_VERSION,
    SERVER_CAPABILITIES,
    SERVER_LIMITS,
    ProtocolError,
    frame,
    op_name,
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


def _raw_payload(value) -> bytes | memoryview:
    if value is None:
        return b""
    if isinstance(value, memoryview):
        return value.cast("B")
    if isinstance(value, (bytes, bytearray)):
        return memoryview(value).cast("B")
    raise TypeError(f"raw reader operation returned {type(value).__name__}, expected bytes")


def _bounded_batch_size(requested: Any) -> int:
    batch_records = int(requested or DEFAULT_MAX_BATCH_RECORDS)
    if batch_records <= 0:
        raise ValueError("batch_records must be positive")
    return min(batch_records, DEFAULT_MAX_BATCH_RECORDS)


def _args(header: Frame, op_name: str, min_len: int = 0) -> tuple:
    values = tuple(header.args or ())
    if len(values) < min_len:
        raise ValueError(f"{op_name} requires {min_len} argument(s)")
    return values


def _describe_feed(base_path: Path, feed_name: str) -> dict[str, Any]:
    metadata = load_feed_metadata(base_path, feed_name)
    if metadata is None:
        raise KeyError(feed_name)
    layout = load_record_schema_for_feed(base_path, feed_name)
    return {
        "feed_name": feed_name,
        "metadata": {
            "chunk_size_bytes": metadata.chunk_size_bytes,
            "retention_hours": metadata.retention_hours,
            "persist": metadata.persist,
            "ring_size_bytes": metadata.ring_size_bytes,
            "segment_tracking": metadata.segment_tracking,
            "prefault_ring": metadata.prefault_ring,
            "uses_ring": metadata.uses_ring,
            "storage": "ring" if metadata.uses_ring else "chunk",
        },
        "clock_level": layout.clock_level,
        "timestamp_fields": [field.to_dict() for field in layout.timestamp_fields],
        "record_fmt": layout.fmt,
        "record_size": layout.record_size,
        "ts_offset": layout.primary_ts_offset,
        "fields": [field.to_dict() for field in layout.fields],
        "created_us": metadata.created_us,
    }


def _segment_store(base_path: Path, feed_name: str) -> SegmentStore:
    if not feed_exists(base_path, feed_name):
        raise KeyError(feed_name)
    return SegmentStore(base_path / "data" / feed_name, feed_name)


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
    reader: Any
    feed_name: str | None
    base_path: Path | None

    def setup(self) -> None:
        self.session_id = next(_SESSION_IDS)
        self.connected_at = time.monotonic()
        self.reader = None
        self.feed_name = None
        self.base_path = None
        self._metadata_payload = None
        self._record_size = 0
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

            request_id = header.id
            op = header.op
            try:
                if op == Op.PING:
                    self._send(Op.PONG, request_id)
                elif op == Op.HELLO:
                    self._hello(request_id, header)
                elif op == Op.OPEN_READER:
                    self._open_reader(request_id, header)
                elif op == Op.READER_DESCRIBE:
                    self._reader_describe(request_id)
                elif op == Op.STATE:
                    self._state(request_id)
                elif op == Op.LIST_FEEDS:
                    self._list_feeds(request_id, header)
                elif op == Op.FEED_EXISTS:
                    self._feed_exists(request_id, header)
                elif op == Op.DESCRIBE_FEED:
                    self._describe_feed(request_id, header)
                elif op == Op.FEED_METADATA:
                    self._feed_metadata(request_id, header)
                elif op == Op.RECORD_FORMAT:
                    self._record_format(request_id, header)
                elif op == Op.LIST_SEGMENTS:
                    self._list_segments(request_id, header)
                elif op == Op.SUGGESTED_READER_RANGE:
                    self._suggested_reader_range(request_id, header)
                elif op == Op.READ_RANGE:
                    self._read_range(request_id, header)
                elif op == Op.READ_RANGE_PAGE:
                    self._read_range_page(request_id, header)
                elif op == Op.FIRST_AFTER:
                    self._first_after(request_id, header)
                elif op == Op.FIRST_BEFORE:
                    self._first_before(request_id, header)
                elif op == Op.LATEST:
                    self._latest(request_id, header)
                elif op == Op.READ_AVAILABLE:
                    self._read_available(request_id, header)
                elif op == Op.SUBSCRIBE_LIVE:
                    self._subscribe_live(request_id, header)
                    break
                elif op == Op.CLOSE:
                    self._close_reader()
                    self._send(Op.CLOSED, request_id)
                    break
                else:
                    self._send_error(request_id, "UNKNOWN_OP", f"unknown op: {op_name(op)}")
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

    def _send(
        self,
        op: Op,
        request_id: Any,
        meta: Any = None,
        payload: bytes | bytearray | memoryview | None = None,
    ) -> None:
        write_frame(self.request, frame(op, int(request_id or 0), meta), payload)

    def _send_error(self, request_id: Any, code: str, message: str) -> None:
        try:
            self._send(Op.ERROR, int(request_id or 0), (code, message))
        except Exception:
            pass

    def _close_reader(self) -> None:
        if self.reader is not None:
            try:
                self.reader.close()
            except Exception:
                pass
        self.reader = None
        self._metadata_payload = None
        self._record_size = 0

    def _open_reader(self, request_id: Any, header: Frame) -> None:
        requested_path, feed_name = _args(header, "OPEN_READER", 2)[:2]
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
        if not feed_exists(base_path, feed_name):
            raise KeyError(feed_name)
        reader = Reader(base_path, feed_name)

        self.reader = reader
        self.feed_name = feed_name
        self.base_path = base_path
        schema_payload = reader.record_format.to_msgpack()
        self._metadata_payload = reader.metadata
        self._record_size = int(reader.record_size)
        self._send(
            Op.READER_OPEN,
            request_id,
            (feed_name, str(base_path), self._metadata_payload),
            schema_payload,
        )

    def _open_platform(self, requested_path: Any) -> Path:
        if not requested_path or not isinstance(requested_path, str):
            raise ValueError("metadata request requires path")
        base_path = resolve_agent_path(self.root_path, requested_path)
        if not base_path.exists() or not base_path.is_dir():
            raise FileNotFoundError(f"Deepwater base path not found: {base_path}")
        if not (base_path / "data").exists():
            raise FileNotFoundError(f"not a Deepwater base path: {base_path}")
        return base_path

    def _metadata(self, request_id: Any, value: Any) -> None:
        self._send(Op.METADATA, request_id, (value,))

    def _send_heartbeat(self, request_id: Any, idle_s: float) -> None:
        self._send(
            Op.HEARTBEAT,
            request_id,
            (self.session_id, time.time_ns() // 1_000, round(float(idle_s), 6)),
        )
        log.debug(
            "%s session=%s peer=%s idle_s=%.6f",
            DW_REMOTE_AGENT_HEARTBEAT,
            self.session_id,
            self.client_address,
            idle_s,
        )

    def _send_data(self, request_id: Any, payload: bytes | bytearray | memoryview, record_size: int, **fields: Any) -> None:
        args = (
            int(record_size),
            len(payload) // record_size if record_size else 0,
            int(fields.get("page_id") or 0),
            bool(fields.get("range_end", False)),
        )
        self._send(Op.DATA, request_id, args, payload)

    def _send_optional_record(self, request_id: Any, raw: Any) -> None:
        payload = b"" if raw is None else _raw_payload(raw)
        self._send_data(request_id, payload, self._record_size)

    def _list_feeds(self, request_id: Any, header: Frame) -> None:
        path = _args(header, "LIST_FEEDS", 1)[0]
        base = self._open_platform(path)
        self._metadata(request_id, list_feeds(base))

    def _feed_exists(self, request_id: Any, header: Frame) -> None:
        path, feed_name = _args(header, "FEED_EXISTS", 2)[:2]
        base = self._open_platform(path)
        self._metadata(request_id, bool(feed_exists(base, str(feed_name))))

    def _describe_feed(self, request_id: Any, header: Frame) -> None:
        path, feed_name = _args(header, "DESCRIBE_FEED", 2)[:2]
        base = self._open_platform(path)
        self._metadata(request_id, _describe_feed(base, str(feed_name)))

    def _feed_metadata(self, request_id: Any, header: Frame) -> None:
        path, feed_name = _args(header, "FEED_METADATA", 2)[:2]
        base = self._open_platform(path)
        self._metadata(request_id, load_feed_metadata_dict(base, str(feed_name)))

    def _record_format(self, request_id: Any, header: Frame) -> None:
        path, feed_name = _args(header, "RECORD_FORMAT", 2)[:2]
        base = self._open_platform(path)
        self._metadata(request_id, load_record_format_dict(base, str(feed_name)))

    def _list_segments(self, request_id: Any, header: Frame) -> None:
        values = _args(header, "LIST_SEGMENTS", 2)
        path, feed_name = values[:2]
        status = None
        if len(values) > 2:
            status = values[2]
        base = self._open_platform(path)
        self._metadata(request_id, _segment_store(base, str(feed_name)).list_segments(status=status))

    def _suggested_reader_range(self, request_id: Any, header: Frame) -> None:
        path, feed_name = _args(header, "SUGGESTED_READER_RANGE", 2)[:2]
        base = self._open_platform(path)
        suggested = _segment_store(base, str(feed_name)).suggested_timestamp_range()
        self._metadata(request_id, list(suggested) if suggested is not None else None)

    def _require_reader(self):
        if self.reader is None or self.feed_name is None:
            raise RuntimeError("reader is not open")
        return self.reader

    def _hello(self, request_id: Any, header: Frame) -> None:
        values = _args(header, "HELLO")
        requested_version = PROTOCOL_VERSION if not values else int(values[0])
        if requested_version > PROTOCOL_VERSION:
            self._send_error(
                request_id,
                "PROTOCOL_MISMATCH",
                f"agent supports protocol v{PROTOCOL_VERSION}, client requested v{requested_version}",
            )
            return
        self._send(
            Op.HELLO,
            request_id,
            (
                PROTOCOL_VERSION,
                SERVER_CAPABILITIES,
                {
                **SERVER_LIMITS,
                "heartbeat_interval_s": self.server.heartbeat_interval_s,
                "idle_timeout_s": self.server.idle_timeout_s,
                },
            ),
        )

    def _reader_describe(self, request_id: Any) -> None:
        self._metadata(request_id, self._require_reader().describe())

    def _state(self, request_id: Any) -> None:
        self._metadata(request_id, self._require_reader().state())

    def _read_range(self, request_id: Any, header: Frame) -> None:
        reader = self._require_reader()
        values = _args(header, "READ_RANGE", 2)
        start = int(values[0])
        end = int(values[1])
        playback = bool(values[2]) if len(values) > 2 else False
        ts_key = values[3] if len(values) > 3 else None
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")
        raw = reader.range(start, end, format="raw", playback=playback, ts_key=ts_key)
        payload = _raw_payload(raw)
        self._send_data(request_id, payload, self._record_size)

    def _read_range_page(self, request_id: Any, header: Frame) -> None:
        reader = self._require_reader()
        values = _args(header, "READ_RANGE_PAGE", 2)
        start = int(values[0])
        end = int(values[1])
        playback = bool(values[2]) if len(values) > 2 else False
        ts_key = values[3] if len(values) > 3 else None
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")

        batch_records = _bounded_batch_size(values[4] if len(values) > 4 else None)
        record_size = self._record_size
        max_records_by_frame = max(1, (MAX_FRAME_BYTES - 4096) // max(record_size, 1))
        batch_records = min(batch_records, max_records_by_frame)
        page_id = 0
        for raw in reader.range_batches(
            start,
            end,
            format="raw",
            playback=playback,
            ts_key=ts_key,
            batch_records=batch_records,
        ):
            payload = memoryview(raw).cast("B")
            if not payload.nbytes:
                continue
            page_id += 1
            self._send_data(request_id, payload, record_size, page_id=page_id, range_end=False)
        self._send(Op.RANGE_END, request_id, (page_id,))

    def _first_after(self, request_id: Any, header: Frame) -> None:
        reader = self._require_reader()
        values = _args(header, "FIRST_AFTER", 1)
        ts_key = values[1] if len(values) > 1 else None
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")
        raw = reader.first_after(int(values[0]), format="raw", ts_key=ts_key)
        self._send_optional_record(request_id, raw)

    def _first_before(self, request_id: Any, header: Frame) -> None:
        reader = self._require_reader()
        values = _args(header, "FIRST_BEFORE", 1)
        ts_key = values[1] if len(values) > 1 else None
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")
        raw = reader.first_before(int(values[0]), format="raw", ts_key=ts_key)
        self._send_optional_record(request_id, raw)

    def _latest(self, request_id: Any, header: Frame) -> None:
        reader = self._require_reader()
        values = _args(header, "LATEST")
        seconds = 60.0 if not values else float(values[0])
        ts_key = values[1] if len(values) > 1 else None
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")
        raw = reader.latest(seconds, format="raw", ts_key=ts_key)
        payload = _raw_payload(raw)
        self._send_data(request_id, payload, self._record_size)

    def _read_available(self, request_id: Any, header: Frame) -> None:
        reader = self._require_reader()
        values = _args(header, "READ_AVAILABLE")
        max_records = values[0] if values else None
        if max_records is not None:
            max_records = int(max_records)
        raw = reader.read_available(max_records=max_records, format="raw")
        payload = _raw_payload(raw)
        self._send_data(request_id, payload, self._record_size)

    def _subscribe_live(self, request_id: Any, header: Frame) -> None:
        reader = self._require_reader()
        values = _args(header, "SUBSCRIBE_LIVE")
        start = values[0] if values else None
        if start is not None:
            start = int(start)
        playback = bool(values[1]) if len(values) > 1 else False
        ts_key = values[2] if len(values) > 2 else None
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")

        record_size = self._record_size
        heartbeat_interval = max(0.01, float(self.server.heartbeat_interval_s))
        self._send(Op.SUBSCRIBED, request_id, (record_size, self.session_id, heartbeat_interval))

        if start is not None or playback:
            for raw in reader.stream(start=start, format="raw", playback=playback, ts_key=ts_key):
                payload = _raw_payload(raw)
                if payload:
                    self._send_data(request_id, payload, record_size)
            return

        # Live-only streams use non-blocking reads so the agent can emit
        # heartbeats while idle and notice dead peers on failed writes.
        reader.read_available(max_records=1, format="raw")
        next_heartbeat = time.monotonic() + heartbeat_interval
        last_data = time.monotonic()
        poll_interval = max(0.001, float(self.server.stream_poll_interval_s))
        while True:
            raw = reader.read_available(max_records=1, format="raw")
            payload = _raw_payload(raw)
            now = time.monotonic()
            if payload:
                self._send_data(request_id, payload, record_size)
                last_data = now
                next_heartbeat = now + heartbeat_interval
                continue
            if now >= next_heartbeat:
                self._send_heartbeat(request_id, now - last_data)
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
