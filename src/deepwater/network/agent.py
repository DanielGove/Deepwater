from __future__ import annotations

import argparse
import logging
import socketserver
from pathlib import Path
from typing import Any, Optional

from deepwater import Platform

from .path import DEFAULT_PORT, resolve_agent_path
from .protocol import ProtocolError, read_frame, write_frame


log = logging.getLogger("dw.network.agent")

DW_REMOTE_AGENT_STARTED = "DW_REMOTE_AGENT_STARTED"
DW_REMOTE_AGENT_REJECTED_PATH = "DW_REMOTE_AGENT_REJECTED_PATH"


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


def _reader_stream(reader, *, start: Optional[int], playback: bool, ts_key: str | None):
    try:
        return reader.stream(start=start, format="raw", playback=playback, ts_key=ts_key)
    except TypeError:
        if start is not None or playback or ts_key:
            raise
        return reader.stream(format="raw")


class DeepwaterAgentServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, handler_class, *, root: str | Path):
        self.root_path = Path(root).expanduser().resolve()
        super().__init__(server_address, handler_class)


class DeepwaterAgentHandler(socketserver.BaseRequestHandler):
    platform: Platform | None
    reader: Any
    feed_name: str | None
    base_path: Path | None

    def setup(self) -> None:
        self.platform = None
        self.reader = None
        self.feed_name = None
        self.base_path = None

    def handle(self) -> None:
        while True:
            try:
                header, payload = read_frame(self.request)
            except EOFError:
                break
            except ProtocolError as exc:
                self._send_error(None, "PROTOCOL_ERROR", str(exc))
                break

            request_id = header.get("id")
            op = header.get("op")
            try:
                if op == "PING":
                    self._send({"op": "PONG", "id": request_id})
                elif op == "OPEN_READER":
                    self._open_reader(request_id, header)
                elif op == "READ_RANGE":
                    self._read_range(request_id, header)
                elif op == "LATEST":
                    self._latest(request_id, header)
                elif op == "SUBSCRIBE_LIVE":
                    self._subscribe_live(request_id, header)
                    break
                elif op == "CLOSE":
                    self._send({"op": "CLOSED", "id": request_id})
                    break
                else:
                    self._send_error(request_id, "UNKNOWN_OP", f"unknown op: {op!r}")
            except Exception as exc:
                self._send_error(request_id, "REQUEST_FAILED", str(exc))

    def finish(self) -> None:
        self._close_reader()

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

    def _require_reader(self):
        if self.reader is None or self.platform is None or self.feed_name is None:
            raise RuntimeError("reader is not open")
        return self.reader

    def _read_range(self, request_id: Any, header: dict[str, Any]) -> None:
        reader = self._require_reader()
        start = int(header["start"])
        end = int(header["end"])
        playback = bool(header.get("playback", False))
        ts_key = header.get("ts_key")
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")
        raw = _reader_range(reader, start, end, playback=playback, ts_key=ts_key)
        payload = bytes(raw)
        record_size = int(self.platform.get_record_format(self.feed_name)["record_size"])
        self._send({
            "op": "DATA",
            "id": request_id,
            "format": "raw",
            "record_size": record_size,
            "count": len(payload) // record_size if record_size else 0,
        }, payload)

    def _latest(self, request_id: Any, header: dict[str, Any]) -> None:
        reader = self._require_reader()
        seconds = float(header.get("seconds", 60.0))
        ts_key = header.get("ts_key")
        if ts_key is not None and not isinstance(ts_key, str):
            raise ValueError("ts_key must be a string or null")
        raw = _reader_latest(reader, seconds, ts_key=ts_key)
        payload = bytes(raw)
        record_size = int(self.platform.get_record_format(self.feed_name)["record_size"])
        self._send({
            "op": "DATA",
            "id": request_id,
            "format": "raw",
            "record_size": record_size,
            "count": len(payload) // record_size if record_size else 0,
        }, payload)

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
        self._send({"op": "SUBSCRIBED", "id": request_id, "record_size": record_size})
        for raw in _reader_stream(reader, start=start, playback=playback, ts_key=ts_key):
            payload = bytes(raw)
            self._send({
                "op": "DATA",
                "id": request_id,
                "format": "raw",
                "record_size": record_size,
                "count": len(payload) // record_size if record_size else 0,
            }, payload)


def parse_bind(value: str) -> tuple[str, int]:
    if ":" not in value:
        return value, DEFAULT_PORT
    host, port_text = value.rsplit(":", 1)
    return host or "0.0.0.0", int(port_text)


def make_server(root: str | Path, bind: tuple[str, int]) -> DeepwaterAgentServer:
    return DeepwaterAgentServer(bind, DeepwaterAgentHandler, root=root)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Deepwater Tailscale-first remote reader agent")
    parser.add_argument("--root", required=True, help="Only open Deepwater base paths under this root")
    parser.add_argument("--bind", default=f"0.0.0.0:{DEFAULT_PORT}", help="Bind address, e.g. 0.0.0.0:7447")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    bind = parse_bind(args.bind)
    with make_server(args.root, bind) as server:
        host, port = server.server_address
        log.info("%s root=%s bind=%s:%s", DW_REMOTE_AGENT_STARTED, Path(args.root).resolve(), host, port)
        server.serve_forever()


if __name__ == "__main__":
    main()
