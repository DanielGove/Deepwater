from __future__ import annotations

import itertools
import logging
import socket
import struct
from typing import Any, Callable, Iterator, Optional

import numpy as np

from .path import DEFAULT_PORT, NetworkTarget, parse_target
from .protocol import ProtocolError, read_frame, write_frame


log = logging.getLogger("dw.network.client")

DW_REMOTE_LINK_OPEN = "DW_REMOTE_LINK_OPEN"
DW_REMOTE_LINK_DOWN = "DW_REMOTE_LINK_DOWN"
DW_REMOTE_READ_ERROR = "DW_REMOTE_READ_ERROR"

_VALID_FORMATS = {"tuple", "dict", "numpy", "raw"}


class RemoteReaderError(RuntimeError):
    pass


class ManagedLocalReader:
    """Owns the temporary Platform created by deepwater.reader(local_path, ...)."""

    def __init__(self, platform, reader):
        self.platform = platform
        self._reader = reader
        self._closed = False

    def __getattr__(self, name: str):
        return getattr(self._reader, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    @property
    def record_size(self) -> int:
        if hasattr(self._reader, "record_size"):
            return int(self._reader.record_size)
        if hasattr(self._reader, "record_format"):
            return int(self._reader.record_format["record_size"])
        if hasattr(self._reader, "_rec_size"):
            return int(self._reader._rec_size)
        raise AttributeError("record_size")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._reader.close()
        finally:
            self.platform.close()


class RemoteReader:
    def __init__(
        self,
        host: str,
        base_path: str,
        feed_name: str,
        *,
        port: int = DEFAULT_PORT,
        timeout: float = 10.0,
        status_callback: Optional[Callable[[dict[str, Any]], None]] = None,
    ):
        self.host = host
        self.port = int(port)
        self.base_path = base_path
        self.feed_name = feed_name
        self.timeout = float(timeout)
        self.status_callback = status_callback
        self._ids = itertools.count(1)
        self._closed = False
        self._sock: socket.socket | None = None
        self.layout: dict[str, Any] = {}
        self.lifecycle: dict[str, Any] = {}
        self.record_size = 0
        self._struct: struct.Struct | None = None
        self._field_names: tuple[str, ...] = ()
        self._dtype = None

        try:
            self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
            self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._emit(DW_REMOTE_LINK_OPEN, host=self.host, port=self.port, path=self.base_path, feed=self.feed_name)
            header, _payload = self._request(
                "OPEN_READER",
                path=self.base_path,
                feed_name=self.feed_name,
            )
            self.layout = dict(header.get("layout") or {})
            self.lifecycle = dict(header.get("lifecycle") or {})
            self.record_size = int(self.layout.get("record_size") or 0)
            if not self.record_size:
                raise RemoteReaderError("remote agent did not return a valid record layout")
            self._struct = struct.Struct(self.layout["fmt"])
            self._field_names = tuple(
                field["name"]
                for field in self.layout.get("fields", [])
                if not str(field.get("type", "")).startswith("_") or field.get("name") != "_"
            )
            dtype_spec = self.layout.get("dtype")
            self._dtype = np.dtype(dtype_spec) if dtype_spec else None
        except Exception:
            self._emit(DW_REMOTE_LINK_DOWN, host=self.host, port=self.port, path=self.base_path, feed=self.feed_name)
            self.close()
            raise

    def _emit(self, event: str, **fields: Any) -> None:
        record = {"event": event, **fields}
        log.info("%s %s", event, fields)
        if self.status_callback is not None:
            self.status_callback(record)

    def _request(
        self,
        op: str,
        payload: bytes | bytearray | memoryview | None = None,
        **fields: Any,
    ) -> tuple[dict[str, Any], bytes]:
        if self._closed or self._sock is None:
            raise RemoteReaderError("remote reader is closed")
        request_id = next(self._ids)
        write_frame(self._sock, {"op": op, "id": request_id, **fields}, payload)
        header, response_payload = read_frame(self._sock)
        if header.get("id") != request_id:
            raise RemoteReaderError(f"response id mismatch: expected {request_id}, got {header.get('id')}")
        if header.get("op") == "ERROR":
            raise RemoteReaderError(str(header.get("message") or "remote agent error"))
        return header, response_payload

    def _decode_payload(self, payload: bytes, format: str):
        if format not in _VALID_FORMATS:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")
        if len(payload) % self.record_size:
            raise ProtocolError(
                f"raw payload size {len(payload)} is not a multiple of record size {self.record_size}"
            )
        if format == "raw":
            return memoryview(payload)
        if format == "numpy":
            if self._dtype is None:
                raise RemoteReaderError("remote layout does not include numpy dtype metadata")
            return np.frombuffer(payload, dtype=self._dtype)

        assert self._struct is not None
        records = list(self._struct.iter_unpack(payload))
        if format == "tuple":
            return records
        return [dict(zip(self._field_names, record)) for record in records]

    def range(
        self,
        start: int,
        end: int,
        format: str = "tuple",
        playback: bool = False,
        ts_key: Optional[str] = None,
    ):
        try:
            _header, payload = self._request(
                "READ_RANGE",
                start=int(start),
                end=int(end),
                playback=bool(playback),
                ts_key=ts_key,
            )
            return self._decode_payload(payload, format)
        except Exception as exc:
            self._emit(DW_REMOTE_READ_ERROR, host=self.host, port=self.port, feed=self.feed_name, error=str(exc))
            raise

    def latest(self, seconds: float = 60.0, format: str = "tuple", ts_key: Optional[str] = None):
        try:
            _header, payload = self._request(
                "LATEST",
                seconds=float(seconds),
                ts_key=ts_key,
            )
            return self._decode_payload(payload, format)
        except Exception as exc:
            self._emit(DW_REMOTE_READ_ERROR, host=self.host, port=self.port, feed=self.feed_name, error=str(exc))
            raise

    def stream(
        self,
        start: Optional[int] = None,
        format: str = "tuple",
        ts_key: Optional[str] = None,
        playback: bool = False,
    ) -> Iterator:
        if format not in _VALID_FORMATS:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")
        if self._closed or self._sock is None:
            raise RemoteReaderError("remote reader is closed")

        request_id = next(self._ids)
        write_frame(
            self._sock,
            {
                "op": "SUBSCRIBE_LIVE",
                "id": request_id,
                "start": start,
                "playback": bool(playback),
                "ts_key": ts_key,
            },
        )
        header, _payload = read_frame(self._sock)
        if header.get("id") != request_id:
            raise RemoteReaderError(f"response id mismatch: expected {request_id}, got {header.get('id')}")
        if header.get("op") == "ERROR":
            raise RemoteReaderError(str(header.get("message") or "remote agent error"))
        if header.get("op") != "SUBSCRIBED":
            raise RemoteReaderError(f"expected SUBSCRIBED, got {header.get('op')}")

        try:
            while True:
                header, payload = read_frame(self._sock)
                if header.get("op") == "ERROR":
                    raise RemoteReaderError(str(header.get("message") or "remote stream error"))
                if header.get("op") != "DATA":
                    continue
                decoded = self._decode_payload(payload, format)
                if format == "raw":
                    yield decoded
                elif format == "numpy":
                    yield decoded
                else:
                    for record in decoded:
                        yield record
        except Exception as exc:
            self._emit(DW_REMOTE_LINK_DOWN, host=self.host, port=self.port, feed=self.feed_name, error=str(exc))
            raise
        finally:
            self.close()

    def ping(self) -> bool:
        header, _payload = self._request("PING")
        return header.get("op") == "PONG"

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                write_frame(sock, {"op": "CLOSE", "id": next(self._ids)})
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


def reader(
    base_path: str | NetworkTarget,
    *,
    stream: str | None = None,
    feed_name: str | None = None,
    port: int = DEFAULT_PORT,
    timeout: float = 10.0,
    status_callback: Optional[Callable[[dict[str, Any]], None]] = None,
):
    feed = feed_name or stream
    if not feed:
        raise ValueError("feed_name is required (or pass stream='feed.name')")

    target = base_path if isinstance(base_path, NetworkTarget) else parse_target(base_path, default_port=port)
    if target.is_remote:
        assert target.host is not None
        return RemoteReader(
            target.host,
            target.path,
            feed,
            port=target.port,
            timeout=timeout,
            status_callback=status_callback,
        )

    from ..platform import Platform

    platform = Platform(target.path)
    local_reader = platform.create_reader(feed)
    return ManagedLocalReader(platform, local_reader)
