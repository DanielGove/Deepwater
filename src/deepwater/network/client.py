from __future__ import annotations

import itertools
import logging
import socket
import struct
from typing import Any, Callable, Iterator, Optional

import numpy as np

from ..metadata.datasets import build_manifest, common_intervals, recommend_train_validation, with_duration
from .path import DEFAULT_PORT, NetworkTarget, parse_target
from .protocol import (
    DEFAULT_MAX_BATCH_RECORDS,
    PROTOCOL_VERSION,
    ProtocolError,
    read_frame,
    write_frame,
)


log = logging.getLogger("dw.network.client")

DW_REMOTE_LINK_OPEN = "DW_REMOTE_LINK_OPEN"
DW_REMOTE_LINK_CLOSED = "DW_REMOTE_LINK_CLOSED"
DW_REMOTE_LINK_DOWN = "DW_REMOTE_LINK_DOWN"
DW_REMOTE_HEARTBEAT = "DW_REMOTE_HEARTBEAT"
DW_REMOTE_READ_ERROR = "DW_REMOTE_READ_ERROR"
DW_REMOTE_STREAM_SUBSCRIBED = "DW_REMOTE_STREAM_SUBSCRIBED"

_VALID_FORMATS = {"tuple", "dict", "numpy", "raw"}


class RemoteReaderError(RuntimeError):
    pass


def _configure_client_socket(sock: socket.socket) -> None:
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if hasattr(socket, "TCP_KEEPIDLE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
    if hasattr(socket, "TCP_KEEPINTVL"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
    if hasattr(socket, "TCP_KEEPCNT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)


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

    def range_batches(
        self,
        start: int,
        end: int,
        format: str = "tuple",
        playback: bool = False,
        ts_key: Optional[str] = None,
        batch_records: int = DEFAULT_MAX_BATCH_RECORDS,
    ) -> Iterator:
        if batch_records <= 0:
            raise ValueError("batch_records must be positive")
        if hasattr(self._reader, "range_batches"):
            try:
                yield from self._reader.range_batches(
                    start,
                    end,
                    format=format,
                    playback=playback,
                    ts_key=ts_key,
                    batch_records=batch_records,
                )
            except TypeError:
                yield from self._reader.range_batches(
                    start,
                    end,
                    format=format,
                    batch_records=batch_records,
                )
            return
        try:
            data = self._reader.range(start, end, format=format, playback=playback, ts_key=ts_key)
        except TypeError:
            data = self._reader.range(start, end, format=format)

        if format == "raw":
            rec_size = self.record_size
            step = int(batch_records) * rec_size
            for pos in range(0, len(data), step):
                yield data[pos:pos + step]
            return
        for pos in range(0, len(data), int(batch_records)):
            yield data[pos:pos + int(batch_records)]

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
        self.protocol_version = 0
        self.capabilities: dict[str, Any] = {}
        self.limits: dict[str, Any] = {}

        try:
            self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
            _configure_client_socket(self._sock)
            self._emit(DW_REMOTE_LINK_OPEN, host=self.host, port=self.port, path=self.base_path, feed=self.feed_name)
            self._hello()
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

    def _hello(self) -> None:
        try:
            header, _payload = self._request(
                "HELLO",
                version=PROTOCOL_VERSION,
                capabilities={
                    "paged_range": True,
                    "heartbeat": True,
                    "resume_stream": False,
                },
            )
        except RemoteReaderError:
            self.protocol_version = 0
            self.capabilities = {}
            self.limits = {}
            return
        if header.get("op") != "HELLO":
            raise RemoteReaderError(f"expected HELLO, got {header.get('op')}")
        self.protocol_version = int(header.get("version", 0))
        self.capabilities = dict(header.get("capabilities") or {})
        self.limits = dict(header.get("limits") or {})

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

    @property
    def record_format(self) -> dict[str, Any]:
        return self.layout

    @property
    def format(self) -> str:
        return str(self.layout["fmt"])

    @property
    def field_names(self) -> tuple[str, ...]:
        return self._field_names

    @property
    def dtype(self):
        return self._dtype

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

    def range_batches(
        self,
        start: int,
        end: int,
        format: str = "tuple",
        playback: bool = False,
        ts_key: Optional[str] = None,
        batch_records: int = DEFAULT_MAX_BATCH_RECORDS,
    ) -> Iterator:
        if format not in _VALID_FORMATS:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")
        if batch_records <= 0:
            raise ValueError("batch_records must be positive")
        if not self.capabilities.get("paged_range"):
            data = self.range(start, end, format=format, playback=playback, ts_key=ts_key)
            if format == "raw":
                step = int(batch_records) * self.record_size
                for pos in range(0, len(data), step):
                    yield data[pos:pos + step]
                return
            for pos in range(0, len(data), int(batch_records)):
                yield data[pos:pos + int(batch_records)]
            return

        try:
            request_id = next(self._ids)
            write_frame(
                self._sock,
                {
                    "op": "READ_RANGE_PAGE",
                    "id": request_id,
                    "start": int(start),
                    "end": int(end),
                    "playback": bool(playback),
                    "ts_key": ts_key,
                    "batch_records": int(batch_records),
                },
            )
            while True:
                header, payload = read_frame(self._sock)
                if header.get("id") != request_id:
                    raise RemoteReaderError(f"response id mismatch: expected {request_id}, got {header.get('id')}")
                op = header.get("op")
                if op == "ERROR":
                    raise RemoteReaderError(str(header.get("message") or "remote agent error"))
                if op == "RANGE_END":
                    return
                if op != "DATA":
                    raise RemoteReaderError(f"expected DATA or RANGE_END, got {op}")
                yield self._decode_payload(payload, format)
        except Exception as exc:
            self._emit(DW_REMOTE_READ_ERROR, host=self.host, port=self.port, feed=self.feed_name, error=str(exc))
            raise

    def read_available(self, max_records: Optional[int] = None, format: str = "tuple"):
        try:
            _header, payload = self._request(
                "READ_AVAILABLE",
                max_records=None if max_records is None else int(max_records),
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
        self._emit(
            DW_REMOTE_STREAM_SUBSCRIBED,
            host=self.host,
            port=self.port,
            path=self.base_path,
            feed=self.feed_name,
            heartbeat_interval_s=header.get("heartbeat_interval_s"),
        )

        try:
            while True:
                header, payload = read_frame(self._sock)
                if header.get("op") == "ERROR":
                    raise RemoteReaderError(str(header.get("message") or "remote stream error"))
                if header.get("op") == "HEARTBEAT":
                    self._emit(
                        DW_REMOTE_HEARTBEAT,
                        host=self.host,
                        port=self.port,
                        path=self.base_path,
                        feed=self.feed_name,
                        server_time_us=header.get("server_time_us"),
                        idle_s=header.get("idle_s"),
                    )
                    continue
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
                sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass
        self._emit(DW_REMOTE_LINK_CLOSED, host=self.host, port=self.port, path=self.base_path, feed=self.feed_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class RemotePlatform:
    def __init__(
        self,
        host: str,
        base_path: str,
        *,
        port: int = DEFAULT_PORT,
        timeout: float = 10.0,
        status_callback: Optional[Callable[[dict[str, Any]], None]] = None,
    ):
        self.host = str(host)
        self.port = int(port)
        self.base_path = str(base_path)
        self.timeout = float(timeout)
        self.status_callback = status_callback
        self._ids = itertools.count(1)
        self._closed = False
        self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        _configure_client_socket(self._sock)
        self.protocol_version = 0
        self.capabilities: dict[str, Any] = {}
        self.limits: dict[str, Any] = {}
        self._readers: list[RemoteReader] = []
        self._hello()

    def _hello(self) -> None:
        header, _payload = self._request(
            "HELLO",
            version=PROTOCOL_VERSION,
            capabilities={
                "paged_range": True,
                "heartbeat": True,
                "remote_platform_metadata": True,
                "resume_stream": False,
            },
        )
        if header.get("op") != "HELLO":
            raise RemoteReaderError(f"expected HELLO, got {header.get('op')}")
        self.protocol_version = int(header.get("version", 0))
        self.capabilities = dict(header.get("capabilities") or {})
        self.limits = dict(header.get("limits") or {})

    def _request(self, op: str, **fields: Any) -> tuple[dict[str, Any], bytes]:
        if self._closed or self._sock is None:
            raise RemoteReaderError("remote platform is closed")
        request_id = next(self._ids)
        write_frame(self._sock, {"op": op, "id": request_id, **fields})
        header, payload = read_frame(self._sock)
        if header.get("id") != request_id:
            raise RemoteReaderError(f"response id mismatch: expected {request_id}, got {header.get('id')}")
        if header.get("op") == "ERROR":
            raise RemoteReaderError(str(header.get("message") or "remote agent error"))
        return header, payload

    def _metadata(self, op: str, **fields: Any):
        header, _payload = self._request(op, path=self.base_path, **fields)
        if header.get("op") != "METADATA":
            raise RemoteReaderError(f"expected METADATA, got {header.get('op')}")
        return header.get("value")

    def create_reader(self, feed_name: str):
        reader = RemoteReader(
            self.host,
            self.base_path,
            feed_name,
            port=self.port,
            timeout=self.timeout,
            status_callback=self.status_callback,
        )
        self._readers.append(reader)
        return reader

    def create_writer(self, feed_name: str):
        raise NotImplementedError("remote writers are not supported")

    def create_feed(self, spec: dict) -> None:
        raise NotImplementedError("remote feed creation is not supported")

    def delete_feed(self, feed_name: str, *, missing_ok: bool = False) -> bool:
        raise NotImplementedError("remote feed deletion is not supported")

    def list_feeds(self) -> list:
        return self._metadata("LIST_FEEDS")

    def feed_exists(self, feed_name: str) -> bool:
        return bool(self._metadata("FEED_EXISTS", feed_name=feed_name))

    def describe_feed(self, feed_name: str) -> dict:
        return self._metadata("DESCRIBE_FEED", feed_name=feed_name)

    def lifecycle(self, feed_name: str) -> dict:
        return self._metadata("LIFECYCLE", feed_name=feed_name)

    def get_record_format(self, feed_name: str) -> dict:
        return self._metadata("RECORD_FORMAT", feed_name=feed_name)

    def list_segments(self, feed_name: str, status: Optional[str] = None) -> list[dict]:
        return self._metadata("LIST_SEGMENTS", feed_name=feed_name, status=status)

    def suggested_reader_range(self, feed_name: str):
        value = self._metadata("SUGGESTED_READER_RANGE", feed_name=feed_name)
        return tuple(value) if value is not None else None

    def common_time_windows(
        self,
        feed_names: list[str],
        *,
        status: str = "usable",
        min_duration_us: int = 0,
        merge_touching: bool = True,
    ) -> list[dict]:
        if not feed_names:
            raise ValueError("feed_names cannot be empty")

        interval_sets = []
        for feed_name in feed_names:
            if not self.feed_exists(feed_name):
                raise KeyError(feed_name)
            intervals = []
            for seg in self.list_segments(feed_name, status=status):
                start = seg.get("start_us")
                end = seg.get("end_us")
                if start is None or end is None:
                    continue
                start_i = int(start)
                end_i = int(end)
                if start_i <= end_i:
                    intervals.append((start_i, end_i))
            interval_sets.append(intervals)

        commons = common_intervals(interval_sets, merge_touching=merge_touching)
        return with_duration(commons, min_duration_us=int(min_duration_us))

    def recommended_train_validation(
        self,
        feed_names: list[str],
        *,
        status: str = "usable",
        train_ratio: float = 0.8,
        min_duration_us: int = 0,
        merge_touching: bool = True,
    ) -> dict:
        intervals = self.common_time_windows(
            feed_names,
            status=status,
            min_duration_us=min_duration_us,
            merge_touching=merge_touching,
        )
        recommendation = recommend_train_validation(intervals, train_ratio=train_ratio)
        return build_manifest(
            base_path=f"dw://{self.host}:{self.port}{self.base_path}",
            feeds=list(feed_names),
            status_filter=status,
            intervals=intervals,
            recommendation=recommendation,
        )

    def close_reader(self, feed_name: str) -> None:
        return None

    def close_writer(self, feed_name: str) -> None:
        return None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for reader in list(self._readers):
            try:
                reader.close()
            except Exception:
                pass
        self._readers = []
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                write_frame(sock, {"op": "CLOSE", "id": next(self._ids)})
            except Exception:
                pass
            try:
                sock.shutdown(socket.SHUT_RDWR)
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
