from __future__ import annotations

import itertools
import logging
import socket
import struct
from typing import Any, Callable, Iterator, Optional

import numpy as np

from ..io.formatting import format_raw_batch, format_record_at, raw_record_formatter
from ..metadata.feed_schema import FeedSchema
from .path import DEFAULT_PORT, NetworkTarget, parse_remote_target
from .protocol import (
    DEFAULT_MAX_BATCH_RECORDS,
    Frame,
    Op,
    PROTOCOL_VERSION,
    ProtocolError,
    frame,
    op_name,
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


def _socket_request(
    sock: socket.socket,
    ids: Iterator[int],
    op: Op,
    meta: Any = None,
    payload: bytes | bytearray | memoryview | None = None,
) -> tuple[Frame, memoryview]:
    request_id = next(ids)
    write_frame(sock, frame(op, request_id, meta), payload)
    header, response_payload = read_frame(sock)
    if header.id != request_id:
        raise RemoteReaderError(f"response id mismatch: expected {request_id}, got {header.id}")
    if header.op == Op.ERROR:
        args = header.args or ()
        raise RemoteReaderError(str(args[1] if len(args) > 1 else "remote agent error"))
    return header, response_payload


def with_remote_metadata(
    base_path: str,
    fn: Callable[[Callable[..., Any]], Any],
    port: int = DEFAULT_PORT,
    timeout: float = 10.0,
):
    target = base_path if isinstance(base_path, NetworkTarget) else parse_remote_target(base_path, default_port=port)
    if not target.is_remote:
        raise ValueError("remote Deepwater target required")
    assert target.host is not None

    ids = itertools.count(1)
    sock = socket.create_connection((target.host, target.port), timeout=float(timeout))
    try:
        _configure_client_socket(sock)
        header, _payload = _socket_request(
            sock,
            ids,
            Op.HELLO,
            (PROTOCOL_VERSION, {"remote_metadata": True}),
        )
        if header.op != Op.HELLO:
            raise RemoteReaderError(f"expected HELLO, got {op_name(header.op)}")

        def request(op: Op, *args: Any):
            header, _payload = _socket_request(sock, ids, op, (target.path, *args))
            if header.op != Op.METADATA:
                raise RemoteReaderError(f"expected METADATA, got {op_name(header.op)}")
            values = header.args or ()
            return values[0] if values else None

        return fn(request)
    finally:
        try:
            write_frame(sock, frame(Op.CLOSE, next(ids)))
        except Exception:
            pass
        sock.close()


def remote_metadata(
    base_path: str,
    op: Op,
    *args: Any,
    port: int = DEFAULT_PORT,
    timeout: float = 10.0,
):
    return with_remote_metadata(
        base_path,
        lambda request: request(op, *args),
        port=port,
        timeout=timeout,
    )


class RemoteReader:
    __slots__ = (
        "host", "port", "base_path", "feed_name", "timeout", "status_callback",
        "_ids", "_closed", "_sock", "_schema", "_feed_metadata", "_storage",
        "_uses_ring", "record_size", "_struct", "_field_names", "_dtype",
        "protocol_version", "capabilities", "limits",
    )

    def __init__(
        self,
        host: str,
        base_path: str,
        feed_name: str,
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
        self._schema: FeedSchema | None = None
        self._feed_metadata: dict[str, Any] | None = None
        self._storage = ""
        self._uses_ring = False
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
            header, schema_payload = self._request_frame(
                Op.OPEN_READER,
                (self.base_path, self.feed_name),
            )
            if header.op != Op.READER_OPEN:
                raise RemoteReaderError(f"expected READER_OPEN, got {op_name(header.op)}")
            opened = header.args or ()
            if len(opened) < 3:
                raise RemoteReaderError("remote agent did not return reader metadata")
            _feed_name, _path, metadata_payload = opened[:3]
            metadata_payload = metadata_payload or {}
            if schema_payload.nbytes <= 0:
                raise RemoteReaderError("remote agent did not return a feed schema")
            self._schema = FeedSchema.from_msgpack(schema_payload)
            self._feed_metadata = dict(metadata_payload)
            ring_size_bytes = int(metadata_payload.get("ring_size_bytes") or 0)
            self._storage = str(metadata_payload.get("storage") or ("ring" if ring_size_bytes > 0 else "chunk"))
            self._uses_ring = bool(metadata_payload.get("uses_ring", ring_size_bytes > 0))
            self.record_size = int(self._schema.record_size)
            if self.record_size <= 0:
                raise RemoteReaderError("remote agent did not return a valid feed schema")
            self._struct = struct.Struct(self._schema.fmt)
            self._field_names = self._schema.field_names
            self._dtype = self._schema.numpy_dtype
        except Exception:
            self._emit(DW_REMOTE_LINK_DOWN, host=self.host, port=self.port, path=self.base_path, feed=self.feed_name)
            self.close()
            raise

    def _hello(self) -> None:
        try:
            header, _payload = self._request_frame(
                Op.HELLO,
                (
                    PROTOCOL_VERSION,
                    {
                    "paged_range": True,
                    "heartbeat": True,
                    "resume_stream": False,
                    },
                ),
            )
        except RemoteReaderError:
            self.protocol_version = 0
            self.capabilities = {}
            self.limits = {}
            return
        if header.op != Op.HELLO:
            raise RemoteReaderError(f"expected HELLO, got {op_name(header.op)}")
        ack = header.args or ()
        if len(ack) < 3:
            raise RemoteReaderError("remote agent did not return hello metadata")
        self.protocol_version = int(ack[0])
        self.capabilities = dict(ack[1] or {})
        self.limits = dict(ack[2] or {})

    def _emit(self, event: str, **fields: Any) -> None:
        record = {"event": event, **fields}
        log.info("%s %s", event, fields)
        if self.status_callback is not None:
            self.status_callback(record)

    def _request_frame(
        self,
        op: Op,
        meta: Any = None,
        payload: bytes | bytearray | memoryview | None = None,
    ) -> tuple[Frame, memoryview]:
        if self._closed or self._sock is None:
            raise RemoteReaderError("remote reader is closed")
        return _socket_request(self._sock, self._ids, op, meta, payload)

    def _data_request(
        self,
        op: Op,
        meta: Any = None,
        payload: bytes | bytearray | memoryview | None = None,
    ) -> memoryview:
        header, response_payload = self._request_frame(op, meta, payload)
        if header.op != Op.DATA:
            raise RemoteReaderError(f"expected DATA, got {op_name(header.op)}")
        return response_payload

    def _validate_raw(self, raw: bytes | bytearray | memoryview) -> memoryview:
        payload = memoryview(raw).cast("B")
        if payload.nbytes % self.record_size:
            raise ProtocolError(
                f"raw payload size {payload.nbytes} is not a multiple of record size {self.record_size}"
            )
        return payload

    def _format_raw_batch(self, raw: bytes | bytearray | memoryview, format: str):
        if format not in _VALID_FORMATS:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")
        payload = self._validate_raw(raw)
        assert self._struct is not None
        return format_raw_batch(
            payload,
            format,
            self._struct.unpack_from,
            self.record_size,
            self._field_names,
            self._dtype,
        )

    def _format_optional_record(self, raw: bytes | bytearray | memoryview, format: str):
        if format not in _VALID_FORMATS:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")
        payload = self._validate_raw(raw)
        if not payload:
            return None
        assert self._struct is not None
        return format_record_at(
            payload,
            0,
            format,
            self._struct.unpack_from,
            self.record_size,
            self._field_names,
            self._dtype,
        )

    def _range_raw(
        self,
        start: int,
        end: int,
        ts_key: Optional[str] = None,
    ) -> memoryview:
        return self._data_request(
            Op.READ_RANGE,
            (int(start), int(end), ts_key, 0),
        )

    def _iter_raw_range(
        self,
        start: int,
        end: int,
        ts_key: Optional[str] = None,
        batch_records: int = DEFAULT_MAX_BATCH_RECORDS,
    ) -> Iterator[memoryview]:
        if batch_records <= 0:
            raise ValueError("batch_records must be positive")
        if not self.capabilities.get("paged_range"):
            raw = self._range_raw(start, end, ts_key=ts_key)
            step = int(batch_records) * self.record_size
            for pos in range(0, raw.nbytes, step):
                yield raw[pos:pos + step]
            return

        if self._closed or self._sock is None:
            raise RemoteReaderError("remote reader is closed")
        request_id = next(self._ids)
        write_frame(
            self._sock,
            frame(
                Op.READ_RANGE_PAGE,
                request_id,
                (int(start), int(end), ts_key, int(batch_records)),
            ),
        )
        while True:
            header, payload = read_frame(self._sock)
            if header.id != request_id:
                raise RemoteReaderError(f"response id mismatch: expected {request_id}, got {header.id}")
            if header.op == Op.ERROR:
                args = header.args or ()
                raise RemoteReaderError(str(args[1] if len(args) > 1 else "remote agent error"))
            if header.op == Op.RANGE_END:
                return
            if header.op != Op.DATA:
                raise RemoteReaderError(f"expected DATA or RANGE_END, got {op_name(header.op)}")
            yield self._validate_raw(payload)

    def _metadata(self, op: Op, *args: Any):
        header, _payload = self._request_frame(op, args or None)
        if header.op != Op.METADATA:
            raise RemoteReaderError(f"expected METADATA, got {op_name(header.op)}")
        values = header.args or ()
        return values[0] if values else None

    @property
    def layout(self) -> FeedSchema:
        assert self._schema is not None
        return self._schema

    @property
    def record_format(self) -> FeedSchema:
        return self.layout

    @property
    def metadata(self) -> dict[str, Any]:
        assert self._feed_metadata is not None
        metadata = dict(self._feed_metadata)
        metadata["storage"] = self._storage
        metadata["uses_ring"] = self._uses_ring
        return metadata

    @property
    def fields(self) -> tuple[dict[str, Any], ...]:
        return tuple(field.to_dict() for field in self.layout.fields)

    @property
    def timestamp_fields(self) -> tuple[dict[str, Any], ...]:
        return tuple(field.to_dict() for field in self.layout.timestamp_fields)

    @property
    def is_persistent(self) -> bool:
        assert self._feed_metadata is not None
        return bool(self._feed_metadata.get("persist"))

    @property
    def format(self) -> str:
        return self.layout.fmt

    @property
    def field_names(self) -> tuple[str, ...]:
        return self._field_names

    @property
    def dtype(self):
        return self._dtype

    def schema(self) -> dict[str, Any]:
        return self._metadata(Op.READER_SCHEMA)

    def ring_state(self) -> dict[str, Any]:
        return self._metadata(Op.RING_STATE)

    def last_timestamp(self, ts_key: Optional[str] = None) -> int | None:
        value = self._metadata(Op.LAST_TIMESTAMP, ts_key)
        return None if value is None else int(value)

    def first_after(
        self,
        start: int,
        format: str = "tuple",
        ts_key: Optional[str] = None,
    ):
        try:
            raw = self._data_request(Op.FIRST_AFTER, (int(start), ts_key))
            return self._format_optional_record(raw, format)
        except Exception as exc:
            self._emit(DW_REMOTE_READ_ERROR, host=self.host, port=self.port, feed=self.feed_name, error=str(exc))
            raise

    def first_before(
        self,
        ts: int,
        format: str = "tuple",
        ts_key: Optional[str] = None,
    ):
        try:
            raw = self._data_request(Op.FIRST_BEFORE, (int(ts), ts_key))
            return self._format_optional_record(raw, format)
        except Exception as exc:
            self._emit(DW_REMOTE_READ_ERROR, host=self.host, port=self.port, feed=self.feed_name, error=str(exc))
            raise

    def range(
        self,
        start: int,
        end: int,
        format: str = "tuple",
        ts_key: Optional[str] = None,
    ):
        try:
            return self._format_raw_batch(
                self._range_raw(start, end, ts_key=ts_key),
                format,
            )
        except Exception as exc:
            self._emit(DW_REMOTE_READ_ERROR, host=self.host, port=self.port, feed=self.feed_name, error=str(exc))
            raise

    def range_columns(
        self,
        start: int,
        end: int,
        columns: list[str] | tuple[str, ...],
        ts_key: Optional[str] = None,
    ) -> dict[str, np.ndarray]:
        arr = self.range(start, end, format="numpy", ts_key=ts_key)
        return {name: arr[name] for name in columns}

    def range_batches(
        self,
        start: int,
        end: int,
        format: str = "tuple",
        ts_key: Optional[str] = None,
        batch_records: int = DEFAULT_MAX_BATCH_RECORDS,
    ) -> Iterator:
        if format not in _VALID_FORMATS:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")
        if batch_records <= 0:
            raise ValueError("batch_records must be positive")
        try:
            for raw in self._iter_raw_range(
                start,
                end,
                ts_key=ts_key,
                batch_records=batch_records,
            ):
                yield self._format_raw_batch(raw, format)
        except Exception as exc:
            self._emit(DW_REMOTE_READ_ERROR, host=self.host, port=self.port, feed=self.feed_name, error=str(exc))
            raise

    def read_available(self, max_records: Optional[int] = None, format: str = "tuple"):
        try:
            raw = self._data_request(
                Op.READ_AVAILABLE,
                (None if max_records is None else int(max_records),),
            )
            return self._format_raw_batch(raw, format)
        except Exception as exc:
            self._emit(DW_REMOTE_READ_ERROR, host=self.host, port=self.port, feed=self.feed_name, error=str(exc))
            raise

    def latest(self, seconds: float = 60.0, format: str = "tuple", ts_key: Optional[str] = None):
        try:
            raw = self._data_request(Op.LATEST, (float(seconds), ts_key))
            return self._format_raw_batch(raw, format)
        except Exception as exc:
            self._emit(DW_REMOTE_READ_ERROR, host=self.host, port=self.port, feed=self.feed_name, error=str(exc))
            raise

    def stream(
        self,
        start: Optional[int] = None,
        format: str = "tuple",
        ts_key: Optional[str] = None,
    ) -> Iterator:
        if format not in _VALID_FORMATS:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")
        if self._closed or self._sock is None:
            raise RemoteReaderError("remote reader is closed")

        request_id = next(self._ids)
        write_frame(
            self._sock,
            frame(Op.SUBSCRIBE_LIVE, request_id, (start, ts_key)),
        )
        header, _payload = read_frame(self._sock)
        if header.id != request_id:
            raise RemoteReaderError(f"response id mismatch: expected {request_id}, got {header.id}")
        if header.op == Op.ERROR:
            args = header.args or ()
            raise RemoteReaderError(str(args[1] if len(args) > 1 else "remote agent error"))
        if header.op != Op.SUBSCRIBED:
            raise RemoteReaderError(f"expected SUBSCRIBED, got {op_name(header.op)}")
        subscribed = header.args or ()
        if len(subscribed) < 3:
            raise RemoteReaderError("remote agent did not return subscribe metadata")
        self._emit(
            DW_REMOTE_STREAM_SUBSCRIBED,
            host=self.host,
            port=self.port,
            path=self.base_path,
            feed=self.feed_name,
            heartbeat_interval_s=subscribed[2],
        )

        try:
            assert self._struct is not None
            format_record = raw_record_formatter(
                format,
                self._struct.unpack_from,
                self._field_names,
                self._dtype,
            )
            while True:
                header, payload = read_frame(self._sock)
                if header.op == Op.ERROR:
                    args = header.args or ()
                    raise RemoteReaderError(str(args[1] if len(args) > 1 else "remote agent error"))
                if header.op == Op.HEARTBEAT:
                    heartbeat = header.args or ()
                    self._emit(
                        DW_REMOTE_HEARTBEAT,
                        host=self.host,
                        port=self.port,
                        path=self.base_path,
                        feed=self.feed_name,
                        server_time_us=heartbeat[1] if len(heartbeat) > 1 else None,
                        idle_s=heartbeat[2] if len(heartbeat) > 2 else None,
                    )
                    continue
                if header.op != Op.DATA:
                    continue
                raw = self._validate_raw(payload)
                for pos in range(0, raw.nbytes, self.record_size):
                    yield format_record(raw[pos:pos + self.record_size])
        except Exception as exc:
            self._emit(DW_REMOTE_LINK_DOWN, host=self.host, port=self.port, feed=self.feed_name, error=str(exc))
            raise
        finally:
            self.close()

    def ping(self) -> bool:
        header, _payload = self._request_frame(Op.PING)
        return header.op == Op.PONG

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                write_frame(sock, frame(Op.CLOSE, next(self._ids)))
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
