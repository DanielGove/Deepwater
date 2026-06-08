from __future__ import annotations

import socket
import struct
from enum import IntEnum
from typing import Any

import msgspec


MAX_FRAME_BYTES = 512 * 1024 * 1024
MAX_HEADER_BYTES = 1024 * 1024
PROTOCOL_VERSION = 3
DEFAULT_MAX_BATCH_RECORDS = 50_000
DEFAULT_HEARTBEAT_INTERVAL_S = 5.0
DEFAULT_STREAM_POLL_INTERVAL_S = 0.01
DEFAULT_IDLE_TIMEOUT_S = 300.0

SERVER_CAPABILITIES = {
    "paged_range": True,
    "heartbeat": True,
    "resume_stream": False,
    "multi_reader_session": False,
}

SERVER_LIMITS = {
    "max_frame_bytes": MAX_FRAME_BYTES,
    "max_header_bytes": MAX_HEADER_BYTES,
    "max_batch_records": DEFAULT_MAX_BATCH_RECORDS,
    "heartbeat_interval_s": DEFAULT_HEARTBEAT_INTERVAL_S,
    "idle_timeout_s": DEFAULT_IDLE_TIMEOUT_S,
}

_FRAME_LEN = struct.Struct("!Q")
_HEADER_LEN = struct.Struct("!I")
_HEADER_ENCODER = msgspec.msgpack.Encoder()


class Op(IntEnum):
    PING = 1
    PONG = 2
    HELLO = 3
    OPEN_READER = 4
    READER_OPEN = 5
    READER_DESCRIBE = 6
    STATE = 7
    LIST_FEEDS = 8
    FEED_EXISTS = 9
    DESCRIBE_FEED = 10
    FEED_METADATA = 11
    RECORD_FORMAT = 12
    LIST_SEGMENTS = 13
    SUGGESTED_READER_RANGE = 14
    READ_RANGE = 15
    READ_RANGE_PAGE = 16
    FIRST_AFTER = 17
    FIRST_BEFORE = 18
    LATEST = 19
    READ_AVAILABLE = 20
    SUBSCRIBE_LIVE = 21
    SUBSCRIBED = 22
    HEARTBEAT = 23
    DATA = 24
    METADATA = 25
    RANGE_END = 26
    CLOSE = 27
    CLOSED = 28
    ERROR = 29


class Frame(msgspec.Struct, array_like=True, frozen=True):
    op: int
    id: int = 0
    args: Any = None


class ProtocolError(RuntimeError):
    pass


def op_name(op: int) -> str:
    try:
        return Op(int(op)).name
    except ValueError:
        return str(op)


def frame(op: Op | int, id: int = 0, args: Any = None) -> Frame:
    return Frame(int(op), int(id), args)


def _payload_view(payload: bytes | bytearray | memoryview | None) -> memoryview:
    if payload is None:
        return memoryview(b"")
    return memoryview(payload).cast("B")


def _encode_header(header: Frame) -> bytes:
    header_bytes = _HEADER_ENCODER.encode(header)
    if len(header_bytes) > MAX_HEADER_BYTES:
        raise ProtocolError(f"header too large: {len(header_bytes)} bytes")
    return header_bytes


def _frame_prefix(header_bytes: bytes, payload: bytes | bytearray | memoryview | None = None) -> bytes:
    payload_bytes = _payload_view(payload)
    body_len = _HEADER_LEN.size + len(header_bytes) + payload_bytes.nbytes
    if body_len > MAX_FRAME_BYTES:
        raise ProtocolError(f"frame too large: {body_len} bytes")
    return b"".join((
        _FRAME_LEN.pack(body_len),
        _HEADER_LEN.pack(len(header_bytes)),
    ))


def encode_frame(header: Frame, payload: bytes | bytearray | memoryview | None = None) -> bytes:
    header_bytes = _encode_header(header)
    payload_bytes = _payload_view(payload)
    return b"".join((_frame_prefix(header_bytes, payload_bytes), header_bytes, payload_bytes.tobytes()))


def _decode_header(header_bytes: bytes | bytearray | memoryview) -> Frame:
    try:
        return msgspec.msgpack.decode(header_bytes, type=Frame)
    except Exception as exc:
        raise ProtocolError(f"invalid frame header: {exc}") from exc


def decode_frame(frame_bytes: bytes | bytearray | memoryview) -> tuple[Frame, memoryview]:
    data = memoryview(frame_bytes).cast("B")
    if data.nbytes < _FRAME_LEN.size + _HEADER_LEN.size:
        raise ProtocolError("truncated frame")

    body_len = _FRAME_LEN.unpack(data[:_FRAME_LEN.size])[0]
    if body_len > MAX_FRAME_BYTES:
        raise ProtocolError(f"frame too large: {body_len} bytes")
    if body_len != data.nbytes - _FRAME_LEN.size:
        raise ProtocolError("frame length mismatch")

    pos = _FRAME_LEN.size
    header_len = _HEADER_LEN.unpack(data[pos:pos + _HEADER_LEN.size])[0]
    pos += _HEADER_LEN.size
    if header_len > MAX_HEADER_BYTES:
        raise ProtocolError(f"header too large: {header_len} bytes")
    if header_len > data.nbytes - pos:
        raise ProtocolError("header length exceeds frame")

    header_bytes = data[pos:pos + header_len]
    pos += header_len
    return _decode_header(header_bytes), data[pos:]


def _read_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            if not chunks:
                raise EOFError("connection closed")
            raise ProtocolError("connection closed mid-frame")
        chunks.extend(chunk)
    return bytes(chunks)


def read_frame(sock: socket.socket) -> tuple[Frame, memoryview]:
    prefix = _read_exact(sock, _FRAME_LEN.size)
    body_len = _FRAME_LEN.unpack(prefix)[0]
    if body_len > MAX_FRAME_BYTES:
        raise ProtocolError(f"frame too large: {body_len} bytes")
    body = _read_exact(sock, body_len)
    data = memoryview(body).cast("B")
    if data.nbytes < _HEADER_LEN.size:
        raise ProtocolError("truncated frame body")
    header_len = _HEADER_LEN.unpack(data[:_HEADER_LEN.size])[0]
    pos = _HEADER_LEN.size
    if header_len > MAX_HEADER_BYTES:
        raise ProtocolError(f"header too large: {header_len} bytes")
    if header_len > data.nbytes - pos:
        raise ProtocolError("header length exceeds frame")
    header = _decode_header(data[pos:pos + header_len])
    return header, data[pos + header_len:]


def write_frame(
    sock: socket.socket,
    header: Frame,
    payload: bytes | bytearray | memoryview | None = None,
) -> None:
    header_bytes = _encode_header(header)
    payload_bytes = _payload_view(payload)
    prefix = _frame_prefix(header_bytes, payload_bytes)
    sock.sendall(prefix)
    sock.sendall(header_bytes)
    if payload_bytes.nbytes:
        sock.sendall(payload_bytes)
