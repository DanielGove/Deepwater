from __future__ import annotations

import json
import socket
import struct
from collections.abc import Mapping
from typing import Any


MAX_FRAME_BYTES = 512 * 1024 * 1024
MAX_HEADER_BYTES = 1024 * 1024

_FRAME_LEN = struct.Struct("!Q")
_HEADER_LEN = struct.Struct("!I")


class ProtocolError(RuntimeError):
    pass


def _payload_view(payload: bytes | bytearray | memoryview | None) -> memoryview:
    if payload is None:
        return memoryview(b"")
    return memoryview(payload).cast("B")


def encode_frame(header: Mapping[str, Any], payload: bytes | bytearray | memoryview | None = None) -> bytes:
    header_bytes = json.dumps(dict(header), separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(header_bytes) > MAX_HEADER_BYTES:
        raise ProtocolError(f"header too large: {len(header_bytes)} bytes")
    payload_bytes = _payload_view(payload)
    body_len = _HEADER_LEN.size + len(header_bytes) + payload_bytes.nbytes
    if body_len > MAX_FRAME_BYTES:
        raise ProtocolError(f"frame too large: {body_len} bytes")
    return b"".join((
        _FRAME_LEN.pack(body_len),
        _HEADER_LEN.pack(len(header_bytes)),
        header_bytes,
        payload_bytes.tobytes(),
    ))


def decode_frame(frame: bytes | bytearray | memoryview) -> tuple[dict[str, Any], bytes]:
    data = memoryview(frame).cast("B")
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

    header_bytes = data[pos:pos + header_len].tobytes()
    pos += header_len
    try:
        header = json.loads(header_bytes.decode("utf-8"))
    except Exception as exc:
        raise ProtocolError(f"invalid frame header: {exc}") from exc
    if not isinstance(header, dict):
        raise ProtocolError("frame header must decode to an object")
    return header, data[pos:].tobytes()


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


def read_frame(sock: socket.socket) -> tuple[dict[str, Any], bytes]:
    prefix = _read_exact(sock, _FRAME_LEN.size)
    body_len = _FRAME_LEN.unpack(prefix)[0]
    if body_len > MAX_FRAME_BYTES:
        raise ProtocolError(f"frame too large: {body_len} bytes")
    body = _read_exact(sock, body_len)
    return decode_frame(prefix + body)


def write_frame(
    sock: socket.socket,
    header: Mapping[str, Any],
    payload: bytes | bytearray | memoryview | None = None,
) -> None:
    sock.sendall(encode_frame(header, payload))
