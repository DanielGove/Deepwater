from __future__ import annotations

import struct
from typing import Iterator

_U64 = struct.Struct("<Q")


def _ts_at(buf, pos: int, ts_off: int) -> int:
    return _U64.unpack_from(buf, pos + ts_off)[0]


def chunk_lower_bound(buf, target: int, write_pos: int, rec_size: int, ts_off: int) -> int:
    """Return byte offset of the first chunk record with timestamp >= target."""
    lo = 0
    hi = int(write_pos) // int(rec_size)
    while lo < hi:
        mid = (lo + hi) >> 1
        pos = mid * rec_size
        if _ts_at(buf, pos, ts_off) < target:
            lo = mid + 1
        else:
            hi = mid
    return lo * rec_size


def chunk_raw_batches(buf, pos: int, end_pos: int, batch_bytes: int = 0) -> Iterator[memoryview]:
    """Yield owned raw batches over a contiguous chunk byte range."""
    pos = int(pos)
    end_pos = int(end_pos)
    if end_pos <= pos:
        return
    if not batch_bytes or batch_bytes <= 0:
        yield memoryview(bytes(buf[pos:end_pos]))
        return
    batch_bytes = int(batch_bytes)
    while pos < end_pos:
        nxt = min(pos + batch_bytes, end_pos)
        yield memoryview(bytes(buf[pos:nxt]))
        pos = nxt


def _ring_pos(start_pos: int, earliest_seq: int, seq: int, rec_size: int, ring_usable_bytes: int) -> int:
    return (start_pos + ((seq - earliest_seq) * rec_size)) % ring_usable_bytes


def ring_lower_bound(
    data,
    start_pos: int,
    earliest_seq: int,
    start_seq: int,
    end_seq: int,
    target: int,
    ts_off: int,
    rec_size: int,
    ring_usable_bytes: int,
) -> int:
    """Return sequence id of the first live ring record with timestamp >= target."""
    lo = int(start_seq)
    hi = int(end_seq)
    while lo < hi:
        mid = (lo + hi) >> 1
        pos = _ring_pos(start_pos, earliest_seq, mid, rec_size, ring_usable_bytes)
        if _ts_at(data, pos, ts_off) < target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def ring_copy_records(data, pos: int, n_records: int, rec_size: int, ring_usable_bytes: int) -> memoryview:
    """Copy records from a circular ring into one contiguous owned byte buffer."""
    n_bytes = int(n_records) * int(rec_size)
    pos = int(pos)
    if n_bytes <= 0:
        return memoryview(b"")
    first = min(n_bytes, int(ring_usable_bytes) - pos)
    if first == n_bytes:
        return memoryview(bytes(data[pos:pos + first]))
    out = bytearray(n_bytes)
    out[:first] = data[pos:pos + first]
    out[first:] = data[:n_bytes - first]
    return memoryview(out)


def ring_raw_batches(
    data,
    start_pos: int,
    earliest_seq: int,
    start_seq: int,
    end_seq: int,
    rec_size: int,
    ring_usable_bytes: int,
    batch_records: int,
) -> Iterator[memoryview]:
    """Yield owned raw batches over a sequence range in a circular ring."""
    if end_seq <= start_seq:
        return
    batch_records = int(batch_records)
    if batch_records <= 0:
        raise ValueError("batch_records must be positive")
    seq = int(start_seq)
    while seq < end_seq:
        take = min(batch_records, int(end_seq) - seq)
        pos = _ring_pos(start_pos, earliest_seq, seq, rec_size, ring_usable_bytes)
        yield ring_copy_records(data, pos, take, rec_size, ring_usable_bytes)
        seq += take
