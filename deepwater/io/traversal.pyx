# cython: boundscheck=False, wraparound=False, cdivision=True, language_level=3
from __future__ import annotations

from libc.stdint cimport uint64_t
from typing import Iterator


cdef inline uint64_t _read_u64_le(const unsigned char[:] buf, Py_ssize_t offset) noexcept nogil:
    return (<const uint64_t *> &buf[offset])[0]


def chunk_lower_bound(
    const unsigned char[:] buf,
    unsigned long long target,
    Py_ssize_t write_pos,
    Py_ssize_t rec_size,
    Py_ssize_t ts_off,
):
    """Return byte offset of the first chunk record with timestamp >= target."""
    cdef Py_ssize_t lo = 0
    cdef Py_ssize_t hi = write_pos // rec_size
    cdef Py_ssize_t mid
    cdef Py_ssize_t pos
    while lo < hi:
        mid = (lo + hi) >> 1
        pos = mid * rec_size
        if _read_u64_le(buf, pos + ts_off) < target:
            lo = mid + 1
        else:
            hi = mid
    return lo * rec_size


def chunk_raw_batches(buf, Py_ssize_t pos, Py_ssize_t end_pos, Py_ssize_t batch_bytes = 0) -> Iterator[memoryview]:
    """Yield owned raw batches over a contiguous chunk byte range."""
    cdef Py_ssize_t nxt
    if end_pos <= pos:
        return
    if batch_bytes <= 0:
        yield memoryview(bytes(buf[pos:end_pos]))
        return
    while pos < end_pos:
        nxt = min(pos + batch_bytes, end_pos)
        yield memoryview(bytes(buf[pos:nxt]))
        pos = nxt


cdef inline Py_ssize_t _ring_pos(
    Py_ssize_t start_pos,
    Py_ssize_t earliest_seq,
    Py_ssize_t seq,
    Py_ssize_t rec_size,
    Py_ssize_t ring_usable_bytes,
) noexcept:
    return (start_pos + ((seq - earliest_seq) * rec_size)) % ring_usable_bytes


def ring_lower_bound(
    const unsigned char[:] data,
    Py_ssize_t start_pos,
    Py_ssize_t earliest_seq,
    Py_ssize_t start_seq,
    Py_ssize_t end_seq,
    unsigned long long target,
    Py_ssize_t ts_off,
    Py_ssize_t rec_size,
    Py_ssize_t ring_usable_bytes,
):
    """Return sequence id of the first live ring record with timestamp >= target."""
    cdef Py_ssize_t lo = start_seq
    cdef Py_ssize_t hi = end_seq
    cdef Py_ssize_t mid
    cdef Py_ssize_t pos
    while lo < hi:
        mid = (lo + hi) >> 1
        pos = _ring_pos(start_pos, earliest_seq, mid, rec_size, ring_usable_bytes)
        if _read_u64_le(data, pos + ts_off) < target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def ring_copy_records(
    data,
    Py_ssize_t pos,
    Py_ssize_t n_records,
    Py_ssize_t rec_size,
    Py_ssize_t ring_usable_bytes,
) -> memoryview:
    """Return records from a circular ring as one contiguous byte view."""
    cdef Py_ssize_t n_bytes = n_records * rec_size
    if n_bytes <= 0:
        return memoryview(b"")
    return data[pos:pos + n_bytes]


def ring_raw_batches(
    data,
    Py_ssize_t start_pos,
    Py_ssize_t earliest_seq,
    Py_ssize_t start_seq,
    Py_ssize_t end_seq,
    Py_ssize_t rec_size,
    Py_ssize_t ring_usable_bytes,
    Py_ssize_t batch_records,
) -> Iterator[memoryview]:
    """Yield contiguous raw batches over a sequence range in a circular ring."""
    cdef Py_ssize_t seq = start_seq
    cdef Py_ssize_t take
    cdef Py_ssize_t pos
    if end_seq <= start_seq:
        return
    if batch_records <= 0:
        raise ValueError("batch_records must be positive")
    while seq < end_seq:
        take = min(batch_records, end_seq - seq)
        pos = _ring_pos(start_pos, earliest_seq, seq, rec_size, ring_usable_bytes)
        yield ring_copy_records(data, pos, take, rec_size, ring_usable_bytes)
        seq += take
