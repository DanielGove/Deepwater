from __future__ import annotations

from typing import Callable, Iterable, Iterator, Optional, Tuple


RawRangeChunk = Tuple[object, int, int]
ChunkState = Tuple[object, int]


def iter_chunk_raw_range(
    chunk_ids: Iterable[int],
    *,
    open_chunk: Callable[[int], None],
    chunk_state: Callable[[], ChunkState],
    binary_search_start: Callable[[int, int, str], int],
    start_us: int,
    end_us: Optional[int],
    rec_size: int,
    ts_off: int,
    batch_records: Optional[int] = None,
) -> Iterator[memoryview]:
    """
    Traverse candidate chunks, find timestamp boundaries, and yield raw batches.

    Reader-owned callbacks keep platform/chunk lifecycle outside this module.
    This module owns the raw historical range iteration shape: chunk traversal,
    binary-search boundary lookup, timestamp window scanning, and raw batch
    production.
    """
    def chunks() -> Iterator[RawRangeChunk]:
        for chunk_id in chunk_ids:
            try:
                open_chunk(chunk_id)
            except FileNotFoundError:
                continue
            buf, write_pos = chunk_state()
            yield buf, write_pos, binary_search_start(start_us, ts_off, "little")

    yield from iter_raw_range(
        chunks(),
        start_us=start_us,
        end_us=end_us,
        rec_size=rec_size,
        ts_off=ts_off,
        batch_records=batch_records,
    )


def iter_raw_range(
    chunks: Iterable[RawRangeChunk],
    *,
    start_us: int,
    end_us: Optional[int],
    rec_size: int,
    ts_off: int,
    batch_records: Optional[int] = None,
) -> Iterator[memoryview]:
    """
    Scan historical chunk buffers and yield owned raw memoryview batches.

    The reader owns chunk opening, lifecycle, playback adjustment, and timestamp
    key resolution. This helper only walks already-open buffer spans and applies
    the existing [start_us, end_us) timestamp semantics.
    """
    from_bytes = int.from_bytes

    if batch_records is None:
        for buf, write_pos, pos in chunks:
            end_pos = pos
            while end_pos + rec_size <= write_pos:
                ts = from_bytes(buf[end_pos + ts_off:end_pos + ts_off + 8], "little")
                if ts < start_us:
                    end_pos += rec_size
                    pos = end_pos
                    continue
                if end_us is not None and ts >= end_us:
                    break
                end_pos += rec_size

            if end_pos > pos:
                yield memoryview(bytes(buf[pos:end_pos]))
        return

    if batch_records <= 0:
        raise ValueError("batch_records must be positive")

    batch_bytes = int(batch_records) * rec_size
    batch = bytearray()

    for buf, write_pos, pos in chunks:
        while pos + rec_size <= write_pos:
            ts = from_bytes(buf[pos + ts_off:pos + ts_off + 8], "little")
            if ts < start_us:
                pos += rec_size
                continue
            if end_us is not None and ts >= end_us:
                break

            if len(batch) + rec_size > batch_bytes and batch:
                yield memoryview(bytes(batch))
                batch = bytearray()
            batch.extend(buf[pos:pos + rec_size])
            pos += rec_size

    if batch:
        yield memoryview(bytes(batch))
