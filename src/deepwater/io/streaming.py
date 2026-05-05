from __future__ import annotations

import time
from typing import Iterator, Optional

import numpy as np

from ..metadata.feed_registry import ON_DISK


def stream_tuples(reader, start_us: Optional[int], ts_off: int, playback: bool = False) -> Iterator[tuple]:
    """Stream tuples from historical replay into live tail."""
    unpack = reader._unpack
    rec_size = reader._rec_size

    if start_us is not None:
        if playback:
            start_us = reader._playback_start(start_us, reader._key_id_for_offset(reader._ts_fields, ts_off))
        latest_chunk_id = reader.registry.get_latest_chunk_idx()

        for chunk_id in reader._iter_chunks_in_range(start_us, None, ts_off=ts_off):
            try:
                reader._open_chunk(chunk_id)
            except FileNotFoundError:
                continue
            buf = reader._chunk.buffer
            write_pos = reader._chunk_meta.write_pos
            pos = reader._binary_search_start(start_us, ts_off, "little")

            while pos + rec_size <= write_pos:
                yield unpack(buf, pos)
                pos += rec_size

            if chunk_id >= latest_chunk_id:
                break

        if reader._chunk is not None:
            read_head = reader._chunk_meta.write_pos
        else:
            reader._ensure_latest_chunk()
            read_head = reader._chunk_meta.write_pos
    else:
        reader._ensure_latest_chunk()
        read_head = reader._chunk_meta.write_pos

    while True:
        if reader._chunk_meta is None:
            reader._ensure_latest_chunk()
            read_head = reader._chunk_meta.write_pos

        buf = reader._chunk.buffer
        write_pos = reader._chunk_meta.write_pos

        while read_head + rec_size <= write_pos:
            yield unpack(buf, read_head)
            read_head += rec_size

        if reader._chunk_meta.status == ON_DISK:
            latest_chunk_id = reader.registry.get_latest_chunk_idx()
            if reader._chunk_id < latest_chunk_id:
                next_id = reader._chunk_id + 1
                next_meta = reader.registry.get_chunk_metadata(next_id)
                if next_meta is not None:
                    try:
                        reader._open_chunk(next_id)
                        read_head = 0
                        next_meta.release()
                        continue
                    except FileNotFoundError:
                        next_meta.release()
                        try:
                            reader._ensure_latest_chunk()
                            read_head = reader._chunk_meta.write_pos
                            continue
                        except Exception:
                            return
                    next_meta.release()
        # Spin-wait for lowest local latency.


def stream_dicts(reader, start_us: Optional[int], ts_off: int, playback: bool = False) -> Iterator[dict]:
    names = reader._field_names
    for rec in stream_tuples(reader, start_us, ts_off=ts_off, playback=playback):
        yield {names[i]: rec[i] for i in range(len(names))}


def stream_numpy(reader, start_us: Optional[int], ts_off: int, playback: bool = False) -> Iterator:
    dtype = reader.dtype
    for rec in stream_tuples(reader, start_us, ts_off=ts_off, playback=playback):
        yield np.array([rec], dtype=dtype)[0]


def stream_raw(reader, start_us: Optional[int], ts_off: int, playback: bool = False) -> Iterator[memoryview]:
    """Stream raw record memoryviews from historical replay into live tail."""
    rec_size = reader._rec_size

    if start_us is not None:
        if playback:
            start_us = reader._playback_start(start_us, reader._key_id_for_offset(reader._ts_fields, ts_off))
        for chunk_id in reader._iter_chunks_in_range(start_us, None, ts_off=ts_off):
            try:
                reader._open_chunk(chunk_id)
            except FileNotFoundError:
                continue
            buf = reader._chunk.buffer
            write_pos = reader._chunk_meta.write_pos
            pos = reader._binary_search_start(start_us, ts_off, "little")

            while pos + rec_size <= write_pos:
                yield buf[pos:pos + rec_size]
                pos += rec_size

    reader._ensure_latest_chunk()
    read_head = reader._chunk_meta.write_pos

    while True:
        if reader._chunk_meta is None:
            reader._ensure_latest_chunk()
            read_head = 0

        buf = reader._chunk.buffer
        write_pos = reader._chunk_meta.write_pos

        while read_head + rec_size <= write_pos:
            yield buf[read_head:read_head + rec_size]
            read_head += rec_size

        if reader._chunk_meta.status == ON_DISK:
            next_id = reader._chunk_id + 1
            try:
                if reader.registry.get_chunk_metadata(next_id) is not None:
                    reader._open_chunk(next_id)
                    read_head = 0
                    continue
            except FileNotFoundError:
                pass

        time.sleep(0.0001)


def read_available(reader, max_records: Optional[int] = None, format: str = "tuple"):
    """Read currently available records from the reader's stateful live cursor."""
    if reader._read_head is None:
        reader._ensure_latest_chunk()
        reader._read_head = reader._chunk_meta.write_pos

    if reader._chunk_meta is None:
        reader._ensure_latest_chunk()
        reader._read_head = reader._chunk_meta.write_pos

    latest_chunk_id = reader.registry.get_latest_chunk_idx()
    if reader._chunk_id < latest_chunk_id:
        reader._open_chunk(latest_chunk_id)
        reader._read_head = 0

    result = []
    buf = reader._chunk.buffer
    write_pos = reader._chunk_meta.write_pos
    rec_size = reader._rec_size
    unpack = reader._unpack
    names = reader._field_names

    while reader._read_head + rec_size <= write_pos:
        if format == "tuple":
            result.append(unpack(buf, reader._read_head))
        elif format == "dict":
            rec = unpack(buf, reader._read_head)
            result.append({names[i]: rec[i] for i in range(len(names))})
        elif format == "raw":
            result.append(buf[reader._read_head:reader._read_head + rec_size])
        else:
            raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', or 'raw'")

        reader._read_head += rec_size

        if max_records and len(result) >= max_records:
            break

    return result
