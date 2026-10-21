"""Bounded parallel chunk reads, yielded in original capture order."""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from operator import index
import os

import numpy as np

from ...metadata.feed_registry import ON_DISK


def iter_raw(reader, start, end, offset, batch_records, read_ahead_chunks,
             buffer_bytes, *, skip_missing=False):
    start, end = index(start), index(end)
    batch_records = index(batch_records)
    slots, budget = index(read_ahead_chunks), index(buffer_bytes)
    if not 1 <= slots <= 64 or budget <= 0 or batch_records <= 0:
        raise ValueError('Read-ahead requires 1..64 chunks, positive buffer and batch size')
    if reader.feed_registry is None:
        raise ValueError('Chunk read-ahead requires a local persistent feed')
    if end <= start:
        return
    size = reader.record_size

    def descriptors():
        for ident in reader.feed_registry.get_chunks_in_range(start, end, qoff=offset):
            meta = reader.feed_registry.get_chunk_metadata(ident)
            if meta is None:
                raise RuntimeError(f'Chunk {ident} metadata not found')
            try:
                written, extent, status = int(meta.write_pos), int(meta.size), int(meta.status)
            finally:
                meta.release()
            if status != ON_DISK or written % size or not 0 <= written <= extent:
                raise RuntimeError('Invalid published chunk extent/status')
            if written > budget:
                raise ValueError('Published chunk exceeds read-ahead buffer budget')
            if written:
                yield int(ident), written

    def read_chunk(ident, written):
        # Opening directly also avoids a separate pathname stat/exists probe.
        fd = os.open(reader.data_dir / f'chunk_{ident:08d}.bin', os.O_RDONLY)
        try:
            raw = os.pread(fd, written, 0)
            if len(raw) != written:
                raise RuntimeError('Short read of published chunk')
            return raw
        finally:
            os.close(fd)

    work = iter(descriptors())
    pending = deque()
    waiting = None
    exhausted = False
    reserved = 0
    with ThreadPoolExecutor(max_workers=slots, thread_name_prefix='dw-scan') as pool:
        def fill():
            nonlocal waiting, exhausted, reserved
            while len(pending) < slots and not exhausted:
                if waiting is None:
                    try:
                        waiting = next(work)
                    except StopIteration:
                        exhausted = True
                        break
                ident, written = waiting
                if reserved + written > budget:
                    break
                pending.append((written, pool.submit(read_chunk, ident, written)))
                reserved += written
                waiting = None

        try:
            fill()
            while pending:
                written, future = pending.popleft()
                try:
                    raw = future.result()
                except FileNotFoundError:
                    if not skip_missing:
                        raise
                    raw = b''
                # Keep the current payload reserved until its batches are consumed.
                for pos in range(0, len(raw), batch_records * size):
                    piece = raw[pos:pos + batch_records * size]
                    count = len(piece) // size
                    keys = np.ndarray((count,), dtype='<u8', buffer=piece,
                                      offset=offset, strides=(size,))
                    keep = np.ones(count, dtype=bool)
                    if start > 2**64-1 or end <= 0:
                        keep[:] = False
                    else:
                        if start > 0:
                            keep &= keys >= np.uint64(start)
                        if end <= 2**64-1:
                            keep &= keys < np.uint64(end)
                    if keep.all():
                        yield memoryview(piece)
                    elif keep.any():
                        yield memoryview(np.frombuffer(piece, dtype=f'V{size}')[keep].tobytes())
                if raw:
                    del piece, keys, keep
                del raw, future
                reserved -= written
                fill()
        finally:
            for _, future in pending:
                future.cancel()
