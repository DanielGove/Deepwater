"""Read-ahead preserves range bytes/order while enforcing resource limits."""
from unittest.mock import patch

import numpy as np
import pytest

from deepwater import Reader, Writer, create_feed


def seed(root):
    names = ['event_ns', 'received_ns', 'written_ns', 'value']
    create_feed(root, dict(feed_name='scan', clock_level=3, persist=True,
        storage='chunk', chunk_size_mb=.001,
        fields=[dict(name=n, type='uint64') for n in names]))
    h = 2**63 + 1000
    with Writer(root, 'scan') as writer:
        for i in range(250):
            writer.write_values(h+i, h+1000+i, h+2000+i, i)
    return h


@pytest.mark.parametrize('format', ['raw', 'numpy', 'tuple', 'dict'])
@pytest.mark.parametrize('workers', [1, 4, 16, 64])
def test_exact_batches_and_order(tmp_path, format, workers):
    h = seed(tmp_path)
    with Reader(tmp_path, 'scan') as reader:
        for key, shift in [('event_ns', 0), ('received_ns', 1000), ('written_ns', 2000)]:
            args = dict(ts_key=key, format=format, batch_records=7, durable_only=True)
            expected = list(reader.range_batches(h+shift+11, h+shift+240, **args))
            actual = list(reader.range_batches(h+shift+11, h+shift+240,
                read_ahead_chunks=workers, read_buffer_bytes=4096, **args))
            if format == 'numpy':
                np.testing.assert_array_equal(np.concatenate(actual), np.concatenate(expected))
                assert all(len(batch) <= 7 for batch in actual)
            elif format == 'raw':
                assert b''.join(bytes(batch) for batch in actual) == b''.join(bytes(batch) for batch in expected)
                assert all(len(batch) <= 7 * reader.record_size for batch in actual)
            else:
                assert [row for batch in actual for row in batch] == [row for batch in expected for row in batch]
                assert all(len(batch) <= 7 for batch in actual)
    if format == 'numpy':
        assert int(actual[0]['value'][0]) == 11  # owns bytes after reader.close()


def test_read_ahead_requires_explicit_durable_scope_and_budget(tmp_path):
    h = seed(tmp_path)
    with Reader(tmp_path, 'scan') as reader:
        with pytest.raises(ValueError, match='durable_only'):
            list(reader.range_batches(h, h+250, read_ahead_chunks=4))
        for kwargs in [dict(read_ahead_chunks=65), dict(read_ahead_chunks=-1),
                       dict(read_ahead_chunks=4, read_buffer_bytes=1)]:
            with pytest.raises(ValueError):
                list(reader.range_batches(h, h+250, durable_only=True, **kwargs))


def test_missing_file_and_short_read(tmp_path):
    h = seed(tmp_path)
    with Reader(tmp_path, 'scan') as reader:
        with patch('deepwater.io.reader.prefetch.os.pread', return_value=b''):
            with pytest.raises(RuntimeError, match='Short read'):
                list(reader.range_batches(h, h+250, durable_only=True, read_ahead_chunks=4))
        (tmp_path/'data/scan/chunk_00000001.bin').unlink()
        try:
            expected = list(reader.range_batches(h, h+250, durable_only=True))
        except FileNotFoundError:
            with pytest.raises(FileNotFoundError):
                list(reader.range_batches(h, h+250, durable_only=True, read_ahead_chunks=4))
        else:
            assert list(reader.range_batches(h, h+250, durable_only=True, read_ahead_chunks=4)) == expected


def test_budget_and_early_close_do_not_submit_extra_chunks(tmp_path):
    import os
    h = seed(tmp_path)
    calls = []
    original = os.pread
    with Reader(tmp_path, 'scan') as reader:
        ident = next(iter(reader.feed_registry.get_chunks_in_range(h, h+250, qoff=0)))
        meta = reader.feed_registry.get_chunk_metadata(ident)
        try:
            budget = int(meta.write_pos)
        finally:
            meta.release()
        def read(fd, size, offset):
            calls.append(size)
            return original(fd, size, offset)
        with patch('deepwater.io.reader.prefetch.os.pread', side_effect=read):
            batches = reader.range_batches(h, h+250, durable_only=True,
                read_ahead_chunks=64, read_buffer_bytes=budget, batch_records=7)
            next(batches)
            assert calls == [budget]
            batches.close()
            assert calls == [budget]
