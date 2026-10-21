"""File scan advice is optional and preserves ordinary reader results."""
import mmap
from unittest.mock import Mock

import pytest

from deepwater import Reader, Writer, create_feed
from deepwater.io.chunk import Chunk


def test_file_mapping_receives_sequential_advice(monkeypatch):
    monkeypatch.setattr(mmap, 'MADV_SEQUENTIAL', 2, raising=False)
    mapping = Mock()
    chunk = Chunk(memoryview(b'1234'), [mapping], True, False)
    chunk.advise_sequential()
    mapping.madvise.assert_called_once_with(2)


@pytest.mark.parametrize('unsupported', ['constant', 'method', 'os_error', 'shm'])
def test_optional_advice_never_prevents_reads(monkeypatch, unsupported):
    mapping = Mock()
    monkeypatch.setattr(mmap, 'MADV_SEQUENTIAL', 2, raising=False)
    if unsupported == 'constant':
        monkeypatch.delattr(mmap, 'MADV_SEQUENTIAL')
    elif unsupported == 'method':
        mapping = object()
    elif unsupported == 'os_error':
        mapping.madvise.side_effect = OSError('advice unsupported')
    chunk = Chunk(memoryview(b'1234'), [mapping], True, unsupported == 'shm')
    chunk.advise_sequential()
    assert bytes(chunk.buffer) == b'1234'
    if unsupported == 'shm':
        mapping.madvise.assert_not_called()


def test_range_scan_advises_files_and_preserves_rows(tmp_path, monkeypatch):
    create_feed(tmp_path, dict(feed_name='scan', clock_level=1, persist=True,
        storage='chunk', chunk_size_mb=.001,
        fields=[dict(name='ts', type='uint64'), dict(name='value', type='uint64')]))
    expected = [(1000+i, i) for i in range(150)]
    with Writer(tmp_path, 'scan') as writer:
        for row in expected:
            writer.write_values(*row)
    calls = []
    original = Chunk.advise_sequential
    def advised(chunk):
        calls.append(chunk.is_shm)
        original(chunk)
    monkeypatch.setattr(Chunk, 'advise_sequential', advised)
    with Reader(tmp_path, 'scan') as reader:
        actual = [row for batch in reader.range_batches(1010, 1140, batch_records=7)
                  for row in batch]
        assert actual == expected[10:140]
        assert len(calls) >= 2 and not any(calls)
        calls.clear()
        assert reader.first_after(1010) == expected[10]
        assert reader.first_before(1139) == expected[139]
        assert calls == []
