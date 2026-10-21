"""Open chunk files directly and release metadata on unsuccessful opens."""
from pathlib import Path
from unittest.mock import Mock

import pytest

from deepwater import Reader, Writer, create_feed
from deepwater.io.chunk import Chunk
from deepwater.metadata.feed_registry import ON_DISK


def make_reader(root):
    create_feed(root, dict(feed_name='direct_open', clock_level=1, persist=True,
        storage='chunk', chunk_size_mb=.001,
        fields=[dict(name='ts', type='uint64'), dict(name='value', type='uint64')]))
    with Writer(root, 'direct_open') as writer:
        writer.write_values(1000, 7)
    return Reader(root, 'direct_open')


def test_chunk_scan_opens_without_exists_check(tmp_path, monkeypatch):
    with make_reader(tmp_path) as reader:
        original = Path.exists
        def exists(path):
            if path.name.startswith('chunk_') and path.suffix == '.bin':
                raise AssertionError('Unexpected chunk existence probe')
            return original(path)
        monkeypatch.setattr(Path, 'exists', exists)
        assert reader.range(1000, 1001) == [(1000, 7)]


@pytest.mark.parametrize('error', [FileNotFoundError, PermissionError, ValueError])
def test_unsuccessful_open_releases_registry_view(tmp_path, monkeypatch, error):
    reader = make_reader(tmp_path)
    original_registry = reader.feed_registry
    meta = Mock(status=ON_DISK)
    reader.feed_registry = Mock(get_chunk_metadata=Mock(return_value=meta))
    monkeypatch.setattr(Chunk, 'open', Mock(side_effect=error('open failed')))
    try:
        with pytest.raises(error, match='open failed'):
            reader._open_chunk(1)
        meta.release.assert_called_once_with()
        assert reader._chunk is None and reader._chunk_meta is None
    finally:
        reader.feed_registry = original_registry
        reader.close()
