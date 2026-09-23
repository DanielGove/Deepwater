"""Deterministic checks at the fixed-index/durable publication handoff."""
from __future__ import annotations

from itertools import islice
import struct

import pytest

from deepwater import Reader, Writer, create_feed
from deepwater.io.writer.persistent_ring import _FeedState, _drain
from deepwater.io.writer.writer import ChunkWriter
from deepwater.metadata.admin import BlobSidecarSpec, FeedSpec, FieldSpec


FEED = "blob_frontier"


def _blob_feed(base):
    create_feed(base, FeedSpec(
        feed_name=FEED, fields=(FieldSpec("ts", "uint64"),),
        clock_level=1, persist=True, uses_ring=True,
        ring_size_mb=0.0625, chunk_size_mb=0.0625,
        sidecars=(BlobSidecarSpec("payload", chunk_size_mb=0.01, ring_size_mb=0.03),),
    ))


def _payloads(reader, rows):
    return [reader.blob(row, sidecar="payload") for row in rows]


def test_committed_index_before_ram_ack_is_not_read_twice(base_path, monkeypatch):
    _blob_feed(base_path)
    with Writer(base_path, FEED) as writer, Reader(base_path, FEED) as reader:
        writer.write_blob(100, b"first", sidecar="payload")
        writer.write_blob(100, b"second", sidecar="payload")
        state = _FeedState(base_path, FEED)
        original_publish = ChunkWriter.publish_chunk_commit
        observed = []

        def at_publication(chunk_writer, chunk_id):
            original_publish(chunk_writer, chunk_id)
            assert int(writer.ring.durable_record_count[0]) == 0
            rows = reader.range(100, 101)
            observed.append(_payloads(reader, rows))
            batched = [row for batch in reader.range_batches(100, 101, batch_records=1) for row in batch]
            assert _payloads(reader, batched) == [b"first", b"second"]
            stream = reader.stream(start=100)
            try:
                assert _payloads(reader, list(islice(stream, 2))) == [b"first", b"second"]
            finally:
                stream.close()

        try:
            with monkeypatch.context() as patch:
                patch.setattr(ChunkWriter, "publish_chunk_commit", at_publication)
                writer.ring.request_drain()
                _drain(base_path, state)
            assert observed == [[b"first", b"second"]]
            assert _payloads(reader, reader.range(100, 101)) == [b"first", b"second"]
        finally:
            state.close()


def test_range_owns_tail_before_slow_disk_scan_and_clips_new_commit(base_path, monkeypatch):
    _blob_feed(base_path)
    with Writer(base_path, FEED) as writer, Reader(base_path, FEED) as reader:
        writer.write_blob(100, b"first", sidecar="payload")
        writer.write_blob(100, b"second", sidecar="payload")
        state = _FeedState(base_path, FEED)
        original_scan = Reader._iter_durable_raw_range
        interrupted = []

        def commit_before_scan(self, *args, **kwargs):
            if not interrupted:
                interrupted.append(True)
                writer.ring.request_drain()
                _drain(base_path, state)
                writer.write_blob(100, b"later", sidecar="payload")
            yield from original_scan(self, *args, **kwargs)

        try:
            with monkeypatch.context() as patch:
                patch.setattr(Reader, "_iter_durable_raw_range", commit_before_scan)
                assert _payloads(reader, reader.range(100, 101)) == [b"first", b"second"]
            assert interrupted
            assert _payloads(reader, reader.range(100, 101)) == [b"first", b"second", b"later"]
        finally:
            state.close()


def test_range_retries_if_ack_advances_during_tail_copy(base_path, monkeypatch):
    _blob_feed(base_path)
    with Writer(base_path, FEED) as writer, Reader(base_path, FEED) as reader:
        writer.write_blob(100, b"first", sidecar="payload")
        writer.write_blob(100, b"second", sidecar="payload")
        state = _FeedState(base_path, FEED)
        original_bound = reader._ring_lower_bound
        interrupted = []

        def commit_during_search(*args):
            result = original_bound(*args)
            if not interrupted:
                interrupted.append(True)
                writer.ring.request_drain()
                _drain(base_path, state)
            return result

        try:
            monkeypatch.setattr(reader, "_ring_lower_bound", commit_during_search)
            assert _payloads(reader, reader.range(100, 101)) == [b"first", b"second"]
            assert interrupted
            assert int(writer.ring.durable_record_count[0]) == 2
        finally:
            state.close()


def _fixed_feed(base):
    create_feed(base, FeedSpec(
        feed_name="fixed_frontier",
        fields=(FieldSpec("ts", "uint64"), FieldSpec("value", "uint64")),
        clock_level=1, persist=False, uses_ring=True,
        ring_size_mb=0.001, chunk_size_mb=0.001,
    ))


def test_fixed_readers_do_not_depend_on_torn_start_position(base_path):
    _fixed_feed(base_path)
    with Writer(base_path, "fixed_frontier") as writer, Reader(base_path, "fixed_frontier") as reader:
        for i in range(5):
            writer.write_values(100 + i, i)
        # Simulate a producer paused after updating start_pos but before its
        # record_count store; the published rows still have absolute positions.
        writer.ring.start_pos[0] = reader.record_size
        assert reader.range(100, 105) == [(100 + i, i) for i in range(5)]
        assert reader.first_after(102) == (102, 2)
        assert reader.first_before(102) == (102, 2)
        assert reader.last_timestamp() == 104
        writer.ring.start_pos[0] = 0


def test_raw_tail_is_owned_and_lagging_cursor_fails_explicitly(base_path):
    _fixed_feed(base_path)
    with Writer(base_path, "fixed_frontier") as writer, Reader(base_path, "fixed_frontier") as reader:
        assert not reader.read_available()
        writer.write_values(100, 0)
        writer.write_values(101, 1)
        raw = reader.read_available(format="raw")
        expected = struct.pack("<QQQQ", 100, 0, 101, 1)
        assert bytes(raw) == expected
        capacity = writer.ring.data_size // reader.record_size
        writer.write_batch_bytes(b"".join(struct.pack("<QQ", 102 + i, 2 + i) for i in range(capacity)))
        writer.write_values(102 + capacity, 2 + capacity)
        assert bytes(raw) == expected, "returned raw rows alias overwritten ring memory"
        with pytest.raises(RuntimeError, match="retained index window"):
            reader.read_available()


def test_range_retries_after_producer_changes_search_window(base_path, monkeypatch):
    _fixed_feed(base_path)
    with Writer(base_path, "fixed_frontier") as writer, Reader(base_path, "fixed_frontier") as reader:
        writer.write_values(100, 0)
        writer.write_values(101, 1)
        original_bound = reader._ring_lower_bound
        interrupted = []

        def append_during_search(*args):
            result = original_bound(*args)
            if not interrupted:
                interrupted.append(True)
                writer.write_values(102, 2)
            return result

        monkeypatch.setattr(reader, "_ring_lower_bound", append_during_search)
        assert reader.range(100, 103) == [(100, 0), (101, 1), (102, 2)]
        assert interrupted
