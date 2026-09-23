"""Small fixed-index publication/recovery regressions; no background workloads."""
from __future__ import annotations

import fcntl
import os
import struct

import pytest

from deepwater import Reader, Writer, create_feed
from deepwater.io.ring import RingBuffer
from deepwater.io.ring_shadow import unlink_shm
from deepwater.metadata.admin import FeedSpec, FieldSpec


FEED = "index_publication"


@pytest.fixture
def index_feed(base_path):
    create_feed(base_path, FeedSpec(
        feed_name=FEED,
        fields=(FieldSpec("ts", "uint64"), FieldSpec("value", "uint64")),
        clock_level=1, persist=False, uses_ring=True,
        ring_size_mb=4096 / (1024 * 1024), chunk_size_mb=4096 / (1024 * 1024),
    ))
    try:
        yield base_path
    finally:
        RingBuffer.unlink_for_feed(base_path, FEED)


def test_missing_index_data_reopen_fails_without_leaking_writer_lock(index_feed):
    with Writer(index_feed, FEED) as writer:
        writer.write_values(100, 11)
        data_name = writer.ring.data_shm_name
    unlink_shm(data_name)
    # Existing control retains the published count, so recreating zeroed data
    # would fabricate records. Both attempts must fail at the missing mapping.
    for _ in range(2):
        with pytest.raises(FileNotFoundError):
            Writer(index_feed, FEED)
        lock_path = index_feed / "data" / FEED / f"{FEED}.writer.lock"
        fd = os.open(lock_path, os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


@pytest.mark.parametrize("count_was_published", [False, True])
def test_interrupted_cursor_updates_recover_from_committed_count(index_feed, count_was_published):
    writer = Writer(index_feed, FEED)
    try:
        writer.write_values(100, 11)
        ring = writer.ring
        ring.begin_write(1, 1, writer._ring_capacity)
        record_size = writer._rec_size
        ring.data[record_size:2 * record_size] = struct.pack("<QQ", 200, 22)
        # Interruption can leave cursor/timestamp fields ahead of the count, or
        # leave only the publication guard unfinished after the count commits.
        ring.write_pos[0] = 2 * record_size
        ring.start_pos[0] = record_size
        ring.last_ts[0] = 999
        if count_was_published:
            ring.record_count[0] = 2
        assert ring.write_sequence & 1
    finally:
        writer.close()  # Simulate releasing ownership with the mutation unfinished.

    expected = [(100, 11)]
    if count_was_published:
        expected.append((200, 22))
    with Writer(index_feed, FEED) as recovered:
        assert recovered.ring.write_sequence % 2 == 0
        assert int(recovered.ring.record_count[0]) == len(expected)
        assert int(recovered.ring.write_pos[0]) == len(expected) * recovered._rec_size
        assert int(recovered.ring.start_pos[0]) == 0
        assert int(recovered.ring.last_ts[0]) == expected[-1][0]
        recovered.write_values(300, 33)
        expected.append((300, 33))
        with Reader(index_feed, FEED) as reader:
            assert reader.range(0, 1000) == expected


def test_failed_struct_pack_at_wrap_preserves_published_record(index_feed):
    with Writer(index_feed, FEED) as writer:
        for sequence in range(writer._ring_capacity):
            writer.write_values(sequence + 1, sequence + 10)
        before = bytes(writer.ring.data[:writer.ring.data_size])
        count = int(writer.ring.record_count[0])
        guard = writer.ring.write_sequence
        floor = writer.ring.retained_record_floor
        assert int(writer.ring.write_pos[0]) == 0
        # The first field is valid but the second overflows. pack_into on the
        # live wrapped slot could corrupt the old timestamp before raising.
        with pytest.raises(struct.error):
            writer.write_values(999, 1 << 100)
        assert bytes(writer.ring.data[:writer.ring.data_size]) == before
        assert int(writer.ring.record_count[0]) == count
        assert writer.ring.write_sequence == guard
        assert writer.ring.retained_record_floor == floor
        with Reader(index_feed, FEED) as reader:
            assert reader.range(1, 2) == [(1, 10)]


def test_reader_rejects_paused_mutation_without_consuming_live_record(index_feed):
    with Writer(index_feed, FEED) as writer, Reader(index_feed, FEED) as reader:
        assert reader.read_available() == []
        writer.write_values(100, 11)
        writer.ring.begin_write(1, 1, writer._ring_capacity)
        try:
            assert reader.read_available() == []
        finally:
            writer.ring.recover_write(writer._rec_size)
        assert reader.read_available() == [(100, 11)]
        assert reader.read_available() == []
