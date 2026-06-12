#!/usr/bin/env python3
"""Persistent ring raw paths should not diverge from tuple/range batch semantics."""
import struct
import tempfile
from pathlib import Path


from deepwater import Reader, Writer, create_feed
from deepwater.io.persistent_ring import _copy_ring_window, _FeedState, _FLUSHED


def _make_feed(base: Path):
    create_feed(base, {
        "feed_name": "events",
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "value", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": True,
        "chunk_size_mb": 1,
        "ring_size_mb": 1,
    })


def test_persistent_raw_range_and_batches_match_tuples():
    with tempfile.TemporaryDirectory(prefix="dw-persist-raw-") as td:
        base = Path(td)
        _make_feed(base)
        w = Writer(base, "events")
        start = 1_800_000_000_000_000
        for i in range(8):
            w.write_values(start + i * 10, i)

        r = Reader(base, "events")
        try:
            tuples = r.range(start + 10, start + 70)
            dicts = r.range(start + 10, start + 30, format="dict")
            numpy_records = r.range(start + 10, start + 30, format="numpy")
            raw = bytes(r.range(start + 10, start + 70, format="raw"))
            raw_batches = b"".join(
                bytes(batch)
                for batch in r.range_batches(start + 10, start + 70, format="raw", batch_records=2)
            )
            dict_batches = [
                row
                for batch in r.range_batches(start + 10, start + 50, format="dict", batch_records=2)
                for row in batch
            ]
            numpy_values = [
                int(value)
                for batch in r.range_batches(start + 10, start + 50, format="numpy", batch_records=2)
                for value in batch["value"]
            ]
            expected = b"".join(struct.pack("<QQ", start + i * 10, i) for i in range(1, 7))

            assert tuples == [(start + i * 10, i) for i in range(1, 7)]
            assert dicts == [
                {"ts": start + 10, "value": 1},
                {"ts": start + 20, "value": 2},
            ]
            assert list(numpy_records["value"]) == [1, 2]
            assert raw == expected
            assert raw_batches == expected
            assert [row["value"] for row in dict_batches] == [1, 2, 3, 4]
            assert numpy_values == [1, 2, 3, 4]
        finally:
            r.close()
            w.close()


def test_persistent_read_available_raw_matches_new_records():
    with tempfile.TemporaryDirectory(prefix="dw-persist-available-") as td:
        base = Path(td)
        _make_feed(base)
        w = Writer(base, "events")
        start = 1_800_000_000_000_000
        w.write_values(start, 0)

        r = Reader(base, "events")
        try:
            assert bytes(r.read_available(format="raw")) == b""
            w.write_values(start + 10, 1)
            w.write_values(start + 20, 2)
            assert bytes(r.read_available(max_records=2, format="raw")) == struct.pack(
                "<QQQQ",
                start + 10,
                1,
                start + 20,
                2,
            )
        finally:
            r.close()
            w.close()


def test_persister_copies_shadow_ring_across_wrap_boundary():
    with tempfile.TemporaryDirectory(prefix="dw-persist-wrap-") as td:
        base = Path(td)
        create_feed(base, {
            "feed_name": "events",
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "value", "type": "uint64"},
            ],
            "clock_level": 1,
            "persist": True,
            "uses_ring": True,
            "ring_size_mb": 0.001,
            "chunk_size_mb": 1,
            "segment_tracking": False,
        })
        w = Writer(base, "events")
        start = 1_900_000_000_000_000
        cap = w._ring_capacity
        for i in range(cap):
            w.write_values(start + i, i)
        w.ring.update_durable_header(
            durable_record_count=cap - 2,
            durable_last_ts=start + cap - 3,
        )
        w.write_values(start + cap, cap)
        w.write_values(start + cap + 1, cap + 1)

        state = _FeedState(base, "events")
        try:
            _, start_pos, _, _, record_count, *_ = w.ring.header()
            earliest_live = record_count - cap
            assert _copy_ring_window(
                state,
                cap - 2,
                cap + 2,
                earliest_live,
                int(start_pos),
            ) == _FLUSHED
        finally:
            state.close()
            w.close()

        r = Reader(base, "events")
        try:
            rows = r.range(start, start + cap + 2)
            assert rows == [(start + i, i) for i in range(cap - 2, cap + 2)]
        finally:
            r.close()
