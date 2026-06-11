#!/usr/bin/env python3
"""Direct Writer workflow without Platform."""
import tempfile
import struct
from pathlib import Path


from deepwater import Reader, Writer, create_feed


def test_direct_chunk_writer_roundtrip():
    with tempfile.TemporaryDirectory(prefix="dw-writer-direct-chunk-") as td:
        base = Path(td)
        create_feed(
            base,
            {
                "feed_name": "events",
                "mode": "UF",
                "fields": [
                    {"name": "ts", "type": "uint64"},
                    {"name": "value", "type": "uint64"},
                ],
                "clock_level": 1,
                "persist": True,
                "storage": "chunk",
                "chunk_size_mb": 0.01,
            },
        )

        w = Writer(base, "events")
        for i in range(5):
            w.write_values(1_000 + i, i)
        w.close()

        with Reader(base, "events") as r:
            assert r.range(1_000, 1_010) == [
                (1_000, 0),
                (1_001, 1),
                (1_002, 2),
                (1_003, 3),
                (1_004, 4),
            ]


def test_direct_ring_writer_roundtrip():
    with tempfile.TemporaryDirectory(prefix="dw-writer-direct-ring-") as td:
        base = Path(td)
        create_feed(
            base,
            {
                "feed_name": "ticks",
                "mode": "UF",
                "fields": [
                    {"name": "ts", "type": "uint64"},
                    {"name": "px", "type": "float64"},
                ],
                "clock_level": 1,
                "persist": False,
                "storage": "ring",
                "ring_size_mb": 1,
            },
        )

        w = Writer(base, "ticks")
        for i in range(3):
            w.write_values(2_000 + i, 10.0 + i)

        with Reader(base, "ticks") as r:
            assert r.range(2_000, 2_010) == [
                (2_000, 10.0),
                (2_001, 11.0),
                (2_002, 12.0),
            ]
        w.close()


def test_chunk_writer_rejects_wrong_raw_record_size():
    with tempfile.TemporaryDirectory(prefix="dw-writer-direct-size-") as td:
        base = Path(td)
        create_feed(
            base,
            {
                "feed_name": "events",
                "mode": "UF",
                "fields": [
                    {"name": "ts", "type": "uint64"},
                    {"name": "value", "type": "uint64"},
                ],
                "clock_level": 1,
                "persist": True,
                "storage": "chunk",
                "chunk_size_mb": 0.01,
            },
        )

        w = Writer(base, "events")
        try:
            try:
                w.write(1_000, struct.pack("<Q", 1_000))
            except ValueError:
                pass
            else:
                raise AssertionError("short raw record write was not rejected")
        finally:
            w.close()
