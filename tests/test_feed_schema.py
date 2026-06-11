#!/usr/bin/env python3
from __future__ import annotations

import struct
import tempfile
from pathlib import Path

import numpy as np
import pytest

from deepwater import create_feed
from deepwater.metadata.admin import FeedSpec, FieldSpec
from deepwater.metadata.feed_metadata import load_feed_metadata
from deepwater.metadata.feed_schema import build_schema, load_schema, save_schema


def test_schema_derives_numpy_dtype_and_struct():
    schema = build_schema(
        [
            ("ts", "uint64"),
            ("price", "float64"),
            ("size", "float32"),
        ],
        clock_level=1,
    )

    assert schema.record_size == 20
    assert schema.fmt == "<Qdf"
    assert schema.struct.size == schema.record_size
    assert schema.numpy_dtype.names == ("ts", "price", "size")
    assert schema.numpy_dtype.itemsize == schema.record_size

    buf = schema.struct.pack(7, 101.5, 2.25)
    arr = np.frombuffer(buf, dtype=schema.numpy_dtype)
    assert arr["ts"][0] == 7
    assert arr["price"][0] == 101.5
    assert arr["size"][0] == 2.25


def test_schema_excludes_padding_from_numpy_dtype():
    schema = build_schema(
        [
            ("ts", "uint64"),
            ("_", "_6"),
            ("v", "uint16"),
        ],
        clock_level=1,
    )

    assert schema.record_size == 16
    assert schema.fmt == "<Q6xH"
    assert schema.numpy_dtype.names == ("ts", "v")
    assert schema.numpy_dtype.fields["ts"][1] == 0
    assert schema.numpy_dtype.fields["v"][1] == 14
    assert struct.Struct(schema.fmt).size == schema.record_size


def test_schema_msgpack_roundtrip():
    schema = build_schema(
        [
            ("ts", "uint64"),
            ("coin", "bytes16"),
            ("px", "float64"),
        ],
        clock_level=1,
    )

    with tempfile.TemporaryDirectory() as td:
        save_schema(td, schema)
        loaded = load_schema(Path(td))

    assert loaded.to_dict() == schema.to_dict()
    assert loaded.fmt == schema.fmt
    assert loaded.numpy_dtype == schema.numpy_dtype


def test_create_feed_writes_msgpack_schema():
    spec = {
        "feed_name": "trades",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "price", "type": "float64"},
            {"name": "size", "type": "float32"},
        ],
        "clock_level": 1,
        "persist": True,
        "chunk_size_mb": 1,
    }

    with tempfile.TemporaryDirectory() as td:
        base = Path(td) / "platform"
        create_feed(base, spec)
        feed_dir = base / "data" / "trades"
        schema_path = feed_dir / "feed_schema.msgpack"
        assert schema_path.exists()
        assert not (feed_dir / "record_format.json").exists()
        schema = load_schema(feed_dir)

    assert schema.record_size == 20
    assert schema.fmt == "<Qdf"
    assert schema.numpy_dtype.names == ("ts", "price", "size")


def test_create_feed_accepts_typed_spec():
    spec = FeedSpec(
        feed_name="quotes",
        fields=(
            FieldSpec("ts", "uint64"),
            FieldSpec("bid", "float64"),
            FieldSpec("ask", "float64"),
        ),
        clock_level=1,
        persist=False,
        uses_ring=True,
        chunk_size_mb=1,
        ring_size_mb=2,
    )

    with tempfile.TemporaryDirectory() as td:
        base = Path(td) / "platform"
        create_feed(base, spec)
        feed_dir = base / "data" / "quotes"
        schema = load_schema(feed_dir)
        config = (feed_dir / "config.json").read_bytes()

    assert schema.record_size == 24
    assert schema.fmt == "<Qdd"
    assert b'"uses_ring":true' in config
    assert b'"storage":"ring"' in config


def test_create_feed_accepts_tuple_fields_and_chunk_storage(base_path):
    create_feed(
        base_path,
        {
            "feed_name": "orders",
            "fields": [("ts", "uint64"), ("order_id", "uint64")],
            "clock_level": 1,
            "persist": True,
            "storage": "chunk",
        },
    )

    metadata = load_feed_metadata(base_path, "orders")
    assert metadata.persist is True
    assert metadata.uses_ring is False
    assert metadata.ring_size_bytes == 0


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"mode": "blob"}, "only UF feeds are supported"),
        ({"storage": "disk"}, "storage must be 'ring' or 'chunk'"),
        ({"storage": "chunk", "uses_ring": True}, "uses_ring contradicts storage"),
        ({"persist": False, "uses_ring": False}, "feed must be persistent"),
    ],
)
def test_create_feed_rejects_invalid_admin_specs(base_path, updates, message):
    spec = {
        "feed_name": "bad",
        "fields": [{"name": "ts", "type": "uint64"}],
        "clock_level": 1,
        "persist": True,
    }
    spec.update(updates)

    with pytest.raises(ValueError, match=message):
        create_feed(base_path, spec)
