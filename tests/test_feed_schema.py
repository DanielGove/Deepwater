#!/usr/bin/env python3
from __future__ import annotations

import struct
import sys
import tempfile
from pathlib import Path

import numpy as np

from deepwater import create_feed
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


def run_tests():
    tests = [
        test_schema_derives_numpy_dtype_and_struct,
        test_schema_excludes_padding_from_numpy_dtype,
        test_schema_msgpack_roundtrip,
        test_create_feed_writes_msgpack_schema,
    ]
    print("Feed Schema Tests")
    print("=" * 60)
    passed = 0
    for test in tests:
        try:
            test()
            print(f"PASS {test.__name__}")
            passed += 1
        except Exception as exc:
            print(f"FAIL {test.__name__} - {exc}")
    print(f"\nPassed: {passed}/{len(tests)}")
    if passed != len(tests):
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
