from __future__ import annotations

import numpy as np


def format_empty_batch(format: str, dtype):
    if format == "raw":
        return memoryview(b"")
    if format == "numpy":
        return np.empty(0, dtype=dtype)
    if format in ("tuple", "dict"):
        return []
    raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")


def format_record_at(raw, pos: int, format: str, unpack, rec_size: int, field_names: tuple, dtype):
    data = memoryview(raw).cast("B")
    pos = int(pos)
    if format == "raw":
        return data[pos:pos + rec_size]
    if format == "numpy":
        return np.frombuffer(data[pos:pos + rec_size], dtype=dtype, count=1)
    rec = unpack(data, pos)
    if format == "tuple":
        return rec
    if format == "dict":
        return {field_names[i]: rec[i] for i in range(len(field_names))}
    raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")


def format_raw_batch(raw, format: str, unpack, rec_size: int, field_names: tuple, dtype):
    """Convert a raw record batch into the requested public reader format."""
    data = memoryview(raw).cast("B")
    if format == "raw":
        return data
    if format == "numpy":
        return np.frombuffer(data, dtype=dtype)

    records = [
        unpack(data, pos)
        for pos in range(0, data.nbytes, rec_size)
    ]
    if format == "tuple":
        return records
    if format == "dict":
        names = field_names
        return [{names[i]: rec[i] for i in range(len(names))} for rec in records]
    raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")


def raw_record_formatter(format: str, unpack, field_names: tuple, dtype):
    if format == "raw":
        return lambda raw: memoryview(raw).cast("B")
    if format == "numpy":
        return lambda raw: np.frombuffer(memoryview(raw).cast("B"), dtype=dtype, count=1)
    if format == "tuple":
        return lambda raw: unpack(memoryview(raw).cast("B"), 0)
    if format == "dict":
        def format_dict(raw):
            rec = unpack(memoryview(raw).cast("B"), 0)
            return {field_names[i]: rec[i] for i in range(len(field_names))}
        return format_dict
    raise ValueError(f"Invalid format: {format}. Use 'tuple', 'dict', 'numpy', or 'raw'")
