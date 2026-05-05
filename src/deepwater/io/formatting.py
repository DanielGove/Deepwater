from __future__ import annotations

import numpy as np


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
