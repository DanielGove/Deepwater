from __future__ import annotations

from typing import Optional, Sequence, Tuple


def resolve_ts_key(ts_fields: Sequence[dict], ts_key: Optional[str]) -> Tuple[int, str, int]:
    """Resolve timestamp key name to (byte offset, byteorder, key id)."""
    if ts_key is None:
        return ts_fields[0]["offset"], "little", 0
    for key_id, field in enumerate(ts_fields):
        if field["name"] == ts_key:
            return field["offset"], "little", key_id
    raise ValueError(f"Timestamp key '{ts_key}' not found, options are: {[f['name'] for f in ts_fields]}")


def key_id_for_offset(ts_fields: Sequence[dict], ts_off: int) -> int:
    """Resolve timestamp byte offset to the clock-level key id."""
    for key_id, field in enumerate(ts_fields):
        if field["offset"] == ts_off:
            return key_id
    raise ValueError(f"Timestamp offset '{ts_off}' not found, options are: {[f['offset'] for f in ts_fields]}")
