"""Regression: closing a live writer should close its active segment."""

from deepwater import Writer, create_feed
from deepwater.metadata.discovery import list_segments


def _trade_spec(feed_name: str) -> dict:
    return {
        "feed_name": feed_name,
        "mode": "UF",
        "fields": [
            {"name": "recv_us", "type": "uint64"},
            {"name": "price", "type": "float64"},
            {"name": "size", "type": "float64"},
        ],
        "clock_level": 1,
        "persist": True,
        "persist": True,
        "chunk_size_mb": 1,
    }


def _write_trade(writer, ts: int) -> int:
    return writer.write_values(
        ts,         # recv_us
        100.0,      # price
        1.0,        # size
    )


def test_writer_close_closes_segment_and_new_writer_reopens(base_path):
    feed_name = "trades"
    create_feed(base_path, _trade_spec(feed_name))

    w1 = Writer(base_path, feed_name)
    assert _write_trade(w1, 1_000_000) > 0
    w1.close()

    segs = list_segments(base_path, feed_name)
    assert segs, "expected at least one segment after write"
    assert segs[-1]["status"] == "closed", f"expected closed segment, got {segs[-1]}"

    w2 = Writer(base_path, feed_name)
    try:
        assert _write_trade(w2, 2_000_000) > 0
    finally:
        w2.close()
