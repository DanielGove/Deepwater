from __future__ import annotations

from deepwater import Reader, Writer, create_feed


def test_read_available_starts_at_live_head_and_returns_new_rows(base_path):
    create_feed(
        base_path,
        {
            "feed_name": "events",
            "mode": "UF",
            "fields": [
                {"name": "ts", "type": "uint64"},
                {"name": "value", "type": "uint64"},
            ],
            "clock_level": 1,
            "persist": False,
            "ring_size_mb": 1,
        },
    )

    writer = Writer(base_path, "events")
    reader = Reader(base_path, "events")

    assert reader.read_available(max_records=10) == []

    writer.write_values(1_000, 1)
    writer.write_values(1_001, 2)
    writer.write_values(1_002, 3)

    assert reader.read_available(max_records=2) == [(1_000, 1), (1_001, 2)]
    assert reader.read_available(max_records=2) == [(1_002, 3)]
    assert reader.read_available(max_records=2) == []

    writer.close()
    reader.close()
