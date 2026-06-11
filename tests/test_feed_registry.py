#!/usr/bin/env python3
"""FeedRegistry public chunk helpers use one-based chunk indices."""
import tempfile
from pathlib import Path


from deepwater import Writer, create_feed
from deepwater.metadata.feed_registry import FeedRegistry


def test_chunk_iteration_and_lookup_are_one_based():
    with tempfile.TemporaryDirectory(prefix="dw-feed-registry-") as td:
        base = Path(td)
        create_feed(
            base,
            {
                "feed_name": "events",
                "mode": "UF",
                "fields": [
                    {"name": "ts", "type": "uint64"},
                    {"name": "v", "type": "uint64"},
                ],
                "clock_level": 1,
                "persist": True,
                "storage": "chunk",
                "chunk_size_mb": 0.001,
            },
        )
        writer = Writer(base, "events")
        for i in range(300):
            writer.write_values(1_000 + i, i)
        writer.close()

        registry = FeedRegistry(str(base / "data" / "events" / "events.reg"), mode="r")
        try:
            latest = registry.get_latest_chunk_idx()
            assert latest and latest > 1, f"expected multiple chunks, got {latest}"

            metas = list(registry.iter_chunks_meta())
            try:
                ids = [int(meta.chunk_id) for meta in metas]
                assert ids == list(range(1, int(latest) + 1))
            finally:
                for meta in metas:
                    meta.release()

            assert registry.find_chunk_by_id(1) == 1
            assert registry.find_chunk_by_id(int(latest)) == int(latest)
            assert registry.find_chunk_by_id(999_999) is None
            assert registry.durable_frontier() == (300, 1_299)

            try:
                registry.get_chunk_metadata(0)
            except IndexError:
                pass
            else:
                raise AssertionError("chunk index 0 should be invalid")

            try:
                registry.get_chunk_metadata(int(latest) + 1)
            except IndexError:
                pass
            else:
                raise AssertionError("chunk index latest+1 should be invalid")
        finally:
            registry.close()
