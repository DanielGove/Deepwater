#!/usr/bin/env python3
"""GlobalRegistry fixed-entry ABI coverage."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater.metadata.global_registry import ENTRY_SIZE, FeedMetadata, GlobalRegistry


def test_feed_metadata_registry_abi_roundtrip():
    with tempfile.TemporaryDirectory(prefix="dw-global-registry-") as td:
        registry = GlobalRegistry(Path(td))
        try:
            assert ENTRY_SIZE == 128
            assert registry.register_feed(
                "events",
                {
                    "chunk_size_bytes": 1_048_576,
                    "ring_size_bytes": 8_388_608,
                    "retention_hours": 12,
                    "persist": True,
                    "segment_tracking": False,
                    "prefault_ring": True,
                    "uses_ring": True,
                },
                clock_level=3,
            )

            offset = registry._find_feed_offset("events")
            assert offset > 0

            decoded = FeedMetadata.from_registry(registry.mmap, offset)
            assert decoded.feed_name == "events"
            assert decoded.chunk_size_bytes == 1_048_576
            assert decoded.ring_size_bytes == 8_388_608
            assert decoded.retention_hours == 12
            assert decoded.persist is True
            assert decoded.segment_tracking is False
            assert decoded.prefault_ring is True
            assert decoded.uses_ring is True
            assert decoded.clock_level == 3
            assert decoded.created_us > 0

            via_registry = registry.get_feed("events")
            assert via_registry is not None
            assert via_registry.to_dict() == decoded.to_dict()

            assert registry.update_metadata(
                "events",
                persist=False,
                segment_tracking=True,
                prefault_ring=False,
                uses_ring=False,
                ring_size_bytes=16_777_216,
            )
            updated = registry.get_feed("events")
            assert updated is not None
            assert updated.persist is False
            assert updated.segment_tracking is True
            assert updated.prefault_ring is False
            assert updated.uses_ring is False
            assert updated.ring_size_bytes == 16_777_216
        finally:
            registry.close()


def run_tests():
    tests = [
        ("feed_metadata_registry_abi_roundtrip", test_feed_metadata_registry_abi_roundtrip),
    ]
    print("Global Registry Tests")
    print("=" * 60)
    passed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"✅ {name}")
            passed += 1
        except Exception as e:
            print(f"❌ {name} - {e}")
            raise
    print(f"\nPassed: {passed}/{len(tests)}")
    if passed != len(tests):
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
