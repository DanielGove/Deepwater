#!/usr/bin/env python3
"""
Core Test: Primitive Feed Creation
Tests basic primitive feed metadata and write/read behavior.
"""
import sys
import time
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater import Reader, Writer, create_feed
from deepwater.metadata.discovery import feed_exists


def test_feed_root_init():
    """Creating a feed initializes the Deepwater base path."""
    with tempfile.TemporaryDirectory(prefix="dw-core-init-") as td:
        base = Path(td) / "data"
        create_feed(base, {
            'feed_name': 'test_basic',
            'mode': 'UF',
            'fields': [
                {'name': 'timestamp_us', 'type': 'uint64'},
                {'name': 'value', 'type': 'float64'},
            ],
            'clock_level': 1,
            'persist': True,
        })
        assert (base / "data").exists()
        assert feed_exists(base, "test_basic")
    return True


def test_feed_creation():
    """Test feed can be created with basic schema"""
    with tempfile.TemporaryDirectory(prefix="dw-core-feed-") as td:
        base = Path(td)
        spec = {
            'feed_name': 'test_basic',
            'mode': 'UF',
            'fields': [
                {'name': 'timestamp_us', 'type': 'uint64'},
                {'name': 'value', 'type': 'float64'},
                {'name': 'id', 'type': 'uint64'},
            ],
            'clock_level': 1,
            'persist': True,
        }
        create_feed(base, spec)
        create_feed(base, spec)
        assert feed_exists(base, "test_basic")
    return True


def test_write_read_cycle():
    """Test write and read basic records"""
    with tempfile.TemporaryDirectory(prefix="dw-core-roundtrip-") as td:
        base = Path(td)
        create_feed(base, {
            'feed_name': 'test_basic',
            'mode': 'UF',
            'fields': [
                {'name': 'timestamp_us', 'type': 'uint64'},
                {'name': 'value', 'type': 'float64'},
                {'name': 'id', 'type': 'uint64'},
            ],
            'clock_level': 1,
            'persist': True,
        })

        writer = Writer(base, 'test_basic')
        base_time = int(time.time() * 1e6)

        for i in range(10):
            writer.write_values(base_time + i, float(i), i)

        writer.close()

        reader = Reader(base, 'test_basic')
        records = reader.range(base_time, base_time + 10)
        assert len(records) == 10, f"Expected 10 records, got {len(records)}"
        assert records[-1] == (base_time + 9, 9.0, 9)
        reader.close()
    return True


def run_tests():
    """Run all core tests"""
    tests = [
        ("Feed Root Init", test_feed_root_init),
        ("Feed Creation", test_feed_creation),
        ("Write/Read Cycle", test_write_read_cycle),
    ]
    
    print("Core Primitive Tests")
    print("=" * 60)
    
    passed = 0
    failed = 0
    
    for name, test_fn in tests:
        try:
            result = test_fn()
            if result:
                print(f"✅ {name}")
                passed += 1
            else:
                print(f"❌ {name} - returned False")
                failed += 1
        except Exception as e:
            print(f"❌ {name} - {e}")
            failed += 1
    
    print(f"\nPassed: {passed}/{len(tests)}")
    
    if failed > 0:
        sys.exit(1)


if __name__ == '__main__':
    run_tests()
