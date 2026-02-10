#!/usr/bin/env python3
"""
Core Test: Platform & Feed Creation
Tests basic platform initialization and feed lifecycle
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform


def test_platform_init():
    """Test platform can be initialized"""
    p = Platform('./data/test-core')
    assert p is not None
    p.close()
    return True


def test_feed_creation():
    """Test feed can be created with basic schema"""
    base = Path('./data/test-core')
    if base.exists():
        import shutil
        shutil.rmtree(base)
    p = Platform('./data/test-core')
    
    p.create_feed({
        'feed_name': 'test_basic',
        'mode': 'UF',
        'fields': [
            {'name': 'id', 'type': 'uint64'},
            {'name': 'value', 'type': 'float64'},
            {'name': 'timestamp_us', 'type': 'uint64'},
        ],
        'clock_level': 1,
        'persist': True,
    })
    
    # Should be idempotent - no error on second call
    p.create_feed({
        'feed_name': 'test_basic',
        'mode': 'UF',
        'fields': [
            {'name': 'id', 'type': 'uint64'},
            {'name': 'value', 'type': 'float64'},
            {'name': 'timestamp_us', 'type': 'uint64'},
        ],
        'clock_level': 1,
        'persist': True,
    })
    
    p.close()
    return True


def test_write_read_cycle():
    """Test write and read basic records"""
    base = Path('./data/test-core')
    if base.exists():
        import shutil
        shutil.rmtree(base)
    p = Platform('./data/test-core')
    
    # Ensure feed exists
    p.create_feed({
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
    
    # Write data
    writer = p.create_writer('test_basic')
    base_time = int(time.time() * 1e6)
    
    for i in range(10):
        writer.write_values(base_time + i, float(i), i)
    
    writer.close()
    
    # Read data
    reader = p.create_reader('test_basic')
    records = reader.latest(60)
    
    # Should have at least the records we just wrote
    # (might have more from previous runs)
    assert len(records) >= 1, f"Expected at least 1 record, got {len(records)}"
    
    # Verify we can read the data
    if len(records) > 0:
        rec = records[-1]
        assert len(rec) == 3, f"Expected 3 fields, got {len(rec)}"
    
    reader.close()
    p.close()
    return True


def run_tests():
    """Run all core tests"""
    tests = [
        ("Platform Init", test_platform_init),
        ("Feed Creation", test_feed_creation),
        ("Write/Read Cycle", test_write_read_cycle),
    ]
    
    print("Core Platform Tests")
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
