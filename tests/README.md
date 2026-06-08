# Deepwater Test Suite

Simple, zero-dependency test framework for Deepwater.

## Quick Start

Run all tests:
```bash
./test.sh
```

## Test Organization

```
tests/
├── test_core.py          # Core feed and I/O functionality
├── test_multikey.py      # Multi-key timestamp queries
└── test_*.py             # Add more tests here

test_non_blocking.py      # Legacy test (to be moved)
```

## Adding New Tests

### 1. Create a test file

Create `tests/test_feature.py`:

```python
#!/usr/bin/env python3
"""Test: Feature Name"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater import Reader, Writer, create_feed

def test_something():
    """Test description"""
    base = './data/test-feature'
    create_feed(base, {
        "feed_name": "events",
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "value", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": True,
    })
    writer = Writer(base, "events")
    writer.write_values(1_000, 7)
    writer.close()
    with Reader(base, "events") as reader:
        assert reader.range(1_000, 1_001) == [(1_000, 7)]
    return True  # or raise exception

def run_tests():
    tests = [("Test Name", test_something)]
    for name, fn in tests:
        try:
            fn()
            print(f"✅ {name}")
        except Exception as e:
            print(f"❌ {name} - {e}")
            sys.exit(1)

if __name__ == '__main__':
    run_tests()
```

### 2. Run the test

```bash
./test.sh  # Runs all tests including new one
```

## Test Categories

- **Core tests** (`test_core.py`) - feeds, Reader/Writer, basic I/O
- **Feature tests** (`test_*.py`) - Specific features
- **Examples** (`examples/`) - Manual workflow scripts, not regression tests

## Guidelines

1. **Fast**: Tests should run in <5 seconds each
2. **Isolated**: Use unique data directories (`./data/test-{name}`)
3. **Deterministic**: No flaky tests, no external network dependencies. Loopback-only socket tests are acceptable for networking code.
4. **Clean**: Each test should clean up after itself (or use temp dirs)
5. **Documented**: Add docstrings explaining what's being tested

## Future Improvements

- [ ] Add pytest integration (optional, for advanced users)
- [ ] Add coverage tracking
- [ ] Add performance regression tests
- [ ] Add CI/CD integration
- [ ] Add test data fixtures
