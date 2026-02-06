# Deepwater

**Zero-copy market data substrate.**

Time-series storage for financial market data with persistent storage and full replay capability.

---

## Installation

```bash
git clone https://github.com/DanielGove/Deepwater.git
cd Deepwater
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
```

---

## Platform Core

```python
from deepwater import Platform

# Initialize
p = Platform('./data')

# Create feed (define schema)
p.create_feed({
    'feed_name': 'trades',
    'mode': 'UF',
    'fields': [
        {'name': 'price', 'type': 'float64'},
        {'name': 'size', 'type': 'float64'},
        {'name': 'timestamp_us', 'type': 'uint64'},
    ],
    'ts_col': 'timestamp_us',
    'persist': True,  # True=disk, False=memory-only ring buffer
})

# Write
writer = p.create_writer('trades')
writer.write_values(123.45, 100.0, int(time.time() * 1e6))
writer.close()

# Read
reader = p.create_reader('trades')
for record in reader.stream():  # Live streaming
    print(record)
    break
records = reader.range(start_us, end_us)  # Historical range
reader.close()

# API docs
help(Platform)
help(reader.stream)
help(reader.range)
```

---

## Testing

### Running Tests

```bash
./test.sh  # Runs all test_*.py files in tests/
```

### Adding Tests

Create `tests/test_yourfeature.py`:

```python
#!/usr/bin/env python3
"""Test: Your Feature"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater import Platform

def test_your_feature():
    p = Platform('./data/test-yourfeature')  # Unique test dir
    p.create_feed({
        'feed_name': 'test',
        'mode': 'UF',
        'fields': [{'name': 'value', 'type': 'uint64'}],
        'ts_col': 'value',
        'persist': True,
    })
    
    writer = p.create_writer('test')
    writer.write_values(12345)
    writer.close()
    
    reader = p.create_reader('test')
    data = reader.range(0, 99999)
    assert len(data) > 0, "Expected data"
    assert data[0][0] == 12345
    
    reader.close()
    p.close()
    return True

def run_tests():
    tests = [("Your Feature", test_your_feature)]
    print("Your Feature Tests")
    print("=" * 60)
    for name, fn in tests:
        try:
            fn()
            print(f"✅ {name}")
        except Exception as e:
            print(f"❌ {name} - {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

if __name__ == '__main__':
    run_tests()
```

Run `./test.sh` to auto-discover and execute.

**Guidelines:**
- Isolated: Unique data directory (`./data/test-*`)
- Fast: <5s per test
- Deterministic: No random data, no network
- Clear assertions: Descriptive error messages

---

## Health Check & Cleanup

*Note: After installation (`pip install -e .`), commands `deepwater-health` and `deepwater-cleanup` are available.*

### Health Check

```bash
deepwater-health --base-path ./data --check-feeds --max-age-seconds 300
```

Validates:
- Manifest exists and version matches
- Global registry accessible
- Feed registries readable
- Recent write activity (if --check-feeds)
- Disk space available

Exit codes: 0=healthy, 1=unhealthy, 2=error

### Cleanup

```bash
# Install cron (runs every 15 minutes)
deepwater-cleanup --install-cron --base-path ./data --interval 15

# Manual cleanup
deepwater-cleanup --base-path ./data --dry-run  # Preview
deepwater-cleanup --base-path ./data            # Execute

# Uninstall cron
deepwater-cleanup --uninstall-cron
```

Removes expired chunks based on retention policy.

---

## License

MIT

---

## Links

- **GitHub**: https://github.com/DanielGove/Deepwater
- **Issues**: https://github.com/DanielGove/Deepwater/issues