# Developer Quick Start Guide

This guide helps developers get started with Deepwater quickly.

## Installation

### From PyPI (when published)
```bash
pip install deepwater
```

### From Source
```bash
git clone https://github.com/DanielGove/Deepwater.git
cd Deepwater
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## Your First Feed

```python
from deepwater.platform import Platform
import time

# Create platform
platform = Platform(base_path="./my_data")

# Define feed schema
feed_spec = {
    "feed_name": "sensor_readings",
    "mode": "UF",
    "fields": [
        {"name": "sensor_id", "type": "uint32", "desc": "Sensor identifier"},
        {"name": "temperature", "type": "float64", "desc": "Temperature in Celsius"},
        {"name": "timestamp_us", "type": "uint64", "desc": "Event time (microseconds)"},
    ],
    "ts_col": "timestamp_us",
    "chunk_size_mb": 64,
    "retention_hours": 24,
    "persist": True,
}

# Create feed (idempotent - safe to call multiple times)
platform.create_feed(feed_spec)

# Write data
writer = platform.create_writer("sensor_readings")
now_us = int(time.time() * 1_000_000)

for i in range(100):
    writer.write_values(
        101,                    # sensor_id
        20.5 + i * 0.1,        # temperature
        now_us + (i * 1000)    # timestamp_us (1ms apart)
    )

writer.close()
print(f"Wrote 100 records")

# Read data back
reader = platform.create_reader("sensor_readings")
count = 0

for record in reader.read():
    count += 1
    if count <= 5:  # Print first 5
        print(f"Sensor {record['sensor_id']}: {record['temperature']:.2f}°C at {record['timestamp_us']}")

print(f"Read {count} records total")

# Time range query (last 50ms)
recent_us = now_us + 50_000
print(f"\nRecords from last 50ms:")
for record in reader.read(start_us=recent_us):
    print(f"  {record['temperature']:.2f}°C")

reader.close()
platform.close()
```

## Field Types

| Type | Size | Description |
|------|------|-------------|
| `char` | 1 byte | Single character |
| `uint8` | 1 byte | 0 to 255 |
| `uint16` | 2 bytes | 0 to 65,535 |
| `uint32` | 4 bytes | 0 to 4,294,967,295 |
| `uint64` | 8 bytes | 0 to 18,446,744,073,709,551,615 |
| `int8` | 1 byte | -128 to 127 |
| `int16` | 2 bytes | -32,768 to 32,767 |
| `int32` | 4 bytes | -2,147,483,648 to 2,147,483,647 |
| `int64` | 8 bytes | -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 |
| `float32` | 4 bytes | Single precision float |
| `float64` | 8 bytes | Double precision float |
| `_N` | N bytes | Padding (e.g., `_8` for 8 bytes) |

## Common Patterns

### Market Data Feed
```python
feed_spec = {
    "feed_name": "trades",
    "mode": "UF",
    "fields": [
        {"name": "symbol", "type": "char", "desc": "T=trade"},
        {"name": "side", "type": "char", "desc": "B=buy, S=sell"},
        {"name": "_", "type": "_6", "desc": "padding"},
        {"name": "trade_id", "type": "uint64", "desc": "Exchange trade ID"},
        {"name": "price", "type": "float64", "desc": "Trade price"},
        {"name": "size", "type": "float64", "desc": "Trade size"},
        {"name": "timestamp_us", "type": "uint64", "desc": "Event time"},
    ],
    "ts_col": "timestamp_us",
    "persist": True,
}
```

### Transient Ring Buffer (No Disk)
```python
feed_spec = {
    "feed_name": "live_quotes",
    "mode": "UF",
    "fields": [
        {"name": "bid", "type": "float64", "desc": "Bid price"},
        {"name": "ask", "type": "float64", "desc": "Ask price"},
        {"name": "timestamp_us", "type": "uint64", "desc": "Quote time"},
    ],
    "ts_col": "timestamp_us",
    "persist": False,  # Shared memory only, no disk writes
    "chunk_size_mb": 1,  # Small for ring buffer
}
```

### Time Range Queries
```python
import time

now_us = int(time.time() * 1_000_000)
hour_ago_us = now_us - (3600 * 1_000_000)
day_ago_us = now_us - (86400 * 1_000_000)

reader = platform.create_reader("trades")

# Last hour
for record in reader.read(start_us=hour_ago_us, end_us=now_us):
    print(record)

# Specific time window
start = 1738368000000000  # Jan 31, 2026 12:00:00 AM UTC
end   = 1738371600000000  # Jan 31, 2026 01:00:00 AM UTC
for record in reader.read(start_us=start, end_us=end):
    print(record)
```

## CLI Tools in Development

During development, use module form before installing:

```bash
# Health check
python -m deepwater.health_check --base-path ./my_data

# Cleanup
python -m deepwater.cleanup --base-path ./my_data --dry-run

# Repair
python -m deepwater.repair --base-path ./my_data --all --dry-run
```

After `pip install`:
```bash
deepwater-health --base-path ./my_data
deepwater-cleanup --base-path ./my_data --dry-run
deepwater-repair --base-path ./my_data --all --dry-run
```

## Troubleshooting

### Version Mismatch
```
RuntimeError: Deepwater version mismatch: manifest has 0.0.1, code is 0.0.2
```

**Solution:** Use matching version or delete `manifest.json` (breaks compatibility).

### Feed Schema Change
```
RuntimeError: layout drift for 'trades': fmt/size/ts_off changed
```

**Solution:** Use a new feed name with version suffix (e.g., `trades_v2`).

### Import Errors After Install
```
ModuleNotFoundError: No module named 'deepwater'
```

**Solution:** Make sure virtual environment is activated, or reinstall:
```bash
pip install --force-reinstall deepwater
```

## Example Applications

See `src/tests/` in the repository:
- `headless_websocket.py` - Live WebSocket data ingestion
- `headless_reader.py` - Interactive data reader
- `main_websocket.py` - Full TUI dashboard

## Performance Tips

1. **Batch writes:** Write multiple records before calling `close()` or `flush()`
2. **Persist=False for transient data:** Use ring buffers for data you don't need to keep
3. **Appropriate chunk sizes:** 64MB is good default, smaller (1-8MB) for ring buffers
4. **Time range queries:** Always specify ranges when possible for faster lookups
5. **Field alignment:** Use padding (`_N`) to align fields on 8-byte boundaries

## Next Steps

- Read the [full README](README.md) for architecture details
- Check [CHANGELOG.md](CHANGELOG.md) for version history
- See [PRODUCTION.md](PRODUCTION.md) for 24/7 deployment guide (if exists)
- Join discussions on [GitHub Issues](https://github.com/DanielGove/Deepwater/issues)
