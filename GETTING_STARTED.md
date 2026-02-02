# Getting Started with Deepwater

This guide will get you from zero to running Deepwater in 5 minutes.

## 1. Installation

```bash
pip install deepwater
```

Or from source:
```bash
git clone https://github.com/DanielGove/Deepwater.git
cd Deepwater
pip install -e .
```

## 2. Your First Feed

Create a simple Python script (`first_feed.py`):

```python
from deepwater import Platform
import time

# Initialize platform (creates ./data directory)
p = Platform('./data')

# Create a feed (one-time setup)
p.create_feed({
    'feed_name': 'prices',
    'mode': 'UF',
    'fields': [
        {'name': 'symbol', 'type': 'char', 'desc': 'Stock symbol'},
        {'name': 'price', 'type': 'float64', 'desc': 'Price in dollars'},
        {'name': 'timestamp_us', 'type': 'uint64', 'desc': 'Time in microseconds'},
    ],
    'ts_col': 'timestamp_us',
})

# Write some data
writer = p.create_writer('prices')
for i in range(10):
    ts = int(time.time() * 1_000_000)
    writer.write_values(b'A', 100.0 + i, ts)
    time.sleep(0.1)
    print(f'Wrote: ${100.0 + i}')

writer.close()
print('\n✓ Data written!')

# Read it back
reader = p.create_reader('prices')
records = reader.latest(60)  # Last 60 seconds
print(f'\n✓ Read {len(records)} records')

for symbol, price, ts in records:
    print(f'  {symbol.decode()}: ${price:.2f}')

reader.close()
p.close()
```

Run it:
```bash
python first_feed.py
```

Output:
```
Wrote: $100.0
Wrote: $101.0
...
✓ Data written!

✓ Read 10 records
  A: $100.00
  A: $101.00
  ...
```

## 3. Live Streaming Example

Create `live_stream.py`:

```python
from deepwater import Platform
import time
import threading

def writer_thread():
    """Write data continuously"""
    p = Platform('./data')
    writer = p.create_writer('prices')
    
    for i in range(100):
        ts = int(time.time() * 1_000_000)
        writer.write_values(b'A', 100.0 + i * 0.1, ts)
        time.sleep(0.1)  # 10 records/second
    
    writer.close()

def reader_thread():
    """Read data in real-time"""
    time.sleep(0.5)  # Let writer start first
    
    p = Platform('./data')
    reader = p.create_reader('prices')
    
    print('Streaming live data (Ctrl+C to stop)...\n')
    for symbol, price, ts in reader.stream():
        print(f'{symbol.decode()}: ${price:.2f}')
        time.sleep(0.05)  # Don't flood terminal

# Start both threads
threading.Thread(target=writer_thread, daemon=True).start()
threading.Thread(target=reader_thread, daemon=True).start()

# Keep main thread alive
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print('\n\n✓ Stopped')
```

Run it:
```bash
python live_stream.py
```

## 4. Multi-Process Example

The power of Deepwater is multi-process communication with microsecond latency.

**Process 1** - Writer (`writer_process.py`):
```python
from deepwater import Platform
import time

p = Platform('./shared_data')

# Create feed if doesn't exist
if 'live_prices' not in [f['name'] for f in p.list_feeds()]:
    p.create_feed({
        'feed_name': 'live_prices',
        'mode': 'UF',
        'fields': [
            {'name': 'price', 'type': 'float64'},
            {'name': 'timestamp_us', 'type': 'uint64'},
        ],
        'ts_col': 'timestamp_us',
    })

writer = p.create_writer('live_prices')

print('Writing data... (Ctrl+C to stop)')
while True:
    ts = int(time.time() * 1_000_000)
    writer.write_values(100.0, ts)
    time.sleep(0.01)  # 100 writes/second
```

**Process 2** - Reader (`reader_process.py`):
```python
from deepwater import Platform

p = Platform('./shared_data')
reader = p.create_reader('live_prices')

print('Reading live data... (Ctrl+C to stop)\n')
for price, ts in reader.stream():
    print(f'Price: ${price:.2f} (latency: ~70µs)')
```

Run in separate terminals:
```bash
# Terminal 1
python writer_process.py

# Terminal 2
python reader_process.py
```

**Result**: 70µs latency between write and read!

## 5. Ring Buffers (Memory-Only)

For ultra-low latency transient data:

```python
from deepwater import Platform
import time

p = Platform('./data')

# Create ring buffer (memory-only, no disk)
p.create_feed({
    'feed_name': 'live_ticks',
    'mode': 'UF',
    'fields': [
        {'name': 'price', 'type': 'float64'},
        {'name': 'timestamp_us', 'type': 'uint64'},
    ],
    'ts_col': 'timestamp_us',
    'persist': False,  # ← Memory-only ring buffer
    'chunk_size_mb': 1,  # Small buffer
})

writer = p.create_writer('live_ticks')
reader = p.create_reader('live_ticks')

# Write to ring
for i in range(100):
    ts = int(time.time() * 1_000_000)
    writer.write_values(100.0 + i, ts)
    time.sleep(0.001)

# Read from ring
recent = reader.latest(1.0)  # Last 1 second
print(f'Ring buffer: {len(recent)} records (40-70µs latency)')

writer.close()
reader.close()
```

## 6. Different Output Formats

```python
from deepwater import Platform

p = Platform('./data')
reader = p.create_reader('prices')

# Tuple format (default, fastest)
for record in reader.stream():
    symbol, price, ts = record
    
# Dict format (readable)
for record in reader.stream(format='dict'):
    print(f"Symbol: {record['symbol']}, Price: {record['price']}")
    
# Numpy format (analysis)
import numpy as np
data = reader.range(start_us, end_us, format='numpy')
avg_price = data['price'].mean()
print(f'Average: ${avg_price:.2f}')

reader.close()
```

## 7. Query Patterns

```python
from deepwater import Platform
import time

p = Platform('./data')
reader = p.create_reader('prices')

# Live streaming (infinite, 70µs latency)
for record in reader.stream():  # Never ends
    process(record)

# Historical range (finite, 920K rec/sec)
now_us = int(time.time() * 1_000_000)
hour_ago = now_us - 3600_000_000
records = reader.range(hour_ago, now_us)
print(f'{len(records)} records in last hour')

# Recent data (convenience)
last_minute = reader.latest(60)
print(f'{len(last_minute)} records in last minute')

# Resume from checkpoint (historical + live)
checkpoint_ts = 1738368000000000
for record in reader.stream(start=checkpoint_ts):
    # Replays from checkpoint at 920K rec/sec
    # Then continues live at 70µs latency
    process(record)

reader.close()
```

## 8. Production Checklist

```python
from deepwater import Platform

# 1. Create feed with proper config
p = Platform('/var/deepwater')

p.create_feed({
    'feed_name': 'production_feed',
    'mode': 'UF',
    'fields': [...],
    'ts_col': 'timestamp_us',
    'chunk_size_mb': 64,      # Good default
    'retention_hours': 168,   # 1 week
    'persist': True,          # Disk storage
    'index_playback': False,  # No index overhead
})

# 2. Setup cleanup cron job
# Run: deepwater-cleanup --install-cron --base-path /var/deepwater

# 3. Setup health monitoring
# Run: deepwater-health --base-path /var/deepwater --check-feeds

# 4. Handle graceful shutdown
import atexit

def cleanup():
    writer.close()
    reader.close()
    p.close()

atexit.register(cleanup)

# 5. Log errors
import logging
logging.basicConfig(filename='app.log', level=logging.INFO)
```

## Next Steps

- **[README.md](README.md)** - Complete API reference and examples
- **[QUICKSTART.md](QUICKSTART.md)** - Detailed tutorial
- **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - Cheat sheet
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Internal design details

## Common Issues

**"Feed not found in registry"**:
```python
# Check which feeds exist
feeds = p.list_feeds()
print([f['name'] for f in feeds])

# Create feed if missing
p.create_feed({...})
```

**"Cannot change persist flag"**:
```python
# Persist flag is immutable - use different feed name
p.create_feed({'feed_name': 'prices_v2', 'persist': False, ...})
```

**Ring buffer not found**:
```python
# Writer must create ring first
writer = p.create_writer('ring_feed')  # Creates shared memory
reader = p.create_reader('ring_feed')  # Attaches to it
```

## Support

- **GitHub**: https://github.com/DanielGove/Deepwater
- **Issues**: https://github.com/DanielGove/Deepwater/issues
- **Email**: dnlgove2@gmail.com

---

**Ready to build?** Start with the examples above and refer to [README.md](README.md) for the complete API.
