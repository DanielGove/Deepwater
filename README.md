# Deepwater

**Zero-copy market data substrate with microsecond latency.**

Ultra-low latency time-series storage optimized for financial market data. Deepwater delivers sub-100-microsecond IPC latency with persistent storage and full replay capability.

📚 **Documentation**:
- **[GETTING_STARTED.md](GETTING_STARTED.md)** - 5-minute tutorial for beginners (start here!)
- **[README.md](README.md)** - Complete API reference and examples (this file)
- **[QUICKSTART.md](QUICKSTART.md)** - Step-by-step guide with real examples
- **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - Command cheat sheet
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Internal design and implementation
- **[CHANGELOG.md](CHANGELOG.md)** - Version history and breaking changes

## Performance

- **Writer**: 60µs per write (cross-process)
- **Reader**: 70µs IPC latency (live streaming)
- **Historical**: 920K records/sec (backtest replay)
- **Zero-copy**: Memory-mapped files, no serialization
- **Persistent**: Crash-resistant, full history replay

## Key Features

- **Sub-millisecond latency**: 70µs read latency beats Redis (150µs) by 2x
- **Historical replay**: 920K rec/sec for backtesting and analysis
- **Zero-copy architecture**: Direct memory access, no parsing overhead
- **Multi-process safe**: Writer in one process, readers in others
- **Automatic chunk rotation**: Transparent, no manual management
- **Persistent storage**: Survives crashes, supports full replay

---

## Quick Start

### Installation

```bash
pip install deepwater
```

Or install from source:

```bash
git clone https://github.com/yourusername/Deepwater.git
cd Deepwater
pip install -e .
```

### Basic Usage

```python
from deepwater import Platform
import time

# 1. Initialize platform
p = Platform('./my_data')

# 2. Create feed (one-time setup)
p.create_feed({
    'feed_name': 'trades',
    'mode': 'UF',  # Unindexed feed (simple, fast)
    'fields': [
        {'name': 'price', 'type': 'float64'},
        {'name': 'size', 'type': 'float64'},
        {'name': 'timestamp_us', 'type': 'uint64'},
    ],
    'ts_col': 'timestamp_us',
    'persist': True,  # Disk storage (vs memory-only)
})

# 3. Write data (60µs per write)
writer = p.create_writer('trades')
ts = int(time.time() * 1e6)
writer.write_values(123.45, 100.0, ts)
writer.write_values(123.50, 200.0, ts + 1000)
writer.close()

# 4. Read data (70µs latency, 920K rec/sec throughput)
reader = p.create_reader('trades')

# Live streaming (infinite, 70µs latency)
for trade in reader.stream():  # Ctrl+C to stop
    price, size, timestamp = trade
    print(f'Live: ${price} x {size}')
    break

# Historical range (finite, 920K rec/sec)
records = reader.range(ts, ts + 60_000_000)  # Last 60 seconds
print(f'Historical: {len(records)} records')

# Recent data (convenience)
recent = reader.latest(60)  # Last 60 seconds
print(f'Recent: {len(recent)} records')

reader.close()
p.close()
```

---

## Usage Patterns

### Live Trading (70µs latency)

```python
from deepwater import Platform

p = Platform('./data')
reader = p.create_reader('trades')

# Stream new data as it arrives (spin-wait, 70µs latency)
for trade in reader.stream():  # start=None = live only
    price, size, timestamp = trade
    if price > 100:
        execute_order()
```

### Backtesting (920K rec/sec)

```python
import numpy as np

# Get historical range
data = reader.range(start_us, end_us, format='numpy')

# Vectorized analysis
avg_price = data['price'].mean()
volume = data['size'].sum()
print(f'Price: {avg_price:.2f}, Volume: {volume:.0f}')
```

### Multi-Process (Writer + Reader)

```python
# Process 1: Writer (websocket ingestion)
from deepwater import Platform

p1 = Platform('./shared_data')
writer = p1.create_writer('trades')

for event in websocket:
    writer.write_values(event.price, event.size, event.timestamp_us)
    # 60µs latency to disk + reader visibility

# Process 2: Reader (strategy execution)
from deepwater import Platform

p2 = Platform('./shared_data')
reader = p2.create_reader('trades')

for trade in reader.stream():  # 70µs latency from write
    # Execute trading logic
```

### Replay from Checkpoint

```python
# Resume from specific timestamp (historical + live)
last_ts = 1738368000000000  # Yesterday

for trade in reader.stream(start=last_ts):
    # Replays historical at 920K rec/sec
    # Then continues live at 70µs latency
```

---

## API Reference

### Platform

Entry point for all operations.

```python
from deepwater import Platform

p = Platform(base_path='./data')

# Create feed (one-time setup)
p.create_feed({
    'feed_name': 'trades',
    'mode': 'UF',  # 'UF' (unindexed) or 'IF' (indexed)
    'fields': [...],
    'ts_col': 'timestamp_us',
    'persist': True,
})

# Get writer/reader
writer = p.create_writer('trades')
reader = p.create_reader('trades')

# List feeds
feeds = p.list_feeds()

p.close()
```

### Writer

Write records to feeds (60µs per write).

```python
writer = p.create_writer('trades')

# Write individual values (fastest)
writer.write_values(123.45, 100.0, 1738368000000000)

# Write tuple (pre-packed)
writer.write_tuple((123.45, 100.0, 1738368000000000))

# Write dict (readable, slower)
writer.write_dict({
    'price': 123.45,
    'size': 100.0,
    'timestamp_us': 1738368000000000,
})

writer.close()  # Always close to seal chunk
```

### Reader

Read records from feeds (70µs latency, 920K rec/sec throughput).

```python
reader = p.create_reader('trades')

# Live streaming (infinite, 70µs latency)
for trade in reader.stream():
    price, size, timestamp = trade

# Historical range (finite, 920K rec/sec)
records = reader.range(start_us, end_us)

# Recent data (convenience)
recent = reader.latest(60)  # Last 60 seconds

# Output formats
tuples = reader.range(start, end, format='tuple')    # Fast
dicts = reader.range(start, end, format='dict')      # Readable
array = reader.range(start, end, format='numpy')     # Vectorized
raw = reader.range(start, end, format='raw')         # Memoryview

reader.close()
```

---

## Feed Configuration

### Unindexed Feed (UF) - Simple, Fast

```python
feed_config = {
    'feed_name': 'trades',
    'mode': 'UF',  # No time index
    'fields': [
        {'name': 'price', 'type': 'float64'},
        {'name': 'size', 'type': 'float64'},
        {'name': 'timestamp_us', 'type': 'uint64'},
    ],
    'ts_col': 'timestamp_us',  # Required for range queries
    'persist': True,
    'chunk_size_bytes': 128 * 1024 * 1024,  # 128MB (default)
}
```

### Indexed Feed (IF) - Time Index

```python
feed_config = {
    'feed_name': 'orderbook',
    'mode': 'IF',  # Time-based index
    'fields': [...],
    'ts_col': 'timestamp_us',
    'persist': True,
    'index_playback': True,  # Enable time index
}
```

### Supported Types

- Integers: `uint8`, `uint16`, `uint32`, `uint64`, `int8`, `int16`, `int32`, `int64`
- Floats: `float32`, `float64`
- Char: `char` (single byte)

---

## Performance Tips

1. **Use tuple format for speed**: Default format is fastest (180ns overhead)
2. **Use numpy for batch analysis**: Vectorized operations on large ranges
3. **stream() is CPU-intensive**: Spin-waits for 70µs latency (use range() for analysis)
4. **Always close() writers**: Seals chunk metadata for readers
5. **Timestamps in microseconds**: uint64, monotonic increasing expected

---

## Common Issues

### Q: stream() is skipping data?
**A**: `start=None` jumps to current write head (live only). Use `start=timestamp_us` to include history.

### Q: Reader shows stale data?
**A**: Writer must call `close()` to seal chunk. Active chunks show live updates.

### Q: High CPU usage?
**A**: `stream()` spin-waits for 70µs latency. This is intentional for low latency.

### Q: Can I have multiple writers?
**A**: No, only one writer per feed. Multiple readers are OK.

---

## Architecture

```
┌─────────────┐
│   Writer    │ 60µs write latency
│ (Process 1) │
└──────┬──────┘
       │ mmap
       ▼
┌─────────────────────────────┐
│   Memory-Mapped Chunks      │
│   128MB binary files        │
│   Zero-copy, persistent     │
└──────┬──────────────────────┘
       │ mmap
       ▼
┌─────────────┐
│   Reader    │ 70µs IPC latency
│ (Process 2) │ 920K rec/sec throughput
└─────────────┘
```

- **Chunks**: 128MB binary files (configurable)
- **Metadata**: Memory-mapped registry (live visibility)
- **Rotation**: Automatic, transparent to callers
- **Cython**: Hot paths optimized (binary search)

---

## Benchmarks

Tested on Linux, Intel Xeon, NVMe SSD:

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Writer | 60µs | 16K writes/sec |
| Reader (live) | 70µs | 14K reads/sec |
| Reader (historical) | - | 920K rec/sec |
| Struct.pack_into | 272ns | 3.6M/sec |

**Comparison**:
- **Redis**: ~150µs latency, no persistence
- **Deepwater**: 70µs latency, full persistence + replay
- **Advantage**: 2x faster + historical replay

**Network context**:
- Coinbase websocket: 17ms average
- AWS us-east-1: 28ms average
- Deepwater: 0.07ms (130x faster than network)

---

## License

MIT License - See LICENSE file

## Contributing

Contributions welcome! Please open issues or PRs.

---

## Links

- **GitHub**: https://github.com/yourusername/Deepwater
- **Docs**: [QUICKSTART.md](QUICKSTART.md)
- **Issues**: https://github.com/yourusername/Deepwater/issues
    "chunk_size_mb": 64,
    "retention_hours": 72,
    "persist": True,  # False for shared memory only
}

# Create feed (idempotent)
platform.create_feed(feed_spec)

# Write data
writer = platform.create_writer("trades")
writer.write_values(123.45, 100.0, 1738368000000000)
writer.close()

# Read data
reader = platform.create_reader("trades")
for record in reader.read():
    print(record["price"], record["size"], record["timestamp_us"])
reader.close()

platform.close()
```

### Time Range Queries

```python
# Query last hour
import time
now_us = int(time.time() * 1_000_000)
hour_ago_us = now_us - (3600 * 1_000_000)

reader = platform.create_reader("trades")
for record in reader.read(start_us=hour_ago_us, end_us=now_us):
    print(record)
```

---

## CLI Tools

After installation, these commands are available globally:

### Health Check
```bash
deepwater-health --base-path ./data --check-feeds --max-age-seconds 300
```

**Exit codes:**
- `0` = healthy
- `1` = unhealthy (see output)
- `2` = error

**Checks:**
- Manifest exists and version matches
- Global registry accessible
- Feed registries readable
- Recent write activity
- Disk space available

### Automatic Cleanup
```bash
# Install cron job (runs every 15 minutes)
deepwater-cleanup --install-cron --base-path ./data --interval 15

# Verify
crontab -l | grep deepwater

# Manual cleanup (dry run first)
deepwater-cleanup --base-path ./data --dry-run
deepwater-cleanup --base-path ./data

# Uninstall cron
deepwater-cleanup --uninstall-cron
```

### Corruption Repair
```bash
# Scan all feeds
deepwater-repair --base-path ./data --all --dry-run

# Repair specific feed
deepwater-repair --base-path ./data --feed trades --dry-run
deepwater-repair --base-path ./data --feed trades  # apply
```

**Note:** Repair logic is complex and untested in production. Use with caution.

---

## Ring Buffers (In-Memory Feeds)

Ring buffers provide sub-100µs IPC latency for transient data that doesn't need persistence.

### When to Use Ring Buffers

- **Ultra-low latency**: 40-70µs P50 IPC latency (vs 60-70µs for disk)
- **Transient data**: Preprocessed/enriched feeds that don't need historical storage
- **IPC coordination**: Share live data between processes without disk overhead
- **Cost optimization**: Save disk I/O for data you'll never replay

### Performance

- **Write latency**: 40-50µs
- **Read latency**: 40-70µs P50 IPC (multiprocessing)
- **Throughput**: 200-400 rec/sec sustained
- **Memory**: Configurable (typically 1-8MB buffers)

### Creating Ring Feeds

```python
from deepwater import Platform

p = Platform('./data')

# Create ring feed (same API as disk feeds)
p.create_feed({
    'feed_name': 'live-l2',
    'mode': 'UF',
    'fields': [
        {'name': 'price', 'type': 'float64'},
        {'name': 'qty', 'type': 'float64'},
        {'name': 'timestamp_us', 'type': 'uint64'},
    ],
    'ts_col': 'timestamp_us',
    'persist': False,  # ← Ring buffer (shared memory)
    'chunk_size_mb': 1,  # Buffer size in memory
})

# Use identical API
writer = p.create_writer('live-l2')  # Routes to RingWriter
reader = p.create_reader('live-l2')  # Routes to RingReader
```

### Multiprocessing Example

```python
from deepwater import Platform
from multiprocessing import Process

def publisher(feed_name):
    """Process 1: Write to ring"""
    p = Platform('./data')
    writer = p.create_writer(feed_name)
    
    while True:
        ts = int(time.time() * 1e6)
        writer.write_values(100.0, 10.0, ts)  # 40µs

def consumer(feed_name):
    """Process 2: Read from ring"""
    p = Platform('./data')
    reader = p.create_reader(feed_name)
    
    for record in reader.stream():  # 70µs latency
        price, qty, ts = record
        # Process with 40-70µs total latency

# Start both processes
Process(target=publisher, args=('live-l2',)).start()
Process(target=consumer, args=('live-l2',)).start()
```

### Ring API (Same as Disk)

Ring readers support the full standard API:

```python
reader = p.create_reader('live-l2')  # persist=False

# Streaming (infinite, live updates)
for record in reader.stream():
    price, qty, ts = record
    
# Range queries (on current buffer contents)
now_us = int(time.time() * 1e6)
recent = reader.range(now_us - 5_000_000, now_us)  # Last 5 seconds

# Latest window (convenience)
last_minute = reader.latest(60.0)  # Last 60 seconds (if in buffer)

# Dict format
for trade in reader.stream(format='dict'):
    print(f"Price: {trade['price']}")
```

**Note**: Ring queries only search data currently in the buffer. Historical depth is limited by buffer size.

### Ring Buffer Behavior

- **Circular**: Old data is overwritten when buffer fills
- **Shared memory**: Uses `/dev/shm` on Linux (automatic cleanup)
- **Multi-reader**: Multiple processes can read simultaneously
- **Auto-routing**: Platform API automatically uses RingWriter/RingReader based on `persist` flag
- **No persistence**: Data lost on process exit/crash

### Architecture

#### Storage Model
- **Global Registry**: Binary catalog of all feeds (mmap)
- **Feed Registry**: Per-feed chunk metadata (mmap)
- **Chunks**: Fixed-size binary files with records (persist=True)
- **Ring Buffers**: Shared memory circular buffers (persist=False)

#### Data Flow
```
Writer → Feed Registry → Chunks (disk) or Ring (SHM)
                      ↓
Reader ← Time-indexed queries or Ring streaming
```

#### Persist Modes
- **`persist=True`** (default): Writes to disk chunks, full historical storage
- **`persist=False`**: Writes to shared memory ring, transient (lost on exit)

#### Ring Buffer Header
```
[write_pos: 8B][start_pos: 8B][generation: 8B][last_ts: 8B][count: 8B][...data...]
```
- **write_pos**: Next write location
- **start_pos**: Oldest valid data (after wrapping)
- **generation**: Wrap counter
- **last_ts**: Most recent timestamp (for latest() queries)
- **count**: Total records written (for catching up)

---

## Production Setup

### 1. Install with monitoring
```bash
pip install deepwater

# Setup retention cleanup (runs every 15 min)
deepwater-cleanup --install-cron --base-path /var/deepwater --interval 15
```

### 2. Health monitoring
Add to your monitoring system (cron/systemd timer):

```bash
*/5 * * * * deepwater-health --base-path /var/deepwater --check-feeds --max-age-seconds 300 || alert_system
```

### 3. Logging
- **Platform logs**: `<base_path>/deepwater.log` (auto-rotates at 10MB, keeps 5 backups)
- **Application logs**: Configure your own handlers

### 4. Version enforcement
The platform uses `manifest.json` to enforce version compatibility:

```
RuntimeError: Deepwater version mismatch: manifest has 0.0.1, code is 0.0.2
```

**To resolve:**
- Use matching Deepwater version
- Delete `manifest.json` to re-init (breaks data compatibility)
- Migrate data (future feature)

---

## Examples

See `src/tests/` for example applications:
- **`headless_websocket.py`**: Live WebSocket ingest with metrics
- **`headless_reader.py`**: Interactive data reader
- **`headless_repair.py`**: Corruption scanner
- **`main_websocket.py`**: Full TUI dashboard (requires `prompt_toolkit`)

Run examples:
```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run live ingest
cd src
python tests/headless_websocket.py
# Commands: subscribe XRP-USD, status, metrics, stop
```

---

## Development

### Setup
```bash
git clone https://github.com/DanielGove/Deepwater.git
cd Deepwater
python -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
pip install -e ".[dev]"
```

### Running Tests
```bash
cd src
python tests/headless_websocket.py  # Live data test
python tests/headless_reader.py     # Read test
```

### Building Release
```bash
# Update version in src/deepwater/__init__.py and pyproject.toml
python -m build
# Outputs: dist/deepwater-0.0.1-py3-none-any.whl
#          dist/deepwater-0.0.1.tar.gz
```

---

## Releasing New Versions

### 1. Update version
Edit **two files**:
- `src/deepwater/__init__.py`: `__version__ = "0.0.2"`
- `pyproject.toml`: `version = "0.0.2"`

### 2. Document changes
Update `CHANGELOG.md`:
```markdown
## [0.0.2] - 2026-02-01
### Added
- New feature X
### Fixed
- Bug Y
### Breaking
- Changed Z (requires data migration)
```

### 3. Build distribution
```bash
python -m build
```

### 4. Test locally
```bash
pip install dist/deepwater-0.0.2-py3-none-any.whl
deepwater-health --version  # Verify
```

### 5. Tag and push
```bash
git add src/deepwater/__init__.py pyproject.toml CHANGELOG.md
git commit -m "Release v0.0.2"
git tag v0.0.2
git push origin main --tags
```

### 6. Publish to PyPI
```bash
pip install twine
twine upload dist/deepwater-0.0.2*
```

Or create GitHub release with artifacts attached.

---

## Complete API Reference

### Platform

Entry point for all operations. Automatically routes to disk or ring based on `persist` flag.

```python
from deepwater import Platform

p = Platform(base_path='./data')
```

#### Methods

**`create_feed(spec: dict) -> None`**

Create a new feed with schema and lifecycle settings. Idempotent - safe to call multiple times.

```python
p.create_feed({
    'feed_name': 'trades',           # Required: Unique feed identifier
    'mode': 'UF',                    # Required: 'UF' (uniform format)
    'fields': [                       # Required: Field definitions
        {'name': 'price', 'type': 'float64', 'desc': 'Trade price'},
        {'name': 'size', 'type': 'float64', 'desc': 'Trade size'},
        {'name': 'timestamp_us', 'type': 'uint64', 'desc': 'Event time'},
    ],
    'ts_col': 'timestamp_us',        # Required: Timestamp field name
    'chunk_size_mb': 64,             # Optional: Chunk size (default: 64)
    'retention_hours': 72,           # Optional: Data retention (default: 72)
    'persist': True,                 # Optional: Disk (True) or memory (False, default: True)
    'index_playback': False,         # Optional: Enable time index (default: False)
})
```

**Safety**: Cannot change `persist` flag for existing feeds (raises RuntimeError).

**`create_writer(feed_name: str) -> Writer | RingWriter`**

Create or return cached writer. Automatically chooses Writer (disk) or RingWriter (ring) based on persist flag.

```python
writer = p.create_writer('trades')  # Cached per process
```

**`create_reader(feed_name: str) -> Reader | RingReader`**

Create or return cached reader. Automatically chooses Reader (disk) or RingReader (ring) based on persist flag.

```python
reader = p.create_reader('trades')  # Cached per process
```

**`list_feeds() -> List[dict]`**

List all registered feeds with metadata.

```python
feeds = p.list_feeds()
# [{'name': 'trades', 'persist': True, ...}, ...]
```

**`lifecycle(feed_name: str) -> dict`**

Get feed lifecycle configuration.

```python
config = p.lifecycle('trades')
# {'chunk_size_bytes': 67108864, 'persist': True, ...}
```

**`get_record_format(feed_name: str) -> dict`**

Get feed record format (layout, fields, format string).

```python
layout = p.get_record_format('trades')
# {'fmt': '<ddQ', 'fields': [...], 'record_size': 24, ...}
```

**`close() -> None`**

Close all writers and readers. Called automatically on exit.

```python
p.close()
```

---

### Writer (Disk Feeds: persist=True)

Write records to disk-backed feeds (60µs per write).

```python
writer = p.create_writer('trades')
```

#### Methods

**`write_values(*values) -> None`**

Write record from individual values (fastest). Values must match field order.

```python
writer.write_values(123.45, 100.0, 1738368000000000)  # 60µs
```

**`write_tuple(record: tuple) -> None`**

Write record from tuple.

```python
record = (123.45, 100.0, 1738368000000000)
writer.write_tuple(record)
```

**`write_dict(record: dict) -> None`**

Write record from dictionary (field names to values).

```python
writer.write_dict({
    'price': 123.45,
    'size': 100.0,
    'timestamp_us': 1738368000000000
})
```

**`close() -> None`**

Flush and close writer. Called automatically on exit.

```python
writer.close()
```

---

### RingWriter (Memory Feeds: persist=False)

Write records to shared memory ring buffers (40-50µs per write).

**Same API as Writer** - all methods identical:
- `write_values(*values)`
- `write_tuple(record: tuple)`  
- `write_dict(record: dict)`
- `close()`

Performance: 10-20µs faster than disk writer due to no fsync.

---

### Reader (Disk Feeds: persist=True)

Read records from disk-backed feeds.

```python
reader = p.create_reader('trades')
```

#### Methods

**`stream(start: Optional[int] = None, format: str = 'tuple') -> Iterator`**

Stream records (infinite iterator, live updates).

```python
# Live only (70µs latency, skips history)
for trade in reader.stream():  # start=None
    price, size, ts = trade
    
# Historical + live (920K rec/sec historical, then 70µs live)
for trade in reader.stream(start=1738368000000000):
    process(trade)
    
# Dict format
for trade in reader.stream(format='dict'):
    print(f"Price: {trade['price']}")
```

**Formats**: `'tuple'` (default, fast), `'dict'` (readable), `'numpy'` (batch), `'raw'` (memoryview)

**`range(start: int, end: int, format: str = 'tuple') -> List`**

Read historical time range (finite, 920K rec/sec).

```python
# Get specific time window
records = reader.range(start_us, end_us)
print(f'{len(records)} records')

# Numpy for analysis
data = reader.range(start_us, end_us, format='numpy')
avg_price = data['price'].mean()
```

**`latest(seconds: float = 60.0, format: str = 'tuple') -> List`**

Get recent records (rolling time window).

```python
# Last minute
recent = reader.latest(60)

# Last hour as numpy
data = reader.latest(3600, format='numpy')
volume = data['size'].sum()
```

**`field_names -> tuple`**

Get field names in order.

```python
names = reader.field_names  # ('price', 'size', 'timestamp_us')
```

**`format -> str`**

Get struct format string.

```python
fmt = reader.format  # '<ddQ'
```

**`close() -> None`**

Close reader and free resources.

```python
reader.close()
```

---

### RingReader (Memory Feeds: persist=False)

Read records from shared memory ring buffers.

**Same API as Reader** - all methods identical:
- `stream(start, format)` - Live streaming (start must be None)
- `range(start, end, format)` - Time queries on current buffer
- `latest(seconds, format)` - Recent data from buffer
- `field_names` - Field metadata
- `format` - Format string
- `close()` - Close reader

**Differences from Reader**:
- **`stream()`**: Must use `start=None` (no historical replay)
- **`range()`**: Only searches data currently in ring buffer (limited depth)
- **`latest()`**: Limited by buffer size (typically seconds to minutes)
- **Performance**: 40-70µs P50 IPC latency (vs 70µs for disk)

---

## Field Types

Supported field types for feed schemas:

| Type | Bytes | Description | Range |
|------|-------|-------------|-------|
| `char` | 1 | Single character | ASCII char |
| `uint8` | 1 | Unsigned integer | 0 to 255 |
| `uint16` | 2 | Unsigned integer | 0 to 65,535 |
| `uint32` | 4 | Unsigned integer | 0 to 4.3B |
| `uint64` | 8 | Unsigned integer | 0 to 18.4E |
| `int8` | 1 | Signed integer | -128 to 127 |
| `int16` | 2 | Signed integer | -32K to 32K |
| `int32` | 4 | Signed integer | -2.1B to 2.1B |
| `int64` | 8 | Signed integer | -9.2E to 9.2E |
| `float32` | 4 | Floating point | ±3.4E38 (7 digits) |
| `float64` | 8 | Floating point | ±1.8E308 (15 digits) |
| `_N` | N | Padding bytes | e.g., `_8` = 8 bytes |

**Example schema**:
```python
'fields': [
    {'name': 'type', 'type': 'char'},           # 1 byte
    {'name': 'side', 'type': 'char'},           # 1 byte
    {'name': '_', 'type': '_6'},                # 6 byte padding (alignment)
    {'name': 'timestamp_us', 'type': 'uint64'}, # 8 bytes
    {'name': 'price', 'type': 'float64'},       # 8 bytes
    {'name': 'qty', 'type': 'float64'},         # 8 bytes
    {'name': '_', 'type': '_8'},                # 8 byte padding (64-byte align)
]
# Total: 40 bytes (padded to 64 for cache line alignment)
```

---

## API Reference

### Platform
```python
Platform(base_path: str)
    .create_feed(spec: dict) -> None
    .create_writer(feed_name: str) -> Writer | RingWriter
    .create_reader(feed_name: str) -> Reader | RingReader
    .list_feeds() -> List[dict]
    .lifecycle(feed_name: str) -> dict
    .get_record_format(feed_name: str) -> dict
    .close() -> None
```

### Writer / RingWriter
```python
Writer | RingWriter
    .write_values(*values) -> None
    .write_tuple(record: tuple) -> None
    .write_dict(record: dict) -> None
    .close() -> None
```

### Reader / RingReader
```python
Reader | RingReader
    .stream(start: Optional[int] = None, format: str = 'tuple') -> Iterator
    .range(start: int, end: int, format: str = 'tuple') -> List
    .latest(seconds: float = 60.0, format: str = 'tuple') -> List
    .field_names -> tuple
    .format -> str
    .close() -> None
```

---

## Performance

- **Write throughput**: ~2000 msg/s sustained (Coinbase L2 @ 1800/s)
- **Read latency**: Sub-microsecond for recent data (mmap)
- **Time queries**: Binary search on timestamp index
- **Memory footprint**: Minimal (mmap, zero-copy)

---

## Production Checklist

- ✅ Logging configured (auto-rotate)
- ✅ Manifest version enforcement (blocks incompatible versions)
- ✅ Graceful shutdown (SIGTERM, SIGINT, atexit)
- ✅ Automatic retention cleanup (cron)
- ✅ Health check script (for monitoring)
- ✅ CLI tools available globally
- ⏳ Repair logic (exists but untested - use cautiously)
- ⏳ Data migration tools (future)

---

## License

MIT

---

## Contributing

Contributions welcome! Please open issues or PRs on GitHub.

**Key areas for contribution:**
- Data migration tools for version upgrades
- Additional field types (decimal, string)
- Performance benchmarks
- Documentation improvements

---

## Support

- **GitHub Issues**: https://github.com/DanielGove/Deepwater/issues
- **Email**: dnlgove2@gmail.com
