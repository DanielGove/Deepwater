# Deepwater

**Zero-copy market data substrate with microsecond precision.**

Deepwater is a high-performance time-series database designed for financial market data. It provides:
- **Zero-copy shared memory** for real-time data access
- **Microsecond-precision** timestamps and queries
- **Binary record formats** with automatic layout generation
- **Ring buffers** for transient feeds (no disk I/O)
- **Automatic retention management** and corruption detection
- **Version-enforced compatibility** to prevent data corruption

Built for 24/7 production environments with auto-rotating logs, graceful shutdown, and health monitoring.

---

## Quick Start

### Installation

```bash
pip install deepwater
```

Or install from source:

```bash
git clone https://github.com/DanielGove/Deepwater.git
cd Deepwater
pip install -e .
```

### Basic Usage

```python
from deepwater.platform import Platform

# Create platform
platform = Platform(base_path="./data")

# Define a feed
feed_spec = {
    "feed_name": "trades",
    "mode": "UF",  # Uniform format
    "fields": [
        {"name": "price", "type": "float64", "desc": "Trade price"},
        {"name": "size", "type": "float64", "desc": "Trade size"},
        {"name": "timestamp_us", "type": "uint64", "desc": "Event time (µs)"},
    ],
    "ts_col": "timestamp_us",
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

## Architecture

### Storage Model
- **Global Registry**: Binary catalog of all feeds (mmap)
- **Feed Registry**: Per-feed chunk metadata (mmap)
- **Chunks**: Fixed-size binary files with records
- **Ring Buffers**: Shared memory for `persist=False` feeds

### Data Flow
```
Writer → Feed Registry → Chunks (disk or SHM)
                      ↓
Reader ← Time-indexed queries ← Chunk index
```

### Persist Modes
- **`persist=True`**: Writes to disk chunks, survives restarts
- **`persist=False`**: Writes to shared memory rings, transient

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

## API Reference

### Platform
```python
Platform(base_path: str)
    .create_feed(spec: dict) -> None
    .create_writer(feed_name: str) -> Writer
    .create_reader(feed_name: str) -> Reader
    .list_feeds() -> List[str]
    .get_feed_info(feed_name: str) -> dict
    .close() -> None
```

### Writer
```python
Writer
    .write_values(*values, create_index: bool = False) -> None
    .close() -> None
```

### Reader
```python
Reader
    .read(start_us: int = None, end_us: int = None) -> Iterator[dict]
    .close() -> None
```

### Field Types
- `char`: 1 byte character
- `uint8`, `uint16`, `uint32`, `uint64`: Unsigned integers
- `int8`, `int16`, `int32`, `int64`: Signed integers
- `float32`, `float64`: Floating point
- `_N`: Padding (e.g., `_8` = 8 bytes padding)

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
