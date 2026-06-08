# Deepwater

Data platform for time-series storage and replay.

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

## Quick Start Folder

After install, run:

```bash
deepwater
```

This creates a local `./deepwater-starter` folder with:
- `START_HERE.md` (2-minute launch checklist)
- `OPS_RUNBOOK.md` (copy-paste incident/run operations)
- `GUIDE.md` (full usage details)
- `configs/trades.json`
- `configs/quotes.json`
- `configs/feeds.json` (bundle format)
- `apps/quickstart_app.py` (runnable integration skeleton)

Also includes segmentation guidance so you can quickly answer:
- what segments exist?
- what timestamp range should be passed to readers/backtests?
- how to run a Tailscale-scoped remote read agent and connect to it from another machine?

Optional:

```bash
deepwater --path ./my-deepwater-guide
deepwater --path ./my-deepwater-guide --force
```

---

## Primitive Core

```python
import time
from deepwater import Reader, Writer, create_feed, delete_feed

base = './data'

# Create feed (define schema)
create_feed(base, {
    'feed_name': 'trades',
    'mode': 'UF',
    'fields': [
        {'name': 'timestamp_us', 'type': 'uint64'},  # time axes first
        {'name': 'price', 'type': 'float64'},
        {'name': 'size', 'type': 'float64'},
    ],
    'clock_level': 1,  # number of time axes (1-3); first N fields must be uint64 timestamps
    'persist': True,  # True=disk, False=memory-only ring buffer
})

# Write
writer = Writer(base, 'trades')
writer.write_values(int(time.time() * 1e6), 123.45, 100.0)
writer.close()

# Read
reader = Reader(base, 'trades')
for record in reader.stream():  # Live streaming
    print(record)
    break
records = reader.range(start_us, end_us)  # Historical range
arrays = reader.range(start_us, end_us, format="numpy")  # Structured NumPy array
columns = reader.range_columns(start_us, end_us, ["price", "size"])
reader.close()

# Delete feed (wipe data + registry entry)
delete_feed(base, 'trades')  # Useful for development resets

# Clock Levels
# -------------
# clock_level defines how many timelines your feed tracks:
#   1 → single timeline (e.g., wall clock)
#   2 → dual timelines (e.g., exchange event time + receive time)
#   3 → triple timelines (e.g., event, received, processed)
# Place those time fields first in `fields` (all uint64). They are all queryable via `ts_key`.

# API docs
help(Reader)
help(Writer)
help(reader.stream)
help(reader.range)
```

---

## Remote Reading (Tailscale-First)

Deepwater Networking v0 is a small remote reader path for machines on the same Tailscale tailnet. It does not manage Tailscale, VPN routing, public TLS, NAT, Mullvad, or port forwarding. Access control for v0 is the network boundary: bind the agent only where trusted tailnet clients can reach it.

Current scope:
- remote reads: supported via `range()`, `range_batches()`, and `stream()`
- large historical reads: supported via `range_batches()`
- non-blocking event-loop reads: supported via `read_available()`
- remote writes: not supported in v0; writers remain local to the data machine
- transport: TCP, length-prefixed frames, JSON control header plus optional binary payload
- payload: Deepwater raw record bytes where possible, decoded by the client into tuple/dict/numpy/raw formats
- stream liveness: TCP keepalive plus lightweight idle heartbeats for live remote streams

### Start the Agent on the Data Machine

```bash
deepwater-agent \
  --root /deepwater/data \
  --bind 0.0.0.0:7447
```

The agent accepts remote base paths under `--root` only. For example, with `--root /deepwater/data`, a client may open `/deepwater/data/hyperliquid-node`, but paths outside `/deepwater/data` are rejected.

Deepwater does not manage Tailscale. MagicDNS hostnames work when the operator's network already routes them to the agent.

### Read from a Laptop

```python
from deepwater import Reader

remote = Reader(
    "deepwater-pioneer:/deepwater/data/hyperliquid-node",
    "hl.status.events",
)

window = remote.range(start_us, end_us, format="dict")
for batch in remote.range_batches(start_us, end_us, batch_records=50_000):
    process(batch)
remote.close()
```

Custom port form:

```python
remote = Reader("dw://deepwater-pioneer:7447/deepwater/data/hyperliquid-node", "trades")
```

The same `Reader(base_path, feed)` constructor opens a local reader for local paths and a remote reader for `host:/path` or `dw://host/path` targets.

For remote metadata, use the explicit helpers in `deepwater.network.client` or the local metadata helpers against a mounted/local base path. The public local workflow is `Reader(base_path, feed)`, `Writer(base_path, feed)`, and standalone metadata functions.

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
sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater import Reader, Writer, create_feed

def test_your_feature():
    base = './data/test-yourfeature'  # Unique test dir
    create_feed(base, {
        'feed_name': 'test',
        'mode': 'UF',
        'fields': [
            {'name': 'value_ts', 'type': 'uint64'},  # time axis first
            {'name': 'value', 'type': 'uint64'},
        ],
        'clock_level': 1,
        'persist': True,
    })
    
    writer = Writer(base, 'test')
    writer.write_values(12345, 7)
    writer.close()
    
    reader = Reader(base, 'test')
    data = reader.range(0, 99999)
    assert len(data) > 0, "Expected data"
    assert data[0][0] == 12345
    
    reader.close()
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

## Feed Management CLI

After installation (`pip install -e .`), these commands are available globally:
- `deepwater-agent`
- `deepwater-create-feed`
- `deepwater-delete-feed`
- `deepwater-feeds`
- `deepwater-segments`
- `deepwater-datasets`

Read-only metadata commands accept the same local or remote base path syntax as the Python API:

```bash
deepwater-feeds --base-path deepwater-pioneer:/deepwater/data/hyperliquid-node
deepwater-segments --base-path deepwater-pioneer:/deepwater/data/hyperliquid-node --feed trades --status usable
deepwater-datasets --base-path deepwater-pioneer:/deepwater/data/hyperliquid-node --feed trades --json
```

Mutating/admin commands (`deepwater-create-feed`, `deepwater-delete-feed`, repair, cleanup, health checks) remain local-side tools until the network protocol has explicit write/admin operations.

### Feed Config Format (JSON)

Single feed file:

```json
{
  "feed_name": "trades",
  "mode": "UF",
  "fields": [
    {"name": "ts", "type": "uint64"},
    {"name": "price", "type": "float64"},
    {"name": "size", "type": "float64"}
  ],
  "clock_level": 1,
  "persist": true,
  "chunk_size_mb": 64,
  "retention_hours": 0
}
```

Bundle file:

```json
{
  "feeds": [
    {"feed_name": "trades", "...": "..."},
    {"feed_name": "quotes", "...": "..."}
  ]
}
```

### Create Feeds

```bash
# one file
deepwater-create-feed --base-path ./data --config ./configs/trades.json

# many files in a directory
deepwater-create-feed --base-path ./data --config-dir ./configs/feeds
```

`deepwater-create-feed` is idempotent: existing feeds are left unchanged (no error).

### Delete Feeds

```bash
# by name
deepwater-delete-feed --base-path ./data --feed trades --feed quotes

# from config(s): uses feed_name values
deepwater-delete-feed --base-path ./data --config ./configs/trades.json

# strict mode (missing feed = error)
deepwater-delete-feed --base-path ./data --feed trades --strict-missing
```

Delete removes feed data, feed registry files, ring shared memory (if used), and the global registry entry.

### Feed Metadata

```bash
# list available feeds
deepwater-feeds --base-path ./data

# inspect one feed: lifecycle, fmt, record size, fields
deepwater-feeds --base-path ./data --feed trades

# inspect all feeds in JSON for automation/tools
deepwater-feeds --base-path ./data --all --json
```

### Segment Metadata (Automatic)

Writers manage per-feed segments automatically:
- starts on first write
- closes on clean writer close
- can be explicitly split without closing writer using `writer.mark_segment_boundary("disconnect")`
- crash-open segment is closed on next writer start using last level-1 timestamp

Query segments:

```bash
deepwater-segments --base-path ./data --feed trades --status usable --suggest-range
```

Text-mode timestamp fields default to human-readable local time.
Use `--ts-fmt epoch` for raw microseconds, `--ts-fmt utc` for UTC, or `--ts-fmt America/New_York` for a specific timezone.

Disconnect/reconnect boundary example:

```python
# websocket disconnected, but process stays alive
trade_writer.mark_segment_boundary("disconnect")
book_writer.mark_segment_boundary("disconnect")

# on next write, a new segment starts automatically
```

Plan common windows across multiple feeds:

```bash
deepwater-datasets --base-path ./data --feeds cb_btcusd,cb_ethusd,cb_solusd,cb_xrpusd,kr_btcusd,kr_ethusd,kr_solusd,kr_xrpusd --json
```

Across two base paths:

```bash
deepwater-datasets \
  --source A=./data_us \
  --source B=./data_de \
  --feed-ref A:cb_btcusd --feed-ref A:cb_ethusd --feed-ref A:cb_solusd --feed-ref A:cb_xrpusd \
  --feed-ref B:kr_btcusd --feed-ref B:kr_ethusd --feed-ref B:kr_solusd --feed-ref B:kr_xrpusd \
  --json
```

---

## License

MIT

---

## Links

- **GitHub**: https://github.com/DanielGove/Deepwater
- **Issues**: https://github.com/DanielGove/Deepwater/issues
