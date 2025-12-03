# Deepwater

High-throughput, zero-copy market data substrate built on shared memory and mmapped files. Writers append fixed-format binary records into chunk buffers; readers tail the same buffers (or disk snapshots) without extra copies. Lightweight registries track feed lifecycle and chunk metadata, and optional index files enable time-based playback.

## Goals
- Low-latency ingestion and replay with zero-copy access (memoryviews over SHM/mmaps).
- Minimal coordination: per-feed registries handle chunk bookkeeping and interprocess locking.
- Durable or in-memory storage per feed (persist to disk or stay ephemeral).
- Index-assisted playback for “snapshot + delta” style consumers.

## Layout at a Glance
- `src/deepwater/platform.py`: Entry point to create feeds, writers, and readers.
- `src/deepwater/writer.py`: Appends records, rotates chunks, and optionally builds index files.
- `src/deepwater/reader.py`: Tails live data or replays from the latest snapshot marker.
- `src/deepwater/feed_registry.py`: Per-feed binary registry of chunk metadata.
- `src/deepwater/index.py`: Fixed-width index records for snapshot offsets.
- `src/deepwater/chunk.py`: Shared memory and mmapped chunk buffers.
- `src/deepwater/global_registry.py`: Global feed catalog + lifecycle defaults.
- `src/tests/`: Demo websocket ingest (`websocket_client.py`), dashboard (`main_websocket.py`), simple reader loop (`orderbook.py`).
- All timestamps are stored as microseconds since epoch.

## Quick Start
Prereqs: Python 3.10+, `pip`, POSIX shared memory support.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

By default, `Platform` writes under `./platform_data`. The demos use `/deepwater/data/coinbase-test`; set a base path appropriate for your machine.

## Core Concepts
- **Feed**: Named stream with a fixed record schema (`layout.json` under `data/<feed>/`).
- **Chunks**: Fixed-size byte buffers (SHM or mmapped files) that store packed records.
- **Feed registry**: Binary file `<feed>.reg` tracking chunk metadata (start/end times, write position, record count, status).
- **Global registry**: Binary catalog of feeds and lifecycle defaults (chunk size, rotation, persist, index flag).
- **Index files (optional)**: Fixed-width records marking offsets for snapshot playback (`chunk_<id>.idx`).

## Define a Feed
Specs follow an “UF” layout (packed struct format derived from fields). Example trade feed:

```python
feed_spec = {
    "feed_name": "CB-TRADES-TEST",
    "mode": "UF",
    "fields": [
        {"name": "type",      "type": "char"},
        {"name": "side",      "type": "char"},
        {"name": "_",         "type": "_6"},     # padding
        {"name": "trade_id",  "type": "uint64"},
        {"name": "packet_us", "type": "uint64"},
        {"name": "recv_us",   "type": "uint64"},
        {"name": "proc_us",   "type": "uint64"},
        {"name": "ev_us",     "type": "uint64"},
        {"name": "price",     "type": "float64"},
        {"name": "size",      "type": "float64"},
    ],
    "ts_col": "proc_us",
    "chunk_size_bytes": 64 * 1024 * 1024,
    "persist": True,
    "index_playback": False,
}
```

Create it once:

```python
from deepwater.platform import Platform
dw = Platform(base_path="./platform_data")
dw.create_feed(feed_spec)
```

## Write Records
`Writer.write_values` packs positional values according to the feed schema and appends them to the current chunk. Set `index_playback=True` in the feed spec and `create_index=True` on writes to emit snapshot markers.

```python
from deepwater.platform import Platform
dw = Platform("./platform_data")
writer = dw.create_writer("CB-TRADES-TEST")

writer.write_values(
    b'T', b'B', 123456789, 100, 200, 300, 400, 42_000.5, 0.01
)

# Optional snapshot marker for playback
writer.write_values(
    b'T', b'B', 123456790, 110, 210, 310, 410, 42_001.0, 0.02,
    create_index=True
)
```

Writers rotate chunks automatically when the current chunk fills. Close writers to flush and release SHM/mmaps:

```python
writer.close()
dw.close()
```

## Read Records
Readers can tail live data (`stream_latest_records`) or fetch the latest record.

```python
from deepwater.platform import Platform
dw = Platform("./platform_data")
r = dw.create_reader("CB-TRADES-TEST")

# Tail live (or from latest snapshot if playback=True and index files exist)
for rec in r.stream_latest_records(playback=True):
    print(rec)

# Or just grab the newest record
print(r.get_latest_record())

# Or stream a time window
for rec in r.stream_time_range(start_time_us, end_time_us):
    handle(rec)
```

## Developer Notes (orderbook snapshot + delta pattern)
- Keep L2 deltas as an event feed. Index entries remain sparse: only mark snapshot boundaries (no per-delta indexing).
- Maintain a separate “orderbook maintainer” process that consumes L2 deltas, applies them to a local book, and periodically emits a full snapshot (e.g., every 5–15 minutes or on reconnect/gap). Snapshots can live in their own feed or as files; the snapshot record should include the L2 sequence/timestamp to resume from.
- A specialized reader can then: load the latest snapshot ≤ target time and replay deltas from the recorded seq/ts using the standard `stream_time_range` on the L2 feed. This bounds replay cost and avoids week-long replays.

## Websocket Ingest Demo
`src/tests/websocket_client.py` connects to Coinbase Advanced Trade WS and writes trades/L2 updates into Deepwater:

```bash
python src/tests/main_websocket.py
```

Commands in the dashboard (prompt at bottom):
- `start` / `stop` to control the engine
- `sub BTC-USD ETH-USD` to add product ids
- `logs` (F2 toggles logs view)

Data lands under `/deepwater/data/coinbase-test` by default; adjust `MarketDataEngine` base_path if needed.

## Orderbook Reader Demo
`src/tests/orderbook.py` shows a minimal reader loop:

```bash
python src/tests/orderbook.py
```

It tails the most recent chunk for `CB-TRADES-MON-USD` and logs records.

## Ingest & Snapshot Apps
- Coinbase ingest: `python src/tests/main_websocket.py` (connects, subscribes, writes trades/L2 into Deepwater feeds). Use `--base-path` to match your data dir.
- Orderbook maintainer: `python src/tests/orderbook_snapshot.py --product BTC-USD --base-path data/coinbase-test --depth 500 --interval 1` (rebuilds book from L2 with playback and emits fixed-depth snapshots into a ring feed).
- Trade window reader: `python src/tests/trades.py --start HH:MM[:SS] --end HH:MM[:SS] --base-path data/coinbase-test` (time-range playback using indices when available).

## Operational Notes
- SHM names: `${feed}-${chunk_id}` for chunks and `${feed}-index-${chunk_id}` for index segments (when enabled). Ensure writers/readers close to release SHM.
- Persistence: set `persist=False` for in-memory-only feeds; `persist=True` writes chunk files + indices to disk.
- Registries are memory-mapped and lock-protected; only one writer per feed should be active.
- Layout stability: `create_feed` enforces schema immutability if `layout.json` already exists.
- Chunk resizing: feed registries auto-resize if chunk count approaches the configured max (doubling up to a cap).
- Live-only rings: set `persist: false` in a feed spec to use a single shared-memory ring sized by `chunk_size_bytes` (capacity ≈ bytes / record_size). `Platform` uses `RingWriter`/`RingReader` automatically; old data is overwritten on wrap and no chunk/index files are produced.

## What to Explore Next
- Add CLI wrappers for feed creation and inspection.
- Extend index usage (snapshot/delta playback) in readers.

## User Manual (runtime basics)
- Config knobs: `chunk_size_bytes` is the single capacity setting. `persist=True` → chunked files and optional indices; `persist=False` → SHM ring sized by the same bytes (capacity ≈ bytes / record_size). `index_playback` only applies when persisted.
- Writers: `write_values` for single writes; use `stage_values` + `commit_values` to batch metadata updates, or `write_batch_bytes` to append many packed records with one metadata bump. `resize_chunk_size` rotates into a new chunk size.
- Readers: `stream_latest_records` for tuples, `stream_latest_records_raw` for memoryviews (no unpack). `read_batch` / `read_batch_raw` yield batches to reduce per-record overhead. Rings expose the same interfaces.
- Resizing: call `Platform.resize_feed(feed_name, new_chunk_size_bytes)` to change size (persisted feeds rotate to a new chunk; rings are recreated).
- Schemas: layouts are immutable; changing fields/record_size requires a new feed. Snapshots with fixed depth should store depth in config for downstream consumers.
