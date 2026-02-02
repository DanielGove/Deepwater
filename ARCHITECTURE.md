# Deepwater Architecture

This document describes the internal architecture and design decisions of Deepwater.

## Overview

Deepwater is a zero-copy time-series storage system optimized for ultra-low latency financial data. It achieves sub-100µs IPC latency through memory-mapped files (disk feeds) and shared memory (ring buffers).

## Core Components

### 1. Platform (`platform.py`)

Entry point for all operations. Manages:
- Feed registry (global metadata catalog)
- Writer/reader caching (one instance per feed per process)
- Automatic routing between disk and memory based on `persist` flag
- Graceful shutdown coordination

**Key Design**:
- Singleton-like behavior per workspace (one Platform instance per base_path)
- Automatic cleanup via atexit handlers
- Metadata protection (prevents accidental persist flag changes)

### 2. Global Registry (`global_registry.py`)

Binary memory-mapped catalog of all feeds in the workspace.

**File**: `<base_path>/registry/global_registry.bin`

**Structure**:
```
Header (128 bytes):
  - feed_count: 8 bytes (uint64)
  - reserved: 120 bytes

Entry (128 bytes each, up to 16383 feeds):
  - feed_name: 32 bytes (null-padded string)
  - chunk_size_bytes: 4 bytes (uint32)
  - retention_hours: 4 bytes (uint32)
  - persist: 1 byte (bool)
  - index_playback: 1 byte (bool)
  - padding: 6 bytes
  - created_us: 8 bytes (uint64)
  - reserved: 72 bytes
```

**Purpose**:
- Fast feed discovery (binary search on sorted names)
- Lifecycle metadata (chunk size, retention, persist flag)
- Cross-process coordination via mmap + fcntl locking

**Invariants**:
- Entries always sorted by feed_name (binary search)
- persist flag is immutable after creation (enforced by Platform.create_feed)
- Maximum 16,383 feeds per workspace

### 3. Feed Registry (`feed_registry.py`)

Per-feed binary registry tracking chunk metadata.

**File**: `<base_path>/data/<feed_name>/<feed_name>.reg`

**Structure**:
```
Header (64 bytes):
  - chunk_count: 4 bytes (uint32)
  - reserved: 60 bytes

Entry (64 bytes each):
  - chunk_id: 4 bytes (uint32)
  - start_time_us: 8 bytes (uint64)
  - end_time_us: 8 bytes (uint64)
  - record_count: 4 bytes (uint32)
  - reserved: 36 bytes
```

**Purpose**:
- Track which chunks exist
- Binary search by timestamp for range queries
- Reader discovers new chunks automatically

**Note**: Only used for persist=True feeds. Ring buffers (persist=False) don't have chunk files.

### 4. Writer (`writer.py`)

Writes records to disk chunks with automatic rotation.

**Performance**: 60µs per write (includes fsync for durability)

**Key Features**:
- Automatic chunk rotation when size limit reached
- Memory-mapped write buffer (zero-copy)
- Atomic metadata updates (write_pos updated after fsync)
- Per-chunk format caching (avoid re-parsing layout.json)

**Chunk Files**:
- `chunk_<8-digit-id>.bin` - Binary records
- No index files (sequential write only)

**Flow**:
```
write_values() → pack into buffer → fsync → update write_pos → update registry
```

### 5. Reader (`reader.py`)

Reads records from disk chunks with time-indexed queries.

**Performance**:
- 70µs IPC latency (live streaming)
- 920K rec/sec (historical replay)

**Key Features**:
- Binary search to find starting chunk (time-indexed)
- Memory-mapped chunk reads (zero-copy)
- Automatic chunk rotation (follows writer)
- Multiple output formats (tuple, dict, numpy, raw)

**Query Types**:

1. **`stream(start=None)`**: Live-only (70µs latency)
   - Jumps to latest chunk
   - Spin-waits for new records
   - Never ends (infinite loop)

2. **`stream(start=ts)`**: Historical + live
   - Binary search to find starting chunk
   - Reads historical at 920K rec/sec
   - Switches to live spin-wait at end
   - Never ends (infinite loop)

3. **`range(start, end)`**: Finite historical query
   - Binary search to find chunks
   - Returns all matching records
   - Terminates when done (finite)

4. **`latest(seconds)`**: Recent window
   - Convenience wrapper: `range(now - seconds*1e6, now)`

### 6. Ring Buffers (`ring.py`)

Shared memory circular buffers for transient data.

**Performance**: 40-70µs P50 IPC latency

**Structure**:
```
Header (40 bytes):
  - write_pos: 8 bytes (offset in data buffer)
  - start_pos: 8 bytes (oldest valid data after wrap)
  - generation: 8 bytes (wrap counter)
  - last_ts: 8 bytes (most recent timestamp)
  - record_count: 8 bytes (total records written)

Data:
  - Circular buffer of fixed-size records
  - Size = chunk_size_bytes from lifecycle config
```

**Shared Memory**:
- Uses `multiprocessing.shared_memory`
- Name: feed_name (e.g., `/CB-L2-XRP-USD-LIVE` on Linux)
- Location: `/dev/shm/` on Linux
- Cleanup: Automatic via atexit, OS cleans up on all processes exit

**RingWriter**:
- Writes to next available position
- Updates start_pos when buffer wraps (tracks oldest valid data)
- Increments generation counter on wrap
- Same API as Writer (write_values, write_tuple, write_dict)

**RingReader**:
- Starts from start_pos (oldest valid data)
- Tracks read_pos and records_read
- Detects if writer lapped (skips to start_pos)
- Same API as Reader (stream, range, latest)

**Wrapping Behavior**:
```
Initial: [_________] write_pos=0, start_pos=0, count=0

After 3 writes: [RRR______] write_pos=3, start_pos=0, count=3

Buffer full: [RRRRRRRRRR] write_pos=0, start_pos=0, count=10

After wrap: [RRR_______] write_pos=3, start_pos=3, count=13
            ^newest     ^oldest (overwritten old data)
```

**Range Queries**:
- Scans current buffer from start_pos to write_pos
- Filters by timestamp
- Limited to data in buffer (no historical depth)

### 7. Layout (`layout_json.py`)

Builds struct format strings and metadata from field definitions.

**Input**: Field definitions
```python
[
  {'name': 'price', 'type': 'float64'},
  {'name': 'size', 'type': 'float64'},
  {'name': 'timestamp_us', 'type': 'uint64'},
]
```

**Output**: `layout.json`
```json
{
  "mode": "UF",
  "fmt": "<ddQ",
  "record_size": 24,
  "fields": [...],
  "ts_name": "timestamp_us",
  "ts_offset": 16,
  "version": 1
}
```

**Purpose**:
- Convert human-readable schema to binary format
- Calculate offsets and alignment
- Store metadata for readers/writers

### 8. Manifest (`manifest.py`)

Version enforcement and workspace initialization.

**File**: `<base_path>/manifest.json`

**Content**:
```json
{
  "deepwater_version": "0.2.0",
  "created_at": "2026-02-01T12:00:00",
  "workspace_id": "uuid-here"
}
```

**Purpose**:
- Prevent version mismatches (data format changes)
- Track workspace creation
- Future: migration hooks

## Data Flow

### Write Path (Disk)
```
Application
    ↓ write_values(price, size, ts)
Writer
    ↓ struct.pack_into(buffer, ...)
Memory-mapped chunk file
    ↓ fsync
Disk
    ↓ update write_pos
Feed Registry (mmap)
```

### Write Path (Ring)
```
Application
    ↓ write_values(price, size, ts)
RingWriter
    ↓ struct.pack_into(shm_buffer, ...)
Shared Memory (/dev/shm)
    ↓ update header (write_pos, start_pos, count)
Ring Header (atomic)
```

### Read Path (Disk, Historical)
```
Application
    ↓ range(start_us, end_us)
Reader
    ↓ binary search registry for chunks
Feed Registry (mmap)
    ↓ open matching chunks
Chunk files (mmap)
    ↓ binary search for start record
    ↓ scan forward, unpack records
Application
```

### Read Path (Disk, Live)
```
Application
    ↓ stream(start=None)
Reader
    ↓ jump to latest chunk
    ↓ jump to write_pos
    ↓ spin-wait for new records
Memory-mapped chunk file
    ↓ read new records as writer adds them
Application (70µs latency)
```

### Read Path (Ring)
```
Application
    ↓ stream()
RingReader
    ↓ read header (write_pos, start_pos, count)
    ↓ calculate read_pos
    ↓ spin-wait if caught up
Shared Memory (/dev/shm)
    ↓ unpack records
Application (40-70µs latency)
```

## Performance Characteristics

### Disk Feeds (persist=True)

**Write**:
- 60µs per write (includes fsync)
- Bottleneck: fsync to disk
- Throughput: ~16K writes/sec (limited by fsync rate)

**Read (Live)**:
- 70µs IPC latency
- Bottleneck: memory copy + cache miss
- Mechanism: spin-wait on write_pos change

**Read (Historical)**:
- 920K records/sec throughput
- Bottleneck: memory bandwidth
- Mechanism: sequential mmap scan

### Ring Buffers (persist=False)

**Write**:
- 40-50µs per write (no fsync)
- Bottleneck: memory copy + cache coherency
- Throughput: 200-400 writes/sec (typical market data rate)

**Read (Live)**:
- 40-70µs P50 IPC latency
- Bottleneck: CPU cache coherency (cross-process)
- Mechanism: spin-wait on count change

**Read (Range)**:
- Microseconds for in-memory scan
- Limited by buffer size (typically last few seconds/minutes)

## Concurrency Model

### Multi-Process Safety

**Writer → Reader (Same Feed)**:
- ✅ Safe: Multiple readers can read while writer writes
- Mechanism: Atomic write_pos updates, readers spin-wait

**Writer → Writer (Same Feed)**:
- ❌ Unsafe: Only one writer per feed (enforced by fcntl lock)
- Attempting second writer raises RuntimeError

**Reader → Reader (Same Feed)**:
- ✅ Safe: Unlimited readers per feed
- Each reader maintains independent read_pos

### Locking Strategy

**Global Registry**: fcntl file lock (exclusive for writes, shared for reads)
**Feed Registry**: fcntl file lock (exclusive for chunk updates)
**Ring Buffers**: Lock-free (atomic header updates via mmap coherency)
**Chunks**: Read-only for readers (no locks needed)

### Memory Ordering

- **Platform guarantees**: All metadata writes are flushed before returning
- **Ring buffers**: Rely on CPU cache coherency for cross-process visibility
- **Chunks**: fsync guarantees durability and visibility

## Design Decisions

### Why Memory-Mapped Files?

**Advantages**:
- Zero-copy: Data read directly from kernel page cache
- Multi-process: OS handles page cache sharing
- Automatic eviction: OS manages memory pressure
- Durability: fsync guarantees persistence

**Tradeoffs**:
- 60µs write latency (fsync cost)
- Not suitable for extremely high-frequency writes (>100K/sec)
- Page faults on first access (cold start penalty)

### Why Shared Memory for Rings?

**Advantages**:
- 40-50µs write latency (no fsync)
- Perfect for transient data (preprocessed feeds)
- Lower memory pressure (no disk I/O)

**Tradeoffs**:
- Data lost on process exit/crash
- Limited buffer size (RAM constraint)
- Not suitable for historical analysis

### Why Fixed-Size Records?

**Advantages**:
- Constant-time indexing (offset = record_id * record_size)
- Efficient memory mapping (aligned pages)
- Predictable performance (no allocation)

**Tradeoffs**:
- Padding overhead for variable-length data
- Schema must be known upfront
- Can't add fields without migration

### Why Binary Search for Time Queries?

**Advantages**:
- O(log N) chunk discovery
- O(log N) record discovery within chunk
- Efficient for sparse queries

**Tradeoffs**:
- Requires sorted timestamps (enforced by writer)
- Cold start penalty (chunk metadata scan)

### Why Separate Disk and Ring Implementations?

**Rationale**:
- Different performance characteristics (disk: 60µs write, ring: 40µs)
- Different durability guarantees (disk: persistent, ring: transient)
- Different query patterns (disk: historical, ring: live-only)

**Unified API**:
- Platform routes transparently based on persist flag
- Application code doesn't need to know implementation
- Same interface (stream, range, latest) for both

## Future Optimizations

### Potential Improvements

1. **Batched Writes**: Group multiple writes into single fsync (reduce latency overhead)
2. **Index Files**: Pre-built time indexes for faster cold queries
3. **Compression**: LZ4/Zstd for historical chunks (reduce storage cost)
4. **Async I/O**: io_uring on Linux for parallel writes
5. **NUMA-aware Rings**: Pin ring buffers to specific NUMA nodes
6. **Lock-free Writes**: CAS-based ring updates (eliminate cache contention)

### Non-Goals

- **Distributed**: Deepwater is single-machine only (network replication out of scope)
- **Transactions**: No ACID guarantees (append-only is sufficient)
- **Schema Evolution**: No field addition/removal (create new feed instead)
- **Query Language**: No SQL/DSL (simple time-range queries only)

## Testing Strategy

### Unit Tests
- `test_ring_wrap.py`: Ring wrapping behavior (50-record buffer, 500 writes)
- `test_ring_explicit.py`: Multiprocessing IPC latency test (10-second stream)

### Integration Tests
- `src/tests/headless_websocket.py`: Live Coinbase WebSocket ingest
- `src/tests/headless_reader.py`: Interactive reader with multiple formats
- `benchmark_latency.py`: Disk write latency measurement

### Performance Tests
- Write latency: 60µs target (99th percentile)
- Read latency: 70µs target (IPC live streaming)
- Throughput: 920K rec/sec (historical replay)
- Ring latency: 40-70µs P50 (multiprocessing)

## Debugging

### Common Issues

**"Feed not found in registry"**:
- Feed not created with `create_feed()`
- Wrong base_path
- Registry corruption (delete `registry/global_registry.bin` and recreate feeds)

**"Cannot change persist flag"**:
- Trying to call `create_feed()` with different persist value
- Solution: Use different feed name or delete feed directory

**"Resource temporarily unavailable" (lock error)**:
- Another writer already open for this feed
- Solution: Close existing writer or kill stale process

**Ring buffer FileNotFoundError**:
- Ring not created (writer must create first)
- Ring cleaned up (writer exited, OS removed /dev/shm entry)
- Solution: Ensure writer process is running

### Debug Tools

**Check registry**:
```python
from deepwater.global_registry import GlobalRegistry
from pathlib import Path

reg = GlobalRegistry(Path('./data'))
print([reg.get_metadata(name) for name in reg.list_feeds()])
```

**Check chunks**:
```bash
ls -lh data/feeds/<feed_name>/chunk_*.bin
```

**Check shared memory**:
```bash
ls -lh /dev/shm/  # Linux
```

## Performance Tuning

### Disk Feeds

**Chunk Size**:
- Larger = fewer files, better sequential I/O (default: 64MB)
- Smaller = faster cold start, more flexibility (min: 1MB)
- Recommend: 64MB for production

**Record Size**:
- Align to cache line (64 bytes) for best performance
- Use padding (`_N` type) to achieve alignment
- Example: 40-byte record → pad to 64 bytes

**File System**:
- XFS or ext4 recommended
- Mount with `noatime` to reduce write overhead
- SSD strongly recommended (fsync performance)

### Ring Buffers

**Buffer Size**:
- Larger = more history, lower wrap frequency
- Smaller = better cache locality, faster scans
- Recommend: 1-8MB (seconds to minutes of data)

**Record Size**:
- Smaller = more records in buffer
- Larger = fewer wraps needed
- Balance: 64-128 bytes typical

**CPU Affinity**:
- Pin writer and reader to same NUMA node
- Reduces cross-NUMA latency (40µs → 20µs possible)

## License

MIT - See LICENSE file
