# Deepwater Runtime Invariants

Date: 2026-05-04

## Local Reader Invariants

1. `range(start, end, ...)` is a materializing API.
   - It returns the complete finite result for `[start, end)`.
   - Tuple/dict formats return complete Python collections.
   - Numpy returns a complete structured array.
   - Raw returns one contiguous memoryview, copying/joining chunks when necessary.
   - A caller asking for an 8 GB local raw range is explicitly asking for an 8 GB materialized object.

2. `range_batches(start, end, batch_records=N, ...)` is the bounded historical iterator API.
   - It represents one logical query over `[start, end)`.
   - It yields ordered batches of at most `N` records.
   - It must not materialize the full range before yielding.
   - It is the correct API for large local scans, backtests, catch-up, and network serving.

3. `stream(start=None, ...)` is the event-driven live API.
   - With `start=None`, it begins at the current live head and skips history.
   - With `start=timestamp`, it replays from that timestamp and then continues live.
   - It yields records as the underlying local runtime observes them.

4. `read_available(max_records=N, ...)` is the non-blocking poll API.
   - It returns immediately.
   - It owns a reader-local cursor.
   - It is for event loops and cooperative runtimes that do not want blocking streams.

5. Timestamp range semantics are `[start, end)`.
   - `start` is inclusive.
   - `end` is exclusive.
   - Local and remote readers must not differ on boundary records.

## Remote Reader Invariants

1. The remote reader is a reader facade, not a file-transfer client.
   - It requests data from a remote agent.
   - The agent opens local Deepwater readers.
   - The agent sends raw record bytes plus control metadata.
   - The client decodes with the feed layout received during `OPEN_READER`.

2. Remote raw memory is client-local.
   - The server may read local memoryviews.
   - Bytes crossing the socket are copies.
   - `RemoteReader(..., format="raw")` returns a memoryview over client-owned frame payload bytes.

3. `range(...)` remains materializing.
   - It returns one complete logical result.
   - The protocol may later page internally, but the API contract is still materialized output.
   - If the result is too large for configured limits, the implementation should fail clearly rather than silently pretending the caller got a cheap view.

4. `range_batches(...)` is the explicit historical microbatch API.
   - It is one logical remote query.
   - Multiple `DATA` frames are transport pages for that query, not independent queries.
   - The client exposes batches because the caller selected a batched API.

5. `stream(...)` is live/event-driven.
   - Historical catch-up before live may be batched by a future stream protocol.
   - Once live, the default should preserve event-driven behavior.
   - Optional live microbatching must be explicit and controlled by a latency/records/bytes budget.
   - Idle remote live streams may carry heartbeat control frames; user iteration never yields them as records.

## Agent Invariants

1. The agent only uses public reader/platform APIs.
   - It must not reach into `_chunk`, `_chunk_meta`, `_iter_chunks_in_range`, `_binary_search_start`, or other local reader internals.
   - Local readers own storage-specific batching.
   - The agent owns framing, sessions, root guard, and network I/O.

2. The agent never copies files.
   - It opens local readers.
   - It streams raw record bytes.
   - It is not a filesystem replication layer.

3. The agent must preserve request identity.
   - A paged range is one request id followed by ordered `DATA` frames and one `RANGE_END`.
   - Transport pages are not separate application-level reads.

4. The agent must enforce resource limits.
   - Max frame bytes.
   - Max records per batch.
   - Socket keepalive and idle read timeout.
   - Live stream heartbeats for dead-peer detection.
   - Future: max sessions, write deadlines, and output-buffer limits.

## Design Consequence

Deepwater has three distinct read shapes:

1. Materialized finite read: `range()`.
2. Bounded finite iterator: `range_batches()`.
3. Open-ended live read: `stream()` / `read_available()`.

Remote networking should not blur these shapes. It should transport each shape honestly and efficiently.
