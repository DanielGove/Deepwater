# Deepwater Reader Boundary Review

Date: 2026-05-05

Scope: local reader boundary first, with the network reader/agent checked only where it exercises local reader APIs. The goal is to preserve the core invariant that application code should not care whether data is local, in shared memory, on disk, or behind `host:/path`.

## Executive Summary

The network read path is now mostly on the correct side of the boundary: the agent opens a normal Deepwater reader and calls public APIs (`range`, `range_batches`, `read_available`, `stream`) instead of walking chunk internals itself.

The remaining work is mainly core-reader work:

- `ChunkReader` is doing too many jobs: storage traversal, chunk lifecycle, timestamp query planning, playback snapshot lookup, binary search, format conversion, live tail policy, and cursor state.
- Raw APIs are not consistently raw. Several paths unpack tuples and repack them into bytes, or copy chunk bytes even when a memoryview over stable bytes would be possible.
- Local and remote reader independence are not identical. `Platform.create_reader()` caches one local reader per feed, while `RemotePlatform.create_reader()` creates an independent remote reader/session.
- `PersistentRingReader` is a useful facade, but it materializes durable + ring records in several paths and therefore becomes expensive exactly where large catch-up and low-latency handoff matter.
- The public reader API has grown, but there is not yet a small explicit interface contract that all reader types implement uniformly.

## Desired Reader Boundary

A reader should own:

- Public iteration/query semantics: `stream`, `range`, `range_batches`, `latest`, `read_available`.
- Cursor state for one consumer.
- Format selection at the final boundary into user-visible objects.
- Metadata properties needed by clients: `record_format`, `record_size`, `format`, `field_names`, `dtype`.

A reader should not own:

- Platform reader caching policy.
- Health/monitoring internals that require private method access.
- Network paging details.
- Storage implementation-specific scanning copied across multiple format paths.
- Persistent ring orchestration beyond composing a durable reader and live ring reader.

Storage-specific readers should provide a common low-level scanner primitive to the public reader API:

```python
iter_raw_spans(start, end, *, ts_key=None, playback=False) -> Iterator[memoryview]
iter_raw_batches(start, end, *, batch_records=N, ...) -> Iterator[memoryview]
```

The important distinction is that the scanner yields byte spans; tuple/dict/numpy decoding should be layered on top.

## Boundary Findings

### 1. `ChunkReader` is over-owning multiple layers

`src/deepwater/io/reader.py` is 1,042 lines and mixes several responsibilities in one class:

- Chunk open/close and registry traversal: `_open_chunk`, `_ensure_latest_chunk`, `_iter_chunks_in_range`.
- Query planning and playback: `_resolve_ts_key`, `_playback_start`, `_get_snapshot_before`.
- Search and scan: `_binary_search_start`, `_range_tuples`, `_range_numpy`, `_range_raw`, `_range_raw_batches`.
- Output codecs: tuple, dict, numpy, raw.
- Live tail policy: `_stream_tuples`, `_stream_raw`.
- Stateful polling cursor: `read_available`.

This is workable but fragile. The recent inclusive/exclusive range bug is a symptom: timestamp scan semantics existed in multiple places and had to be aligned across Python and Cython code.

Recommended split, without changing the public API:

- `ChunkStorageCursor`: opens chunks, exposes `(chunk_id, buffer, write_pos, metadata)`.
- `TimestampScanner`: owns `[start, end)` range search and yields raw spans or raw record offsets.
- `RecordCodec`: owns `raw -> tuple/dict/numpy` conversion and layout metadata.
- `ChunkReader`: thin facade preserving current public API.

This can be done incrementally by extracting helpers from `reader.py` while keeping `ChunkReader` as the only public object.

### 2. `Platform.create_reader()` caching violates independent-reader expectations

`src/deepwater/platform.py` caches one reader per feed in `_readers` and returns the same instance for repeated `create_reader(feed_name)` calls.

That creates shared cursor state for:

- `read_available`, via `_read_head` / `_tail_read_seq`.
- live stream cursor fields in ring readers.
- any future reader-local cursor state.

Remote behavior is different: `RemotePlatform.create_reader()` returns a new `RemoteReader` and therefore a new agent-side local reader session.

This matters for the invariant. If local and remote APIs are meant to be indistinguishable, then either:

- `create_reader()` must return an independent reader every time, with optional explicit pooling elsewhere, or
- both local and remote platform readers must document and implement shared-reader caching identically.

The better design is independent readers by default. Reader construction is not the hot path compared to data iteration, and correctness/semantic clarity is worth more than saving one object allocation.

### 3. Monitoring reaches through private reader state

`src/deepwater/ops/health_check.py` calls `reader._ring_state()` when present. This is a private method on `PersistentRingReader` and makes health checks depend on implementation detail.

Add a public reader/platform status method instead:

```python
reader.runtime_status() -> dict
platform.feed_status(feed_name) -> dict
```

It should include ring presence, record counts, durable counts, backlog, overrun/lost counters, last timestamps, and maybe reader type. Health checks and remote metadata can both use that public method.

### 4. Network agent boundary is mostly correct now

`src/deepwater/network/agent.py` currently calls public reader APIs:

- `reader.range(..., format="raw")`
- `reader.range_batches(..., format="raw")`
- `reader.read_available(..., format="raw")`
- `reader.stream(..., format="raw")`

It no longer walks `_chunk`, `_chunk_meta`, or `_iter_chunks_in_range`. That is the right boundary.

The remaining agent inefficiency is unavoidable for TCP today: payloads must become bytes before socket send. The agent currently calls `payload.tobytes()` for paged range frames and `_raw_payload()` for live/read_available/latest. That copy is acceptable at the network boundary, but it makes the upstream local raw-copy inefficiencies more important to remove.

## Hot-Path Inefficiencies

### P0: Ring raw stream unpacks and repacks every record

`RingReader.stream_from_seq(format="raw")` currently does this:

1. Unpack record from ring bytes into a Python tuple.
2. Allocate `bytearray(record_size)`.
3. Pack the tuple back into bytes.
4. Yield `memoryview(raw)`.

That is exactly the wrong shape for the raw hot path. Raw ring stream should yield a memoryview over ring bytes when the record is contiguous, or copy only when wraparound forces it. Most records are contiguous.

This affects local raw consumers and remote live streams because the agent asks for raw records.

### P0: Ring range batches also unpack/repack for raw

`RingReader.read_seq_range()` and `RingReader.read_seq_batches()` append unpacked tuples and then call `_format_records(format="raw")`, which packs them back into a fresh bytearray.

For raw range/range_batches, the ring reader should scan sequence positions and emit contiguous byte spans directly. Filtering by timestamp can still use `unpack_u64` for the timestamp only; it does not require unpacking the full record.

### P0: Persistent reader materializes before formatting

`PersistentRingReader.range()` reads durable tuples, reads ring tuples, concatenates the two lists, and then formats the combined list.

For large historical windows or catch-up this causes:

- tuple allocation for every record even when caller requested raw or numpy.
- a second list allocation when concatenating durable + ring records.
- packing into raw bytes after tuple materialization.

`range_batches()` is the right direction, but `range()` still has unbounded behavior by design. That may be acceptable only if explicitly documented as materializing. The important work is to ensure remote and CLI paths prefer `range_batches()` for large reads.

### P0: Persistent stream catch-up materializes durable history

`PersistentRingReader.stream(start=...)` reads historical durable records into a list before yielding. This is not compatible with large catch-up semantics. It should stream durable catch-up through `range_batches()` or a direct durable stream and then transition to ring streaming.

### P1: `ChunkReader._range_raw()` is not actually zero-copy

The docstring says "zero-copy if single chunk", but the implementation appends `bytes(buf[pos:end_pos])`, then returns `memoryview(blocks[0])`. That is a copy even for one chunk.

This may have been chosen to provide stable memory independent of chunk lifecycle, but the docstring and API semantics should be honest. If stable ownership is required, call it owned raw bytes. If zero-copy is required, return a span object that pins the chunk until the view is released.

### P1: `ChunkReader._range_raw_batches()` copies record-by-record

The current batching loop extends a `bytearray` once per matching record. For timestamp-contiguous ranges the scanner should first find maximal contiguous spans and copy span-sized blocks into batches. That removes thousands/millions of Python loop-level slice copies for large windows.

Target behavior:

- Scan timestamps to find `[pos, end_pos)` per chunk.
- Yield chunk memoryviews directly when possible for local consumers.
- For owned/network batches, copy whole spans into bounded byte buffers, not one record at a time.

### P1: Numpy range pays the same copy cost as raw

`ChunkReader._range_numpy()` builds `blocks` with `bytes(...)`, joins them, then creates `np.frombuffer`. This is reasonable for a single returned array, but it should share the same span scanner as raw range. Single contiguous spans can become numpy views directly if the lifetime contract pins the underlying chunk.

### P1: `read_available(format="raw")` return type differs by reader

`ChunkReader.read_available(format="raw")` returns a list of per-record memoryviews.

`RingReader.read_available(format="raw")` returns one memoryview because it delegates through `read_seq_range` and `_pack_raw`.

`PersistentRingReader.read_available(format="raw")` can return either shape depending on whether ring state exists.

The agent compensates with `_raw_payload()`, but this inconsistency leaks complexity into every consumer. Raw `read_available` should have one canonical shape. Prefer one bounded batch memoryview for batch APIs, and per-event memoryview only for event stream APIs.

### P1: `ChunkReader.read_available()` can skip intermediate chunks

When chunk rotation is detected, `read_available()` opens the latest chunk and sets `_read_head = 0`. If a reader falls behind by more than one chunk, this can skip sealed intermediate chunks. That may be acceptable for "live only, latest state" behavior, but it is dangerous unless named/documented as lossy.

For a reliable consumer cursor, it should advance chunk-by-chunk or report overrun/loss explicitly.

### P1: Duplicate scanner semantics invite drift

Timestamp scan logic appears in:

- `_range_tuples`
- `_range_numpy`
- `_range_raw`
- `_range_raw_batches`
- `_stream_tuples`
- `_stream_raw`
- `reader_fast.pyx`
- ring and persistent reader wrappers

This caused the `[start, end)` alignment work already. The long-term fix is one scanner contract per storage type, with formats layered above it.

### P2: Repeated field-name and dtype normalization logic

`field_names` filtering and duplicate dtype-name normalization are duplicated across `ChunkReader`, `RingReader`, `PersistentRingReader`, and `RemoteReader`.

Move this into a layout helper, for example:

```python
visible_field_names(layout) -> tuple[str, ...]
normalized_dtype(layout) -> np.dtype | None
record_size(layout) -> int
clock_fields(layout) -> tuple[ClockField, ...]
```

This reduces tiny per-reader setup cost and avoids layout interpretation drift.

### P2: Timestamp key lookup allocates and reverse-lookups in hot-ish paths

`ChunkReader._resolve_ts_key()` builds a list when resolving by name, and playback paths reverse-lookup key IDs using a generator over `_ts_fields`. This is not the highest priority, but it is needless work and complexity.

Cache this once in `__init__`:

```python
_ts_by_name: dict[str, tuple[offset, key_id]]
_ts_key_by_offset: dict[int, key_id]
```

### P2: Minor stale code/comments

- `from sys import byteorder` is unused in `reader.py`.
- `ProcessUtils` is imported but unused in `reader.py`.
- `_binary_search_start(..., ts_byteorder="little")` accepts `ts_byteorder` but ignores it.
- `_stream_raw()` doc says "64 bytes each" even though record size is dynamic.

These are small, but they add noise in the fattest file.

## API Invariants To Lock Down

These should become tests across `ChunkReader`, `RingReader`, `PersistentRingReader`, `RemoteReader`, and `ManagedLocalReader` wherever the storage mode supports the operation.

1. `range(start, end)` returns records in `[start, end)`.
2. `range()` is materializing and may be large; `range_batches()` is the bounded-memory equivalent.
3. `range_batches(..., batch_records=N)` yields batches with at most `N` records, except possibly format-specific empty behavior.
4. `stream(start=None)` is live-only and does not replay historical records.
5. `stream(start=T)` catches up from `T` and then continues live.
6. `read_available()` is non-blocking and stateful per reader instance.
7. Two readers created for the same feed have independent cursors.
8. Raw stream yields raw records/events.
9. Raw range batch yields bounded raw batches.
10. Tuple/dict/numpy are decoded client-side for remote reads from the remote layout/spec.
11. Metadata APIs behave the same through `Platform("/path")` and `Platform("host:/path")`.
12. Lossy live-tail behavior, if allowed, is explicit and observable.

## Proposed V1 Refactor Order

### Step 1: Define the reader interface contract

Create a small internal protocol or documented abstract contract for reader-like objects. Do not force inheritance yet; use tests to enforce behavior.

Required properties:

- `record_format`
- `record_size`
- `format`
- `field_names`
- `dtype`

Required methods:

- `range`
- `range_batches`
- `stream`
- `read_available`
- `latest`
- `close`
- `runtime_status` or equivalent public status method

### Step 2: Fix reader independence

Change local `Platform.create_reader()` to return an independent reader by default. If caching is still needed, add an explicit method like `get_cached_reader()` or `reader_pool()` rather than hiding shared cursors behind `create_reader()`.

Remote behavior already creates independent reader sessions.

### Step 3: Normalize raw return shapes

Define canonical raw shapes:

- `stream(format="raw")`: yields one raw record/event memoryview at a time.
- `range(format="raw")`: returns one owned contiguous memoryview, clearly materializing.
- `range_batches(format="raw")`: yields bounded owned memoryviews.
- `read_available(format="raw")`: returns one owned bounded memoryview or a list consistently across all readers. Prefer memoryview for batch semantics.

Then remove `_raw_payload()` shape guessing from the agent once all readers comply.

### Step 4: Add raw fast paths for ring

Implement direct raw span/batch paths in `RingReader`:

- Avoid full tuple unpack for raw.
- Avoid pack-back into bytearray for contiguous records.
- Copy only across ring wrap or when producing an owned network frame.

This is probably the highest ROI latency change because remote live streams use raw.

### Step 5: Extract chunk range scanner

Create one chunk scanner that yields raw spans or `(buffer, pos, count)` records. Rebuild tuple/dict/numpy/raw/range_batches on that. Keep Cython optimization behind the scanner rather than duplicating semantics in each output format.

### Step 6: Make persistent catch-up bounded

Change `PersistentRingReader.stream(start=...)` and `range_batches()` to use bounded durable batches and direct ring batches. Keep `range()` as materializing for API compatibility, but internally it should concatenate batches rather than materialize tuples first for every format.

### Step 7: Public status instead of private reach-in

Add `reader.runtime_status()` and update health check/network metadata to call public status. Include ring and durable counters without exposing `_ring_state()`.

## Network Implications

The current remote reader does get the remote feed spec/layout from the agent during `OPEN_READER` and decodes tuple/dict/numpy client-side. That is the correct model: the server streams raw Deepwater record bytes, and the client uses the remote layout to decode them.

The agent is no longer the main boundary problem. Its remaining performance profile is determined by local raw reader efficiency plus unavoidable TCP framing copies. Improving local raw paths directly improves network latency and CPU use.

The largest remaining invariant gap is local/remote reader identity and cursor behavior, not MagicDNS/Tailscale/TCP mechanics.

## Immediate Pain Points Ranked

P0:

- Ring raw stream unpack/repack allocation.
- Ring raw range/range_batches unpack/repack allocation.
- Persistent stream historical catch-up materializes durable history.
- Local `create_reader()` returns shared cursor-bearing readers, unlike remote.

P1:

- Chunk raw range doc claims zero-copy but copies.
- Chunk raw batch copies per record instead of span-sized copies.
- `read_available(format="raw")` has inconsistent shapes across reader types.
- `ChunkReader.read_available()` may skip intermediate chunks if the consumer falls behind.
- Scanner semantics duplicated across formats/storage layers.

P2:

- Layout field/dtype normalization duplicated.
- Timestamp key lookup does small avoidable allocations.
- Stale imports/comments and ignored `ts_byteorder` argument.

## Bottom Line

Network read-side behavior is viable for the prototype, but Deepwater's next performance ceiling is in the local reader stack. The correct next implementation pass is not more network feature work; it is normalizing the reader contract and making raw local paths actually raw. Once local raw event/range batching is clean, remote reads become a thin transport over the same invariant instead of a special case.
