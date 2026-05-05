# Deepwater Network Stack Review

Date: 2026-05-04

## Executive Status

Deepwater now has a minimal remote read plane that preserves the intended application-level shape:

```python
import deepwater as dw
from deepwater import Platform

r = dw.reader("deepwater-pioneer:/deepwater/data/hyperliquid-node", stream="trades")
rows = r.range(start_us, end_us)

p = Platform("deepwater-pioneer:/deepwater/data/hyperliquid-node")
reader = p.create_reader("trades")
```

The implementation is not embedded into the hot local reader implementation. Remote functionality is concentrated in `src/deepwater/network`, with one small factory hook in `Platform.__new__` and public exports in `deepwater.__init__` / `deepwater.network.__init__`.

Current network scope:

1. Remote reader open.
2. Remote metadata queries.
3. Remote historical range reads.
4. Paged historical range reads.
5. Remote latest reads.
6. Remote live stream.
7. Remote non-blocking `read_available`.
8. Read-only metadata CLIs over remote paths.
9. Public local `range_batches()` used by the agent for paged historical reads.
10. TCP keepalive, idle read timeout, connection logs, and live stream heartbeat frames.

Current non-scope:

1. Remote writes.
2. Remote feed creation/deletion.
3. Remote repair/cleanup/admin operations.
4. Protocol-level authentication.
5. Stream resume tokens.
6. Multiplexed reader sessions on one socket.

## Code Ownership Map

### Core Local Substrate

These files remain responsible for local machine behavior:

1. `src/deepwater/platform.py` (~931 LOC)
   - Feed lifecycle.
   - Registry and layout access.
   - Writer/reader construction.
   - Local dataset window helpers.
   - Persister process management.
   - Only network hook: `Platform.__new__` detects remote locator syntax and returns `RemotePlatform`.

2. `src/deepwater/io/reader.py` (~946 LOC)
   - Durable chunk reader.
   - Local historical range/latest/stream/read_available.
   - Local raw memoryviews over chunk buffers where possible.
   - No remote socket/protocol logic.

3. `src/deepwater/io/ring.py` (~914 LOC)
   - Shared-memory ring writer/reader.
   - Live in-memory feed behavior.
   - Local raw memoryviews over ring data.
   - No remote socket/protocol logic.

4. `src/deepwater/io/persistent_ring.py` (~709 LOC)
   - Persistent feed composition: durable chunks plus live ring tail.
   - Persister integration.
   - Hybrid read behavior.
   - Owns persistent-feed batching through public reader APIs.

5. `src/deepwater/metadata/*`
   - Layout, registry, segments, dataset interval helpers.
   - Remote platform reuses dataset helper functions rather than copying the algorithms fully.

### Network Layer

These files own remote transport behavior:

1. `src/deepwater/network/path.py` (~87 LOC)
   - Parses local paths vs remote locators.
   - Supported remote forms:
     - `host:/absolute/path`
     - `dw://host/absolute/path`
     - `dw://host:7447/absolute/path`
   - Resolves and guards requested paths under the agent root.

2. `src/deepwater/network/protocol.py` (~113 LOC)
   - Length-prefixed frame format.
   - JSON control header.
   - Optional binary payload containing raw Deepwater record bytes.
   - Protocol version, limits, and capabilities.

3. `src/deepwater/network/client.py` (~626 LOC)
   - `RemoteReader`.
   - `RemotePlatform`.
   - `ManagedLocalReader` for `dw.reader(local_path, stream=...)` parity.
   - `reader(...)` helper that dispatches local vs remote target syntax.
   - Client-side decode from remote raw bytes into tuple/dict/numpy/raw.

4. `src/deepwater/network/agent.py` (~560 LOC)
   - TCP server on port 7447 by default.
   - Opens local `Platform`/reader instances.
   - Serves metadata operations.
   - Streams binary batches to clients.
   - Enforces root path guard.

## Public API Boundary

### `Platform(...)`

Local:

```python
p = Platform("/data/node")
```

Returns a local `Platform` object.

Remote:

```python
p = Platform("deepwater-pioneer:/deepwater/data/hyperliquid-node")
```

Returns a `RemotePlatform` object from `src/deepwater/network/client.py`.

This is the main boundary decision. It keeps remote concerns out of local platform internals while preserving the call site. The tradeoff is that `isinstance(p, Platform)` is false for remote platforms. That is probably acceptable for the invariant because application behavior should be method/API based, not type-check based. If strict type identity becomes important, use an abstract protocol/base class later.

### `dw.reader(...)`

Local:

```python
r = dw.reader("/data/node", stream="trades")
```

Creates a temporary local `Platform`, opens a local reader, and returns `ManagedLocalReader` so the platform closes with the reader.

Remote:

```python
r = dw.reader("deepwater-pioneer:/deepwater/data/hyperliquid-node", stream="trades")
```

Returns `RemoteReader` directly.

This is clean from an application perspective. The only boundary smell is that `ManagedLocalReader` lives in `network/client.py`; it is not network code. It exists there because `dw.reader(...)` was introduced with the network helper. If this grows, move `ManagedLocalReader` and `reader(...)` into a neutral top-level API module and leave `network/client.py` remote-only.

## Protocol Walkthrough

### Connection Setup

1. Client connects to `host:7447`.
2. Client sends `HELLO` with protocol version and requested capabilities.
3. Agent replies with selected protocol version, server capabilities, and limits.

Current capabilities:

1. `paged_range=True`.
2. `resume_stream=False`.
3. `multi_reader_session=False`.

### Reader Open

1. Client sends `OPEN_READER` with `path` and `feed_name`.
2. Agent resolves `path` under `--root`.
3. Agent opens local `Platform(str(base_path))`.
4. Agent opens local reader via `platform.create_reader(feed_name)`.
5. Agent replies `READER_OPEN` with:
   - `layout = platform.get_record_format(feed_name)`
   - `lifecycle = platform.lifecycle(feed_name)`

The remote reader then builds its own client-side decode state:

1. `record_size`.
2. `struct.Struct(layout["fmt"])`.
3. `field_names`.
4. `numpy.dtype` when metadata includes dtype.

This means the agent sends literal record bytes and the client decodes them with a copy of the remote feed spec. The client does not read remote files and does not assume access to remote memory.

### Metadata Requests

`RemotePlatform` uses one socket for metadata RPCs. Each request sends a path and operation:

1. `LIST_FEEDS`.
2. `FEED_EXISTS`.
3. `DESCRIBE_FEED`.
4. `LIFECYCLE`.
5. `RECORD_FORMAT`.
6. `LIST_SEGMENTS`.
7. `SUGGESTED_READER_RANGE`.

The agent currently opens and closes a local `Platform` per metadata request. This is simple and avoids long-lived metadata state, but it adds per-request latency.

### Range Reads

Compatibility mode:

1. Client sends `READ_RANGE`.
2. Agent calls local `reader.range(..., format="raw")`.
3. Agent converts the raw local view/result to bytes.
4. Agent sends one `DATA` frame.
5. Client decodes locally.

Paged mode:

1. Client calls `range_batches(...)`.
2. Client sends `READ_RANGE_PAGE` with `batch_records`.
3. Agent streams one or more `DATA` frames.
4. Agent ends with `RANGE_END`.
5. Client yields each decoded batch.

Local batching path:

1. Agent calls the public local `reader.range_batches(..., format="raw")` API.
2. Durable chunk readers, ring readers, and persistent-ring readers own storage-specific batching.
3. Agent frames each yielded batch as a `DATA` page for the same request id.
4. Agent never reaches into `_chunk`, `_chunk_meta`, or durable-reader private methods.

### Latest Reads

1. Client sends `LATEST` with seconds and optional `ts_key`.
2. Agent calls local `reader.latest(..., format="raw")`.
3. Agent sends a single `DATA` frame.
4. Client decodes locally.

### Non-Blocking Reads

1. Client sends `READ_AVAILABLE` with optional `max_records`.
2. Agent calls local `reader.read_available(..., format="raw")`.
3. Agent sends one `DATA` frame.
4. Client decodes locally.

Semantics match local reader statefulness: each remote reader session has its own server-side local reader object and therefore its own read head.

### Live Stream

1. Client sends `SUBSCRIBE_LIVE`.
2. Agent replies `SUBSCRIBED`.
3. Live-only streams use local `read_available(..., format="raw")` polling so the agent can send heartbeat control frames while idle.
4. Each raw record is sent as a `DATA` frame.
5. Idle streams send `HEARTBEAT` frames with session/time/idle metadata.
6. Client decodes `DATA` frames and suppresses `HEARTBEAT` frames from user iteration while emitting status callbacks.

Current limitation: there are no resume tokens, explicit cancel messages, or reconnect catch-up policy. Historical-start streams still use the local stream iterator path and should be revisited when resume/catch-up is formalized.

## Memory Ownership

Remote raw output is client-side memory only.

1. Local readers may expose memoryviews over mmap chunks or ring buffers.
2. Agent converts local raw views/results to bytes before or during socket transmission.
3. Protocol `read_frame()` returns bytes.
4. `RemoteReader._decode_payload(format="raw")` returns `memoryview(payload)` over those client-side bytes.

So remote userland never receives a view into server hot memory. This preserves the memory safety distinction while keeping `format="raw"` shape compatible.

## CLI Integration

Read-only metadata CLIs use `Platform(...)` and therefore accept remote paths:

```bash
deepwater-feeds --base-path deepwater-pioneer:/deepwater/data/hyperliquid-node
deepwater-segments --base-path deepwater-pioneer:/deepwater/data/hyperliquid-node --feed trades --status usable
deepwater-datasets --base-path deepwater-pioneer:/deepwater/data/hyperliquid-node --feed trades --json
```

`deepwater-agent` is exposed as a console script and maps to `deepwater.network.agent:main`.

Mutating/admin CLIs remain local-only:

1. `deepwater-create-feed`.
2. `deepwater-delete-feed`.
3. `deepwater-repair`.
4. `deepwater-cleanup`.
5. `deepwater-health`.

This is intentional until the protocol defines remote write/admin commands.

## Core Boundary Assessment

### Healthy Boundaries

1. `reader.py` does not know about sockets, hostnames, or protocol frames.
2. `ring.py` does not know about sockets, hostnames, or protocol frames.
3. `persistent_ring.py` does not own network behavior.
4. `platform.py` has a small remote locator hook but otherwise stays local-platform focused.
5. Binary row encoding stays in the local layout/reader machinery; network only transports raw bytes plus metadata.
6. CLI metadata commands reuse `Platform(...)`, which is the right abstraction boundary.

### Boundary Smells

1. `Platform.__new__` returns an object that is not a `Platform` instance.
   - Practical API works.
   - Type identity and subclassing are less clean.
   - Long-term fix: define a `PlatformProtocol`/base facade or move construction to a top-level factory while keeping `Platform(...)` compatibility.

2. `ManagedLocalReader` lives in `network/client.py`.
   - It is not network-specific.
   - If `dw.reader(...)` becomes core API, move helper/factory to `src/deepwater/api.py` or similar.

3. `RemotePlatform.common_time_windows()` duplicates the loop structure from `Platform.common_time_windows()`.
   - It reuses shared dataset algorithms but repeats feed/segment collection.
   - Long-term fix: extract a shared helper that operates on any object with `feed_exists()` and `list_segments()`.

4. Metadata RPCs open a new local `Platform` per request.
   - Clean lifetime, but latency inefficient.
   - Long-term fix: metadata session cache or a unified remote platform session with server-side platform retained.

## Invariant Assessment

Core invariant: aside from locator/hostname, applications should not care whether data is local or remote.

### Satisfied Today

1. `dw.reader(local, stream=feed)` and `dw.reader(remote, stream=feed)` both work.
2. `range()`, `latest()`, `stream()`, `read_available()`, and `range_batches()` exist on remote readers.
3. `tuple`, `dict`, `numpy`, and `raw` decode formats exist remotely.
4. `Platform(remote).create_reader(feed)` works.
5. Key read-side metadata methods work on remote platforms.
6. Read-only metadata CLIs work against remote paths.

### Partial or Violated

1. `Platform(remote).create_writer(...)` raises `NotImplementedError`.
   - Intentional for v0.
   - Violates full platform indistinguishability if apps expect writers.

2. `Platform(remote).create_feed(...)` and `delete_feed(...)` raise `NotImplementedError`.
   - Intentional for v0.
   - Remote platform is read-side only.

3. `isinstance(Platform(remote), Platform)` is false.
   - Could break code that type-checks rather than uses methods.

4. Local readers now expose `range_batches()` directly.
   - `Platform(local).create_reader(...).range_batches()` is part of the direct local reader API.
   - `dw.reader(local, ...)` delegates to the underlying local reader when available.

5. Local and remote stream failure semantics differ.
   - Remote stream can fail due to network disconnect.
   - No resume token yet.
   - Applications that require exactly-once continuity still need policy above the reader.

6. Remote platform close-reader methods are no-ops.
   - `RemotePlatform.create_reader()` returns independent reader sockets, so this is not harmful.
   - It differs from local `Platform.close_reader(feed)` semantics.

7. `read_available(format="raw")` returns `memoryview(b"")` remotely/local ring, but documentation/type hints still often say `List`.
   - API shape works, typing is behind reality.

## Latency and Resource Review

### Good Choices

1. TCP_NODELAY is enabled on client sockets.
2. Payload rows remain binary Deepwater bytes; no JSON row encoding.
3. Paged range reads avoid one huge client response.
4. Client decodes locally, avoiding server CPU for tuple/dict conversion.
5. Root path guard prevents arbitrary filesystem access.
6. Default port is stable at 7447.
7. Network transport is not opinionated: Tailscale/MagicDNS works because ordinary TCP/DNS works.

### Latency Costs

1. Reader open costs two round trips:
   - `HELLO`
   - `OPEN_READER`
   - Optimization: allow `OPEN_READER` to include hello/version fields or pipeline hello/open.

2. `RemotePlatform.create_reader()` opens a new socket even if the platform already has one.
   - Clean and simple.
   - Higher connection setup cost.
   - Optimization: session/multiplex support or platform-owned reader session creation.

3. Metadata methods are one RPC each and server opens a local `Platform` each time.
   - `deepwater-feeds --all` can call list then describe each feed, causing multiple platform opens.
   - Optimization: add batch metadata ops, e.g. `DESCRIBE_FEEDS`, `PLATFORM_SNAPSHOT`, `DATASET_WINDOWS`.

4. Protocol header uses stdlib `json` while the repo depends on `orjson`.
   - Header sizes are small, so not critical.
   - For very high message rates, `orjson` would reduce CPU.

5. `read_frame()` assembles `prefix + body` and then `decode_frame()` copies payload to bytes.
   - There are avoidable allocations.
   - Optimization: decode header directly from body and return payload slice/bytes without recombining.

6. `write_frame()` builds a full contiguous bytes object before `sendall()`.
   - This copies payload via `payload_bytes.tobytes()`.
   - Optimization: send prefix/header/payload with `sendmsg()` or sequential `sendall()` calls, preserving memoryview payloads.

7. Agent live-only stream sends one frame per live record by default.
   - For small records, per-frame overhead and syscall count can dominate.
   - This preserves event-driven live semantics.
   - Future optimization must be explicit: coalesce live records with max latency budget, e.g. flush every N records or X microseconds.

8. `range()` and `latest()` still return single frames.
   - Good for compatibility.
   - Risky for large windows.
   - Encourage `range_batches()` for large reads; possibly enforce size limit or auto-page internally later.

### Resource Risks

1. Agent uses `ThreadingMixIn` with no max connection/session limit.
   - Risk: too many clients or stuck streams consume threads and file descriptors.
   - Add max concurrent sessions and per-client limits.

2. No write deadline or bounded output queue.
   - Socket keepalive, server read idle timeout, and live heartbeats now exist.
   - Slow clients can still block a stream handler in `sendall()`.
   - Add write deadlines or bounded output queues.

3. No backpressure beyond TCP and client-pulled range pages.
   - Live streams can block server handler on slow clients.
   - Add bounded output queue or write deadline.

4. Materialized `range()` can still allocate huge local/server memory.
   - `range_batches()` is now the bounded alternative.
   - Remote `range()` should eventually fail early above a configured byte/record threshold with a clear `RANGE_TOO_LARGE` error.

5. Maximum frame is 512 MiB.
   - Safe as a hard cap, but too high for latency-sensitive systems.
   - Consider lower default plus page-size negotiation.

6. No stable typed error classes client-side.
   - Agent sends code strings, but `RemoteReaderError` collapses them to message text.
   - Add error subclasses or attach `code` to exception.

7. No audit identity.
   - On Tailscale, peer identity is available outside Deepwater, but the process does not record it.
   - At minimum log remote socket address, requested path, feed, op, result, and count.

8. No protocol-level auth.
   - Acceptable only when bound to a trusted tailnet/LAN interface with ACLs/firewall.
   - Not safe on public interfaces.

9. No compression.
   - Good default for hot binary records on fast WireGuard.
   - For cross-region or slower links, optional compression might matter for large historical reads.

10. No TLS.
   - Correct for tailnet-only minimalism.
   - If deployed outside trusted encrypted transport, add optional TLS or require an external secure tunnel.

## Immediate Open Items

Priority order:

1. Add stream cancel and write-deadline semantics.
   - Heartbeats and socket idle timeouts now cover silent idle streams.
   - Explicit cancel/write deadlines are still needed for long-lived production streams.

2. Add typed remote errors.
   - Preserve `code`, `message`, and operation context.
   - Better app behavior and observability.

3. Batch metadata calls.
   - Reduces CLI and remote platform latency.
   - Good first op: `PLATFORM_SNAPSHOT(path)` returning feeds, descriptions, lifecycle, formats, segments summary.

4. Add remote session/resource limits.
   - Max concurrent handlers.
   - Max streams.
   - Max requested range span/page bytes.
   - Write deadlines.

5. Reduce protocol copies.
   - Avoid `prefix + body` in `read_frame()`.
   - Avoid `payload.tobytes()` in `write_frame()`.
   - Consider `orjson` for headers.

6. Add live micro-batching.
   - Keep latency budget explicit.
   - Example policy: flush on `max_records`, `max_bytes`, or `max_delay_us`.

7. Add stream resume/catch-up.
   - Resume token per stream page.
   - Client reconnect policy.
   - Fallback to `range_batches(last_ts, now)` when resume is too old.

8. Decide on `Platform(remote)` type semantics.
   - Keep duck typing, or introduce a shared abstract facade/protocol.

9. Extend tests to real tailnet host smoke tests.
   - Loopback tests validate protocol deterministically.
   - Tailnet tests should validate MagicDNS, bind behavior, latency, and large batch behavior.

## Recommended Design Direction

Keep the current layering:

1. Local readers stay local and zero-copy focused.
2. Network agent is an adapter over local platform/readers.
3. Remote reader/platform are facades that match the local API shape.
4. Protocol transports raw bytes plus feed layout metadata.
5. Shared algorithms move into metadata/helper modules, not into network or platform copies.

The most important next refactor is not moving remote code into `reader.py`; it is making live stream batching/resume explicit while keeping event-driven live behavior available by default.
