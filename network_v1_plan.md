# Deepwater Networking v1 Plan

## Purpose

Define Deepwater Networking v1 as a production-oriented remote read protocol with stronger parity to local readers, explicit failure/recovery behavior, and better scalability than v0.

This plan is focused on distributed-read workloads, not remote write ingestion.

Current implementation and boundary review: see `network_stack_review.md`.
Runtime API invariants: see `runtime_invariants.md`.

## Core Invariant

Applications should not branch on where data lives.

Aside from the locator (`/local/base/path` vs `host:/remote/base/path`) and optional port in a `dw://` URL, the reader API must be indistinguishable across local and remote execution. The same application code should work whether records are read from local memory, a local disk-backed feed, another machine on the same tailnet, or a remote site reachable through an operator-provided transport.

Deepwater must not own transport policy. It opens a TCP connection to the requested host and port. Operators decide whether that address is MagicDNS over Tailscale, private LAN DNS, WireGuard routing, Kubernetes service DNS, or another network substrate.

The same rule applies at the platform layer. `Platform("host:/base").create_reader("feed")` and read-side metadata methods should behave like `Platform("/base").create_reader("feed")`. Remote mutation APIs may remain explicitly unsupported until the write protocol exists.

## Status

- Current implementation: Networking v0 plus initial v1 protocol foundations
- Current capabilities: remote `range`, `range_batches`, `latest`, `stream`, live stream heartbeat
- Current transport: TCP + framed messages (`json header` + `binary payload`)
- Current security stance: trusted tailnet boundary

## v1 Goals

1. Keep read API ergonomic and close to local Deepwater reader usage.
2. Make large range reads memory-safe and resumable.
3. Make live streams recoverable after disconnect without silent gaps.
4. Provide protocol versioning and capability negotiation.
5. Preserve binary payloads (no JSON row conversion).
6. Keep local single-machine path fast and unchanged by default.
7. Avoid hard dependencies on Tailscale APIs, host inspection, firewall manipulation, or VPN management.

## v1 Non-Goals

1. Remote writes in this phase.
2. WAN/public internet feature set (global auth provider, edge routing, etc.).
3. Replacing Tailscale/network policy controls.

## Current v0 Pain Points (Why v1)

1. `range/latest` return a single materialized payload per request, which is hard on memory for large windows.
2. Live stream reconnect has no first-class resume token/offset contract.
3. Connection model is effectively one feed stream per socket during `SUBSCRIBE_LIVE`.
4. Security is network-boundary only, with no protocol-level authn/authz.
5. No explicit capability/version negotiation for forward compatibility.

## API Design (v1)

v1 keeps `deepwater.reader(...)` as the public entry point and adds optional controls for pagination and resume.

### Public API

```python
import deepwater as dw

# Local path: unchanged behavior
r_local = dw.reader("/deepwater/data/hyperliquid-node", stream="trades")

# Remote path: same constructor shape
r_remote = dw.reader("deepwater-pioneer:/deepwater/data/hyperliquid-node", stream="trades")

# New: paged historical read (generator of batches)
for batch in r_remote.range_batches(start_us, end_us, batch_records=50000):
    ...

# Existing: keeps list-return behavior for convenience
rows = r_remote.range(start_us, end_us)

# New: resumable stream
stream = r_remote.stream(start=start_us, resume="auto")
for rec in stream:
    ...
```

### Reader Interface Contract

All reader instances returned by `dw.reader(...)` must satisfy this contract:

1. `range(start, end, format='tuple', ...) -> list|numpy|memoryview`
2. `latest(seconds=60, format='tuple', ...) -> list|numpy|memoryview`
3. `stream(start=None, format='tuple', ...) -> iterator`
4. `range_batches(start, end, format='tuple', batch_records=..., ...) -> iterator[batch]` (new)
5. `read_available(max_records=None, format='tuple') -> list|memoryview`
6. `checkpoint() -> opaque_token|None` (new, remote meaningful; local optional)
7. `close()`

### Platform Interface Contract

Remote platforms should support read-side metadata and reader creation:

1. `create_reader(feed_name)`
2. `list_feeds()`
3. `feed_exists(feed_name)`
4. `describe_feed(feed_name)`
5. `lifecycle(feed_name)`
6. `get_record_format(feed_name)`
7. `list_segments(feed_name, status=None)`
8. `suggested_reader_range(feed_name)`
9. `common_time_windows(feed_names, ...)`
10. `recommended_train_validation(feed_names, ...)`
11. `close()`

Remote write-side methods should fail loudly until supported.

Local-only code should not need to branch by transport type.

Read-only CLIs must use this same platform contract. `deepwater-feeds`, `deepwater-segments`, and `deepwater-datasets` should accept local paths and remote paths with identical flags. Mutating/admin CLIs stay local-only until explicit remote operations exist in the protocol.

## Local vs Remote Semantics

### Same Machine (Local Path)

Path form:
- `dw.reader("/abs/or/rel/path", stream="feed")`

Behavior:
1. No network hop.
2. No framing overhead.
3. Lowest latency and minimal CPU overhead.
4. Failure domain is process/filesystem only.
5. `range_batches` implemented as local chunked iteration wrapper.
6. `checkpoint()` may return `None` or local sequence metadata.

### Remote Machine (Tailnet Path)

Path forms:
- `host:/absolute/path`
- `dw://host:7447/absolute/path`

Behavior:
1. Network hop through TCP transport.
2. Framed control/data protocol.
3. Server-side reader session state.
4. Explicit backpressure via batch pull/credit.
5. Resume token support for stream continuity after reconnect.
6. Protocol errors mapped to typed client exceptions.

## Protocol v1

## Handshake and Negotiation

Client opens connection and sends:
1. protocol major/minor
2. requested capabilities (`paged_range`, `resume_stream`, `auth_mode`)
3. client metadata (`app`, `instance`, optional trace id)

Server responds with:
1. selected protocol version
2. enabled capabilities
3. limits (`max_frame_bytes`, `max_batch_records`, `idle_timeout_s`)

If unsupported, server returns deterministic incompatibility error.

## Reader Session Lifecycle

1. `OPEN_READER(path, feed_name, options)`
2. `READER_OPEN(layout, lifecycle, session_id)`
3. `READ_RANGE_PAGE` loop or `SUBSCRIBE_LIVE`
4. optional `CHECKPOINT` exchange
5. `CLOSE_READER`

A connection can host multiple reader sessions sequentially; multiplexing support is optional but protocol-ready.

## Data Transport

1. Metadata remains compact structured header.
2. Record payload stays binary raw Deepwater bytes.
3. No JSON row encoding.
4. Data pages contain:
   - `session_id`
   - `page_id`
   - `record_count`
   - `start_ts` / `end_ts`
   - `resume_token` (for stream pages)

## Range Reads in v1

Two modes:
1. Compatibility mode: `range(...)` returns full result (same as v0 semantics).
2. Streaming mode: `range_batches(...)` pages from server and yields incremental batches.

Server enforces `max_batch_records` and `max_frame_bytes`; client auto-continues until `RANGE_END`.

## Live Streams in v1

1. Stream emits data pages plus periodic heartbeat/control frames. Live-only heartbeat frames are implemented; resume tokens are not.
2. Each stream page carries a monotonic resume token.
3. Client may reconnect with `RESUME_STREAM(token=...)`.
4. Server replies with:
   - resumed stream,
   - or `RESUME_TOO_OLD` (client falls back to bounded catchup/range).

## Error Model

Introduce typed remote errors with stable codes:
1. `AUTH_FAILED`
2. `PATH_REJECTED`
3. `FEED_NOT_FOUND`
4. `SESSION_EXPIRED`
5. `RESUME_TOO_OLD`
6. `LIMIT_EXCEEDED`
7. `PROTOCOL_MISMATCH`

Client maps these to specific exception subclasses.

## Security Model (v1)

v1 keeps a transport-agnostic TCP protocol and treats Tailscale as the recommended deployment substrate for trusted private networks.

When deployed on a tailnet:
1. MagicDNS supplies the stable hostname (`deepwater-pioneer`).
2. Tailscale/WireGuard supplies peer authentication and encrypted transport.
3. Tailscale ACLs and host firewalls decide which clients may reach port `7447`.

Deepwater should not call Tailscale APIs, create ACLs, manage routes, or infer security policy from the host environment.

Deepwater-level controls still matter for defense in depth:

1. Mandatory path root guard (already present).
2. Optional pre-shared token or signed capability token per connection.
3. Optional feed allowlist/denylist by agent config.
4. Structured audit logs (`who`, `what path`, `feed`, `session`, `result`).

Default mode remains operable on trusted private networks, but production agents should be bound only on an operator-approved interface and protected by network policy.

## Backpressure and Resource Controls

Agent controls:
1. max concurrent sessions
2. max sessions per client identity
3. max frame bytes
4. max records per page
5. stream idle timeout
6. per-session output buffer limit

Client controls:
1. requested page size
2. read timeout
3. heartbeat timeout
4. resume policy (`none|auto|required`)

## Observability

Standard event fields for both local and remote:
1. `event`
2. `mode=local|remote`
3. `host`
4. `path`
5. `feed`
6. `session_id`
7. `records`
8. `bytes`
9. `latency_ms`
10. `error_code`

Recommended metrics:
1. range pages/sec
2. stream records/sec
3. reconnect count
4. resume success rate
5. mean page latency
6. p95/p99 frame sizes

## Compatibility and Migration

1. Keep v0 API calls working.
2. Add v1 features behind capability negotiation.
3. Fallback behavior:
   - v1 client -> v0 server: use v0 methods, disable resume/paging.
   - v0 client -> v1 server: v1 server accepts v0 core ops.

No forced rewrite for existing scripts.

## Rollout Plan

### Phase 1: Protocol Foundations

1. Add version handshake. (implemented)
2. Add typed error codes.
3. Add server limits advertisement. (implemented)
4. Add compatibility shim for v0 ops.

### Phase 2: Paged Range

1. Add `range_batches` client API. (implemented)
2. Add `READ_RANGE_PAGE` protocol ops. (implemented)
3. Add continuation cursor and `RANGE_END` frame. (`RANGE_END` implemented; continuation cursor remains open)
4. Add tests for very large ranges without OOM.

### Phase 3: Resumable Stream

1. Add stream page resume token generation.
2. Add `RESUME_STREAM` op.
3. Add reconnect/replay tests with forced disconnect.
4. Add resume expiration policy docs.

### Phase 4: Security and Policy Controls

1. Add optional token auth.
2. Add feed allowlist/denylist configuration.
3. Add audit log schema and tests.

## Testing Strategy

1. Deterministic loopback unit tests.
2. Fault-injection tests:
   - mid-frame drop
   - reconnect after N records
   - stale resume token
3. Load tests:
   - large range pagination
   - many parallel stream consumers
4. Parity tests:
   - local vs remote output equality for same feed windows.

## Practical Usage Guidance

Use local reader when:
1. compute and data are colocated.
2. you need minimum latency and max throughput.

Use remote reader when:
1. data machine must remain centralized.
2. analysts/services on other nodes need read access.
3. operational boundary requires read-only access.

## Open Decisions

1. Should stream resume token be time-based, seq-based, or hybrid?
2. Do we require per-feed ACL at v1 launch or make it v1.1?
3. Should `range(...)` remain eager list by default for remote, or become adaptive with size thresholds?
4. Should multiplexing multiple feed sessions over one socket be first-class in v1 or deferred?

## Summary

v1 should preserve Deepwater’s core contract (binary records, local-first performance) while making remote reading robust for distributed systems: paged historical reads, resumable live streams, typed failures, and explicit capability negotiation.
