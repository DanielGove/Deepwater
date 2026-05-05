# Deepwater Networking v0

Deepwater Networking v0 is a Tailscale-first remote reader and agent system. It is intentionally small: a TCP agent runs next to the local Deepwater data directory, and a client on another tailnet machine asks that agent to open a local reader and return records.

## What Works

- Remote `range(start, end)`
- Remote `latest(seconds)`
- Remote `stream(...)`
- Tuple, dict, numpy, and raw formats on the client side
- Local fallback through `dw.reader(local_path, stream="feed")`
- Path guard on the agent so clients cannot open data outside `--root`

## What Does Not Exist in v0

- Remote writes
- Public internet mode
- TLS, auth tokens, or user management
- Tailscale CLI management
- Mullvad, NAT traversal, port forwarding, QUIC, UDP, RDMA
- Reconnect logic beyond simple error surfacing

Writers stay local to the data machine. For now, run ingestion on the server that owns the Deepwater base path, and use the network layer for reads from laptops or other tailnet hosts.

## Mental Model

Local read:

```text
Python process -> Platform("/deepwater/data/node") -> local reader -> chunks/ring
```

Remote read:

```text
laptop Python process
  -> dw.reader("deepwater-pioneer:/deepwater/data/node", stream="feed")
  -> TCP 7447 over Tailscale
  -> deepwater.network.agent on deepwater-pioneer
  -> Platform("/deepwater/data/node")
  -> local reader
  -> raw Deepwater record bytes
  -> client decodes to tuple/dict/numpy/raw
```

The hot ingestion path is not modified. Remote reads use normal Deepwater reader APIs inside the agent.

## Paths

Supported remote target forms:

```text
host:/absolute/base/path
dw://host/absolute/base/path
dw://host:7447/absolute/base/path
```

Examples:

```text
deepwater-pioneer:/deepwater/data/hyperliquid-node
dw://deepwater-pioneer:7447/deepwater/data/hyperliquid-node
```

Plain paths are local:

```text
./data
/deepwater/data/hyperliquid-node
```

## Agent

Run this on the data machine:

```bash
PYTHONPATH=src python -m deepwater.network.agent \
  --root /deepwater/data \
  --bind 0.0.0.0:7447
```

`--root` is the maximum filesystem scope the agent will open. If the client requests a path outside that root, the agent rejects it and logs `DW_REMOTE_AGENT_REJECTED_PATH`.

Binding notes:

- `0.0.0.0:7447` is convenient when host firewall/Tailscale policy limits reachability.
- Binding directly to a Tailscale IP is also fine if you want the process reachable only on that interface.
- Deepwater does not create firewall rules or Tailscale ACLs.

## Client

```python
import deepwater as dw

reader = dw.reader(
    "deepwater-pioneer:/deepwater/data/hyperliquid-node",
    stream="hl.status.events",
)

records = reader.latest(60)
window = reader.range(start_us, end_us, format="dict")

reader.close()
```

Context manager form:

```python
with dw.reader("dw://deepwater-pioneer:7447/deepwater/data/node", stream="trades") as reader:
    data = reader.range(start_us, end_us, format="numpy")
```

Runnable script:

```bash
PYTHONPATH=src python examples/network_remote_read.py \
  deepwater-pioneer:/deepwater/data/hyperliquid-node \
  hl.status.events \
  --seconds 60
```

## Protocol

Frames are length-prefixed:

```text
uint64 frame_body_len
uint32 json_header_len
json header bytes
optional binary payload bytes
```

Control metadata is JSON. Data payloads are binary raw Deepwater record bytes. The client receives the remote feed layout during `OPEN_READER` and decodes raw bytes locally for `tuple`, `dict`, `numpy`, or `raw` output.

Operations:

- `PING`
- `OPEN_READER`
- `READ_RANGE`
- `LATEST`
- `SUBSCRIBE_LIVE`
- `CLOSE`
- `ERROR`
- `DATA`

## Status Events

The client/agent use structured event names in logs and callbacks:

- `DW_REMOTE_LINK_OPEN`
- `DW_REMOTE_LINK_DOWN`
- `DW_REMOTE_READ_ERROR`
- `DW_REMOTE_AGENT_STARTED`
- `DW_REMOTE_AGENT_REJECTED_PATH`

Pass `status_callback=` to `dw.reader(...)` or `RemoteReader(...)` when a process wants to route these into its own telemetry.

## Testing

Network tests are deterministic loopback tests. They do not require Tailscale or an external network:

```bash
PYTHONPATH=src python tests/test_network.py
PYTHONPATH=src python tests/__init__.py
```

The loopback test starts an in-process agent, writes a local ring feed, reads it through `RemoteReader`, and also exercises the public `dw.reader("dw://127.0.0.1:port/path", stream="feed")` helper.
