# 🧠 Backlog and Ideas

Ideas that are useful but not on the critical path.

### 🚀 Ingest + Storage
- [ ] Coinbase WebSocket client streaming live trades
- [ ] Schema for trade messages (symbol, price, size, side, ts)
- [ ] `CrashResilientWriter` accepts and stores messages
- [ ] Chunk rotation + disk persist working
- [ ] Test: ingest 1000 messages, survive restart, resume write

### 🔍 Reader + Query
- [ ] `DeepwaterReader` for a single feed
- [ ] Read last N messages
- [ ] Read from timestamp range
- [ ] Read from disk if outside SHM
- [ ] Test: read back messages written in ingest test

### 🪟 Playback + Slicing
- [ ] Playback engine: yield messages in 50ms time windows
- [ ] Support max N messages per window
- [ ] Skip gaps in data
- [ ] Validate timestamp ordering

### 🧪 CLI / Dev Tools
- [ ] `deepwater read coinbase.trade --last 5s`
- [ ] `deepwater replay --start T --end T+1s --window 50ms`
- [ ] Print decoded messages to stdout
- [ ] Debug CLI: inspect chunk metadata

### 🔒 Execution Safety Boundary
- [ ] No access to private keys or strategy logic
- [ ] Platform repo cannot place trades
- [ ] All secrets live outside repo (or encrypted vault)
- [ ] Feed explorer interface limited to read-only feeds

## 🧰 Config & Dev Tools
- [ ] Use TOML for feed configuration
- [ ] Auto-generate feed schemas from spec
- [ ] Auto-archive old data after N days
- [ ] Visualizer UI for chunk/index layout

## 💡 Stretch System Features
- [ ] Real-time Grafana export
- [ ] LLM event tagging of playback windows
- [ ] Support for compressed chunk storage

### 🛡️ Access Boundaries

- [ ] Data stored in safe, structured path (e.g. `/mnt/spinning_metal/`)
- [ ] No access to secrets, keys, or strategy


- ## 🔐 Platform Split: Dev vs Prod

We separate Deepwater into two mirrored layers:

### 🟩 Platform Dev (User Space)
- [x] Ingests public market data
- [ ] Lets users create + run custom feeds + strategies
- [ ] Can store and replay feeds locally
- [ ] Strategies run in sandbox with no secret access
- [ ] Cannot read from Platform Prod

### 🟥 Platform Prod (Private Vault)
- [ ] Holds secret keys, real execution logic, sensitive models
- [ ] Can read from Dev to use community/public signals
- [ ] Isolated: only trusted processes can touch it
- [ ] Separate data root (e.g. `/mnt/secure_platform`)
- [ ] Encrypted at rest

### 🔁 Strategy Engine (Bridge Layer)
- [ ] Reads data from both Dev and Prod
- [ ] Runs unified strategy interface
- [ ] Sends trades to executor only if authorized
- [ ] Controlled by you — secrets never leave

### 🧠 Core Rules
- [ ] Dev can never see Prod
- [ ] Prod can see Dev (but logs usage)
- [ ] Execution layer holds final control
- [ ] Platform code stays symmetrical (same API)