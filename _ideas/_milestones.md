# 🛠️ Current Milestones

## 🟢 Real-Time Ingest Online
- [x] Split deepwater_core.py into modules
- [x] Set up project layout with __init__.py
- [ ] Imports working across modules
- [ ] Coinbase websocket pulling live
  - [ ] Independant platform component
- [ ] CrashResilientWriter accepts Coinbase trades
- [ ] Data persisted to spinning metal path

## 🔵 Reader + Playback MVP
- [ ] Build DeepwaterReader for one feed
- [ ] Query by timestamp range
- [ ] Group results in 50ms buckets

## 🧪 Manual QA Tools
- [ ] CLI script to replay messages
- [ ] CLI: show last 10 messages from chunk
- [ ] Decoder for binary records

## 🗂️ Structure & Stability
- [x] Markdown task system working
- [ ] Archive task cleanup system in place
- [ ] Logbook auto-template/shortcut