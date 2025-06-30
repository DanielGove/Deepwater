# Deepwater Story 2: Reader Lifecycle

## Overview

This story describes how a reader process discovers a feed, accesses its registry, attaches to live or historical chunks, and reads data efficiently without interfering with the writer. The system ensures readers can always find valid data in memory or on disk, even if the writer is offline.

---

## Components Used

| Component           | Location                         | Description                                                                  |
| ------------------- | -------------------------------- | ---------------------------------------------------------------------------- |
| **Global Registry** | `registry.bin` (disk) + shm mmap | Stores all feed names, paths, and settings (e.g., persistable, live window). |
| **Feed Registry**   | `feed_registry.bin` per feed     | Tracks chunk metadata and index info for a specific feed.                    |
| **Chunk Data**      | `/dev/shm/` or `.data` file      | Live or persisted chunk containing raw binary event data.                    |
| **Chunk Index**     | `/dev/shm/` or `.idx` file       | Metadata/index info about a specific chunk.                                  |
| **Locks**           | None required for readers        | Readers only attach and read; system ensures writer isolation.               |

---

## 🔄 Story Flow

### 1. Reader Process Starts

- Attaches to the **global registry** in shared memory.
- If shm does not exist, loads `registry.bin` from disk and maps it into memory.
- Searches for the desired feed (e.g., `coinbase-BTC-USD`).
- If feed is not found, exits with an error.

---

### 2. Load Feed Registry

- Loads or attaches to the **feed registry** for the selected feed.
  - If shm exists: attaches directly.
  - If not: loads `feed_registry.bin` from disk and memory-maps it.
- Reader now has access to all known chunks for this feed, including metadata and order.

---

### 3. Determine Read Target

- The feed registry contains an **ordered list** of chunks.
- For each chunk, metadata includes:
  - Location (shm name or file path)
  - Time range or sequence number range
  - Validity/version stamp
- Reader uses its own policy to select a chunk:
  - Read latest live chunk
  - Scan historical data range
  - Seek to timestamp or offset

---

### 4. Attach to Chunk and Read

- Reader attaches to the chunk via:
  - `SharedMemory(name=...)` if it's live
  - `mmap.open(filename)` if it's on disk
- It then reads:
  - **Chunk index** to determine offsets, timestamps, or schema
  - **Chunk data** based on those offsets
- Reader may maintain its own internal cursor or checkpointing.

---

### 5. Handle Chunk Expiration or Rotation

- If a live chunk is unlinked while being read:
  - Reader continues reading as long as it holds an open reference.
  - Chunk disappears from namespace but remains accessible until close.
- If switching to a new chunk:
  - Detach from the old one.
  - Re-attach to the next based on the feed registry.

---

### 6. Long-Term Looping Behavior

- Reader periodically refreshes the feed registry to detect new chunks.
- It may sleep or block between reads.
- Reader decides whether to:
  - Stay in sync with live data
  - Rewind to earlier point
  - Exit when feed goes inactive

---

### 7. Zero-Copy Data Awareness

- The feed registry in shared memory exposes lightweight methods for querying live state:
  - `get_latest_chunk()` → returns shm name + metadata
  - `has_data_since(ts)` → used to quickly verify if desired data exists
- These methods allow readers to:
  - Avoid polling or locking
  - Attach directly to live chunks with zero latency
  - Be abstracted through a thin `Reader` class API that handles registry attachment, cursor tracking, and chunk transitions

---

## 🔒 Locking Summary

| Resource         | Lock Type | Notes                                                        |
| ---------------- | --------- | ------------------------------------------------------------ |
| Global Registry  | None      | Read-only access. Safe.                                      |
| Feed Registry    | None      | Read-only access. Feed writers are append-only.              |
| Chunk Index/Data | None      | One writer, many readers. Readers do not block or interfere. |

---

## 🧬 Key Principles

- Readers must be able to discover and attach to feeds using only the global registry.
- All registry structures are memory-mapped and append-only, enabling fast reader access.
- Readers do not hold locks or block writers.
- Shared memory segments are reference-counted: unlinking is safe while attached.
- Disk is the fallback — never missing data even if no writer is live.
- Registry utility methods allow readers to efficiently track available data without scanning or polling.

---

## ⏹ Outcome

- Reader successfully streams from latest or historical data.
- Operates safely even during chunk rotations or writer restarts.
- Can be used for live analytics, backtesting, monitoring, or feature extraction.

---