# Deepwater storage work

Start with [blob-ring.md](blob-ring.md) for the live/durable blob contract,
recovery ordering, compatibility, tests, and remaining deployment work.

The `codex/blob-ring` change is developed in an isolated checkout. It has not
been deployed to the Hyperliquid node or capture workers. Node files remain the
upstream NVMe spool; Deepwater remains the intended durable replay source.

Run the [complete-record latency probe](../benchmarks/README.md) before and after
storage changes. Correctness tests and latency measurements are separate gates.
Use small temporary feeds; do not restart services, modify production feeds, or
run large allocations/I/O stress as an incidental validation step.
