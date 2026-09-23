# Complete-record latency probe

Run from the Deepwater checkout using its existing Python environment:

```sh
.venv/bin/python benchmarks/live_latency.py --output benchmarks/results/before.json
# After an engine change, repeat on the same host with the same workload:
.venv/bin/python benchmarks/live_latency.py --compare benchmarks/results/before.json --output benchmarks/results/after.json
```

The probe creates and deletes its own temporary feeds. It does not attach to
existing feeds or change capture settings. Producer, consumer and persistent-ring
persister run in separate processes. Both cases use persistent ring feeds and
deliver identical payload contents: one inline in fixed records, one in blob
sidecars. This comparison does not prescribe padding arbitrary blobs into
fixed-width records.

Defaults: 100 warmup records, 1,000 measured records, a fixed schedule of 1,000
records/second, 256 payload bytes, and a requested 64 KiB index ring.
The actual ring is rounded for page/record alignment. No per-record reader acknowledgement gates
publication. Each payload is checked for identity, order, length and content.

| Metric | Boundaries |
| --- | --- |
| `write_call` | Public write method invocation through return. |
| `delivery` | Producer starts building the record through another process obtaining the entire payload. |
| `scheduled_delivery` | Intended arrival time through full-payload receipt; includes writer backlog. |
| `producer_lateness` | Intended arrival time through producer actually starting that record. |

All clocks use same-host `perf_counter_ns`. The consumer polls
`read_available(max_records=64)`, yielding with `sleep(0)` when idle. Blob timing
includes `Reader.blob`. Validation occurs after the receipt timestamp; its CPU
work can affect later records. Warmed p50/p95/p99/max/mean are reported separately
from the first record and writer opening. The final index drain is also separate:
the new engine watermark covers payload plus index durability. Baselines from
the old engine only acknowledged the index, a different durability guarantee.
The report includes the durable index count before that final drain, showing
whether persistence progressed during the workload. Small smoke workloads may
not reach a drain threshold. The default small ring exercises wrap and draining;
`--ring-kib 2048` provides a larger-buffer profile. Nonzero loss/overrun counters
invalidate a run even if the live reader received every payload.

Generated reports under `benchmarks/results/` are kept locally and ignored by Git.
JSON includes raw samples, code and probe fingerprints, loaded native-module
hashes, Python/kernel versions, CPU affinity, load average and filesystem identity.
Comparison reports p99 changes and refuses differing workloads, probe versions,
hosts, Python/kernel versions, affinities or filesystems. No automatic percentage
gate is imposed: a short run is sensitive to scheduling and background work.
Repeat suspicious differences under matched conditions before calling them a
regression. This is an iteration probe, not a live operations dashboard,
universal throughput benchmark or power-loss test.

Optional controls:

```sh
# Select two CPUs allowed by the current affinity.
.venv/bin/python benchmarks/live_latency.py --writer-cpu 2 --reader-cpu 3 --output /tmp/dw-pinned.json
# Select an existing scratch directory on the filesystem to measure.
.venv/bin/python benchmarks/live_latency.py --storage-root /path/to/scratch --output /tmp/dw-storage.json
```

Default storage is Python's temporary directory. If that is tmpfs, blob timing
covers filesystem/API overhead without physical disk. Disk-backed runs still
benefit from the OS page cache. The probe does not force cold-cache reads or
inject stalls; never describe temporary-storage measurements as an HDD stress
test. The two cases run sequentially; matched configuration does not guarantee
identical background load.

Limits per case: 32 MiB of logical payload, 40 MiB of physical index-ring memory,
30 seconds of scheduled input and a 55-second watchdog. Cleanup targets only
the temporary feed's shared-memory names; there is no global `dw_*` sweep.
No engine storage formats or production services are changed by the probe.
