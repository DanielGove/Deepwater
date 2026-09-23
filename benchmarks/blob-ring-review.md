# Blob ring review measurements — 2026-09-22

The steady-state payload-file I/O dependency is removed and tested. Warm publish
CPU cost increased. These measurements support review of that tradeoff; they do
not establish an HDD performance result or production latency improvement.

Three matched pairs compare unchanged `e2e643b` with the reviewed source.
Each case uses a separate producer, reader and persister, 100 warmup records,
1,000 measured records at 1,000 records/s, 256-byte payloads, and a requested
64 KiB index ring. The producer and reader were pinned to distinct physical
cores 2 and 3 of an AMD Ryzen 9 5950X. Scratch storage was ext4 on
`/dev/nvme0n1p1`; the machine was not otherwise isolated from background load.
Pairs 2 and 3 alternated baseline/current immediately. The probe validates every
payload's sequence/content; there are no per-record reader acknowledgements.

The table contains the median of the three run-level statistics, in microseconds.

| Measurement | Baseline | Blob ring |
| --- | ---: | ---: |
| Blob write call p50 | 13.68 | 21.17 |
| Blob write call p99 | 30.82 | 43.84 |
| Blob complete delivery p50 | 87.59 | 66.41 |
| Blob complete delivery p99 | 2,216.09 | 1,970.33 |
| Inline write call p50 | 1.77 | 4.05 |
| Inline write call p99 | 7.10 | 11.44 |
| Inline complete delivery p50 | 43.25 | 63.53 |
| Inline complete delivery p99 | 1,896.08 | 996.82 |

Blob delivery p99 ranged from 1,791–2,932 µs before and 448–2,400 µs after.
The large variation prevents a reliable tail-speedup claim from these short
runs. Publish-call overhead increased in every matched pair. The implementation
adds bounded reservation, integrity and publication checks, including index
copy safety for both blob and inline records. Moving more of those operations
into a combined native operation is a future optimization candidate; it has not
been measured here.

All six runs verified every payload and reported `lost_records=0` and
`overrun_count=0`. Each case had persisted 1,024 of 1,100 records before its final
drain and all 1,100 afterward. The new engine's durable watermark covers payloads
and index together; the baseline only synchronized the fixed index.

[Structured summaries](reports/blob-ring-2026-09-22.json) include all distributions,
first-record and startup costs, drain timing, environment details and source /
loaded-native hashes. Raw reports are retained locally under `benchmarks/results/`
and are intentionally ignored by Git. These measurements ran before the CR
commit, so `source_sha256` identifies the changed engine; the reported Git base
is `e2e643b` for both worktrees. Final trailing-blank-line cleanup followed
measurement; executable code was unchanged.

```sh
python benchmarks/live_latency.py --writer-cpu 2 --reader-cpu 3 --output before.json
python benchmarks/live_latency.py --writer-cpu 2 --reader-cpu 3 --compare before.json --output after.json
```

Correctness validation: native extensions rebuilt; full suite **201 passed**.
Failure injection covers payload/index fsync, journal replacement, commit/ack
interruption, corruption, replay after a fresh RAM epoch, and concurrent reuse.
No production services or datasets were changed.
