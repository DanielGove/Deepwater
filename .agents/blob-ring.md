# Blob publication and persistence contract

## Storage ownership

A sidecar is typed metadata plus storage handles. Its payloads are bytes, not
Python dictionaries. Ring-backed feeds now own a shadow-mapped shared-memory byte
ring per sidecar. Direct chunk feeds retain their file-backed payload writer.
The adapter choice is made at initialization; callers use `Writer.write_blob`.

`BlobSidecarSpec.ring_size_mb` controls byte capacity independently of index-row
capacity. The default is twice `chunk_size_mb`, rounded to a page. The minimum
is two payload chunks, so one maximum-size payload fits across chunk rollover.
Capacity includes unused chunk-tail bytes. Shadow mapping doubles virtual
address space, not physical backing. Pages are touched as payloads arrive;
normal OS page faults and scheduling still apply.

A blob reference retains the existing on-disk coordinates and fields. Its
logical address is `(chunk_id - 1) * chunk_size_bytes + offset`. The byte ring
uses that logical address modulo capacity. No per-record dictionary is needed
for locating live payloads. CRC32 validates copies from memory and disk.

## Publication

1. The sole producer reserves byte capacity and copies a complete payload.
2. The producer publishes the fixed index row; that row makes the blob visible.
3. The byte reservation is finalized. Recovery resolves an interruption at
   step 2 using the parent's published record count.

Steady-state ring publication and a retained live payload read perform no
payload-file I/O. Opening feeds still reads metadata and may inspect existing
chunk tails. `index_values={...}` supplies additional index fields through the
public API; reserved reference fields and the primary timestamp cannot be
replaced. Upstream code that bypasses the API through `_blob_writers` or `_file`
needs migration before deployment.

Readers copy payloads into owned bytes. Atomic retained-range checks reject
concurrent reuse; failed reservations never make overwritten bytes valid again.
Fixed index writes also have a mutation sequence and retained-record floor;
readers check them around owned copies. An interrupted writer reconstructs
cursors from published record count under its existing exclusive lock.

`Reader.blob(..., memory_only=True)` never falls back to payload files. An expired
or unavailable reference raises `PayloadUnavailable`. Default reads use memory
first and durable storage after eviction. Volatile payload capacity is separate
from index capacity: an old index may remain after its payload expires. A reader
can explicitly retry a busy index snapshot; lagged streams report overwritten
rows instead of returning mixed records.

Persistent writers apply backpressure at either capacity limit. Payload pressure
requests an early index drain, including batches below the normal row threshold.
An arbitrarily stalled disk eventually fills finite RAM; writes then wait for
persistence. This design removes synchronous file operations from publication,
not the need for sufficient capacity and sustained disk throughput.

## Durable commit and recovery

The persister copies a protected index window, validates referenced live bytes,
writes payload files at their stable coordinates, and synchronizes those files
and their directories. It then:

1. Writes and synchronizes a pending batch descriptor in `.commit-journal.msgpack`.
2. Writes the fixed index chunk, synchronizes it, and synchronizes its registry
   and directory. The chunk remains `IN_MEMORY` and hidden from durable reads.
3. Commits the journal through a checksummed atomic replacement and directory fsync.
4. Publishes the index chunk, then advances the RAM durable record count, then
   acknowledges reclaimable payload bytes.

Descriptors use a RAM epoch UUID plus record sequences, index SHA256, index
chunk ID, timestamp, and payload highwaters. Repeated timestamps are valid and
are never used as transaction identity. Recovery holds the registry writer
lock, rolls back incomplete index batches, validates the latest committed index
and referenced payloads, and repeats acknowledgements without writing duplicate
rows. Failed I/O poisons the current persister state; retry reconstructs it.
Unpersisted index loss fails closed and reports loss counters.

The reserved final 16 bytes of each 128-byte chunk-registry entry now hold the
batch's ending ring sequence and a 64-bit tag of its epoch. Readers take one
RAM durable sequence, copy its live tail before disk queries, and clip matching
chunk epochs to that sequence. A newly visible disk chunk cannot also appear in
the live tail. Old/direct chunk entries keep zero metadata and retain their
existing reading behavior.

RAM is not durable across reboot. Fully committed history survives; unpublished
or unacknowledged tails may require upstream replay. Fresh RAM epochs append
after durable blob coordinates. An existing control mapping with missing data,
or a sidecar from a different parent epoch, fails instead of silently recreating
memory under old references. Explicit feed deletion removes both index and
payload shared-memory objects.

## Compatibility and rollout

Record schemas, BlobRef fields, and payload-file layout remain unchanged.
Sidecar metadata adds an optional trailing capacity field with an old-file
default. The index control page uses formerly reserved bytes for an epoch and
copy guards. Chunk-registry entry size remains 128 bytes.

Mixed old/new running engines are unsupported: old producers bypass the new
publication protocol and old readers do not understand pending index commits.
Review and coordinate a stopped-worker upgrade separately. No production
migration or service restart is part of this CR. Existing native extensions must
be rebuilt from this source; stale compiled `reader` modules shadow Python edits.

The latest committed batch is validated on restart, not every historical chunk.
Tests inject exceptions and controlled concurrent reuse; they do not simulate
all hardware power-loss behavior or prove arbitrary storage devices honor fsync.
Retention remains a separate policy; blob chunks still follow existing sidecar
retention conventions and this CR adds no automatic NVMe pruning.

## Validation

Focused tests cover byte wrap/reuse, pending publication, owned reads,
cross-process payload visibility, memory-only I/O guards, capacity pressure,
CRC failures, interrupted data fsync/journal/ack, repeated recovery, corrupted
committed storage, and fresh RAM epochs. Run the full suite after rebuilding
native code. See the CR and benchmark report for measured results; do not infer
HDD latency gains from a warm NVMe scratch run.

Review validation on 2026-09-22: native rebuild and full suite **201 passed**.
The [three-pair latency report](../benchmarks/blob-ring-review.md) records both
improved observed blob delivery and increased warm publish-call overhead.
