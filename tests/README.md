# Deepwater Tests

Deepwater uses `pytest` as the test runner.

Install the dev extra before running the suite:

```bash
pip install -e ".[dev]"
```

```bash
./test.sh
python -m pytest
python -m pytest tests/test_ring_buffer.py
python -m pytest --cov=deepwater
```

`test.sh` activates `.venv` if present; without one, it runs the active `python`.

## Layout

- `test_ring_buffer.py`, `test_reader_*.py`, `test_range_*.py`, `test_writer_direct.py`, `test_persistent_raw.py`, `test_non_blocking.py`: hot I/O primitives, wraparound, live rings, chunk ranges, and raw reads.
- `test_feed_schema.py`, `test_global_registry.py`, `test_feed_registry.py`, `test_metadata_bounds.py`, `test_timestamps.py`: schema, registry ABI, metadata bounds, and timestamp parsing contracts.
- `test_catalog.py`, `test_datasets.py`: durable chunk coverage and research/catalog surfaces.
- `test_health_cleanup.py`, `test_reader_cleanup.py`, `test_delete_feed.py`: ops paths, retention cleanup, deleted chunk behavior, and feed deletion.
- `test_network.py`: loopback network reader/protocol behavior using local sockets only.
- `test_blob_sidecar_api.py`: variable-size blob sidecars indexed by fixed-row feeds.
- `test_*_cli.py`, `test_bootstrap_cli.py`, `test_feeds_cli.py`: command-line boundaries and generated starter docs.

Shared fixtures live in `conftest.py`. Tests should use temp dirs, avoid external services, and leave no `/dev/shm/dw_*` objects behind.

## Standards

- Assert behavior directly; avoid demo-style prints.
- Put slow or environment-sensitive checks behind an explicit `pytest.mark.integration` or `pytest.mark.perf`.
- New hot-path changes need edge coverage for wraparound, chunk boundaries, malformed metadata, and empty inputs.
- Blob support is first-class sidecar storage exercised through `Writer.write_blob()` and `Reader.blob*()`.
- Network tests should remain loopback-only and avoid artificial teardown waits.

For the blob-ring change, rebuild extensions before testing: compiled reader
modules shadow edited Python sources.

```bash
python setup.py build_ext --inplace
python -m pytest -q
```

`test_blob_ring.py`, `test_index_publication.py`, `test_blob_live_path.py`,
`test_blob_reader_frontier.py`, `test_blob_publication_contract.py`,
`test_blob_commit_failures.py`, `test_blob_recovery_integrity.py`, and
`test_commit_journal.py` cover publication, wrap/reuse, durable ordering and
recovery. `test_latency_probe.py` covers measurement accounting and isolation.
Run this suite serially: the temporary shared-memory cleanup fixture assumes
one pytest runner on the host.
