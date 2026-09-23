from __future__ import annotations

import struct
import pytest

from deepwater import Reader, Writer, create_feed
from deepwater.metadata.admin import BlobSidecarSpec, FeedSpec, FieldSpec


def test_custom_index_values_and_failed_publication_recover(base_path):
    create_feed(base_path, FeedSpec(
        feed_name="custom_blob", fields=(FieldSpec("ts", "uint64"), FieldSpec("block", "uint64")),
        clock_level=2, persist=False, uses_ring=True, ring_size_mb=.0625,
        sidecars=(BlobSidecarSpec("payload", chunk_size_mb=.01),),
    ))
    with Writer(base_path, "custom_blob") as writer, Reader(base_path, "custom_blob") as reader:
        for values in ({"payload_size": 10}, {"absent": 1}, {"ts": 1}):
            with pytest.raises(ValueError, match="custom blob index"):
                writer.write_blob(100, b"unpublished", sidecar="payload", index_values=values)
        with pytest.raises((struct.error, OverflowError)):
            writer.write_blob(-1, b"unpublished", sidecar="payload")
        with pytest.raises(ValueError):
            writer.write_blob(100, b"unpublished", sidecar="payload", schema_id="invalid")
        assert int(writer.ring.record_count[0]) == 0
        writer.write_blob(100, b"complete", sidecar="payload", index_values={"block": 42})
        row = reader.range(100, 101, format="dict")[0]
        assert row["block"] == 42
        assert reader.blob(row, sidecar="payload", memory_only=True) == b"complete"


def test_old_sidecar_metadata_loads_with_default_ring_capacity(tmp_path):
    import msgspec
    from deepwater.io.blob_sidecar import ring_capacity
    from deepwater.metadata.sidecar import load_sidecars

    directory = tmp_path / "data" / "legacy"
    directory.mkdir(parents=True)
    fields = [f"payload_{name}" for name in ("chunk_id", "offset", "size", "codec", "schema_id", "flags", "crc")]
    legacy = [1, [["payload", "raw", 0.0625, 65536, "follows_parent", "blob_chunks", fields]]]
    (directory / "sidecars.msgpack").write_bytes(msgspec.msgpack.encode(legacy))
    sidecar = load_sidecars(tmp_path, "legacy")["payload"]
    assert sidecar.ring_size_bytes == 0
    assert ring_capacity(sidecar) == 131072


def test_closed_blob_writer_does_not_create_payload_files(base_path):
    create_feed(base_path, FeedSpec(
        feed_name="closed_blob", fields=(FieldSpec("ts", "uint64"),),
        clock_level=1, persist=False, uses_ring=True, ring_size_mb=.0625,
        sidecars=(BlobSidecarSpec("payload", chunk_size_mb=.01),),
    ))
    writer = Writer(base_path, "closed_blob")
    writer.close()
    with pytest.raises(RuntimeError, match="closed"):
        writer.write_blob(100, b"must not reach disk", sidecar="payload")
    assert not list((base_path / "data" / "closed_blob" / "blobs").rglob("*.bin"))
