from __future__ import annotations

import msgspec
def _book_snapshot_spec():
    from deepwater.metadata.admin import BlobSidecarSpec, FeedSpec, FieldSpec

    return FeedSpec(
        feed_name="book_snapshots",
        fields=(FieldSpec("ts", "uint64"),),
        clock_level=1,
        persist=True,
        uses_ring=True,
        chunk_size_mb=1,
        ring_size_mb=2,
        sidecars=(
            BlobSidecarSpec(
                name="snapshot",
                codec="msgpack",
                chunk_size_mb=16,
                retention="follows_parent",
            ),
        ),
    )


def test_create_feed_declares_blob_sidecar_metadata_and_index_schema(base_path):
    from deepwater import create_feed
    from deepwater.metadata.feed_schema import load_record_schema_for_feed
    from deepwater.metadata.sidecar import load_sidecars

    create_feed(base_path, _book_snapshot_spec())

    schema = load_record_schema_for_feed(base_path, "book_snapshots")
    assert schema.field_names == (
        "ts",
        "snapshot_chunk_id",
        "snapshot_offset",
        "snapshot_size",
        "snapshot_codec",
        "snapshot_schema_id",
        "snapshot_flags",
        "snapshot_crc",
    )

    sidecars = load_sidecars(base_path, "book_snapshots")
    assert tuple(sidecars.names) == ("snapshot",)

    snapshot = sidecars["snapshot"]
    assert snapshot.storage == "blob_chunks"
    assert snapshot.codec == "msgpack"
    assert snapshot.retention == "follows_parent"
    assert snapshot.ref_fields.chunk_id == "snapshot_chunk_id"
    assert snapshot.ref_fields.offset == "snapshot_offset"
    assert snapshot.ref_fields.size == "snapshot_size"
    assert snapshot.ref_fields.codec == "snapshot_codec"
    assert snapshot.ref_fields.schema_id == "snapshot_schema_id"
    assert snapshot.ref_fields.flags == "snapshot_flags"
    assert snapshot.ref_fields.crc == "snapshot_crc"

    sidecar_dir = base_path / "data" / "book_snapshots" / "blobs" / "snapshot"
    assert sidecar_dir.exists()


def test_writer_write_blob_appends_payload_and_writes_index_row(base_path):
    from deepwater import Reader, Writer, create_feed

    create_feed(base_path, _book_snapshot_spec())
    payload = msgspec.msgpack.encode({"bids": [[100.0, 2.0]], "asks": [[101.0, 1.5]]})

    with Writer(base_path, "book_snapshots") as writer:
        ref = writer.write_blob(
            1_000,
            payload,
            sidecar="snapshot",
            codec="msgpack",
            schema_id=7,
            flags=3,
        )

    assert ref.sidecar == "snapshot"
    assert ref.chunk_id == 1
    assert ref.offset == 0
    assert ref.size == len(payload)
    assert ref.codec == "msgpack"
    assert ref.schema_id == 7
    assert ref.flags == 3

    with Reader(base_path, "book_snapshots") as reader:
        rows = reader.range(999, 1_001)
        assert len(rows) == 1
        row = rows[0]
        assert row[0] == 1_000
        assert row[1] == ref.chunk_id
        assert row[2] == ref.offset
        assert row[3] == ref.size
        assert row[5] == ref.schema_id
        assert row[6] == ref.flags
        assert reader.blob(row, sidecar="snapshot") == payload


def test_reader_can_batch_payload_fetch_for_range(base_path):
    from deepwater import Reader, Writer, create_feed

    create_feed(base_path, _book_snapshot_spec())

    payloads = [
        msgspec.msgpack.encode({"seq": 1, "bids": [[100.0, 1.0]]}),
        msgspec.msgpack.encode({"seq": 2, "bids": [[100.5, 1.25]]}),
        msgspec.msgpack.encode({"seq": 3, "bids": [[101.0, 1.5]]}),
    ]

    with Writer(base_path, "book_snapshots") as writer:
        for i, payload in enumerate(payloads):
            writer.write_blob(2_000 + i, payload, sidecar="snapshot", codec="msgpack")

    with Reader(base_path, "book_snapshots") as reader:
        rows = reader.range(2_000, 2_003)
        assert [row[0] for row in rows] == [2_000, 2_001, 2_002]
        assert reader.blobs(rows, sidecar="snapshot") == payloads
        assert [payload for _row, payload in reader.range_with_blobs(2_000, 2_003, sidecar="snapshot")] == payloads


def test_blob_write_commit_order_never_exposes_missing_payload(base_path):
    from deepwater import Reader, Writer, create_feed

    create_feed(base_path, _book_snapshot_spec())
    payload = msgspec.msgpack.encode({"book": "complete"})

    writer = Writer(base_path, "book_snapshots")
    try:
        ref = writer.write_blob(3_000, payload, sidecar="snapshot", codec="msgpack")
    finally:
        writer.close()

    with Reader(base_path, "book_snapshots") as reader:
        row = reader.range(3_000, 3_001)[0]
        assert reader.blob_ref(row, sidecar="snapshot") == ref
        assert reader.blob(row, sidecar="snapshot") == payload
