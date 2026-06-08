#!/usr/bin/env python3
"""Blob-reference feeds are fixed-row feeds plus colocated payload blobs."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater import BlobReferenceWriter, BlobStore, Reader, create_blob_feed, read_blob_ref


def test_blob_reference_feed_roundtrip():
    with tempfile.TemporaryDirectory(prefix="dw-blob-ref-") as td:
        base = Path(td)
        create_blob_feed(base, "snapshots", chunk_size_mb=0.01)

        with BlobReferenceWriter(base, "snapshots") as writer:
            ref_a = writer.write_blob(1_000, b'{"book":"a"}', flags=7)
            ref_b = writer.write_blob(1_010, b'{"book":"b"}')

        store = BlobStore(base, "snapshots")
        assert store.path(ref_a.digest).exists()
        assert store.path(ref_b.digest).exists()
        assert store.read(ref_a.digest) == b'{"book":"a"}'

        with Reader(base, "snapshots") as reader:
            rows = reader.range(1_000, 1_020)
            assert len(rows) == 2
            assert rows[0][0] == 1_000
            assert rows[0][1] == ref_a.hex.encode("ascii")
            assert rows[0][2] == len(b'{"book":"a"}')
            assert rows[0][3] == 7
            assert read_blob_ref(base, "snapshots", rows[0]) == b'{"book":"a"}'

            arr = reader.range(1_000, 1_020, format="numpy")
            assert arr.dtype.names == ("ts", "blob_sha256", "size", "flags")
            assert read_blob_ref(base, "snapshots", arr[1]) == b'{"book":"b"}'


def run_tests():
    tests = [
        ("blob_reference_feed_roundtrip", test_blob_reference_feed_roundtrip),
    ]
    print("Blob Reference Tests")
    print("=" * 60)
    passed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"✅ {name}")
            passed += 1
        except Exception as e:
            print(f"❌ {name} - {e}")
            raise
    print(f"\nPassed: {passed}/{len(tests)}")
    if passed != len(tests):
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
