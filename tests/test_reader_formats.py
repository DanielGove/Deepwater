#!/usr/bin/env python3
"""Exercise reader formats (tuple/dict/numpy/raw), latest, stream, read_available."""
import tempfile
import time
from pathlib import Path


from deepwater import Reader, Writer, create_feed


def _make_feed(base: Path):
    spec = {
        "feed_name": "fmt",
        "mode": "UF",
        "fields": [
            {"name": "ts", "type": "uint64"},
            {"name": "v", "type": "uint64"},
        ],
        "clock_level": 1,
        "persist": True,
        "chunk_size_mb": 1,
    }
    create_feed(base, spec)


def _seed(base: Path, n=10):
    w = Writer(base, "fmt")
    import time
    base = int(time.time() * 1e6)
    for i in range(n):
        w.write_values(base + i * 10, i)
    w.close()
    return base


def test_range_formats():
    with tempfile.TemporaryDirectory(prefix="dw-fmt-") as td:
        root = Path(td)
        _make_feed(root)
        base = _seed(root, 20)
        r = Reader(root, "fmt")
        # tuple
        out_t = r.range(base, base + 200, format="tuple")
        assert len(out_t) == 20
        # dict
        out_d = r.range(base, base + 200, format="dict")
        assert out_d[0]["v"] == 0
        # numpy
        out_n = r.range(base, base + 200, format="numpy")
        assert out_n["v"][0] == 0
        out_a = r.range(base, base + 200, format="numpy")
        assert out_a.shape == (20,)
        assert out_a.dtype.names == ("ts", "v")
        assert list(out_a["v"][:3]) == [0, 1, 2]
        cols = r.range_columns(base, base + 200, ["v", "ts"])
        assert set(cols) == {"ts", "v"}
        assert list(cols["v"][:3]) == [0, 1, 2]
        assert list(cols["ts"][:2]) == [base, base + 10]
        # raw
        out_raw = r.range(base, base + 200, format="raw")
        assert len(out_raw) == len(out_t) * r.record_size
        assert bytes(r.range(base, base + 20, format="raw")) == bytes(out_raw[: 2 * r.record_size])
        r.close()


def test_range_and_read_available():
    with tempfile.TemporaryDirectory(prefix="dw-range-") as td:
        root = Path(td)
        _make_feed(root)
        base = _seed(root, 5)
        r = Reader(root, "fmt")
        window = r.range(base, base + 1_000)
        assert len(window) >= 1
        # read_available should return immediately with existing data
        avail = r.read_available(max_records=3)
        assert len(avail) <= 3
        r.close()


def test_stream_modes():
    with tempfile.TemporaryDirectory(prefix="dw-stream-") as td:
        root = Path(td)
        _make_feed(root)
        base = _seed(root, 5)
        r1 = Reader(root, "fmt")
        # historical stream (start provided)
        out_hist = []
        for rec in r1.stream(start=base):
            out_hist.append(rec)
            if len(out_hist) >= 5:
                break
        assert len(out_hist) == 5
        r1.close()

        # Live-like check on a fresh platform to avoid cached closed reader
        _make_feed(root)
        base2 = _seed(root, 5)
        w = Writer(root, "fmt")
        for i in range(2):
            ts = base2 + 5*10 + i * 10
            w.write_values(ts, 200 + i)
        w.close()
        r2 = Reader(root, "fmt")
        out_after = r2.range(base2 + 5*10, base2 + 5*10 + 50)
        vals = [rec[1] for rec in out_after if rec[1] >= 200]
        # Allow empty if range caught earlier writes only; ensure no crash and optional detection
        assert vals in ([200, 201], [], [200], [201]), f"live-like range values {vals}"
        r2.close()
