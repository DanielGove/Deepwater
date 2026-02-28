import struct
import tempfile
from pathlib import Path

from deepwater import Platform
from deepwater.metadata.feed_registry import FeedRegistry


def _make_feed(tmpdir: Path, name: str = "test_bounds"):
    p = Platform(tmpdir)
    p.create_feed({
        "feed_name": name,
        "mode": "UF",
        "fields": [
            {"name": "trade_id", "type": "uint64"},
            {"name": "recv_us", "type": "uint64"},
            {"name": "proc_us", "type": "uint64"},  # primary ts
            {"name": "ev_us", "type": "uint64"},
            {"name": "price", "type": "float64"},
        ],
        "clock_level": 3,
        "persist": True,
        "index_playback": False,
    })
    return p


def _records(base: int = 1_000_000):
    # trade_id, recv_us, proc_us, ev_us, price
    return [
        (1, base + 10, base + 20, base + 0, 101.0),
        (2, base + 110, base + 120, base + 100, 102.0),
        (3, base + 210, base + 220, base + 200, 103.0),
    ]


def test_chunk_metadata_bounds_per_key():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "data"
        p = _make_feed(root)
        w = p.create_writer("test_bounds")
        for rec in _records():
            w.write_values(*rec)
        w.close()

        reg_path = root / "test_bounds" / "test_bounds.reg"
        reg = FeedRegistry(str(reg_path), mode="r")
        meta = reg.get_latest_chunk()

        assert meta.clock_level == 3
        # primary proc_us
        assert meta.get_qmin(0) == 1_000_020
        assert meta.get_qmax(0) == 1_000_220
        # recv_us
        assert meta.get_qmin(1) == 1_000_010
        assert meta.get_qmax(1) == 1_000_210
        # ev_us (earliest)
        assert meta.get_qmin(2) == 1_000_000
        assert meta.get_qmax(2) == 1_000_200

        meta.release()
        reg.close()
        p.close()


def test_reader_range_uses_per_key_bounds():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "data"
        p = _make_feed(root)
        recs = _records()

        # write_values path
        w = p.create_writer("test_bounds")
        for rec in recs:
            w.write_values(*rec)
        w.close()

        # write path
        w = p.create_writer("test_bounds")
        for rec in recs:
            ts = rec[2]
            buf = struct.pack("<QQQQd", *rec)
            w.write(ts, buf)
        w.close()

        # write_batch_bytes path
        w = p.create_writer("test_bounds")
        batch = b"".join(struct.pack("<QQQQd", *rec) for rec in recs)
        w.write_batch_bytes(batch)
        w.close()

        r = p.create_reader("test_bounds")
        # Query on earliest timestamp; should include all rows
        ev_start = recs[0][3]  # ev_us of first
        ev_end = recs[-1][3] + 1
        out = r.range(ev_start, ev_end, ts_key="ev_us")
        assert len(out) == len(recs)

        # Query on recv_us; middle window should include last two
        recv_start = recs[1][1]
        recv_end = recs[-1][1] + 1
        out_recv = r.range(recv_start, recv_end, ts_key="recv_us")
        assert len(out_recv) == 2

        r.close()
        p.close()
