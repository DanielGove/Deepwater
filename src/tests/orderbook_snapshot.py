import sys
import argparse
import logging
import time
from pathlib import Path
from typing import Dict, Tuple
import threading

sys.path.insert(0, str(Path(__file__).parent.parent))
log = logging.getLogger("dw.orderbook.snapshot")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from deepwater.platform import Platform
from deepwater.utils.timestamps import us_to_iso
from websocket_client import l2_spec  # reuse feed spec helper


def _field_names(reader) -> list[str]:
    fmt = reader.record_format
    return [f["name"] for f in fmt["fields"] if f.get("name") != "_"]


def snapshot_spec(pid: str, depth: int, chunk_size_bytes: int = 1_000_000) -> dict:
    """
    Build a ring-only snapshot feed with fixed depth.
    Fields: type, snapshot_us, then bid_px/qty[depth], ask_px/qty[depth].
    """
    fields = [
        {"name": "type", "type": "char"},
        {"name": "_", "type": "_7"},  # pad to 8-byte alignment
        {"name": "snapshot_us", "type": "uint64"},
    ]
    for i in range(depth):
        fields.append({"name": f"bid_px_{i:03d}", "type": "float64"})
        fields.append({"name": f"bid_sz_{i:03d}", "type": "float64"})
    for i in range(depth):
        fields.append({"name": f"ask_px_{i:03d}", "type": "float64"})
        fields.append({"name": f"ask_sz_{i:03d}", "type": "float64"})

    return {
        "feed_name": f"CB-L2SNAP-{pid}",
        "mode": "UF",
        "fields": fields,
        "ts_col": "snapshot_us",
        "chunk_size_bytes": chunk_size_bytes,
        "persist": False,
        "index_playback": False,
    }


def _update_book(book: Dict[float, float], side: bytes, price: float, qty: float) -> None:
    if qty <= 0.0:
        book.pop(price, None)
        return
    book[price] = qty


def _snapshot(depth: int, bids: Dict[float, float], asks: Dict[float, float]) -> Tuple[list, int]:
    snap_ts = int(time.time() * 1_000_000)
    vals = [b"S", snap_ts]

    top_bids = sorted(bids.items(), key=lambda kv: kv[0], reverse=True)[:depth]
    for px, sz in top_bids:
        vals.append(px)
        vals.append(sz)
    vals.extend([0.0, 0.0] * (depth - len(top_bids)))

    top_asks = sorted(asks.items(), key=lambda kv: kv[0])[:depth]
    for px, sz in top_asks:
        vals.append(px)
        vals.append(sz)
    vals.extend([0.0, 0.0] * (depth - len(top_asks)))
    return vals, snap_ts


def main():
    parser = argparse.ArgumentParser(description="Maintain L2 book and emit ring-only snapshots.")
    parser.add_argument("--product", default="BTC-USD", help="Product id (e.g., BTC-USD)")
    parser.add_argument("--base-path", default="data/coinbase-test", help="Deepwater base path")
    parser.add_argument("--depth", type=int, default=500, help="Levels per side in snapshot")
    parser.add_argument("--interval", type=float, default=1.0, help="Snapshot interval seconds")
    args = parser.parse_args()

    pid = args.product.upper()
    platform = Platform(base_path=args.base_path)

    # Ensure L2 feed exists and open reader
    reader = platform.create_reader(feed_name=f"CB-L2-{pid}")

    # Snapshot feed (ring-only)
    snap_spec = snapshot_spec(pid, args.depth)
    platform.create_feed(snap_spec)
    snap_writer = platform.create_writer(snap_spec["feed_name"])

    bids: Dict[float, float] = {}
    asks: Dict[float, float] = {}
    lock = threading.Lock()
    emitted = 0
    stop_evt = threading.Event()
    fields = _field_names(reader)

    def snapshot_loop():
        nonlocal emitted
        interval = args.interval
        next_emit = time.time() + interval
        while not stop_evt.is_set():
            now = time.time()
            if now < next_emit:
                stop_evt.wait(max(0, next_emit - now))
                continue
            with lock:
                vals, snap_ts = _snapshot(args.depth, dict(bids), dict(asks))
            snap_writer.write_values(*vals)
            emitted += 1
            log.info("snapshot %s ts=%s bids=%s asks=%s",
                     emitted, us_to_iso(snap_ts),
                     min(len(bids), args.depth), min(len(asks), args.depth))
            next_emit += interval

    snap_thread = threading.Thread(target=snapshot_loop, name="snapshot", daemon=True)
    snap_thread.start()

    log.info("Maintaining book for %s (depth=%s); snapshots -> %s every %.2fs",
             pid, args.depth, snap_spec["feed_name"], args.interval)
    try:
        for rec in reader.stream_latest_records(playback=True):
            rec_map = dict(zip(fields, rec))
            side = rec_map.get("side")
            price = rec_map.get("price")
            qty = rec_map.get("qty") or rec_map.get("size")
            if side is None or price is None or qty is None:
                continue
            # normalize side to uppercase byte
            if isinstance(side, str):
                side_b = side.encode("ascii")
            elif isinstance(side, (bytes, bytearray, memoryview)):
                side_b = bytes(side[:1])
            else:
                continue
            side_b = side_b.upper()
            with lock:
                target = bids if side_b == b"B" else asks
                _update_book(target, side_b, float(price), float(qty))
    except KeyboardInterrupt:
        log.info("Stopped by user.")
    except Exception as e:
        log.error("Error in maintainer: %s", e, exc_info=True)
    finally:
        log.info("Emitted %s snapshots", emitted)
        stop_evt.set()
        snap_thread.join(timeout=1.0)
        try:
            snap_writer.close()
        except Exception:
            pass
        try:
            reader.close()
        except Exception:
            pass
        platform.close()


if __name__ == "__main__":
    main()
