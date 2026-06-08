import sys
import argparse
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
log = logging.getLogger("dw.orderbook")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from deepwater import Reader
from deepwater.metadata.discovery import list_feeds
from deepwater.utils.timestamps import us_to_iso


def _field_names(reader) -> list[str]:
    """Non-padding field names in order."""
    fmt = reader.record_format
    return [f["name"] for f in fmt["fields"] if f.get("name") != "_"]


def main():
    parser = argparse.ArgumentParser(description="Tail an L2 feed and log book updates.")
    parser.add_argument("--feed", default="CB-L2-XRP-USD", help="Feed name to read")
    parser.add_argument("--base-path", default="data/coinbase-test", help="Deepwater base path")
    args = parser.parse_args()

    print(list_feeds(args.base_path))
    reader = Reader(args.base_path, args.feed)
    fields = _field_names(reader)

    try:
        stream = reader.stream()

        for rec in stream:
            rec_map = dict(zip(fields, rec))
            ev = rec_map.get("ev_us") or rec_map.get("proc_us") or rec_map.get("ts_us")
            side = rec_map.get("side", b"?")
            price = rec_map.get("price")
            qty = rec_map.get("qty")
            log.info("side=%s ev=%s price=%.8f qty=%.8f",
                     side.decode("ascii") if isinstance(side, (bytes, bytearray)) else side,
                     us_to_iso(ev) if ev else "?", price or 0.0, qty or 0.0)
    except KeyboardInterrupt:
        log.info("Stopped by user.")
    except Exception as e:
        log.error("Error reading records: %s", e, exc_info=True)
    finally:
        reader.close()


if __name__ == "__main__":
    main()
