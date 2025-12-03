import sys
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime, time as dtime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))
log = logging.getLogger("dw.trades")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from deepwater.platform import Platform
from deepwater.utils.timestamps import us_to_iso


def _field_names(reader) -> list[str]:
    """Non-padding field names in order."""
    fmt = reader.record_format
    return [f["name"] for f in fmt["fields"] if f.get("name") != "_"]


def _parse_hms(arg: str) -> int:
    """Parse HH:MM[:SS] (UTC today) to microseconds since epoch."""
    parts = arg.split(":")
    if len(parts) not in (2, 3):
        raise ValueError("time must be HH:MM or HH:MM:SS")
    hh, mm = int(parts[0]), int(parts[1])
    ss = int(parts[2]) if len(parts) == 3 else 0
    today = datetime.now(timezone.utc).date()
    dt = datetime.combine(today, dtime(hour=hh, minute=mm, second=ss), tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def main():
    parser = argparse.ArgumentParser(description="Replay trades over a time window (indexed if available).")
    parser.add_argument("--feed", default="CB-TRADES-MON-USD", help="Feed name to read")
    parser.add_argument("--base-path", default="data/coinbase-test", help="Deepwater base path")
    parser.add_argument("--start", help="Window start HH:MM[:SS] UTC (defaults to now-5m)")
    parser.add_argument("--end", help="Window end HH:MM[:SS] UTC (optional; open-ended if unset)")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    start_ts = _parse_hms(args.start) if args.start else int((now - timedelta(minutes=1)).timestamp() * 1_000_000)
    end_ts = _parse_hms(args.end) if args.end else None

    platform = Platform(base_path=args.base_path)
    reader = platform.create_reader(feed_name=args.feed)
    fields = _field_names(reader)

    log.info("Reading %s from %s to %s",
             args.feed,
             us_to_iso(start_ts),
             us_to_iso(end_ts) if end_ts else "live")
    count = 0
    try:
        for rec in reader.stream_time_range(start_ts, end_ts):
            rec_map = dict(zip(fields, rec))
            ts = rec_map.get("ev_us") or rec_map.get("proc_us") or rec_map.get("ts_us")
            side = rec_map.get("side", b"?")
            price = rec_map.get("price")
            size = rec_map.get("size")
            log.info("ts=%s side=%s price=%.8f size=%.8f",
                     us_to_iso(ts) if ts else "?",
                     side.decode("ascii") if isinstance(side, (bytes, bytearray)) else side,
                     price or 0.0, size or 0.0)
            count += 1
    except KeyboardInterrupt:
        log.info("Stopped by user.")
    except Exception as e:
        log.error("Error reading records: %s", e, exc_info=True)
    finally:
        log.info("Read %s records", count)
        reader.close()
        platform.close()


if __name__ == "__main__":
    main()
