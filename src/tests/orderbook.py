import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, time, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))
log = logging.getLogger("dw.orderbook")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from deepwater.platform import Platform
from deepwater.utils.timestamps import us_to_iso


def _time_range_us(start_hhmm: str, end_hhmm: str) -> tuple[int, int]:
    """Convert HH:MM strings (UTC) into microsecond epoch bounds for today."""
    today = datetime.now(timezone.utc).date()
    start_dt = datetime.combine(today, time.fromisoformat(start_hhmm), tzinfo=timezone.utc)
    end_dt = datetime.combine(today, time.fromisoformat(end_hhmm), tzinfo=timezone.utc)
    return int(start_dt.timestamp() * 1_000_000), int(end_dt.timestamp() * 1_000_000)


def main():
    parser = argparse.ArgumentParser(description="Query trades in a time window.")
    parser.add_argument("--feed", default="CB-TRADES-MON-USD", help="Feed name to read")
    parser.add_argument("--start", default="19:25", help="Start time HH:MM (UTC)")
    parser.add_argument("--end", default="19:30", help="End time HH:MM (UTC)")
    parser.add_argument("--base-path", default="/deepwater/data/coinbase-test", help="Deepwater base path")
    args = parser.parse_args()

    start_us, end_us = _time_range_us(args.start, args.end)
    log.info("Querying trades from %s to %s UTC (%s to %s microseconds)",
             args.start, args.end, start_us, end_us)

    platform = Platform(base_path=args.base_path)
    reader = platform.create_reader(feed_name=args.feed)

    count = 0
    try:
        for rec in reader.stream_time_range(start_us, end_us):
            # rec fields (trades feed): type, side, trade_id, packet_us, recv_us, proc_us, ev_us, price, size
            log.info("trade_id=%s side=%s recv=%s price=%.8f size=%.8f",
                     rec[2], rec[1].decode("ascii"), us_to_iso(rec[4]), rec[7], rec[8])
            count += 1
    finally:
        reader.close()
        platform.close()

    log.info("Done. %d trades in window.", count)


if __name__ == "__main__":
    main()
