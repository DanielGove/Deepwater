import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
log = logging.getLogger("dw.ws")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from deepwater.platform import Platform

if __name__ == "__main__":
    dw_platform = Platform(base_path="/deepwater/data/coinbase-test")
    feeds = dw_platform.list_feeds()
    reader = dw_platform.create_reader(feed_name="CB-TRADES-XRP-USD")
    last_record = None
    while True:
        try:
            stream = reader.stream_latest_records()
            for record in stream:
                log.info(record)
                last_record = record
        except Exception as e:
            log.error(f"Error reading latest record: {e}")