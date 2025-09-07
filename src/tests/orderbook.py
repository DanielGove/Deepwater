import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
log = logging.getLogger("dw.ws")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from core.platform import Platform

if __name__ == "__main__":
    dw_platform = Platform(base_path="/deepwater/data/coinbase-test")
    feeds = dw_platform.list_feeds()
    reader = dw_platform.create_reader(feed_name=feeds[0])
    record = reader.get_latest_record()
    print(record)