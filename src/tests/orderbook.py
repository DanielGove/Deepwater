import sys
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime, time as dtime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))
log = logging.getLogger("dw.orderbook")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from deepwater.platform import Platform
from deepwater.feed_registry import FeedRegistry
from deepwater.index import ChunkIndex
from deepwater.utils.timestamps import us_to_iso


def _index_summary(base_path: Path, feed_name: str):
    """Log available index files with counts and latest timestamp."""
    idx_dir = base_path / "data" / feed_name
    found = 0
    for path in sorted(idx_dir.glob("chunk_*.idx")):
        try:
            idx = ChunkIndex.open_file(str(path))
            try:
                count = idx.count
                latest_ts = None
                if count > 0:
                    rec = idx.get_latest_index()
                    latest_ts = rec.timestamp
                    rec.release()
                    log.info("idx %s -> count=%s latest_ts=%s", path.name, count,
                             us_to_iso(latest_ts) if latest_ts else None)
                    found += 1
            finally:
                idx.close_file()
        except Exception as e:
            log.warning("could not read %s: %s", path, e)
    if found == 0:
        log.info("No index files with entries found for %s", feed_name)


def _latest_snapshot_marker(base_path: Path, feed_name: str):
    """Return (timestamp_us, chunk_id) for the latest index entry if present."""
    reg_path = base_path / "data" / feed_name / f"{feed_name}.reg"
    reg = FeedRegistry(str(reg_path), mode="r")
    try:
        latest = reg.get_latest_chunk_idx()
        if latest is None:
            return None, None
        cid = latest
        while cid > 0:
            idx_path = base_path / "data" / feed_name / f"chunk_{cid:08d}.idx"
            if idx_path.exists():
                idx = ChunkIndex.open_file(str(idx_path))
                try:
                    rec = idx.get_latest_index()
                    if rec:
                        ts = rec.timestamp
                        rec.release()
                        return ts, cid
                finally:
                    idx.close_file()
            cid -= 1
    finally:
        reg.close()
    return None, None


def _field_names(reader) -> list[str]:
    """Non-padding field names in order."""
    fmt = reader.record_format
    return [f["name"] for f in fmt["fields"] if f.get("name") != "_"]


def main():
    parser = argparse.ArgumentParser(description="Tail from latest snapshot marker (index) to present.")
    parser.add_argument("--feed", default="CB-L2-XRP-USD", help="Feed name to read")
    parser.add_argument("--base-path", default="data/coinbase-test", help="Deepwater base path")
    parser.add_argument("--inspect-only", action="store_true", help="Only show index summary and exit")
    parser.add_argument("--end", help="Optional end time HH:MM[:SS] UTC; if set, replay from snapshot to this time and stop")
    args = parser.parse_args()

    base_path = Path(args.base_path)
    _index_summary(base_path, args.feed)

    if args.inspect_only:
        return

    platform = Platform(base_path=args.base_path)
    print(platform.list_feeds())
    reader = platform.create_reader(feed_name=args.feed)
    fields = _field_names(reader)

    try:
        snap_ts, snap_chunk = _latest_snapshot_marker(base_path, args.feed)
        now_us = int(time.time() * 1_000_000)
        end_ts = None
        if args.end:
            today = datetime.now(timezone.utc).date()
            parts = args.end.split(":")
            if len(parts) not in (2, 3):
                raise ValueError("--end must be HH:MM or HH:MM:SS")
            hh, mm = int(parts[0]), int(parts[1])
            ss = int(parts[2]) if len(parts) == 3 else 0
            end_dt = datetime.combine(today, dtime(hour=hh, minute=mm, second=ss), tzinfo=timezone.utc)
            end_ts = int(end_dt.timestamp() * 1_000_000)

        if snap_ts:
            age_s = max(0, now_us - snap_ts) / 1_000_000
            log.info("Latest index entry: %s (chunk %s), age %.2fs",
                     us_to_iso(snap_ts), snap_chunk, age_s)
            if end_ts:
                log.info("Replaying from snapshot to %s", us_to_iso(end_ts))
                stream = reader.stream_time_range(snap_ts, end_ts)
            else:
                log.info("Replay + tail from snapshot (playback=True)")
                stream = reader.stream_latest_records(playback=True)
        else:
            log.info("No index entries found; tailing live only.")
            stream = reader.read(playback=False)

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
    except FileNotFoundError as e:
        log.error("Chunk missing during playback: %s", e)
    except Exception as e:
        log.error("Error reading records: %s", e, exc_info=True)
    finally:
        reader.close()
        platform.close()


if __name__ == "__main__":
    main()
