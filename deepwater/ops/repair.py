#!/usr/bin/env python3
"""
Deepwater repair utility - validates and repairs corrupted chunk metadata.

Handles power loss and crash scenarios where registry metadata (num_records, 
timestamps, write_pos) doesn't match actual chunk file contents.

Only works with persist=True (disk chunks). Rings (persist=False) don't need repair.

Usage:
    # Repair specific feed
    python -m deepwater.ops.repair --feed BTCUSDT --base-path ./data
    
    # Repair all idle feeds
    python -m deepwater.ops.repair --all --base-path ./data
    
    # Check without fixing (dry run)
    python -m deepwater.ops.repair --all --base-path ./data --dry-run
"""
import argparse
import fcntl
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

# Allow running as standalone script
if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    src_dir = script_dir.parent
    if src_dir.name == "src":
        sys.path.insert(0, str(src_dir))

from deepwater.metadata.feed_registry import FeedRegistry, ON_DISK, IN_MEMORY, EXPIRED, UINT64_MAX
from deepwater.metadata.feed_metadata import list_feeds, load_record_format_dict
from deepwater.metadata.feed_schema import FeedSchema
from deepwater.metadata.segments import SegmentStore, USABLE_SEGMENT_STATUSES

log = logging.getLogger("dw.repair")


class CorruptedFeedError(Exception):
    """Raised when feed integrity check fails."""
    pass


# ============================================================================
# PUBLIC API - Called by Writer on startup validation
# ============================================================================

def validate_and_repair_chunk(
    chunk_id: int,
    meta,
    feed_name: str,
    feed_dir: Path,
    record_format: FeedSchema,
) -> None:
    """
    Validate chunk integrity and auto-repair if corrupted.
    Called by Writer on startup to ensure previous chunk is clean.
    
    Writer only handles persist=True (disk chunks), so this only validates
    ON_DISK chunks. Rings (persist=False) don't need validation.
    
    Handles:
    - Missing chunk files (marks EXPIRED)
    - Corrupted metadata (repairs from actual data)
    """
    # ON_DISK chunk validation
    chunk_path = feed_dir / f"chunk_{chunk_id:08d}.bin"
    
    if not chunk_path.exists():
        # Registry claims file exists but it's gone - mark EXPIRED
        log.warning(f"Chunk {chunk_id} file missing, marking EXPIRED")
        meta.status = EXPIRED
        return
    
    record_size = int(record_format.record_size)
    if record_size <= 0:
        raise ValueError(f"invalid record_size={record_size} for feed '{feed_name}'")

    expected_records = int(meta.num_records)
    write_pos = int(meta.write_pos)
    size_bytes = int(meta.size)
    max_records = max(0, size_bytes // record_size)
    by_write_pos = max(0, write_pos // record_size)
    if by_write_pos > max_records:
        by_write_pos = max_records
    actual_records = max(expected_records, by_write_pos)
    
    qmin0 = meta.get_qmin(0)
    qmax0 = meta.get_qmax(0)
    qbounds_invalid = actual_records > 0 and (qmax0 == 0 or qmin0 == UINT64_MAX or qmin0 > qmax0)
    needs_repair = (actual_records != expected_records) or qbounds_invalid

    if needs_repair:
        if actual_records < expected_records:
            log.warning(
                f"Chunk {chunk_id} corrupted: file has {actual_records} records, "
                f"registry claims {expected_records}. Auto-repairing..."
            )
        elif actual_records > expected_records:
            log.warning(
                f"Chunk {chunk_id}: file ahead of registry "
                f"({actual_records} vs {expected_records}). Reconciling metadata..."
            )
        else:
            log.warning(
                f"Chunk {chunk_id}: invalid q-bounds qmin={qmin0} qmax={qmax0}. "
                "Rebuilding metadata from file..."
            )

        _repair_chunk_from_file(chunk_path, meta, record_format, actual_records=actual_records)
        log.info(f"Chunk {chunk_id} repaired successfully")


def _repair_chunk_from_file(
    chunk_path: Path,
    meta,
    record_format: FeedSchema,
    actual_records: Optional[int] = None,
) -> None:
    """
    Repair chunk metadata by reading actual first/last records from file.
    Updates num_records, write_pos, start_time, end_time atomically.
    """
    record_size = int(record_format.record_size)
    if actual_records is None:
        by_write_pos = int(meta.write_pos) // record_size
        by_num_records = int(meta.num_records)
        by_capacity = int(meta.size) // record_size
        actual_records = max(0, min(max(by_write_pos, by_num_records), by_capacity))
    else:
        by_capacity = int(meta.size) // record_size
        actual_records = max(0, min(int(actual_records), by_capacity))

    write_pos = int(actual_records) * record_size
    
    clock_level = int(record_format.clock_level)
    ts_offsets = [
        int(field.offset)
        for field in record_format.timestamp_fields[: max(1, min(clock_level, 3))]
    ]
    if not ts_offsets:
        ts_offsets.append(0)

    if actual_records == 0:
        meta.num_records = 0
        meta.write_pos = 0
        meta.start_time = 0
        meta.end_time = 0
        meta.clock_level = clock_level
        for i in range(3):
            meta.set_qbounds(i, UINT64_MAX, 0)
        meta.last_update = time.time_ns() // 1_000
        return

    # Read first record for start_time
    with open(chunk_path, 'rb') as f:
        first_record = f.read(record_size)
        start_time_us = int.from_bytes(first_record[ts_offsets[0]:ts_offsets[0]+8], 'little')
        
        # Read last record for end_time
        if actual_records > 1:
            f.seek((actual_records - 1) * record_size)
            last_record = f.read(record_size)
            end_time_us = int.from_bytes(last_record[ts_offsets[0]:ts_offsets[0]+8], 'little')
        else:
            last_record = first_record
            end_time_us = start_time_us
    
    # Update metadata atomically
    meta.num_records = actual_records
    meta.write_pos = write_pos
    meta.start_time = start_time_us
    meta.end_time = end_time_us
    meta.clock_level = clock_level
    for i in range(3):
        if i < len(ts_offsets):
            off = ts_offsets[i]
            qmin = int.from_bytes(first_record[off:off+8], 'little')
            qmax = int.from_bytes(last_record[off:off+8], 'little')
            meta.set_qbounds(i, qmin, qmax)
        else:
            meta.set_qbounds(i, UINT64_MAX, 0)
    meta.last_update = time.time_ns() // 1_000
    
    log.debug(
        f"Repaired chunk {meta.chunk_id}: {actual_records} records, "
        f"timestamps {start_time_us}-{end_time_us}"
    )


def _latest_level1_timestamp(registry: FeedRegistry) -> Optional[int]:
    """Return latest level-1 qmax from feed registry, if available."""
    latest_idx = registry.get_latest_chunk_idx()
    if latest_idx is None:
        return None
    idx = int(latest_idx)
    while idx > 0:
        meta = registry.get_chunk_metadata(idx)
        try:
            qmin = int(meta.get_qmin(0))
            qmax = int(meta.get_qmax(0))
            if qmax != 0 and qmin != UINT64_MAX and qmin <= qmax:
                return qmax
        finally:
            meta.release()
        idx -= 1
    return None


def _count_records_in_span(
    *,
    feed_dir: Path,
    feed_name: str,
    record_format: dict,
    start_us: int,
    end_us: int,
) -> Optional[int]:
    """
    Count records whose level-1 timestamp falls in [start_us, end_us].
    Returns None if counting cannot be completed.
    """
    if end_us < start_us:
        return None

    rec_size = int(record_format["record_size"])
    ts_offset = int(record_format.get("ts", {}).get("offset", 0))
    if rec_size <= 0 or ts_offset < 0 or (ts_offset + 8) > rec_size:
        return None

    reg_path = feed_dir / f"{feed_name}.reg"
    if not reg_path.exists():
        return None

    registry = FeedRegistry(str(reg_path), mode="r")
    total = 0
    counted_any = False
    try:
        for chunk_idx in registry.get_chunks_in_range(int(start_us), int(end_us), qoff=0):
            meta = registry.get_chunk_metadata(chunk_idx)
            try:
                chunk_id = int(meta.chunk_id)
                qmin = int(meta.get_qmin(0))
                qmax = int(meta.get_qmax(0))
                num_records = int(meta.num_records)
            finally:
                meta.release()

            if num_records <= 0:
                continue

            if qmin >= start_us and qmax <= end_us:
                total += num_records
                counted_any = True
                continue

            chunk_path = feed_dir / f"chunk_{chunk_id:08d}.bin"
            if not chunk_path.exists():
                continue

            to_read = num_records * rec_size
            if to_read <= 0:
                continue

            local_count = 0
            with open(chunk_path, "rb") as f:
                remaining = to_read
                carry = b""
                while remaining > 0:
                    block = f.read(min(remaining, 4 * 1024 * 1024))
                    if not block:
                        break
                    remaining -= len(block)
                    data = carry + block
                    full = (len(data) // rec_size) * rec_size
                    mv = memoryview(data)
                    try:
                        for pos in range(0, full, rec_size):
                            ts = int.from_bytes(mv[pos + ts_offset:pos + ts_offset + 8], "little")
                            if start_us <= ts <= end_us:
                                local_count += 1
                    finally:
                        mv.release()
                    carry = data[full:]

            total += local_count
            counted_any = True
    finally:
        registry.close()

    if not counted_any:
        return None
    return int(total)


def repair_feed_segments(base_path: Path, feed_name: str, record_format: dict, dry_run: bool = False) -> Tuple[int, int]:
    """
    Validate/repair segment metadata for a feed.
    Returns (segments_checked, segments_repaired).
    """
    feed_dir = base_path / "data" / feed_name
    reg_path = feed_dir / f"{feed_name}.reg"
    if not reg_path.exists():
        return 0, 0

    store = SegmentStore(feed_dir, feed_name)
    checked = 0
    repaired = 0

    # Close crash-open segment first so it is visible as crash_closed for backfill.
    if dry_run:
        has_open = any(s.get("status") == "open" for s in store.list_segments(status="all"))
        if has_open:
            checked += 1
            repaired += 1
            log.info("[DRY RUN] Would crash-close open segment for feed '%s'", feed_name)
    else:
        latest_ts = None
        reg = FeedRegistry(str(reg_path), mode="r")
        try:
            latest_ts = _latest_level1_timestamp(reg)
        finally:
            reg.close()
        if store.recover_open_segment(latest_ts):
            checked += 1
            repaired += 1

    fd, mm = store._open_mmap()
    try:
        header = store._read_header(mm)
        count = int(header["segment_count"])
        dirty = False

        for idx in range(1, count + 1):
            seg = store._read_entry(mm, idx)
            status = seg.get("status")
            if status not in USABLE_SEGMENT_STATUSES:
                continue
            if seg.get("records") not in (None, 0):
                continue

            start_us = seg.get("start_us")
            end_us = seg.get("end_us")
            if start_us is None or end_us is None:
                continue

            checked += 1
            cnt = _count_records_in_span(
                feed_dir=feed_dir,
                feed_name=feed_name,
                record_format=record_format,
                start_us=int(start_us),
                end_us=int(end_us),
            )
            if cnt is None:
                continue
            if int(seg.get("records") or 0) == int(cnt):
                continue

            repaired += 1
            if not dry_run:
                seg["records"] = int(cnt)
                store._write_entry(mm, idx, seg)
                dirty = True

        if dirty:
            mm.flush()
    finally:
        mm.close()
        os.close(fd)

    return checked, repaired


# ============================================================================
# INTERNAL HELPERS - Used by CLI repair tool
# ============================================================================

def is_feed_locked(reg_path: Path) -> bool:
    """Check if feed has active writer via non-blocking lock test."""
    if not reg_path.exists():
        return False
    try:
        fd = os.open(reg_path, os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False  # Got lock = no active writer
        except BlockingIOError:
            return True  # Blocked = active writer
        finally:
            os.close(fd)
    except Exception:
        return False


def check_chunk_integrity(chunk_path: Path, meta, record_size: int) -> Tuple[bool, Optional[str]]:
    """
    Fast integrity check: compare registry counters for consistency.
    Returns (is_valid, error_message).
    """
    if not chunk_path.exists():
        return False, f"Chunk file missing: {chunk_path}"

    expected_records = int(meta.num_records)
    by_write_pos = int(meta.write_pos) // int(record_size)
    by_capacity = int(meta.size) // int(record_size)
    if by_write_pos > by_capacity:
        return False, f"Corruption: write_pos implies {by_write_pos} records beyond capacity {by_capacity}"
    if by_write_pos != expected_records:
        return False, f"Corruption: write_pos implies {by_write_pos} records, registry claims {expected_records}"

    qmin0 = int(meta.get_qmin(0))
    qmax0 = int(meta.get_qmax(0))
    if expected_records > 0 and (qmax0 == 0 or qmin0 == UINT64_MAX or qmin0 > qmax0):
        return False, f"Corruption: invalid q-bounds qmin={qmin0} qmax={qmax0}"

    return True, None


def repair_chunk_metadata(
    chunk_path: Path, 
    meta, 
    record_format: dict,
    dry_run: bool = False
) -> Tuple[int, int, int, int]:
    """
    Repair chunk metadata by reading actual first/last records.
    Returns (actual_records, start_time_us, end_time_us, write_pos).
    """
    record_size = int(record_format["record_size"])
    ts_offset = int(record_format.get("ts", {}).get("offset", 0))
    clock_level = int(record_format.get("clock_level") or 1)
    fields = list(record_format.get("fields") or [])

    ts_offsets: list[int] = []
    for i in range(max(1, min(clock_level, 3))):
        if i < len(fields):
            ts_offsets.append(int(fields[i].get("offset", 0)))
        else:
            ts_offsets.append(ts_offset)

    by_write_pos = int(meta.write_pos) // int(record_size)
    by_num_records = int(meta.num_records)
    by_capacity = int(meta.size) // int(record_size)
    actual_records = max(0, min(max(by_write_pos, by_num_records), by_capacity))
    write_pos = actual_records * int(record_size)
    
    if actual_records == 0:
        return 0, 0, 0, 0
    
    # Read first record for start_time
    with open(chunk_path, 'rb') as f:
        first_record = f.read(record_size)
        start_time_us = int.from_bytes(first_record[ts_offsets[0]:ts_offsets[0]+8], 'little')
        
        # Read last record for end_time
        if actual_records > 1:
            f.seek((actual_records - 1) * record_size)
            last_record = f.read(record_size)
            end_time_us = int.from_bytes(last_record[ts_offsets[0]:ts_offsets[0]+8], 'little')
        else:
            last_record = first_record
            end_time_us = start_time_us
    
    if not dry_run:
        # Update metadata
        meta.num_records = actual_records
        meta.write_pos = write_pos
        meta.start_time = start_time_us
        meta.end_time = end_time_us
        meta.clock_level = clock_level
        for i in range(3):
            if i < len(ts_offsets):
                off = ts_offsets[i]
                qmin = int.from_bytes(first_record[off:off+8], 'little')
                qmax = int.from_bytes(last_record[off:off+8], 'little')
                meta.set_qbounds(i, qmin, qmax)
            else:
                meta.set_qbounds(i, UINT64_MAX, 0)
        meta.last_update = time.time_ns() // 1_000
        log.info(f"Repaired: {actual_records} records, {start_time_us}-{end_time_us} us")
    
    return actual_records, start_time_us, end_time_us, write_pos


def repair_feed(base_path: Path, feed_name: str, dry_run: bool = False) -> Tuple[int, int]:
    """
    Validate and repair chunk + segment metadata in a feed.
    Returns (entries_checked, entries_repaired).
    """
    feed_dir = base_path / "data" / feed_name
    reg_path = feed_dir / f"{feed_name}.reg"
    
    if not reg_path.exists():
        log.warning(f"Feed '{feed_name}' has no registry, skipping")
        return 0, 0
    
    # Check if feed is active
    if is_feed_locked(reg_path):
        log.info(f"Feed '{feed_name}' has active writer, skipping")
        return 0, 0
    
    # Load feed metadata
    try:
        record_format = load_record_format_dict(base_path, feed_name)
    except Exception as e:
        log.error(f"Cannot load feed '{feed_name}' metadata: {e}")
        return 0, 0
    
    record_size = record_format["record_size"]
    # Open registry for repair (read-write)
    registry = FeedRegistry(str(reg_path), mode="r")
    
    chunks_checked = 0
    chunks_repaired = 0
    
    try:
        latest_chunk = registry.get_latest_chunk_idx()
        if not latest_chunk:
            log.debug(f"Feed '{feed_name}' has no chunks")
            return 0, 0
        
        for chunk_id in range(1, latest_chunk + 1):
            meta = registry.get_chunk_metadata(chunk_id)
            if meta is None:
                continue
            
            try:
                # Skip non-persistent chunks
                if meta.status == IN_MEMORY:
                    log.debug(f"Skipping IN_MEMORY chunk {chunk_id}")
                    continue
                
                if meta.status == EXPIRED:
                    log.debug(f"Skipping EXPIRED chunk {chunk_id}")
                    continue
                
                # Only validate ON_DISK chunks
                if meta.status == ON_DISK:
                    chunk_path = feed_dir / f"chunk_{chunk_id:08d}.bin"
                    
                    chunks_checked += 1
                    is_valid, error_msg = check_chunk_integrity(chunk_path, meta, record_size)
                    
                    if not is_valid:
                        log.warning(f"Chunk {chunk_id}: {error_msg}")
                        
                        # Attempt repair
                        if dry_run:
                            log.info(f"[DRY RUN] Would repair chunk {chunk_id}")
                            chunks_repaired += 1
                        else:
                            try:
                                # Need write access for repair
                                registry.close()
                                registry = FeedRegistry(str(reg_path), mode="w")
                                meta = registry.get_chunk_metadata(chunk_id)
                                
                                repair_chunk_metadata(
                                    chunk_path, meta, record_format, dry_run=False
                                )
                                chunks_repaired += 1
                                
                                # Back to read mode
                                meta.release()
                                registry.close()
                                registry = FeedRegistry(str(reg_path), mode="r")
                            except Exception as e:
                                log.error(f"Failed to repair chunk {chunk_id}: {e}")
                    else:
                        log.debug(f"Chunk {chunk_id}: OK")
            
            finally:
                if meta is not None:
                    meta.release()
    
    finally:
        registry.close()
    
    seg_checked = 0
    seg_repaired = 0
    try:
        seg_checked, seg_repaired = repair_feed_segments(base_path, feed_name, record_format, dry_run=dry_run)
    except Exception as e:
        log.error(f"Segment metadata repair failed for '{feed_name}': {e}")

    return chunks_checked + seg_checked, chunks_repaired + seg_repaired


def repair_all_feeds(base_path: Path, dry_run: bool = False) -> None:
    """Repair all feeds under a Deepwater base path."""
    total_checked = 0
    total_repaired = 0
    feeds_repaired = 0

    for feed_name in list_feeds(base_path):
        log.info(f"Checking feed: {feed_name}")
        try:
            checked, repaired = repair_feed(base_path, feed_name, dry_run)
            total_checked += checked
            total_repaired += repaired
            
            if repaired > 0:
                feeds_repaired += 1
                log.info(f"  → Repaired {repaired}/{checked} chunks")
            elif checked > 0:
                log.info(f"  → All {checked} chunks OK")
        except Exception as e:
            log.error(f"Error repairing feed '{feed_name}': {e}")
    
    if total_repaired > 0:
        log.info(f"Summary: repaired {total_repaired}/{total_checked} chunks across {feeds_repaired} feeds")
    else:
        log.info(f"Summary: all {total_checked} chunks validated successfully")
    



def main():
    parser = argparse.ArgumentParser(description="Deepwater repair utility")
    parser.add_argument("--base-path", type=str, required=True, help="Deepwater base path")
    parser.add_argument("--feed", type=str, help="Feed name to repair (omit for --all)")
    parser.add_argument("--all", action="store_true", help="Repair all feeds")
    parser.add_argument("--dry-run", action="store_true", help="Check without repairing")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    base_path = Path(args.base_path)
    if not base_path.exists():
        log.error(f"Base path does not exist: {base_path}")
        sys.exit(1)
    
    if args.dry_run:
        log.info("DRY RUN MODE - no metadata will be modified")
    
    try:
        if args.all:
            repair_all_feeds(base_path, dry_run=args.dry_run)
        elif args.feed:
            checked, repaired = repair_feed(base_path, args.feed, dry_run=args.dry_run)
            if repaired > 0:
                log.info(f"Repaired {repaired}/{checked} chunks")
            else:
                log.info(f"All {checked} chunks validated successfully")
        else:
            log.error("Must specify either --feed or --all")
            parser.print_help()
            sys.exit(1)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as e:
        log.error(f"Repair failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
