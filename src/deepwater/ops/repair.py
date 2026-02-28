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
import struct
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

from deepwater.platform import Platform
from deepwater.metadata.feed_registry import FeedRegistry, ON_DISK, IN_MEMORY, EXPIRED

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
    record_format: dict
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
    
    # Check file size vs metadata
    actual_size = chunk_path.stat().st_size
    record_size = record_format["record_size"]
    actual_records = actual_size // record_size
    expected_records = meta.num_records
    
    if actual_records < expected_records:
        # Corruption - file has fewer records than registry claims
        log.warning(
            f"Chunk {chunk_id} corrupted: file has {actual_records} records, "
            f"registry claims {expected_records}. Auto-repairing..."
        )
        _repair_chunk_from_file(chunk_path, meta, record_format)
        log.info(f"Chunk {chunk_id} repaired successfully")
    elif actual_records > expected_records:
        log.debug(
            f"Chunk {chunk_id}: file ahead of registry "
            f"({actual_records} vs {expected_records}) - writer was mid-update"
        )


def _repair_chunk_from_file(chunk_path: Path, meta, record_format: dict) -> None:
    """
    Repair chunk metadata by reading actual first/last records from file.
    Updates num_records, write_pos, start_time, end_time atomically.
    """
    record_size = record_format["record_size"]
    actual_size = chunk_path.stat().st_size
    actual_records = actual_size // record_size
    write_pos = actual_records * record_size
    
    if actual_records == 0:
        meta.num_records = 0
        meta.write_pos = 0
        meta.start_time = 0
        meta.end_time = 0
        meta.last_update = time.time_ns() // 1_000
        return
    
    # Get timestamp offset from record format
    ts_offset = record_format.get("ts", {}).get("offset", 0)
    
    # Read first record for start_time
    with open(chunk_path, 'rb') as f:
        first_record = f.read(record_size)
        start_time_us = int.from_bytes(first_record[ts_offset:ts_offset+8], 'little')
        
        # Read last record for end_time
        if actual_records > 1:
            f.seek((actual_records - 1) * record_size)
            last_record = f.read(record_size)
            end_time_us = int.from_bytes(last_record[ts_offset:ts_offset+8], 'little')
        else:
            end_time_us = start_time_us
    
    # Update metadata atomically
    meta.num_records = actual_records
    meta.write_pos = write_pos
    meta.start_time = start_time_us
    meta.end_time = end_time_us
    meta.last_update = time.time_ns() // 1_000
    
    log.debug(
        f"Repaired chunk {meta.chunk_id}: {actual_records} records, "
        f"timestamps {start_time_us}-{end_time_us}"
    )


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
    Fast integrity check: compare file size with registry metadata.
    Returns (is_valid, error_message).
    """
    if not chunk_path.exists():
        return False, f"Chunk file missing: {chunk_path}"
    
    actual_size = chunk_path.stat().st_size
    actual_records = actual_size // record_size
    expected_records = meta.num_records
    
    # File has fewer records than registry claims = corruption
    if actual_records < expected_records:
        return False, f"Corruption: file has {actual_records} records, registry claims {expected_records}"
    
    # File has more records = race condition (writer mid-update), not corruption
    if actual_records > expected_records:
        log.debug(f"File ahead of registry: {actual_records} vs {expected_records} (writer lag)")
    
    return True, None


def repair_chunk_metadata(
    chunk_path: Path, 
    meta, 
    record_size: int, 
    record_struct: struct.Struct,
    ts_offset: int,
    dry_run: bool = False
) -> Tuple[int, int, int, int]:
    """
    Repair chunk metadata by reading actual first/last records.
    Returns (actual_records, start_time_us, end_time_us, write_pos).
    """
    actual_size = chunk_path.stat().st_size
    actual_records = actual_size // record_size
    write_pos = actual_records * record_size
    
    if actual_records == 0:
        return 0, 0, 0, 0
    
    # Read first record for start_time
    with open(chunk_path, 'rb') as f:
        first_record = f.read(record_size)
        start_time_us = int.from_bytes(first_record[ts_offset:ts_offset+8], 'little')
        
        # Read last record for end_time
        if actual_records > 1:
            f.seek((actual_records - 1) * record_size)
            last_record = f.read(record_size)
            end_time_us = int.from_bytes(last_record[ts_offset:ts_offset+8], 'little')
        else:
            end_time_us = start_time_us
    
    if not dry_run:
        # Update metadata
        meta.num_records = actual_records
        meta.write_pos = write_pos
        meta.start_time = start_time_us
        meta.end_time = end_time_us
        meta.last_update = time.time_ns() // 1_000
        log.info(f"Repaired: {actual_records} records, {start_time_us}-{end_time_us} us")
    
    return actual_records, start_time_us, end_time_us, write_pos


def repair_feed(base_path: Path, feed_name: str, dry_run: bool = False) -> Tuple[int, int]:
    """
    Validate and repair all chunks in a feed.
    Returns (chunks_checked, chunks_repaired).
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
    platform = Platform(str(base_path))
    try:
        record_format = platform.get_record_format(feed_name)
        lifecycle = platform.lifecycle(feed_name)
    except Exception as e:
        log.error(f"Cannot load feed '{feed_name}' metadata: {e}")
        return 0, 0
    finally:
        platform.close()
    
    record_size = record_format["record_size"]
    record_struct = struct.Struct(record_format["fmt"])
    ts_offset = record_format.get("ts", {}).get("offset", 0)
    
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
                                    chunk_path, meta, record_size, 
                                    record_struct, ts_offset, dry_run=False
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
    
    return chunks_checked, chunks_repaired


def repair_all_feeds(base_path: Path, dry_run: bool = False) -> None:
    """Repair all feeds in the platform."""
    platform = Platform(str(base_path))
    
    total_checked = 0
    total_repaired = 0
    feeds_repaired = 0
    
    for feed_name in platform.list_feeds():
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
    
    platform.close()


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
