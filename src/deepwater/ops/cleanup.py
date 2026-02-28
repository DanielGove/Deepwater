#!/usr/bin/env python3
"""
Deepwater cleanup daemon - deletes expired chunks based on retention policy.

Usage:
    # One-time cleanup
    python -m deepwater.ops.cleanup --base-path ./data
    
    # Install cron job (runs every 15 minutes)
    python -m deepwater.ops.cleanup --install-cron --base-path ./data [--interval 15]
    
    # Remove cron job
    python -m deepwater.ops.cleanup --uninstall-cron
    
    # Check what would be deleted (dry run)
    python -m deepwater.ops.cleanup --base-path ./data --dry-run
"""
import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Tuple

# Allow running as standalone script
if __name__ == "__main__":
    # Add src/ to path if running directly
    script_dir = Path(__file__).resolve().parent
    src_dir = script_dir.parent
    if src_dir.name == "src":
        sys.path.insert(0, str(src_dir))

from deepwater.platform import Platform
from deepwater.metadata.feed_registry import FeedRegistry, EXPIRED, ON_DISK

log = logging.getLogger("dw.cleanup")


def cleanup_feed(base_path: Path, feed_name: str, retention_hours: int, dry_run: bool = False) -> Tuple[int, int]:
    """
    Delete expired chunks for a single feed.
    Returns (chunks_deleted, bytes_freed).
    """
    feed_dir = base_path / "data" / feed_name
    reg_path = feed_dir / f"{feed_name}.reg"
    
    if not reg_path.exists():
        return 0, 0
    
    registry = FeedRegistry(str(reg_path), mode="r")
    now_us = time.time_ns() // 1_000
    retention_us = retention_hours * 3600 * 1_000_000
    
    chunks_deleted = 0
    bytes_freed = 0
    
    try:
        for chunk_id in range(1, registry.get_latest_chunk_idx() + 1 if registry.get_latest_chunk_idx() else 1):
            meta = registry.get_chunk_metadata(chunk_id)
            if meta is None:
                continue
            
            try:
                # EXPIRED chunks: always delete (SHM already unlinked by writer usually)
                if meta.status == EXPIRED:
                    # SHM cleanup (best effort, may already be gone)
                    shm_name = f"{feed_name}-{chunk_id}"
                    try:
                        from multiprocessing import shared_memory
                        shm = shared_memory.SharedMemory(name=shm_name, create=False)
                        if not dry_run:
                            shm.close()
                            shm.unlink()
                        log.debug(f"Deleted SHM: {shm_name}")
                    except FileNotFoundError:
                        pass  # Already gone
                    except Exception as e:
                        log.debug(f"SHM cleanup error for {shm_name}: {e}")
                
                # ON_DISK chunks: delete if older than retention
                elif meta.status == ON_DISK:
                    end_ts = meta.end_time or meta.last_update or meta.start_time
                    age_us = now_us - end_ts
                    if retention_hours == 0:
                        pass  # keep forever
                    elif age_us >= retention_us:
                        chunk_file = feed_dir / f"chunk_{chunk_id:08d}.bin"
                        idx_file = feed_dir / f"chunk_{chunk_id:08d}.idx"
                        
                        size = 0
                        if chunk_file.exists():
                            size = chunk_file.stat().st_size
                            if not dry_run:
                                chunk_file.unlink()
                            log.info(f"Deleted chunk: {chunk_file.name} ({size / 1024 / 1024:.1f} MB)")
                            chunks_deleted += 1
                            bytes_freed += size
                        
                        if idx_file.exists():
                            if not dry_run:
                                idx_file.unlink()
                            log.debug(f"Deleted index: {idx_file.name}")
            finally:
                meta.release()
    finally:
        registry.close()
    
    return chunks_deleted, bytes_freed


def cleanup_all_feeds(base_path: Path, dry_run: bool = False) -> None:
    """Run cleanup on all feeds according to their retention policies."""
    platform = Platform(str(base_path))
    
    total_chunks = 0
    total_bytes = 0
    
    for feed in platform.list_feeds():
        feed_name = feed
        try:
            metadata = platform.lifecycle(feed_name)
            retention_hours = metadata.get("retention_hours", 0)
            
            if retention_hours == 0:
                log.debug(f"Skipping {feed_name}: retention_hours=0 (keep forever)")
                continue
            
            log.info(f"Cleaning {feed_name} (retention={retention_hours}h)")
            chunks, bytes_freed = cleanup_feed(base_path, feed_name, retention_hours, dry_run)
            total_chunks += chunks
            total_bytes += bytes_freed
            
            if chunks > 0:
                log.info(f"  → Deleted {chunks} chunks, freed {bytes_freed / 1024 / 1024:.1f} MB")
        except Exception as e:
            log.error(f"Error cleaning {feed_name}: {e}")
    
    if total_chunks > 0:
        log.info(f"Total: deleted {total_chunks} chunks, freed {total_bytes / 1024 / 1024:.1f} MB")
    else:
        log.info("No chunks deleted")
    
    platform.close()


def install_cron(base_path: str, interval: int = 15) -> None:
    """Install cron job to run cleanup periodically."""
    from crontab import CronTab
    
    script_path = Path(__file__).resolve()
    python_path = sys.executable
    command = f"{python_path} -m deepwater.ops.cleanup --base-path {base_path}"
    
    # Get current user's crontab
    cron = CronTab(user=True)
    
    # Remove existing deepwater-cleanup jobs
    cron.remove_all(comment="deepwater-cleanup")
    
    # Add new job
    job = cron.new(command=command, comment="deepwater-cleanup")
    job.minute.every(interval)
    
    cron.write()
    log.info(f"Installed cron job: runs every {interval} minutes")
    log.info(f"Command: {command}")


def uninstall_cron() -> None:
    """Remove deepwater-cleanup cron job."""
    from crontab import CronTab
    
    cron = CronTab(user=True)
    removed = cron.remove_all(comment="deepwater-cleanup")
    cron.write()
    
    if removed:
        log.info(f"Removed {removed} deepwater-cleanup cron job(s)")
    else:
        log.info("No deepwater-cleanup cron jobs found")


def main():
    parser = argparse.ArgumentParser(description="Deepwater cleanup daemon")
    parser.add_argument("--base-path", type=str, help="Deepwater base path (e.g., ./data)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without deleting")
    parser.add_argument("--install-cron", action="store_true", help="Install cron job")
    parser.add_argument("--uninstall-cron", action="store_true", help="Remove cron job")
    parser.add_argument("--interval", type=int, default=15, help="Cron interval in minutes (default: 15)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Handle cron operations
    if args.uninstall_cron:
        try:
            uninstall_cron()
        except ImportError:
            log.error("python-crontab not installed. Run: pip install python-crontab")
            sys.exit(1)
        return
    
    if args.install_cron:
        if not args.base_path:
            log.error("--base-path required for --install-cron")
            sys.exit(1)
        try:
            install_cron(args.base_path, args.interval)
        except ImportError:
            log.error("python-crontab not installed. Run: pip install python-crontab")
            sys.exit(1)
        return
    
    # One-time cleanup
    if not args.base_path:
        log.error("--base-path required")
        parser.print_help()
        sys.exit(1)
    
    base_path = Path(args.base_path)
    if not base_path.exists():
        log.error(f"Base path does not exist: {base_path}")
        sys.exit(1)
    
    if args.dry_run:
        log.info("DRY RUN MODE - no files will be deleted")
    
    try:
        cleanup_all_feeds(base_path, dry_run=args.dry_run)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as e:
        log.error(f"Cleanup failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
