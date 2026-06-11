#!/usr/bin/env python3
"""
Deepwater health check - validates base/feed state for monitoring.

Exit codes:
    0 = healthy
    1 = unhealthy (with details)
    2 = error running check

Usage:
    python -m deepwater.ops.health_check --base-path ./data [--check-feeds] [--max-age-seconds 300]
    
Checks:
    - Global registry accessible
    - Feed registries not corrupted
    - Recent activity (if --max-age-seconds specified)
    - Disk space available
"""
import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

log = logging.getLogger("dw.health")


def check_global_registry(base_path: Path) -> Tuple[bool, str]:
    """Verify global registry is accessible."""
    reg_path = base_path / "registry" / "global_registry.bin"
    if not reg_path.exists():
        return False, "global_registry.bin missing"
    
    try:
        from deepwater.metadata.global_registry import GlobalRegistry
        reg = GlobalRegistry(base_path)
        feed_count = len(reg.list_feeds())
        reg.close()
        return True, f"global registry OK ({feed_count} feeds)"
    except Exception as e:
        return False, f"global registry error: {e}"


def _reader_state(reader) -> Optional[dict]:
    if hasattr(reader, "_ring_state"):
        try:
            return reader._ring_state()
        except FileNotFoundError:
            return None
    if hasattr(reader, "state"):
        try:
            return reader.state()
        except FileNotFoundError:
            return None
    return None


def check_feeds(base_path: Path, *, include_details: bool = False):
    """Verify feed registries/readers are readable and rings are not dropping data."""
    from deepwater import Reader
    from deepwater.metadata.global_registry import GlobalRegistry
    from deepwater.metadata.feed_metadata import list_feeds, load_feed_metadata

    if not (base_path / "data").exists():
        result = (True, "no feeds yet", [])
        return result if include_details else result[:2]

    issues = []
    details = []

    feed_names = list_feeds(base_path)
    persistent_ring_feed_count = 0
    live_ring_count = 0

    for feed_name in feed_names:
        metadata = load_feed_metadata(base_path, feed_name)
        persist = True if metadata is None else metadata.persist
        uses_ring = False if metadata is None else metadata.uses_ring
        if persist and uses_ring:
            persistent_ring_feed_count += 1

        feed_dir = base_path / "data" / feed_name
        reg_file = feed_dir / f"{feed_name}.reg"
        detail = {
            "feed": feed_name,
            "persist": persist,
            "uses_ring": uses_ring,
            "registry_present": reg_file.exists(),
        }

        if persist and not reg_file.exists():
            detail["healthy"] = True
            detail["ring_present"] = False
            details.append(detail)
            continue

        try:
            reader = Reader(base_path, feed_name)
            state = _reader_state(reader)
            detail["ring_present"] = state is not None

            if state is not None:
                live_ring_count += 1
                record_count = int(state.get("record_count", 0))
                durable_record_count = int(state.get("durable_record_count", 0))
                overrun_count = int(state.get("overrun_count", 0))
                lost_records = int(state.get("lost_records", 0))
                detail.update({
                    "record_count": record_count,
                    "durable_record_count": durable_record_count,
                    "backlog": record_count - durable_record_count,
                    "ring_capacity": int(state.get("ring_capacity", 0) or 0),
                    "overrun_count": overrun_count,
                    "lost_records": lost_records,
                })
                if overrun_count or lost_records:
                    issues.append(
                        f"{feed_name}: overrun_count={overrun_count} lost_records={lost_records}"
                    )
            detail["healthy"] = "error" not in detail and not (
                int(detail.get("overrun_count", 0)) or int(detail.get("lost_records", 0))
            )
        except Exception as e:
            detail["healthy"] = False
            detail["error"] = str(e)
            issues.append(f"{feed_name}: {e}")
        finally:
            try:
                reader.close()
            except Exception:
                pass

        details.append(detail)

    global_registry = GlobalRegistry(base_path)
    try:
        owner = global_registry.get_persistent_ring_owner()
    finally:
        global_registry.close()
    if live_ring_count and persistent_ring_feed_count and (not owner.get("healthy") or owner.get("stale")):
        issues.append(
            "persistent ring owner unhealthy"
            f" (pid={owner.get('pid')} alive={owner.get('alive')} stale={owner.get('stale')})"
        )

    if issues:
        result = (
            False,
            f"{len(feed_names)} feeds checked, {len(issues)} issues, {live_ring_count} live rings",
            details,
        )
        return result if include_details else result[:2]

    result = (
        True,
        f"{len(feed_names)} feeds checked, 0 issues, {live_ring_count} live rings",
        details,
    )
    return result if include_details else result[:2]


def check_recent_activity(base_path: Path, max_age_seconds: int) -> Tuple[bool, str]:
    """Check if any feed has recent writes."""
    data_dir = base_path / "data"
    if not data_dir.exists():
        return True, "no feeds (skipping activity check)"
    
    now = time.time()
    newest_write = 0
    feed_with_activity = None
    
    for feed_dir in data_dir.iterdir():
        if not feed_dir.is_dir():
            continue
        
        # Check mtime of registry (updated on writes)
        reg_file = feed_dir / f"{feed_dir.name}.reg"
        if reg_file.exists():
            mtime = reg_file.stat().st_mtime
            if mtime > newest_write:
                newest_write = mtime
                feed_with_activity = feed_dir.name
    
    if newest_write == 0:
        return True, "no activity detected (feeds exist but not written to)"
    
    age = now - newest_write
    if age > max_age_seconds:
        return False, f"no writes in {age:.0f}s (threshold: {max_age_seconds}s, last: {feed_with_activity})"
    
    return True, f"recent activity: {feed_with_activity} wrote {age:.0f}s ago"


def check_disk_space(base_path: Path, min_free_gb: float = 1.0) -> Tuple[bool, str]:
    """Check available disk space."""
    try:
        import shutil
        stat = shutil.disk_usage(base_path)
        free_gb = stat.free / (1024**3)
        total_gb = stat.total / (1024**3)
        used_pct = (stat.used / stat.total) * 100
        
        if free_gb < min_free_gb:
            return False, f"low disk space: {free_gb:.1f} GB free ({used_pct:.1f}% used)"
        
        return True, f"disk OK: {free_gb:.1f}/{total_gb:.1f} GB free ({used_pct:.1f}% used)"
    except Exception as e:
        return False, f"disk check error: {e}"


def main():
    parser = argparse.ArgumentParser(description="Deepwater health check")
    parser.add_argument("--base-path", type=str, required=True, help="Deepwater base path")
    parser.add_argument("--check-feeds", action="store_true", help="Check individual feed registries")
    parser.add_argument("--max-age-seconds", type=int, help="Fail if no writes in this many seconds")
    parser.add_argument("--min-free-gb", type=float, default=1.0, help="Minimum free disk space in GB")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    base_path = Path(args.base_path)
    if not base_path.exists():
        print(f"UNHEALTHY: base path does not exist: {base_path}")
        sys.exit(1)
    
    checks = []
    
    # Core checks (always run)
    checks.append(("Global Registry", check_global_registry(base_path)))
    checks.append(("Disk Space", check_disk_space(base_path, args.min_free_gb)))
    
    # Optional checks
    if args.check_feeds:
        feeds_healthy, feeds_message, feed_details = check_feeds(base_path, include_details=True)
        checks.append(("Feeds", (feeds_healthy, feeds_message)))
    else:
        feed_details = []
    
    if args.max_age_seconds:
        checks.append(("Recent Activity", check_recent_activity(base_path, args.max_age_seconds)))
    
    # Evaluate results
    all_healthy = True
    for name, (healthy, message) in checks:
        status = "✓" if healthy else "✗"
        print(f"{status} {name}: {message}")
        if not healthy:
            all_healthy = False

    if args.verbose and feed_details:
        print("\nFeed Details:")
        for detail in sorted(feed_details, key=lambda x: x["feed"]):
            status = "✓" if detail.get("healthy") else "✗"
            feed = detail["feed"]
            persist = "persist" if detail.get("persist") else "ring"
            ring = "live-ring" if detail.get("ring_present") else "no-ring"
            if "error" in detail:
                print(f"  {status} {feed} [{persist} {ring}] error={detail['error']}")
                continue
            backlog = detail.get("backlog")
            overrun = detail.get("overrun_count", 0)
            lost = detail.get("lost_records", 0)
            cap = detail.get("ring_capacity", 0)
            if backlog is None:
                print(f"  {status} {feed} [{persist} {ring}]")
            else:
                print(
                    f"  {status} {feed} [{persist} {ring}] "
                    f"backlog={backlog} cap={cap} overrun={overrun} lost={lost}"
                )
    
    if all_healthy:
        print("\nHEALTHY")
        sys.exit(0)
    else:
        print("\nUNHEALTHY")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        logging.exception("Health check failed")
        sys.exit(2)
