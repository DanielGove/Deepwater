#!/usr/bin/env python3
"""
Deepwater health check - validates platform state for monitoring.

Exit codes:
    0 = healthy
    1 = unhealthy (with details)
    2 = error running check

Usage:
    python -m deepwater.ops.health_check --base-path ./data [--check-feeds] [--max-age-seconds 300]
    
Checks:
    - Manifest exists and is readable
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
from typing import Tuple

log = logging.getLogger("dw.health")


def check_manifest(base_path: Path) -> Tuple[bool, str]:
    """Verify manifest exists and is readable."""
    manifest_path = base_path / "manifest.json"
    if not manifest_path.exists():
        return False, "manifest.json missing"
    
    try:
        import orjson
        data = orjson.loads(manifest_path.read_bytes())
        if "deepwater_version" not in data:
            return False, "manifest.json missing version"
        return True, f"manifest OK (version {data['deepwater_version']})"
    except Exception as e:
        return False, f"manifest.json corrupt: {e}"


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


def check_feeds(base_path: Path) -> Tuple[bool, str]:
    """Verify feed registries are readable."""
    data_dir = base_path / "data"
    if not data_dir.exists():
        return True, "no feeds yet"
    
    issues = []
    healthy_count = 0
    
    for feed_dir in data_dir.iterdir():
        if not feed_dir.is_dir():
            continue
        
        reg_file = feed_dir / f"{feed_dir.name}.reg"
        if not reg_file.exists():
            continue
        
        try:
            from deepwater.metadata.feed_registry import FeedRegistry
            reg = FeedRegistry(str(reg_file), mode="r")
            latest = reg.get_latest_chunk_idx()
            reg.close()
            healthy_count += 1
        except Exception as e:
            issues.append(f"{feed_dir.name}: {e}")
    
    if issues:
        return False, f"{len(issues)} feeds unhealthy: {'; '.join(issues)}"
    
    return True, f"{healthy_count} feeds healthy"


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
    checks.append(("Manifest", check_manifest(base_path)))
    checks.append(("Global Registry", check_global_registry(base_path)))
    checks.append(("Disk Space", check_disk_space(base_path, args.min_free_gb)))
    
    # Optional checks
    if args.check_feeds:
        checks.append(("Feed Registries", check_feeds(base_path)))
    
    if args.max_age_seconds:
        checks.append(("Recent Activity", check_recent_activity(base_path, args.max_age_seconds)))
    
    # Evaluate results
    all_healthy = True
    for name, (healthy, message) in checks:
        status = "✓" if healthy else "✗"
        print(f"{status} {name}: {message}")
        if not healthy:
            all_healthy = False
    
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
