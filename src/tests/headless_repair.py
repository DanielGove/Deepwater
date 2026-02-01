#!/usr/bin/env python3
"""
Headless Deepwater Repair Tool - Interactive corruption detection/repair
-------------------------------------------------------------------------
Commands: scan/validate/repair/info
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater.platform import Platform
from deepwater import repair


def scan_feeds(platform):
    """Scan all feeds for corruption"""
    feeds = platform.list_feeds()
    if not feeds:
        print("No feeds found.")
        return
    
    print(f"\n=== Scanning {len(feeds)} feeds ===\n")
    
    results = {}
    for feed_name in sorted(feeds):
        print(f"Checking {feed_name}...", end=" ", flush=True)
        try:
            issues = repair.scan_feed(platform, feed_name)
            results[feed_name] = issues
            if issues:
                print(f"ISSUES FOUND: {len(issues)}")
                for issue in issues:
                    print(f"  - {issue}")
            else:
                print("OK")
        except Exception as e:
            print(f"ERROR: {e}")
            results[feed_name] = [f"Scan failed: {e}"]
    
    # Summary
    print(f"\n=== Summary ===")
    clean = sum(1 for v in results.values() if not v)
    dirty = len(results) - clean
    print(f"Clean feeds: {clean}")
    print(f"Feeds with issues: {dirty}")


def validate_feed(platform, feed_name):
    """Validate a specific feed"""
    print(f"\n=== Validating {feed_name} ===\n")
    
    try:
        issues = repair.scan_feed(platform, feed_name)
        if not issues:
            print("✓ No issues found - feed is clean")
            return True
        else:
            print(f"✗ Found {len(issues)} issue(s):")
            for i, issue in enumerate(issues, 1):
                print(f"  {i}. {issue}")
            return False
    except Exception as e:
        print(f"✗ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def repair_feed(platform, feed_name, dry_run=True):
    """Repair a specific feed"""
    mode = "DRY RUN" if dry_run else "LIVE REPAIR"
    print(f"\n=== Repairing {feed_name} ({mode}) ===\n")
    
    try:
        results = repair.repair_feed(platform, feed_name, dry_run=dry_run)
        
        if not results:
            print("✓ No repairs needed - feed is clean")
            return True
        
        print(f"{'Would repair' if dry_run else 'Repaired'} {len(results)} issue(s):")
        for i, result in enumerate(results, 1):
            print(f"  {i}. {result}")
        
        if dry_run:
            print(f"\n(This was a dry run - no changes were made)")
            confirm = input("Apply repairs for real? (yes/no): ").strip().lower()
            if confirm == "yes":
                print("\nApplying repairs...")
                repair_feed(platform, feed_name, dry_run=False)
        else:
            print(f"\n✓ Repairs applied successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Repair failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def feed_info(platform, feed_name):
    """Show feed information and health"""
    print(f"\n=== {feed_name} Info ===\n")
    
    try:
        info = platform.get_feed_info(feed_name)
        print(f"Mode: {info.get('mode', '?')}")
        print(f"Persist: {info.get('persist', '?')}")
        print(f"Record size: {info.get('record_size', '?')} bytes")
        print(f"Chunk size: {info.get('chunk_size_bytes', '?')} bytes")
        
        # Check for chunks
        feed_path = platform.base_path / "data" / feed_name
        if feed_path.exists():
            chunks = list(feed_path.glob("chunk_*.idx"))
            print(f"\nChunks found: {len(chunks)}")
            if chunks:
                total_size = sum(c.stat().st_size for c in chunks)
                print(f"Total size: {total_size:,} bytes ({total_size / (1024**2):.2f} MB)")
        else:
            print("\nNo data directory found")
        
    except Exception as e:
        print(f"Error: {e}")


def main():
    print("Deepwater Headless Repair Tool")
    print("=" * 60)
    
    # Get data path
    data_path = input("Data path [data/coinbase-test]: ").strip()
    if not data_path:
        data_path = "data/coinbase-test"
    
    print(f"Opening platform at: {data_path}")
    try:
        platform = Platform(base_path=data_path)
    except Exception as e:
        print(f"Error opening platform: {e}")
        return 1
    
    print("Platform opened successfully.")
    print("\nCommands:")
    print("  scan                     - Scan all feeds for corruption")
    print("  validate <FEED_NAME>     - Validate specific feed")
    print("  repair <FEED_NAME>       - Repair feed (dry run first)")
    print("  info <FEED_NAME>         - Show feed information")
    print("  quit                     - Exit")
    print()
    
    try:
        while True:
            try:
                cmd = input("cmd> ").strip()
            except EOFError:
                print("\nEOF received, exiting...")
                break
            
            if not cmd:
                continue
            
            parts = cmd.split()
            action = parts[0].lower()
            
            if action == "scan":
                scan_feeds(platform)
            
            elif action == "validate":
                if len(parts) < 2:
                    print("Error: validate requires FEED_NAME")
                    continue
                feed_name = parts[1]
                validate_feed(platform, feed_name)
            
            elif action == "repair":
                if len(parts) < 2:
                    print("Error: repair requires FEED_NAME")
                    continue
                feed_name = parts[1]
                repair_feed(platform, feed_name, dry_run=True)
            
            elif action == "info":
                if len(parts) < 2:
                    print("Error: info requires FEED_NAME")
                    continue
                feed_name = parts[1]
                feed_info(platform, feed_name)
            
            elif action in ("quit", "exit", "stop"):
                print("Exiting...")
                break
            
            else:
                print(f"Unknown command: {action}")
                print("Valid commands: scan, validate, repair, info, quit")
    
    except KeyboardInterrupt:
        print("\nInterrupted, exiting...")
    finally:
        platform.close()
        print("Closed.")


if __name__ == "__main__":
    sys.exit(main() or 0)
