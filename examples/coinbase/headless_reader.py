#!/usr/bin/env python3
"""
Headless Deepwater Reader - Query recorded data via stdin/stdout
-----------------------------------------------------------------
Commands: list/read/info/search
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from deepwater import Reader
from deepwater.metadata.discovery import describe_feed, list_feeds
from deepwater.utils.timestamps import format_us_timestamp


def print_feeds(base_path):
    """List all available feeds"""
    feeds = list_feeds(base_path)
    if not feeds:
        print("No feeds found.")
        return
    
    print(f"\n=== FEEDS ({len(feeds)}) ===")
    for feed_name in sorted(feeds):
        info = describe_feed(base_path, feed_name)
        print(f"\n{feed_name}:")
        lifecycle = info.get("lifecycle", {})
        print(f"  Mode: {info.get('config', {}).get('mode', '?')}")
        print(f"  Persist: {lifecycle.get('persist', '?')}")
        print(f"  Fields: {len(info.get('fields', []))} columns")
        if info.get('clock_level'):
            print(f"  Clock level: {info['clock_level']}")


def print_feed_info(base_path, feed_name):
    """Show detailed feed information"""
    try:
        info = describe_feed(base_path, feed_name)
    except Exception as e:
        print(f"Error: {e}")
        return
    
    print(f"\n=== {feed_name} ===")
    lifecycle = info.get("lifecycle", {})
    print(f"Mode: {info.get('config', {}).get('mode', '?')}")
    print(f"Persist: {lifecycle.get('persist', '?')}")
    print(f"Chunk size: {lifecycle.get('chunk_size_bytes', 0)} bytes")
    print(f"Clock level: {info.get('clock_level', '(unknown)')}")
    
    fields = info.get('fields', [])
    if fields:
        print(f"\nFields ({len(fields)}):")
        for field in fields:
            print(f"  {field['name']:15} {field['type']:10} - {field.get('desc', '')}")


def read_feed(base_path, feed_name, start_us=None, end_us=None, limit=10):
    """Read records from a feed"""
    try:
        reader = Reader(base_path, feed_name)
    except Exception as e:
        print(f"Error creating reader: {e}")
        return
    
    try:
        count = 0
        print(f"\n=== Reading {feed_name} ===")
        
        if start_us or end_us:
            print(f"Time range: {format_us_timestamp(start_us) if start_us else '(start)'} to {format_us_timestamp(end_us) if end_us else '(end)'}")
        print(f"Limit: {limit} records\n")
        
        start = 0 if start_us is None else int(start_us)
        end = (1 << 63) - 1 if end_us is None else int(end_us)
        for batch in reader.range_batches(start, end, format="dict", batch_records=max(1, limit)):
            for record in batch:
                count += 1
                print(f"Record {count}:")
                for name, value in record.items():
                    if 'us' in name and isinstance(value, int) and value > 1_000_000_000:
                        value_str = f"{value} ({format_us_timestamp(value)})"
                    else:
                        value_str = str(value)
                    print(f"  {name}: {value_str}")
                print()

                if count >= limit:
                    print(f"(Stopped at limit {limit})")
                    break
            if count >= limit:
                break
        
        if count == 0:
            print("No records found.")
        else:
            print(f"Total records read: {count}")
    
    except Exception as e:
        print(f"Error reading: {e}")
        import traceback
        traceback.print_exc()
    finally:
        reader.close()


def main():
    print("Deepwater Headless Reader")
    print("=" * 60)
    
    # Get data path
    data_path = input("Data path [data/coinbase-test]: ").strip()
    if not data_path:
        data_path = "data/coinbase-test"
    
    print(f"Using Deepwater base path: {data_path}")
    print("\nCommands:")
    print("  list                  - List all feeds")
    print("  info <FEED_NAME>      - Show feed details")
    print("  read <FEED_NAME>      - Read recent records (default: 10)")
    print("  read <FEED_NAME> N    - Read N records")
    print("  quit                  - Exit")
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
            
            if action == "list":
                print_feeds(data_path)
            
            elif action == "info":
                if len(parts) < 2:
                    print("Error: info requires FEED_NAME")
                    continue
                feed_name = parts[1]
                print_feed_info(data_path, feed_name)
            
            elif action == "read":
                if len(parts) < 2:
                    print("Error: read requires FEED_NAME")
                    continue
                feed_name = parts[1]
                limit = 10
                if len(parts) >= 3:
                    try:
                        limit = int(parts[2])
                    except ValueError:
                        print(f"Error: invalid limit '{parts[2]}'")
                        continue
                read_feed(data_path, feed_name, limit=limit)
            
            elif action in ("quit", "exit", "stop"):
                print("Exiting...")
                break
            
            else:
                print(f"Unknown command: {action}")
                print("Valid commands: list, info, read, quit")
    
    except KeyboardInterrupt:
        print("\nInterrupted, exiting...")
    finally:
        print("Closed.")


if __name__ == "__main__":
    sys.exit(main() or 0)
