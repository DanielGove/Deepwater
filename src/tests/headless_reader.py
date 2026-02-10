#!/usr/bin/env python3
"""
Headless Deepwater Reader - Query recorded data via stdin/stdout
-----------------------------------------------------------------
Commands: list/read/info/search
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from deepwater.platform import Platform
from deepwater.utils.timestamps import format_us_timestamp


def print_feeds(platform):
    """List all available feeds"""
    feeds = platform.list_feeds()
    if not feeds:
        print("No feeds found.")
        return
    
    print(f"\n=== FEEDS ({len(feeds)}) ===")
    for feed_name in sorted(feeds):
        info = platform.get_feed_info(feed_name)
        print(f"\n{feed_name}:")
        print(f"  Mode: {info.get('mode', '?')}")
        print(f"  Persist: {info.get('persist', '?')}")
        print(f"  Fields: {len(info.get('fields', []))} columns")
        if info.get('clock_level'):
            print(f"  Clock level: {info['clock_level']}")


def print_feed_info(platform, feed_name):
    """Show detailed feed information"""
    try:
        info = platform.get_feed_info(feed_name)
    except Exception as e:
        print(f"Error: {e}")
        return
    
    print(f"\n=== {feed_name} ===")
    print(f"Mode: {info.get('mode', '?')}")
    print(f"Persist: {info.get('persist', '?')}")
    print(f"Chunk size: {info.get('chunk_size_bytes', 0)} bytes")
    print(f"Clock level: {info.get('clock_level', '(unknown)')}")
    
    fields = info.get('fields', [])
    if fields:
        print(f"\nFields ({len(fields)}):")
        for field in fields:
            print(f"  {field['name']:15} {field['type']:10} - {field.get('desc', '')}")


def read_feed(platform, feed_name, start_us=None, end_us=None, limit=10):
    """Read records from a feed"""
    try:
        reader = platform.create_reader(feed_name)
    except Exception as e:
        print(f"Error creating reader: {e}")
        return
    
    try:
        count = 0
        print(f"\n=== Reading {feed_name} ===")
        
        if start_us or end_us:
            print(f"Time range: {format_us_timestamp(start_us) if start_us else '(start)'} to {format_us_timestamp(end_us) if end_us else '(end)'}")
        print(f"Limit: {limit} records\n")
        
        for record in reader.read(start_us=start_us, end_us=end_us):
            count += 1
            # Print record fields
            print(f"Record {count}:")
            for i, (name, value) in enumerate(record.items()):
                # Format timestamps nicely
                if 'us' in name and isinstance(value, int) and value > 1_000_000_000:
                    value_str = f"{value} ({format_us_timestamp(value)})"
                else:
                    value_str = str(value)
                print(f"  {name}: {value_str}")
            print()
            
            if count >= limit:
                print(f"(Stopped at limit {limit})")
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
    
    print(f"Opening platform at: {data_path}")
    try:
        platform = Platform(base_path=data_path)
    except Exception as e:
        print(f"Error opening platform: {e}")
        return 1
    
    print("Platform opened successfully.")
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
                print_feeds(platform)
            
            elif action == "info":
                if len(parts) < 2:
                    print("Error: info requires FEED_NAME")
                    continue
                feed_name = parts[1]
                print_feed_info(platform, feed_name)
            
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
                read_feed(platform, feed_name, limit=limit)
            
            elif action in ("quit", "exit", "stop"):
                print("Exiting...")
                break
            
            else:
                print(f"Unknown command: {action}")
                print("Valid commands: list, info, read, quit")
    
    except KeyboardInterrupt:
        print("\nInterrupted, exiting...")
    finally:
        platform.close()
        print("Closed.")


if __name__ == "__main__":
    sys.exit(main() or 0)
