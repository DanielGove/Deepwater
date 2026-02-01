#!/usr/bin/env python3
"""
Headless Deepwater Monitor - stdin/stdout control for AI interaction
---------------------------------------------------------------------
No TUI, just print() and input() prompts.
Commands: subscribe/unsubscribe/status/metrics/reset/stop
"""
import sys, time, logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from websocket_client import MarketDataEngine

# Configure logging to file only (not stdout, so it doesn't interfere with our print statements)
log_file = Path(__file__).parent.parent.parent.parent / "data" / "coinbase-test" / "headless.log"
log_file.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(log_file),
    ]
)
log = logging.getLogger("headless")


def format_number(val):
    """Format large numbers with K/M suffixes"""
    if val >= 1_000_000:
        return f"{val/1_000_000:.2f}M"
    if val >= 1_000:
        return f"{val/1_000:.1f}K"
    return f"{val}"


def format_latency(us):
    """Format latency in microseconds"""
    if us is None or us <= 0:
        return "—"
    if us >= 1_000_000:
        return f"{us/1_000_000:.2f}s"
    if us >= 1_000:
        return f"{us/1_000:.2f}ms"
    return f"{us:.0f}µs"


def print_status(engine):
    """Print current engine status"""
    status = engine.status_snapshot()
    print("\n=== STATUS ===")
    print(f"Running: {status['running']}")
    print(f"Connected: {status['connected']}")
    print(f"Subscriptions: {', '.join(status['subs']) if status['subs'] else '(none)'}")
    print(f"Sequence gaps: {status['seq_gap_trades']}")
    print(f"Heartbeat age: {status['hb_age']:.1f}s" if status['hb_age'] is not None else "Heartbeat age: —")
    print(f"Last message: {status['msg_age']:.1f}s ago" if status['msg_age'] is not None else "Last message: —")


def print_metrics(engine):
    """Print current metrics snapshot"""
    snap = engine.metrics_snapshot()
    
    print("\n=== METRICS ===")
    
    # Ingress
    if "ingress" in snap:
        ing = snap["ingress"]
        rates = ing.get("rates", {})
        print(f"\nIngress:")
        print(f"  Total: {format_number(ing.get('total_count', 0))} msgs, {format_number(ing.get('total_bytes', 0))} bytes")
        print(f"  Rate: {rates.get('rps_10s', 0):.1f} msg/s, {rates.get('bps_10s', 0):.1f} B/s")
        print(f"  Latency: p50={format_latency(ing.get('p50'))} p95={format_latency(ing.get('p95'))} p99={format_latency(ing.get('p99'))}")
    
    # Trades
    if "trades" in snap and snap["trades"]:
        print(f"\nTrades:")
        for pid, data in sorted(snap["trades"].items()):
            rates = data.get("rates", {})
            print(f"  {pid}: {format_number(data.get('total_count', 0))} trades @ {rates.get('rps_10s', 0):.1f}/s")
            print(f"    Latency: p50={format_latency(data.get('p50'))} p95={format_latency(data.get('p95'))} p99={format_latency(data.get('p99'))}")
    
    # L2
    if "l2" in snap and snap["l2"]:
        print(f"\nL2 Updates:")
        for pid, data in sorted(snap["l2"].items()):
            rates = data.get("rates", {})
            print(f"  {pid}: {format_number(data.get('total_count', 0))} updates @ {rates.get('rps_10s', 0):.1f}/s")
            print(f"    Latency: p50={format_latency(data.get('p50'))} p95={format_latency(data.get('p95'))} p99={format_latency(data.get('p99'))}")


def main():
    print("Deepwater Headless Monitor")
    print("=" * 60)
    print(f"Logs: {log_file}")
    print("=" * 60)
    
    # Create engine
    engine = MarketDataEngine(sample_size=16)
    
    print("\nStarting engine...")
    engine.start()
    print("Engine started. Waiting for connection...")
    time.sleep(2)
    
    print("\nCommands:")
    print("  subscribe <PRODUCT_ID>   - Subscribe to a product (e.g., XRP-USD)")
    print("  unsubscribe <PRODUCT_ID> - Unsubscribe from a product")
    print("  status                   - Show connection status")
    print("  metrics                  - Show performance metrics")
    print("  reset                    - Reset metrics counters")
    print("  stop                     - Stop engine and exit")
    print()
    
    try:
        while True:
            try:
                cmd = input("cmd> ").strip()
            except EOFError:
                print("\nEOF received, stopping...")
                break
            
            if not cmd:
                continue
            
            parts = cmd.split(maxsplit=1)
            action = parts[0].lower()
            arg = parts[1].upper() if len(parts) > 1 else None
            
            if action == "subscribe":
                if not arg:
                    print("Error: subscribe requires PRODUCT_ID (e.g., subscribe XRP-USD)")
                    continue
                print(f"Subscribing to {arg}...")
                try:
                    engine.subscribe(arg)
                    print(f"OK - subscribed to {arg}")
                except Exception as e:
                    print(f"Error: {e}")
                    log.error(f"Subscribe failed: {e}", exc_info=True)
            
            elif action == "unsubscribe":
                if not arg:
                    print("Error: unsubscribe requires PRODUCT_ID")
                    continue
                print(f"Unsubscribing from {arg}...")
                try:
                    engine.unsubscribe(arg)
                    print(f"OK - unsubscribed from {arg}")
                except Exception as e:
                    print(f"Error: {e}")
                    log.error(f"Unsubscribe failed: {e}", exc_info=True)
            
            elif action == "status":
                try:
                    print_status(engine)
                except Exception as e:
                    print(f"Error: {e}")
                    log.error(f"Status failed: {e}", exc_info=True)
            
            elif action == "metrics":
                try:
                    print_metrics(engine)
                except Exception as e:
                    print(f"Error: {e}")
                    log.error(f"Metrics failed: {e}", exc_info=True)
            
            elif action == "reset":
                try:
                    engine.metrics_reset()
                    print("OK - metrics reset")
                except Exception as e:
                    print(f"Error: {e}")
                    log.error(f"Reset failed: {e}", exc_info=True)
            
            elif action == "stop":
                print("Stopping engine...")
                break
            
            else:
                print(f"Unknown command: {action}")
                print("Valid commands: subscribe, unsubscribe, status, metrics, reset, stop")
    
    except KeyboardInterrupt:
        print("\nInterrupted, stopping...")
    finally:
        print("\nShutting down...")
        engine.stop()
        print("Stopped.")


if __name__ == "__main__":
    main()
