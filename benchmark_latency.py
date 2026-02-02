#!/usr/bin/env python3
"""
Deepwater End-to-End Latency Benchmark

Measures complete pipeline latency from Coinbase exchange to reader consumption.
Tests both L2 order book updates and trade executions.
"""

import time
import subprocess
import statistics
from typing import List, Dict
from src.deepwater.platform import Platform
from src.deepwater.reader import Reader


def measure_network_baseline() -> Dict[str, float]:
    """Ping Coinbase websocket to establish network baseline"""
    print("=" * 80)
    print("NETWORK BASELINE")
    print("=" * 80)
    print("Pinging Coinbase websocket (ws-feed.exchange.coinbase.com)...")
    
    try:
        result = subprocess.run(
            ['ping', '-c', '10', '-W', '2', 'ws-feed.exchange.coinbase.com'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # Parse ping output for statistics
        lines = result.stdout.split('\n')
        for line in lines:
            if 'rtt min/avg/max/mdev' in line:
                # Format: rtt min/avg/max/mdev = 11.549/17.060/40.148/8.159 ms
                stats = line.split('=')[1].strip().split()[0]
                min_ms, avg_ms, max_ms, mdev = map(float, stats.split('/'))
                
                print(f"  Min:     {min_ms:6.2f}ms")
                print(f"  Average: {avg_ms:6.2f}ms")
                print(f"  Max:     {max_ms:6.2f}ms")
                print(f"  Std Dev: {mdev:6.2f}ms")
                print()
                
                return {
                    'min': min_ms,
                    'avg': avg_ms,
                    'max': max_ms,
                    'stdev': mdev
                }
    except Exception as e:
        print(f"  ⚠ Could not measure network baseline: {e}")
        print()
        return {'min': 0, 'avg': 0, 'max': 0, 'stdev': 0}
    
    return {'min': 0, 'avg': 0, 'max': 0, 'stdev': 0}


def benchmark_feed(feed_name: str, num_samples: int = 500, timeout_sec: int = 30) -> Dict:
    """Benchmark a single feed and return statistics"""
    
    print("=" * 80)
    print(f"BENCHMARKING: {feed_name}")
    print("=" * 80)
    
    # Initialize reader
    p = Platform('./data/coinbase-test')
    r = Reader(p, feed_name)
    
    # Get field indices
    try:
        ev_us_idx = r.field_names.index('ev_us')
        recv_us_idx = r.field_names.index('recv_us')
        proc_us_idx = r.field_names.index('proc_us')
    except ValueError as e:
        print(f"  ✗ Missing required timestamp fields: {e}")
        return None
    
    print(f"Fields: {', '.join(r.field_names)}")
    print(f"Target: {num_samples} samples (timeout: {timeout_sec}s)")
    print()
    
    # Collect samples
    samples = []
    count = 0
    start_time = time.time()
    last_print = 0
    
    print("Streaming live data...")
    print()
    
    try:
        for rec in r.stream(start=None, format='tuple'):
            read_us = time.time_ns() // 1000
            ev_us = rec[ev_us_idx]
            recv_us = rec[recv_us_idx]
            proc_us = rec[proc_us_idx]
            
            # Calculate latency components (all in ms)
            network_ms = (recv_us - ev_us) / 1000
            websocket_ms = (proc_us - recv_us) / 1000
            reader_ms = (read_us - proc_us) / 1000
            total_ms = (read_us - ev_us) / 1000
            
            samples.append({
                'network': network_ms,
                'websocket': websocket_ms,
                'reader': reader_ms,
                'total': total_ms
            })
            
            count += 1
            
            # Print progress every 100 samples or every 2 seconds
            elapsed = time.time() - start_time
            if count % 100 == 0 or elapsed - last_print >= 2:
                rate = count / elapsed if elapsed > 0 else 0
                print(f"  {count:4d} samples | {rate:5.1f} rec/sec | "
                      f"latency: {total_ms:5.2f}ms", end='\r')
                last_print = elapsed
            
            # Stop conditions
            if count >= num_samples:
                break
            if elapsed > timeout_sec:
                print(f"\n  ⚠ Timeout after {elapsed:.1f}s")
                break
                
    except KeyboardInterrupt:
        print("\n  ⚠ Interrupted by user")
    
    elapsed = time.time() - start_time
    print()  # Clear progress line
    print()
    
    if not samples:
        print("  ✗ No data received")
        return None
    
    # Calculate statistics
    network = [s['network'] for s in samples]
    websocket = [s['websocket'] for s in samples]
    reader = [s['reader'] for s in samples]
    total = [s['total'] for s in samples]
    
    def stats(data: List[float]) -> Dict[str, float]:
        sorted_data = sorted(data)
        return {
            'min': min(data),
            'p50': sorted_data[len(data) // 2],
            'p95': sorted_data[int(len(data) * 0.95)],
            'p99': sorted_data[int(len(data) * 0.99)],
            'avg': statistics.mean(data),
            'max': max(data),
            'stdev': statistics.stdev(data) if len(data) > 1 else 0
        }
    
    return {
        'feed': feed_name,
        'count': count,
        'duration': elapsed,
        'rate': count / elapsed,
        'network': stats(network),
        'websocket': stats(websocket),
        'reader': stats(reader),
        'total': stats(total)
    }


def print_results(results: Dict):
    """Print formatted benchmark results"""
    if not results:
        return
    
    print("RESULTS")
    print("=" * 80)
    print(f"Feed:     {results['feed']}")
    print(f"Samples:  {results['count']} records in {results['duration']:.1f}s ({results['rate']:.1f} rec/sec)")
    print()
    
    # Table header
    print(f"{'Component':<20} {'Min':<10} {'P50':<10} {'P95':<10} {'P99':<10} {'Avg':<10} {'Max':<10}")
    print("-" * 80)
    
    # Print each component
    for component in ['network', 'websocket', 'reader', 'total']:
        s = results[component]
        label = {
            'network': 'Network (CB→Recv)',
            'websocket': 'WebSocket (Parse)',
            'reader': 'Reader (IPC)',
            'total': 'Total E2E'
        }[component]
        
        print(f"{label:<20} "
              f"{s['min']:>7.2f}ms "
              f"{s['p50']:>7.2f}ms "
              f"{s['p95']:>7.2f}ms "
              f"{s['p99']:>7.2f}ms "
              f"{s['avg']:>7.2f}ms "
              f"{s['max']:>7.2f}ms")
    
    print()


def main():
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "DEEPWATER LATENCY BENCHMARK" + " " * 31 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    # Measure network baseline
    network_baseline = measure_network_baseline()
    
    # Benchmark L2 feed
    l2_results = benchmark_feed('CB-L2-XRP-USD', num_samples=500, timeout_sec=30)
    if l2_results:
        print_results(l2_results)
    
    # Benchmark trades feed
    trades_results = benchmark_feed('CB-TRADES-XRP-USD', num_samples=100, timeout_sec=60)
    if trades_results:
        print_results(trades_results)
    
    # Summary comparison
    if l2_results and trades_results:
        print("=" * 80)
        print("COMPARISON")
        print("=" * 80)
        print(f"{'Metric':<30} {'L2 Updates':<20} {'Trades':<20}")
        print("-" * 80)
        print(f"{'Throughput':<30} {l2_results['rate']:>7.1f} rec/sec     {trades_results['rate']:>7.1f} rec/sec")
        print(f"{'Reader Latency (P50)':<30} {l2_results['reader']['p50']:>7.2f}ms          {trades_results['reader']['p50']:>7.2f}ms")
        print(f"{'Reader Latency (P99)':<30} {l2_results['reader']['p99']:>7.2f}ms          {trades_results['reader']['p99']:>7.2f}ms")
        print(f"{'Total E2E Latency (P50)':<30} {l2_results['total']['p50']:>7.2f}ms          {trades_results['total']['p50']:>7.2f}ms")
        print()
    
    print("=" * 80)
    print("✓ BENCHMARK COMPLETE")
    print("=" * 80)
    print()


if __name__ == '__main__':
    main()
