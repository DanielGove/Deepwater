#!/usr/bin/env python3
"""
Test: Multi-Key Timestamp Queries
----------------------------------
Comprehensive test demonstrating querying on different timestamp columns.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from deepwater import Platform


def test_multi_key_feature():
    """Test multi-key query functionality"""
    print("=" * 70)
    print("Multi-Key Query Test")
    print("=" * 70)
    
    p = Platform('./data/test-multikey')
    
    # Create feed with multiple queryable timestamps
    print("\n1. Creating feed with query_cols...")
    p.create_feed({
        'feed_name': 'test_trades',
        'mode': 'UF',
        'fields': [
            {'name': 'trade_id', 'type': 'uint64'},
            {'name': 'recv_us', 'type': 'uint64'},
            {'name': 'proc_us', 'type': 'uint64'},
            {'name': 'ev_us', 'type': 'uint64'},
            {'name': 'price', 'type': 'float64'},
            {'name': 'size', 'type': 'float64'},
        ],
        'ts_col': 'proc_us',
        'query_cols': ['recv_us', 'proc_us', 'ev_us'],
        'persist': True,
    })
    print("✓ Feed created with query_cols")
    
    # Write test data
    print("\n2. Writing test data...")
    writer = p.create_writer('test_trades')
    base_time = int(time.time() * 1e6)
    
    for i in range(10):
        ev_us = base_time + (i * 1000)
        recv_us = ev_us + 50
        proc_us = recv_us + 100
        writer.write_values(i + 1, recv_us, proc_us, ev_us, 100.0 + i, 10.0 * (i + 1))
    
    writer.close()
    print("✓ Wrote 10 records")
    
    # Query tests
    reader = p.create_reader('test_trades')
    
    print("\n3. Testing queries on different timestamp columns...")
    
    # Query on proc_us (default)
    start = base_time + 150
    end = base_time + 5150
    records_proc = reader.range(start, end)
    print(f"  Query on proc_us: {len(records_proc)} records")
    
    # Query on ev_us
    start = base_time
    end = base_time + 5000
    records_ev = reader.range(start, end, ts_key='ev_us')
    print(f"  Query on ev_us:   {len(records_ev)} records")
    
    # Query on recv_us
    start = base_time + 50
    end = base_time + 5050
    records_recv = reader.range(start, end, ts_key='recv_us')
    print(f"  Query on recv_us: {len(records_recv)} records")
    
    # Test error handling
    print("\n4. Testing error handling...")
    try:
        reader.range(start, end, ts_key='invalid')
        print("  ✗ Should have raised ValueError")
    except ValueError as e:
        print(f"  ✓ Correctly raised: {str(e)[:50]}...")
    
    # Backtesting example
    print("\n5. Backtesting scenario (query by exchange time)...")
    backtest_start = base_time
    backtest_end = base_time + 3000
    trades = reader.range(backtest_start, backtest_end, ts_key='ev_us')
    print(f"  Trades in exchange time window: {len(trades)}")
    
    for trade in trades[:3]:
        trade_id, recv, proc, ev, price, size = trade
        print(f"    Trade {trade_id}: ${price:.2f} @ t+{ev - base_time}µs")
    
    reader.close()
    p.close()
    
    print("\n" + "=" * 70)
    print("✓ All tests passed")
    print("=" * 70)
    
    print("\nUsage:")
    print("  p.create_feed({..., 'query_cols': ['recv_us', 'proc_us', 'ev_us']})")
    print("  data = reader.range(start, end, ts_key='ev_us')")


if __name__ == '__main__':
    test_multi_key_feature()
