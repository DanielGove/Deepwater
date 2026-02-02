# Deepwater Quick Reference

**Ultra-low latency market data storage: 70µs read, 60µs write, 920K rec/sec replay**

## Import

```python
from deepwater import Platform
```

## Setup (One-Time)

```python
p = Platform('./data')

p.create_feed({
    'feed_name': 'trades',
    'mode': 'UF',  # UF = unindexed (fast), IF = indexed (time queries)
    'fields': [
        {'name': 'price', 'type': 'float64'},
        {'name': 'size', 'type': 'float64'},
        {'name': 'timestamp_us', 'type': 'uint64'},
    ],
    'ts_col': 'timestamp_us',
    'persist': True,
})
```

## Write (60µs)

```python
writer = p.create_writer('trades')
writer.write_values(123.45, 100.0, timestamp_us)  # Fastest
writer.write_tuple((123.45, 100.0, timestamp_us))  # Pre-packed
writer.write_dict({'price': 123.45, ...})  # Readable
writer.close()  # Always close!
```

## Read

```python
reader = p.create_reader('trades')
```

### Live Streaming (70µs latency)

```python
# Jump to live (skip history)
for trade in reader.stream():  # start=None
    price, size, timestamp = trade
    # Process immediately

# Resume from timestamp
for trade in reader.stream(start=yesterday_us):
    # Replays history at 920K rec/sec, then goes live at 70µs
```

### Historical (920K rec/sec)

```python
# Time range
records = reader.range(start_us, end_us)

# Recent data
recent = reader.latest(60)  # Last 60 seconds

# Formats
tuples = reader.range(start, end, format='tuple')    # Fast (default)
dicts = reader.range(start, end, format='dict')      # Readable
array = reader.range(start, end, format='numpy')     # Vectorized
raw = reader.range(start, end, format='raw')         # Memoryview
```

### Cleanup

```python
reader.close()
p.close()
```

## Common Patterns

### Live Trading

```python
for trade in reader.stream():  # 70µs latency
    if should_buy(trade):
        execute_order()
```

### Backtesting

```python
data = reader.range(start, end, format='numpy')  # 920K rec/sec
avg = data['price'].mean()
```

### Multi-Process

```python
# Process 1: Writer
writer = p.create_writer('trades')
for event in websocket:
    writer.write_values(...)  # 60µs

# Process 2: Reader
reader = p.create_reader('trades')
for trade in reader.stream():  # 70µs from write
    # Execute strategy
```

## Performance

| Operation | Speed |
|-----------|-------|
| Writer | 60µs per write |
| Reader (live) | 70µs IPC latency |
| Reader (historical) | 920K rec/sec |
| vs Redis | 2x faster (70µs vs 150µs) |
| vs Network | 130x faster (0.07ms vs 9ms) |

## Gotchas

- **stream(start=None)**: Skips history (live only)
- **stream(start=ts)**: Includes history then live
- **stream() is CPU-intensive**: Spin-waits for 70µs latency
- **Always close() writer**: Seals chunk for readers
- **One writer per feed**: Multiple readers OK
- **Timestamps**: uint64 microseconds

## Help

```python
help(Platform)  # Comprehensive docs
help(Reader)    # Reader API
help(Writer)    # Writer API
```

## Types

`uint8`, `uint16`, `uint32`, `uint64`, `int8`, `int16`, `int32`, `int64`, `float32`, `float64`, `char`
