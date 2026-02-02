"""Deepwater - Zero-copy market data substrate

Ultra-low latency time-series storage optimized for financial market data.

Quick Start:
    >>> from deepwater import Platform
    >>> 
    >>> # Initialize platform
    >>> p = Platform('./my_data')
    >>> 
    >>> # Create feed
    >>> p.create_feed({
    ...     'feed_name': 'trades',
    ...     'mode': 'UF',  # Unindexed feed
    ...     'fields': [
    ...         {'name': 'price', 'type': 'float64'},
    ...         {'name': 'size', 'type': 'float64'},
    ...         {'name': 'timestamp_us', 'type': 'uint64'},
    ...     ],
    ...     'ts_col': 'timestamp_us',
    ...     'persist': True,
    ... })
    >>> 
    >>> # Write data (60µs latency)
    >>> writer = p.create_writer('trades')
    >>> writer.write_values(123.45, 100.0, 1738368000000000)
    >>> writer.close()
    >>> 
    >>> # Read live stream (70µs latency)
    >>> reader = p.create_reader('trades')
    >>> for record in reader.stream():  # Infinite stream of new data
    ...     print(record)
    >>> 
    >>> # Read historical range (920K rec/sec)
    >>> start_us = 1738368000000000
    >>> end_us = start_us + 60_000_000  # +60 seconds
    >>> records = reader.range(start_us, end_us)
    >>> print(f'Got {len(records)} records')

Performance:
    - Writer: 60µs per write (cross-process)
    - Reader: 70µs IPC latency (live streaming)
    - Historical: 920K records/sec (backtest replay)
    - Zero-copy: Direct memory access, no serialization
    - Persistent: Survives crashes, supports replay

Core API:
    Platform: Entry point for all operations
    Writer: Write records to feeds (60µs)
    Reader: Read records from feeds (70µs latency, 920K rec/sec throughput)
"""

__version__ = "0.0.1"

# Export public API
from .platform import Platform
from .writer import Writer
from .reader import Reader

__all__ = ["Platform", "Writer", "Reader", "__version__"]
