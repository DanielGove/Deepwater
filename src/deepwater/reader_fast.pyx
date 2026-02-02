# cython: boundscheck=False, wraparound=False, cdivision=True, language_level=3
"""
Cython-optimized hot paths for Reader.
Compiles to C for 5-10x speedup on binary search and iteration loops.
"""
cimport cython
from libc.stdint cimport uint64_t, uint8_t, int64_t
from libc.string cimport memcpy

cdef inline uint64_t read_u64_le(const uint8_t[:] buf, size_t offset) nogil:
    """Extract little-endian uint64 from buffer - pure C speed"""
    return (<uint64_t*>&buf[offset])[0]

@cython.boundscheck(False)
@cython.wraparound(False)
def binary_search_fast(
    const uint8_t[:] buf,
    uint64_t start_us,
    size_t write_pos,
    size_t rec_size,
    size_t ts_offset
) -> size_t:
    """
    Binary search for start position in sorted chunk.
    
    Args:
        buf: Chunk buffer (memoryview)
        start_us: Target timestamp in microseconds
        write_pos: Current write position in buffer
        rec_size: Size of each record in bytes
        ts_offset: Offset to timestamp field in record
    
    Returns:
        Byte position of first record >= start_us
    """
    cdef size_t left = 0
    cdef size_t right = write_pos // rec_size
    cdef size_t mid, mid_pos
    cdef uint64_t ts
    
    while left < right:
        mid = (left + right) >> 1  # Bit shift faster than // 2
        mid_pos = mid * rec_size
        ts = read_u64_le(buf, mid_pos + ts_offset)
        
        if ts < start_us:
            left = mid + 1
        else:
            right = mid
    
    return left * rec_size


@cython.boundscheck(False)
@cython.wraparound(False)
def range_tuples_fast(
    const uint8_t[:] buf,
    size_t start_pos,
    size_t write_pos,
    uint64_t start_us,
    uint64_t end_us,
    size_t rec_size,
    size_t ts_offset,
    object unpack_func
) -> list:
    """
    Fast iteration over range with timestamp filtering.
    
    Args:
        buf: Chunk buffer
        start_pos: Starting byte position (from binary search)
        write_pos: Write head position
        start_us: Start timestamp filter
        end_us: End timestamp filter (or 0 for no limit)
        rec_size: Record size in bytes
        ts_offset: Timestamp field offset
        unpack_func: struct.unpack_from function
    
    Returns:
        List of unpacked tuples
    """
    cdef list result = []
    cdef size_t pos = start_pos
    cdef uint64_t ts
    cdef object record
    
    while pos + rec_size <= write_pos:
        ts = read_u64_le(buf, pos + ts_offset)
        
        # Skip records before start
        if ts < start_us:
            pos += rec_size
            continue
        
        # Stop at end timestamp
        if end_us > 0 and ts > end_us:
            break
        
        # Unpack and append (Python call, but minimized)
        record = unpack_func(buf, pos)
        result.append(record)
        pos += rec_size
    
    return result


@cython.boundscheck(False)
@cython.wraparound(False)
def extract_timestamp_fast(
    const uint8_t[:] buf,
    size_t pos,
    size_t ts_offset
) -> uint64_t:
    """Fast timestamp extraction for single record"""
    return read_u64_le(buf, pos + ts_offset)
