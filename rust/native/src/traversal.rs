use crate::buffer::{read_u64_le, validate_record_layout, BufferView};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyfunction]
fn chunk_lower_bound(
    buf: &Bound<'_, PyAny>,
    start_us: u64,
    write_pos: usize,
    rec_size: usize,
    ts_offset: usize,
) -> PyResult<usize> {
    validate_record_layout(rec_size, ts_offset)?;
    let view = BufferView::new(buf)?;
    let data = view.as_slice()?;
    if write_pos > data.len() {
        return Err(PyValueError::new_err("write_pos exceeds buffer length"));
    }

    let mut left = 0usize;
    let mut right = write_pos / rec_size;
    while left < right {
        let mid = (left + right) >> 1;
        let mid_pos = mid
            .checked_mul(rec_size)
            .ok_or_else(|| PyValueError::new_err("record offset overflow"))?;
        let ts = read_u64_le(data, mid_pos + ts_offset)?;
        if ts < start_us {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left.checcccccccccccccccccked_mul(rec_size)
        .ok_or_else(|| PyValueError::new_err("record offset overflow"))
}

#[pyfunction]
fn ring_lower_bound(
    buf: &Bound<'_, PyAny>,
    start_pos: usize,
    earliest_seq: u64,
    start_seq: u64,
    end_seq: u64,
    target_ts: u64,
    ts_offset: usize,
    rec_size: usize,
    usable: usize,
) -> PyResult<u64> {
    validate_record_layout(rec_size, ts_offset)?;
    if usable == 0 {
        return Err(PyValueError::new_err("usable must be positive"));
    }
    if start_pos >= usable {
        return Err(PyValueError::new_err(
            "start_pos must be within usable bytes",
        ));
    }
    let view = BufferView::new(buf)?;
    let data = view.as_slice()?;
    if usable > data.len() {
        return Err(PyValueError::new_err("usable exceeds buffer length"));
    }

    let mut left = start_seq;
    let mut right = end_seq;
    while left < right {
        let mid = (left + right) >> 1;
        let rel = mid
            .checked_sub(earliest_seq)
            .ok_or_else(|| PyValueError::new_err("sequence precedes earliest_seq"))?;
        let byte_delta = (rel as usize)
            .checked_mul(rec_size)
            .ok_or_else(|| PyValueError::new_err("record offset overflow"))?;
        let pos = (start_pos + byte_delta) % usable;
        let ts = read_u64_le(data, pos + ts_offset)?;
        if ts < target_ts {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    Ok(left)
}

#[pyfunction]
fn copy_raw_batch<'py>(
    py: Python<'py>,
    buf: &Bound<'_, PyAny>,
    start_pos: usize,
    end_pos: usize,
) -> PyResult<Bound<'py, PyBytes>> {
    let view = BufferView::new(buf)?;
    let data = view.as_slice()?;
    if end_pos < start_pos {
        return Err(PyValueError::new_err("end_pos must be >= start_pos"));
    }
    let slice = data
        .get(start_pos..end_pos)
        .ok_or_else(|| PyValueError::new_err("batch span exceeds buffer length"))?;
    Ok(PyBytes::new_bound(py, slice))
}

#[pyfunction]
fn ring_copy_records<'py>(
    py: Python<'py>,
    buf: &Bound<'_, PyAny>,
    pos: usize,
    n_records: u64,
    rec_size: usize,
    usable: usize,
) -> PyResult<Bound<'py, PyBytes>> {
    if rec_size == 0 {
        return Err(PyValueError::new_err("rec_size must be positive"));
    }
    if usable == 0 {
        return Err(PyValueError::new_err("usable must be positive"));
    }
    if pos >= usable {
        return Err(PyValueError::new_err("pos must be within usable bytes"));
    }
    let view = BufferView::new(buf)?;
    let data = view.as_slice()?;
    if usable > data.len() {
        return Err(PyValueError::new_err("usable exceeds buffer length"));
    }
    let nbytes = (n_records as usize)
        .checked_mul(rec_size)
        .ok_or_else(|| PyValueError::new_err("copy size overflow"))?;
    if nbytes == 0 {
        return Ok(PyBytes::new_bound(py, b""));
    }
    if nbytes > usable {
        return Err(PyValueError::new_err("copy size exceeds usable ring bytes"));
    }

    let end_pos = pos
        .checked_add(nbytes)
        .ok_or_else(|| PyValueError::new_err("copy offset overflow"))?;
    if end_pos <= usable {
        return Ok(PyBytes::new_bound(py, &data[pos..end_pos]));
    }

    let second = end_pos - usable;
    let mut out = Vec::with_capacity(nbytes);
    out.extend_from_slice(&data[pos..usable]);
    out.extend_from_slice(&data[..second]);
    Ok(PyBytes::new_bound(py, &out))
}

pub(crate) fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(chunk_lower_bound, m)?)?;
    m.add_function(wrap_pyfunction!(ring_lower_bound, m)?)?;
    m.add_function(wrap_pyfunction!(copy_raw_batch, m)?)?;
    m.add_function(wrap_pyfunction!(ring_copy_records, m)?)?;
    Ok(())
}
