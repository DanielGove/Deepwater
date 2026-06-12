# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False
from __future__ import annotations

import datetime as _dt

from cpython.bytearray cimport PyByteArray_AsString, PyByteArray_Check, PyByteArray_Size
from cpython.bytes cimport PyBytes_AsStringAndSize, PyBytes_Check
from cpython.unicode cimport PyUnicode_AsUTF8AndSize, PyUnicode_Check
from libc.stdint cimport int64_t


cdef inline int64_t _days_from_civil(int y, unsigned int m, unsigned int d) noexcept:
    cdef int era
    cdef unsigned int yoe, doy, doe
    if m <= 2:
        y -= 1
    era = y // 400 if y >= 0 else (y - 399) // 400
    yoe = <unsigned int>(y - era * 400)
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + yoe // 400 + doy
    return <int64_t>era * 146097 + doe - 719468


cdef inline int _d2(const unsigned char* s, Py_ssize_t i) noexcept:
    return (s[i] - 48) * 10 + s[i + 1] - 48


cdef inline int _d4(const unsigned char* s, Py_ssize_t i) noexcept:
    return (s[i] - 48) * 1000 + (s[i + 1] - 48) * 100 + (s[i + 2] - 48) * 10 + s[i + 3] - 48


cdef int64_t _parse_us(const unsigned char* s, Py_ssize_t n) noexcept:
    cdef int year = _d4(s, 0)
    cdef unsigned int month = <unsigned int>_d2(s, 5)
    cdef unsigned int day = <unsigned int>_d2(s, 8)
    cdef int64_t secs = _days_from_civil(year, month, day) * 86400
    cdef Py_ssize_t end = n - 1 if n > 0 and s[n - 1] == 90 else n
    cdef Py_ssize_t i = 20
    cdef int frac = 0
    cdef int digits = 0
    cdef unsigned char c

    secs += _d2(s, 11) * 3600 + _d2(s, 14) * 60 + _d2(s, 17)
    while i < end and digits < 6:
        c = s[i]
        if c < 48 or c > 57:
            break
        frac = frac * 10 + c - 48
        digits += 1
        i += 1
    while digits < 6:
        frac *= 10
        digits += 1
    return secs * 1000000 + frac


def parse_us_timestamp(object value) -> int:
    cdef char* raw
    cdef Py_ssize_t n
    if PyBytes_Check(value):
        PyBytes_AsStringAndSize(value, &raw, &n)
    elif PyByteArray_Check(value):
        raw = PyByteArray_AsString(value)
        n = PyByteArray_Size(value)
    elif PyUnicode_Check(value):
        raw = <char*>PyUnicode_AsUTF8AndSize(value, &n)
    else:
        raise TypeError("timestamp must be str, bytes, or bytearray")
    if n < 19 or raw == NULL:
        raise ValueError("invalid timestamp")
    return _parse_us(<const unsigned char*>raw, n)


def us_to_iso(ts_us: int, *, with_z: bool = True, precision: int = 6) -> str:
    if precision < 0 or precision > 6:
        raise ValueError("precision must be between 0 and 6")
    seconds, micros = divmod(int(ts_us), 1000000)
    dt = _dt.datetime.fromtimestamp(seconds, _dt.UTC)
    frac = "" if precision == 0 else f".{int(micros):06d}"[: precision + 1]
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + frac + ("Z" if with_z else "")
