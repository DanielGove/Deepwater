# cython: boundscheck=False, wraparound=False, cdivision=True, language_level=3
from __future__ import annotations

from cpython.buffer cimport PyBUF_WRITE
from libc.errno cimport errno
from libc.stddef cimport size_t
from libc.stdint cimport intptr_t, uintptr_t


ctypedef long off_t


cdef extern from "Python.h":
    object PyMemoryView_FromMemory(char *mem, Py_ssize_t size, int flags)


cdef extern from "fcntl.h" nogil:
    int O_RDWR


cdef extern from "unistd.h" nogil:
    int close(int fd)


cdef extern from "sys/mman.h" nogil:
    int PROT_NONE
    int PROT_READ
    int PROT_WRITE
    int MAP_PRIVATE
    int MAP_ANONYMOUS
    int MAP_SHARED
    int MAP_FIXED

    void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset)
    int munmap(void *addr, size_t length)
    int shm_open(const char *name, int oflag, unsigned int mode)


cdef inline bint _mmap_failed(void *addr) noexcept:
    return <intptr_t>addr == -1


cdef class ShadowMap:
    cdef void *_addr
    cdef size_t _size
    cdef public object view

    def __cinit__(self):
        self._addr = NULL
        self._size = 0
        self.view = None

    cdef void bind(self, void *addr, size_t size):
        self._addr = addr
        self._size = size
        self.view = PyMemoryView_FromMemory(<char *>addr, <Py_ssize_t>size, PyBUF_WRITE)

    def close(self):
        if self.view is not None:
            self.view.release()
            self.view = None
        if self._addr != NULL:
            munmap(self._addr, self._size)
            self._addr = NULL
            self._size = 0

    def __dealloc__(self):
        if self._addr != NULL and self.view is None:
            munmap(self._addr, self._size)
            self._addr = NULL
            self._size = 0


def map_shadow(str shm_name, Py_ssize_t data_offset, Py_ssize_t data_size):
    cdef bytes raw_name
    cdef int fd
    cdef int err
    cdef size_t size = <size_t>data_size
    cdef size_t total = <size_t>(data_size * 2)
    cdef void *base
    cdef void *first
    cdef void *second_addr
    cdef void *second
    cdef ShadowMap shadow

    if data_offset <= 0:
        raise ValueError("data_offset must be > 0")
    if data_size <= 0:
        raise ValueError("data_size must be > 0")

    raw_name = shm_name.encode("utf-8") if shm_name.startswith("/") else ("/" + shm_name).encode("utf-8")
    fd = shm_open(raw_name, O_RDWR, 0)
    if fd < 0:
        raise OSError(errno, "shm_open failed")

    base = mmap(NULL, total, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0)
    if _mmap_failed(base):
        err = errno
        close(fd)
        raise OSError(err, "mmap reserve failed")

    first = mmap(base, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, <off_t>data_offset)
    if _mmap_failed(first) or first != base:
        err = errno
        munmap(base, total)
        close(fd)
        raise OSError(err, "mmap first data view failed")

    second_addr = <void *>(<uintptr_t>base + <uintptr_t>size)
    second = mmap(second_addr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, <off_t>data_offset)
    close(fd)
    if _mmap_failed(second) or second != second_addr:
        err = errno
        munmap(base, total)
        raise OSError(err, "mmap second data view failed")

    shadow = ShadowMap()
    shadow.bind(base, total)
    return shadow
