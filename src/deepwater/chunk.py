import numpy as np
import mmap
import os
from multiprocessing import shared_memory
from typing import Optional


class Chunk:

    def __init__(self, buffer: memoryview, closeables: list, read_only: bool, is_shm:bool):
        self.buffer = buffer
        self.closeables = closeables
        self.read_only = read_only
        self.is_shm = is_shm
        self.size = len(buffer)

    @classmethod
    def create_shm(cls, name: str, size: int):
        shm = shared_memory.SharedMemory(name=name, create=True, size=size)
        return cls(buffer=shm.buf, closeables=[shm], read_only=False, is_shm=True)
    
    @classmethod
    def open_shm(cls, name: str):
        shm = shared_memory.SharedMemory(name=name, create=False)
        return cls(buffer=shm.buf, closeables=[shm], read_only=True, is_shm=True)

    def close_shm(self):
        self.buffer.release()
        self.closeables[0].close()
        if self.read_only is False:
            self.closeables[0].unlink()
    
    @classmethod
    def create_file(cls, path: str, size: int):
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
        os.ftruncate(fd, size)
        mm = mmap.mmap(fd, length=size, access=mmap.ACCESS_WRITE)
        return cls(memoryview(mm), [mm, fd], read_only=False, is_shm=False)
    
    @classmethod
    def open_file(cls, file_path: str):
        fd = os.open(file_path, os.O_RDONLY)
        mm = mmap.mmap(fd, length=0, access=mmap.ACCESS_READ)
        return cls(memoryview(mm), [mm, fd], read_only=True, is_shm=False)

    def close_file(self):
        self.buffer.release()
        mm = self.closeables[0]
        fd = self.closeables[1]
        if not self.read_only:
            mm.flush()
            os.ftruncate(fd, self.size)
            os.fsync(fd)
        mm.close()
        os.close(fd)