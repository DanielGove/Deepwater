import numpy as np
import mmap
from multiprocessing import shared_memory
from typing import Optional


class Chunk:
    """Memory chunk with proper SHM coordination"""

    def __init__(self, name: str, size: int, create: bool = False, file_path: Optional[str] = None):
        self.name = name
        self.size = size
        self.file_path = file_path

        if file_path:
            if create:
                with open(file_path, 'wb') as f:
                    f.write(b'\x00' * size)

            self.file = open(file_path, 'r+b')
            self.mmap = mmap.mmap(self.file.fileno(), size)
            self.buffer = memoryview(self.mmap)
            self.is_shm = False
        else:
            if create:
                self.shm = shared_memory.SharedMemory(name=name, create=True, size=size)
            else:
                self.shm = shared_memory.SharedMemory(name=name)

            self.buffer = memoryview(self.shm.buf)
            self.is_shm = True

    def memview(self) -> memoryview:
        return self.buffer

    def write_bytes(self, position: int, data: bytes) -> int:
        """Write bytes and return new position"""
        if position + len(data) > self.size:
            raise ValueError(f"Write would exceed chunk capacity")
        self.buffer[position:position + len(data)] = data
        return position + len(data)

    def read_bytes(self, position: int, length: int) -> bytes:
        """Read bytes from chunk"""
        if position + length > self.size:
            raise ValueError(f"Read would exceed chunk capacity")
        return bytes(self.buffer[position:position + length])

    def persist_to_disk(self, path: str):
        """Save SHM chunk to disk file"""
        with open(path, 'wb') as f:
            f.write(bytes(self.buffer))

    def close(self):
        if self.is_shm:
            self.buffer = None
            self.shm.close()
            self.shm.unlink()
        else:
            self.buffer = None
            self.mmap.close()
            self.file.close()