import numpy as np
import mmap
from multiprocessing import shared_memory
from typing import Optional


class ChunkHeader:
    """Enhanced chunk header with crash recovery support"""
    # Layout: write_pos(8) + record_count(8) + start_time(8) + end_time(8) +
    #         owner_pid(8) + last_update(8) + checksum(8) + reserved(16) = 64 bytes
    SIZE = 64

    __slots__ = (
        "write_pos", "record_cnt", "start_time",
        "end_time", "owner_pid", "last_update",
    )

    def __init__(self, buffer: memoryview):
        self.write_pos    = buffer[0 :8 ].cast("Q")
        self.record_cnt   = buffer[8 :16].cast("Q")
        self.start_time   = buffer[16:24].cast("Q")
        self.end_time     = buffer[24:32].cast("Q")
        self.last_update  = buffer[32:40].cast("Q")
        self.owner_pid    = buffer[40:48].cast("Q")

    def cleanup(self):
        """Clean up references for proper SHM closure"""
        self.write_pos.release()
        self.record_cnt.release()
        self.start_time.release()
        self.end_time.release()
        self.last_update.release()
        self.owner_pid.release()


class Chunk:
    """Memory chunk with proper SHM coordination"""

    def __init__(self, name: str, size: int, create: bool = False, file_path: Optional[str] = None):
        self.name = name
        self.size = size
        self.file_path = file_path

        if file_path:
            # File-backed chunk
            if create:
                with open(file_path, 'wb') as f:
                    f.write(b'\x00' * size)

            self.file = open(file_path, 'r+b')
            self.mmap = mmap.mmap(self.file.fileno(), size)
            self.buffer = memoryview(self.mmap)
            self.is_shm = False
        else:
            # Shared memory chunk
            if create:
                self.shm = shared_memory.SharedMemory(name=name, create=True, size=size)
            else:
                self.shm = shared_memory.SharedMemory(name=name)

            self.buffer = memoryview(self.shm.buf)
            self.is_shm = True

        self.header = ChunkHeader(self.buffer)

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

    def available_space(self) -> int:
        """Available space for writing"""
        return self.size - self.header.write_pos[0]

    def persist_to_disk(self, path: str):
        """Save SHM chunk to disk file"""
        with open(path, 'wb') as f:
            f.write(bytes(self.buffer))

    def close(self):
        if self.is_shm:
            self.header.cleanup()
            self.buffer = None
            self.shm.close()
            self.shm.unlink()
        else:
            self.header.cleanup()
            self.buffer = None
            self.mmap.close()
            self.file.close()