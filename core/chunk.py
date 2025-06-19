import numpy as np
import mmap
from multiprocessing import shared_memory
from typing import Optional


class ChunkHeader:
    """Enhanced chunk header with crash recovery support"""
    # Layout: write_pos(8) + record_count(8) + start_time(8) + end_time(8) +
    #         owner_pid(8) + last_update(8) + checksum(8) + reserved(16) = 64 bytes
    SIZE = 64

    def __init__(self, buffer: memoryview, offset: int = 0):
        self.buffer = buffer
        self.offset = offset
        self._header = np.frombuffer(buffer[offset:offset + self.SIZE], dtype=np.int64)

    @property
    def write_pos(self) -> int:
        return int(self._header[0])

    @write_pos.setter
    def write_pos(self, value: int):
        self._header[0] = value
        self._update_checksum()

    @property
    def record_count(self) -> int:
        return int(self._header[1])

    @record_count.setter
    def record_count(self, value: int):
        self._header[1] = value
        self._update_checksum()

    @property
    def start_time(self) -> int:
        return int(self._header[2])

    @start_time.setter
    def start_time(self, value: int):
        self._header[2] = value
        self._update_checksum()

    @property
    def end_time(self) -> int:
        return int(self._header[3])

    @end_time.setter
    def end_time(self, value: int):
        self._header[3] = value
        self._update_checksum()

    @property
    def owner_pid(self) -> int:
        return int(self._header[4])

    @owner_pid.setter
    def owner_pid(self, value: int):
        self._header[4] = value
        self._update_checksum()

    @property
    def last_update(self) -> int:
        return int(self._header[5])

    @last_update.setter
    def last_update(self, value: int):
        self._header[5] = value
        self._update_checksum()

    @property
    def checksum(self) -> int:
        return int(self._header[6])

    def _update_checksum(self):
        """Update checksum of header data (excluding checksum field itself)"""
        data = self._header[:6].tobytes()  # All fields except checksum
        self._header[6] = hash(data) & 0x7FFFFFFFFFFFFFFF  # Keep positive

    def validate_checksum(self) -> bool:
        """Validate header integrity"""
        expected = hash(self._header[:6].tobytes()) & 0x7FFFFFFFFFFFFFFF
        return self.checksum == expected

    def cleanup(self):
        """Clean up references for proper SHM closure"""
        self._header = None
        self.buffer = None


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

        # Header management
        self.header = ChunkHeader(self.buffer)
        self.data_offset = ChunkHeader.SIZE
        self.data_size = size - self.data_offset

        # Initialize header if creating
        if create and not file_path:
            self.header.write_pos = 0
            self.header.record_count = 0
            self.header.start_time = 0
            self.header.end_time = 0
            self.header.owner_pid = 0
            self.header.last_update = 0

    def write_bytes(self, position: int, data: bytes) -> int:
        """Write bytes and return new position"""
        if position + len(data) > self.data_size:
            raise ValueError(f"Write would exceed chunk capacity")

        offset = self.data_offset + position
        self.buffer[offset:offset + len(data)] = data
        return position + len(data)

    def read_bytes(self, position: int, length: int) -> bytes:
        """Read bytes from chunk"""
        if position + length > self.data_size:
            raise ValueError(f"Read would exceed chunk capacity")

        offset = self.data_offset + position
        return bytes(self.buffer[offset:offset + length])

    def available_space(self) -> int:
        """Available space for writing"""
        return self.data_size - self.header.write_pos

    def persist_to_disk(self, path: str):
        """Save SHM chunk to disk file"""
        with open(path, 'wb') as f:
            f.write(bytes(self.buffer))

    def close(self):
        if self.is_shm:
            try:
                if hasattr(self, 'header'):
                    self.header.cleanup()
                self.buffer = None
                self.shm.close()
            except Exception:
                pass
        else:
            try:
                if hasattr(self, 'header'):
                    self.header.cleanup()
                self.buffer = None
                if hasattr(self, 'mmap'):
                    self.mmap.close()
                if hasattr(self, 'file'):
                    self.file.close()
            except Exception:
                pass

    def unlink(self):
        if self.is_shm:
            try:
                self.shm.unlink()
            except Exception:
                pass