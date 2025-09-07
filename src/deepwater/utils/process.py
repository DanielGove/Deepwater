import os

class ProcessUtils:
    """Utilities for process management and validation"""

    @staticmethod
    def is_process_alive(pid: int) -> bool:
        """Check if process is alive"""
        if pid <= 0:
            return False
        try:
            # Send signal 0 to check if process exists
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False

    @staticmethod
    def get_current_pid() -> int:
        return os.getpid()