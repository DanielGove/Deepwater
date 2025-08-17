import signal
from pathlib import Path
from core.global_registry import GlobalRegistry
from core.feed_registry import FeedRegistry
from core.writer import Writer
from core.index import ChunkIndex
#from core.reader import FeedReader  # Assuming you have a reader implementation

class Platform:
    """Enhanced HFT platform with crash recovery"""

    def __init__(self, base_path: str = "./platform_data"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Binary registry instead of JSON
        self.registry = GlobalRegistry(self.base_path)

        # Process-local caches
        self.writers = {}
        self.readers = {}
        self.indexes = {}

        print(f"🚀 HFT Platform initialized at {self.base_path}")

    def create_feed(self, feed_name: str, **config) -> Writer:
        """Create or get feed writer with crash recovery"""
        return Writer(self, feed_name, config)
    
    def register_feed(self, feed_name: str, config: dict):
        self.registry.register_feed(feed_name, config)

        # Create the directory for the feed
        feed_dir = self.base_path / "data" / feed_name
        feed_dir.mkdir(parents=True, exist_ok=True)

    def get_or_create_index(self, feed_name: str) -> ChunkIndex:
        """Get or create feed index"""
        if feed_name not in self.indexes:
            self.indexes[feed_name] = ChunkIndex(feed_name, 0, base_path=self.base_path, create=True)

        return self.indexes[feed_name]

    def cleanup_dead_writers(self):
        """Clean up chunks owned by dead processes"""
        print("🧹 Cleaning up dead writers...")
        # Implementation would scan SHM segments and clean up dead owners

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"🛑 Received signal {signum}, shutting down...")
        self.close()

    def feed_exists(self, feed_name: str) -> bool:
        """Check if a feed exists in the registry"""
        return self.registry.feed_exists(feed_name)

    def close(self):
        """Clean shutdown"""
        for writer in self.writers.values():
            writer.close()

        for index in self.indexes.values():
            index.close()

        self.registry.close()

        self.writers.clear()
        self.readers.clear()
        self.indexes.clear()

        print("✅ Platform shutdown complete")