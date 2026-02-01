# core/platform.py
from __future__ import annotations

import atexit
import signal
from pathlib import Path
import orjson
import struct
from typing import Dict, Optional, Tuple

from .global_registry import GlobalRegistry
from .layout_json import build_layout, save_layout, load_layout
from .feed_registry import FeedRegistry
from .writer import Writer
from .ring import RingWriter
from .reader import Reader
from .manifest import write_manifest, read_manifest
from . import __version__


class Platform:
    """
    Deepwater platform - zero-copy market data substrate.
    
    Entry point for creating feeds, writing data, and querying time-series records.
    Supports both persistent disk storage and transient shared memory rings.
    
    Args:
        base_path: Root directory for all data and metadata (default: "./platform_data")
    
    Raises:
        RuntimeError: If manifest version mismatch detected (prevents data corruption)
    
    Example:
        >>> platform = Platform(base_path="./data")
        >>> platform.create_feed({
        ...     "feed_name": "trades",
        ...     "mode": "UF",
        ...     "fields": [
        ...         {"name": "price", "type": "float64"},
        ...         {"name": "size", "type": "float64"},
        ...         {"name": "timestamp_us", "type": "uint64"},
        ...     ],
        ...     "ts_col": "timestamp_us",
        ...     "persist": True,
        ... })
        >>> writer = platform.create_writer("trades")
        >>> writer.write_values(123.45, 100.0, 1738368000000000)
        >>> writer.close()
        >>> platform.close()
    """

    def __init__(self, base_path: str = "./platform_data"):
        self.base_path = Path(base_path)
        self.data_path = self.base_path / "data"
        self.data_path.mkdir(parents=True, exist_ok=True)
        
        # Configure logging to base_path
        self._setup_logging()
        
        # ensure manifest exists and enforce version compatibility
        manifest = read_manifest(self.base_path)
        if manifest is None:
            write_manifest(self.base_path)
        else:
            mf_ver = manifest.get("deepwater_version")
            if mf_ver and mf_ver != __version__:
                raise RuntimeError(
                    f"Deepwater version mismatch: manifest has {mf_ver}, code is {__version__}. "
                    f"Data was written by a different version. Either use the correct version or "
                    f"migrate the data (delete manifest.json to force re-init, but you may lose data compatibility)."
                )

        self.registry = GlobalRegistry(self.base_path)

        # process-local caches
        self._writers: Dict[str, Writer] = {}
        self._readers: Dict[str, Reader] = {}
        self._layouts: Dict[str, dict] = {}
        self._structs: Dict[str, Tuple[struct.Struct, int]] = {}  # name -> (Struct(fmt), ts_off)
        
        # Graceful shutdown on signals
        self._shutdown_handlers_registered = False
        self._register_shutdown_handlers()

    # -------------------------------------------------------------------------
    # LOGGING
    # -------------------------------------------------------------------------
    def _setup_logging(self):
        """Configure logging to write to base_path/deepwater.log."""
        import logging
        from logging.handlers import RotatingFileHandler
        
        # Only configure if not already configured
        root_logger = logging.getLogger()
        if root_logger.hasHandlers():
            return
        
        log_file = self.base_path / "deepwater.log"
        
        # Rotating file handler (10MB max, keep 5 backups)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        )
        
        # Console handler for warnings and errors
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARNING)
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        )
        
        # Configure root logger
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

    # -------------------------------------------------------------------------
    # FEED CREATION (idempotent, failsafe)
    # -------------------------------------------------------------------------
    def create_feed(self, spec: dict) -> None:
        """
        Create a new feed with specified schema and lifecycle settings.
        
        Idempotent - safe to call multiple times. Enforces schema stability:
        changing field layout requires new feed name or version.
        
        Args:
            spec: Feed specification dictionary with keys:
                - feed_name (str, required): Unique feed identifier
                - mode (str, required): "UF" for uniform format (only mode supported)
                - fields (list[dict], required): Field definitions, each with:
                    - name (str): Field name
                    - type (str): Field type (uint8/16/32/64, int8/16/32/64, 
                                  float32/64, char, _N for padding)
                    - desc (str, optional): Field description
                - ts_col (str, required): Name of timestamp field for time queries
                - chunk_size_mb (int, optional): Chunk size in MB (default: 64)
                - retention_hours (int, optional): Data retention in hours (default: 72)
                - persist (bool, optional): True for disk, False for SHM only (default: True)
                - index_playback (bool, optional): Enable time-indexed queries (default: False)
        
        Raises:
            ValueError: If required fields missing from spec
            RuntimeError: If feed exists with different schema (prevents corruption)
            NotImplementedError: If mode != "UF"
        
        Example:
            >>> platform.create_feed({
            ...     "feed_name": "sensor_data",
            ...     "mode": "UF",
            ...     "fields": [
            ...         {"name": "sensor_id", "type": "uint32"},
            ...         {"name": "value", "type": "float64"},
            ...         {"name": "timestamp_us", "type": "uint64"},
            ...     ],
            ...     "ts_col": "timestamp_us",
            ...     "chunk_size_mb": 32,
            ...     "retention_hours": 24,
            ...     "persist": True,
            ... })
        """
        name = spec["feed_name"]
        mode = spec.get("mode", "UF")
        if mode != "UF":
            raise NotImplementedError("NUF not enabled yet")

        # 1) ensure feed dir
        fdir = self.feed_dir(name)
        fdir.mkdir(parents=True, exist_ok=True)

        # 2) build & persist layout.json atomically (UF format)
        if "fields" not in spec or "ts_col" not in spec:
            raise ValueError("UF spec requires 'fields' and 'ts_col'")
        layout = build_layout(spec["fields"], ts_col=spec["ts_col"])
        # if layout.json exists, enforce schema stability
        lpath = fdir / "layout.json"
        if lpath.exists():
            current = load_layout(fdir)
            if (current["fmt"] != layout["fmt"] or
                current["record_size"] != layout["record_size"] or
                current["ts"]["offset"] != layout["ts"]["offset"]):
                raise RuntimeError(
                    f"layout drift for '{name}': fmt/size/ts_off changed; "
                    f"use a new feed name or version bump"
                )
        else:
            save_layout(fdir, layout)

        # 3) persist lifecycle defaults into GlobalRegistry (create or update)
        lifecycle = {
            "chunk_size_bytes": int(spec.get("chunk_size_mb", 64)*1024*1024),
            "retention_hours":  int(spec.get("retention_hours", 72)),
            "persist":          bool(spec.get("persist", True)),
            "index_playback":   bool(spec.get("index_playback", False)),
        }
        if not self.registry.feed_exists(name):
            self.registry.register_feed(name, lifecycle)
        else:
            self.registry.update_metadata(name, **lifecycle)

        # 4) keep full app spec for ops/debug (optional)
        (fdir / "config.json").write_bytes(orjson.dumps(spec))

        # 5) ensure per-feed registry (binary) exists
        # TODO: confirm your FeedRegistry constructor & semantics
        FeedRegistry(path=fdir / f"{name}.reg", mode="w").close()

        # clear caches for this feed
        self._layouts.pop(name, None)
        self._structs.pop(name, None)

    # -------------------------------------------------------------------------
    # OPEN/CLOSE
    # -------------------------------------------------------------------------
    def create_writer(self, feed_name: str):
        """
        Create or return cached writer for a feed.
        
        Returns Writer (disk) if persist=True, RingWriter (SHM) if persist=False.
        Writers are cached per process - multiple calls return same instance.
        
        Args:
            feed_name: Name of feed to write to (must exist via create_feed)
        
        Returns:
            Writer or RingWriter instance
        
        Raises:
            KeyError: If feed does not exist
        
        Example:
            >>> writer = platform.create_writer("trades")
            >>> writer.write_values(123.45, 100.0, 1738368000000000)
            >>> writer.close()
        """
        w = self._writers.get(feed_name)
        if w is None:
            lifecycle = self.lifecycle(feed_name)
            # persist=True -> Writer (disk chunks), persist=False -> RingWriter (SHM ring)
            if lifecycle.get("persist", True):
                w = Writer(self, feed_name)
            else:
                w = RingWriter(self, feed_name)
            self._writers[feed_name] = w
        return w
    
    def create_reader(self, feed_name: str):
        """
        Create or return cached reader for a feed.
        
        Returns Reader (disk) if persist=True, RingReader (SHM) if persist=False.
        Readers are cached per process - multiple calls return same instance.
        
        Args:
            feed_name: Name of feed to read from (must exist via create_feed)
        
        Returns:
            Reader or RingReader instance
        
        Raises:
            KeyError: If feed does not exist
        
        Example:
            >>> reader = platform.create_reader("trades")
            >>> for record in reader.read(start_us=1738368000000000):
            ...     print(record["price"], record["size"])
            >>> reader.close()
        """
        r = self._readers.get(feed_name)
        if r is None:
            lifecycle = self.lifecycle(feed_name)
            # persist=True -> Reader (chunks), persist=False -> RingReader (ring)
            if lifecycle.get("persist", True):
                r = Reader(self, feed_name)
            else:
                from .ring import RingReader
                r = RingReader(self, feed_name)
            self._readers[feed_name] = r
        return r  

    def close_writer(self, feed_name: str) -> None:
        w = self._writers.pop(feed_name, None)
        if w: w.close()

    # -------------------------------------------------------------------------
    # DISCOVERY / METADATA
    # -------------------------------------------------------------------------
    def feed_dir(self, feed_name: str) -> Path:
        return self.data_path / feed_name

    def get_record_format(self, feed_name: str) -> dict:
        """Load and cache layout.json."""
        lay = self._layouts.get(feed_name)
        if lay is None:
            lay = load_layout(self.feed_dir(feed_name))
            self._layouts[feed_name] = lay
        return lay

    def codec(self, feed_name: str) -> Tuple[struct.Struct, int]:
        """
        Return (Struct(fmt), ts_off) for fast pack/unpack + seek.
        Readers/writers MAY use this to avoid re-parsing layout.json repeatedly.
        """
        c = self._structs.get(feed_name)
        if c is None:
            lay = self.get_record_format(feed_name)
            S = struct.Struct(lay["fmt"])
            c = (S, int(lay["ts"]["offset"]))
            self._structs[feed_name] = c
        return c

    def lifecycle(self, feed_name: str) -> dict:
        """Return lifecycle defaults from the global registry."""
        lc = self.registry.get_metadata(feed_name)
        if lc is None:
            raise KeyError(f"feed '{feed_name}' not found in registry")
        return lc

    def set_lifecycle(self, feed_name: str, **kwargs) -> None:
        """Partial update of lifecycle defaults (e.g., retention_hours=168)."""
        if not self.registry.update_lifecycle(feed_name, **kwargs):
            raise KeyError(f"feed '{feed_name}' not found")

    def list_feeds(self) -> list[dict]:
        """
        List all registered feed names.
        
        Returns:
            List of feed name strings
        
        Example:
            >>> feeds = platform.list_feeds()
            >>> print(feeds)
            ['trades', 'quotes', 'orderbook']
        """
        return self.registry.list_feeds()
    
    def describe_feed(self, feed_name: str) -> dict:
        """Combine registry lifecycle + layout summary for ops."""
        md = self.registry.get_metadata(feed_name)
        if not md:
            raise KeyError(feed_name)
        lay = self.get_record_format(feed_name)
        return {
            "feed_name": feed_name,
            "lifecycle": {
                "chunk_size_bytes": md["chunk_size_bytes"],
                "retention_hours": md["retention_hours"],
                "persist": md["persist"],
                "index_playback": md["index_playback"],
            },
            "record_fmt": lay["fmt"],
            "record_size": lay["record_size"],
            "ts_offset": lay["ts"]["offset"],
            "fields": lay["fields"],
            "created_us": md.get("created_us"),
        }

    # -------------------------------------------------------------------------
    # SHUTDOWN
    # -------------------------------------------------------------------------
    def _register_shutdown_handlers(self):
        """Register signal handlers and atexit for graceful shutdown."""
        if self._shutdown_handlers_registered:
            return
        
        def shutdown_handler(signum, frame):
            try:
                import logging
                log = logging.getLogger("dw.platform")
                log.info(f"Received signal {signum}, shutting down gracefully...")
            except Exception:
                pass
            self.close()
            import sys
            sys.exit(0)
        
        # Register signal handlers
        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
        
        # Register atexit cleanup (catches normal exit)
        atexit.register(self.close)
        
        self._shutdown_handlers_registered = True
    
    def close(self):
        """
        Close all writers, readers, and registries.
        
        Flushes pending writes and releases file locks. Called automatically
        on exit via atexit handler, but explicit call recommended.
        
        Example:
            >>> platform = Platform("./data")
            >>> # ... use platform ...
            >>> platform.close()  # Clean shutdown
        """
        for w in list(self._writers.values()):
            try:
                w.close()
            except Exception as e:
                try:
                    import logging
                    logging.getLogger("dw.platform").error(f"Error closing writer: {e}")
                except Exception:
                    pass
        self._writers = dict()
        
        for r in list(self._readers.values()):
            try:
                r.close()
            except Exception:
                pass
        self._readers = dict()
        
        try:
            self.registry.close()
        except Exception:
            pass
