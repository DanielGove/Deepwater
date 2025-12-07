# core/platform.py
from __future__ import annotations

import signal
from pathlib import Path
import orjson
import struct
from typing import Dict, Optional, Tuple

from .global_registry import GlobalRegistry
from .layout_json import build_layout, save_layout, load_layout
from .feed_registry import FeedRegistry
from .writer import Writer
from .reader import Reader
from .ring import RingWriter, RingReader


class Platform:
    """
    Single entry-point for feeds.
    - Lifecycle defaults live in GlobalRegistry (binary, mmap).
    - Record formatting lives in data/<feed>/layout.json (UF only for now).
    - Writers/readers are opened explicitly (create_feed never returns a writer).
    """

    def __init__(self, base_path: str = "./platform_data"):
        self.base_path = Path(base_path)
        self.data_path = self.base_path / "data"
        self.data_path.mkdir(parents=True, exist_ok=True)

        self.registry = GlobalRegistry(self.base_path)

        # process-local caches
        self._writers: Dict[str, Writer] = {}
        self._readers: Dict[str, Reader] = {}
        self._layouts: Dict[str, dict] = {}
        self._structs: Dict[str, Tuple[struct.Struct, int]] = {}  # name -> (Struct(fmt), ts_off)
        self._specs: Dict[str, dict] = {}

    # -------------------------------------------------------------------------
    # FEED CREATION (idempotent, failsafe)
    # -------------------------------------------------------------------------
    def create_feed(self, spec: dict) -> None:
        """
        spec (UF today):
          {
            "feed_name": "...",
            "mode": "UF",
            "fields": [ {name,type}, ... ],
            "ts_col": "ws_ts_us",
            # lifecycle knobs (optional; defaults applied into registry)
            "chunk_size_bytes": 64*1024*1024,
            "rotate_s": 3600,
            "persist": True,
            "index_playback": False,
            "schema_id": "cb.l2.uf.v1"   # kept only in config.json for ops
          }
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
        # normalize sizing to bytes (support legacy chunk_size_mb)
        chunk_bytes = spec.get("chunk_size_bytes")
        if chunk_bytes is None:
            chunk_bytes = int(spec.get("chunk_size_mb", 64) * 1024 * 1024)
        else:
            chunk_bytes = int(chunk_bytes)

        persist = bool(spec.get("persist", True))
        lifecycle = {
            "chunk_size_bytes": chunk_bytes,
            "rotate_s":         int(spec.get("rotate_s", 3600)),
            "persist":          persist,
            # indices only make sense for persisted chunk files
            "index_playback":   bool(spec.get("index_playback", False)) if persist else False,
        }
        if not self.registry.feed_exists(name):
            self.registry.register_feed(name, lifecycle)
        else:
            self.registry.update_metadata(name, **lifecycle)

        # 4) keep full app spec for ops/debug (optional)
        spec_to_save = dict(spec)
        spec_to_save["chunk_size_bytes"] = chunk_bytes
        spec_to_save.pop("chunk_size_mb", None)
        (fdir / "config.json").write_bytes(orjson.dumps(spec_to_save))

        # 5) ensure per-feed registry (binary) exists for persisted feeds
        if lifecycle["persist"]:
            FeedRegistry(path=fdir / f"{name}.reg", mode="w").close()

        # clear caches for this feed
        self._layouts.pop(name, None)
        self._structs.pop(name, None)
        self._specs.pop(name, None)

    # -------------------------------------------------------------------------
    # OPEN/CLOSE
    # -------------------------------------------------------------------------
    def create_writer(self, feed_name: str) -> Writer:
        """Return a cached Writer for the feed, creating if needed."""
        w = self._writers.get(feed_name)
        if w is None:
            lifecycle = self.lifecycle(feed_name)
            # non-persistent feeds -> ring
            if lifecycle.get("persist", True):
                w = Writer(self, feed_name)
            else:
                w = RingWriter(self, feed_name)
            self._writers[feed_name] = w
        return w
    
    def create_reader(self, feed_name: str):
        r = self._readers.get(feed_name)
        if r is None:
            lifecycle = self.lifecycle(feed_name)
            r = Reader(self, feed_name) if lifecycle.get("persist", True) else RingReader(self, feed_name)
            self._readers[feed_name] = r
        return r

    # def open_reader(self, feed_name: str) -> UFReader:
    #     """TODO: wire your reader impl; should mmap the latest chunk and use layout.json."""
    #     r = self._readers.get(feed_name)
    #     if r is None:
    #         layout = self.get_layout(feed_name)
    #         # r = UFReader(self.latest_chunk_path(feed_name), layout["fmt"], layout["ts"]["offset"])
    #         self._readers[feed_name] = r
    #     return r

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

    def feed_spec(self, feed_name: str) -> dict:
        """Load and cache the app-level config spec (config.json)."""
        spec = self._specs.get(feed_name)
        if spec is None:
            path = self.feed_dir(feed_name) / "config.json"
            if not path.exists():
                raise FileNotFoundError(f"config.json missing for feed '{feed_name}'")
            spec = orjson.loads(path.read_bytes())
            # legacy support: chunk_size_mb → chunk_size_bytes
            if "chunk_size_bytes" not in spec and "chunk_size_mb" in spec:
                spec["chunk_size_bytes"] = int(spec["chunk_size_mb"] * 1024 * 1024)
            self._specs[feed_name] = spec
        return spec

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
        """Partial update of lifecycle defaults."""
        if not self.registry.update_metadata(feed_name, **kwargs):
            raise KeyError(f"feed '{feed_name}' not found")

    def resize_feed(self, feed_name: str, new_chunk_size_bytes: int) -> None:
        """Update chunk/ring size and rotate/rescale active writer if present."""
        if new_chunk_size_bytes <= 0:
            raise ValueError("new_chunk_size_bytes must be > 0")
        # update registry/config
        if self.registry.feed_exists(feed_name):
            self.registry.update_metadata(feed_name, chunk_size_bytes=int(new_chunk_size_bytes))
        # update config file
        spec = self.feed_spec(feed_name)
        spec["chunk_size_bytes"] = int(new_chunk_size_bytes)
        cfg_path = self.feed_dir(feed_name) / "config.json"
        cfg_path.write_bytes(orjson.dumps(spec))
        self._specs[feed_name] = spec
        # resize live writer if cached
        w = self._writers.get(feed_name)
        if w:
            if hasattr(w, "resize_chunk_size"):
                w.resize_chunk_size(int(new_chunk_size_bytes))
            elif hasattr(w, "resize"):
                w.resize(int(new_chunk_size_bytes))
        # refresh lifecycle cache for future readers
        self.registry.get_metadata(feed_name)

    def list_feeds(self) -> list[dict]:
        """Linear scan of registry entries (small; fine for ops)."""
        return self.registry.list_feeds()
    
    def describe_feed(self, feed_name: str) -> dict:
        """Combine registry lifecycle + layout summary for ops."""
        md = self.registry.get_metadata(feed_name)
        if not md:
            raise KeyError(feed_name)
        lay = self.get_record_format(feed_name)
        spec = self.feed_spec(feed_name)
        persist = md.get("persist", True)
        capacity_records = md["chunk_size_bytes"] // lay["record_size"] if lay["record_size"] else None
        return {
            "feed_name": feed_name,
            "lifecycle": {
                "chunk_size_bytes": md["chunk_size_bytes"],
                "rotate_s": md["rotate_s"],
                "persist": md["persist"],
                "index_playback": md["index_playback"],
            },
            "record_fmt": lay["fmt"],
            "record_size": lay["record_size"],
            "ts_offset": lay["ts_offset"],
            "fields": lay["fields"],
            "created_us": md.get("created_us"),
            "persist": persist,
            "capacity_records": capacity_records,
            "config": spec,
        }

    # -------------------------------------------------------------------------
    # SHUTDOWN
    # -------------------------------------------------------------------------
    def close(self):
        for w in list(self._writers.values()):
            w.close()
        self._writers = dict()
        for r in list(self._readers.values()):
            try:
                r.close()
            except Exception:
                pass
        self._readers = dict()
        self.registry.close()
