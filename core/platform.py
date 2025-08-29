# core/platform.py
from __future__ import annotations

import signal
from pathlib import Path
import orjson
import struct
from typing import Dict, Optional, Tuple

from core.global_registry import GlobalRegistry
from core.layout_json import build_layout, save_layout, load_layout
# ORM bits you already have:
from core.feed_registry import FeedRegistry          # per-feed binary reg (chunks, times)  # TODO: confirm API
from core.writer import Writer                        # your existing writer; loads layout.json itself
# from core.uf_reader import UFReader                # TODO: add your reader impl
# from core.index import ChunkIndex                  # TODO: if/when you build UF/NUF chunk indices


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
        # self._readers: Dict[str, UFReader] = {}
        self._layouts: Dict[str, dict] = {}
        self._structs: Dict[str, Tuple[struct.Struct, int]] = {}  # name -> (Struct(fmt), ts_off)
        # self._indexes: Dict[str, ChunkIndex] = {}

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
            "ts_col": "ws_ts_ns",
            # lifecycle knobs (optional; defaults applied into registry)
            "chunk_size_mb": 64,
            "rotate_s": 3600,
            "retention_hours": 72,
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
        lifecycle = {
            "chunk_size_bytes": int(spec.get("chunk_size_mb", 64)*1024*1024),
            "rotate_s":         int(spec.get("rotate_s", 3600)),
            "retention_hours":  int(spec.get("retention_hours", 72)),
            "persist":          bool(spec.get("persist", True)),
            "index_playback":   bool(spec.get("index_playback", False)),
        }
        if not self.registry.feed_exists(name):
            self.registry.register_feed(name, lifecycle)
        else:
            self.registry.update_lifecycle(name, **lifecycle)

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
    def create_writer(self, feed_name: str) -> Writer:
        """Return a cached Writer for the feed, creating if needed."""
        w = self._writers.get(feed_name)
        if w is None:
            # pass platform & feed_name; Writer will load layout.json & registry on its own
            w = Writer(self, feed_name)
            self._writers[feed_name] = w
        return w

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
        lc = self.registry.get_lifecycle(feed_name)
        if lc is None:
            raise KeyError(f"feed '{feed_name}' not found in registry")
        return lc

    def set_lifecycle(self, feed_name: str, **kwargs) -> None:
        """Partial update of lifecycle defaults (e.g., retention_hours=168)."""
        if not self.registry.update_lifecycle(feed_name, **kwargs):
            raise KeyError(f"feed '{feed_name}' not found")

    def list_feeds(self) -> list[dict]:
        """Linear scan of registry entries (small; fine for ops)."""
        n = self.registry.feed_count()
        out = []
        for i in range(n):
            md = self.registry._unpack_entry(self.registry._read_raw(i))  # small internal helper; OK for ops
            out.append(md)
        return out

    def describe_feed(self, feed_name: str) -> dict:
        """Combine registry lifecycle + layout summary for ops."""
        md = self.registry.get_metadata(feed_name)
        if not md:
            raise KeyError(feed_name)
        lay = self.get_record_format(feed_name)
        return {
            "feed_name": feed_name,
            "lifecycle": md["lifecycle"],
            "record_fmt": lay["fmt"],
            "record_size": lay["record_size"],
            "ts_offset": lay["ts"]["offset"],
            "fields": lay["fields"],
            "created_ns": md["created_ns"],
            "updated_ns": md["updated_ns"],
            "chunk_count": md["chunk_count"],
            "first_time": md["first_time"],
            "last_time": md["last_time"],
        }

    # -------------------------------------------------------------------------
    # INDEX / TAIL (optional hooks; keep stubs if not ready)
    # -------------------------------------------------------------------------
    # def get_or_create_chunk_index(self, feed_name: str):
    #     """TODO: return a ChunkIndex for UF/NUF once implemented."""
    #     idx = self._indexes.get(feed_name)
    #     if idx is None:
    #         idx = ChunkIndex(feed_name, 0, base_path=self.base_path, create=True)
    #         self._indexes[feed_name] = idx
    #     return idx

    # def tail_latest(self, feed_name: str):
    #     """TODO: read tail.meta for O(1) latest (ts, file_off, rec_size)."""
    #     raise NotImplementedError

    # -------------------------------------------------------------------------
    # SHUTDOWN
    # -------------------------------------------------------------------------
    def close(self):
        for w in list(self._writers.values()):
            w.close()
        self._writers = dict()
        self.registry.close()