# core/layout_json.py
from __future__ import annotations

import json
import struct
from pathlib import Path
from typing import Dict, List


# ---- type → struct token map (endian handled separately) --------------------
_TMAP: Dict[str, tuple[str, int]] = {
    # 1 byte
    "char":    ("c", 1),
    "bool":    ("?", 1),
    "i8":      ("b", 1), "int8":   ("b", 1),
    "u8":      ("B", 1), "uint8":  ("B", 1), "byte": ("B", 1),

    # 2 bytes
    "i16":     ("h", 2), "int16":  ("h", 2),
    "u16":     ("H", 2), "uint16": ("H", 2),
    "f16":     ("e", 2), "float16":("e", 2),

    # 4 bytes
    "i32":     ("i", 4), "int32":  ("i", 4),
    "u32":     ("I", 4), "uint32": ("I", 4),
    "f32":     ("f", 4), "float32":("f", 4),

    # 8 bytes
    "i64":     ("q", 8), "int64":  ("q", 8),
    "u64":     ("Q", 8), "uint64": ("Q", 8),
    "f64":     ("d", 8), "float64":("d", 8),
}


def _pad(tok: str) -> tuple[str, int] | None:
    """
    Support explicit padding tokens like "_6", "_16" → ("6x", 6).
    """
    if tok.startswith("_") and tok[1:].isdigit():
        n = int(tok[1:])
        return f"{n}x", n
    return None


def build_layout(fields: List[Dict], *, ts_col: str, endian: str = "<") -> Dict:
    """
    Build a UF layout dictionary:
      {
        "mode": "UF",
        "fmt": "<...>",                   # struct format string
        "record_size": <int>,             # bytes
        "fields": [{"name","type","offset","size"}, ...],
        "ts": {"name","offset","size","endian"},
        "version": 1
      }
    """
    parts: List[str] = []
    out_fields: List[Dict] = []
    size = 0
    ts_off = None

    for f in fields:
        typ = f["type"]
        tok_sz = _TMAP.get(typ) or _pad(typ)
        if tok_sz is None:
            raise ValueError(f"unknown field type: {typ}")
        tok, sz = tok_sz
        parts.append(tok)
        out_fields.append({"name": f["name"], "type": typ, "offset": size, "size": sz})
        if f["name"] == ts_col:
            if typ != "uint64":
                raise ValueError("ts_col must be a uint64 field")
            ts_off = size
        size += sz

    fmt = endian + "".join(parts)
    S = struct.Struct(fmt)
    # Guard in case of future alignment semantics (we use '<' so should match)
    record_size = S.size
    if record_size != size:
        size = record_size  # align to struct's computed size

    if ts_off is None:
        raise ValueError(f"ts_col '{ts_col}' not present in fields[]")

    return {
        "mode": "UF",
        "fmt": fmt,
        "record_size": size,
        "fields": out_fields,
        "ts": {"name": ts_col, "offset": ts_off, "size": 8, "endian": endian},
        "version": 1,
    }


def save_layout(feed_dir: Path | str, layout: Dict) -> None:
    feed_dir = Path(feed_dir)
    feed_dir.mkdir(parents=True, exist_ok=True)
    path = feed_dir / "record_format.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(layout, indent=2))
    tmp.replace(path)


def load_layout(feed_dir: Path | str) -> Dict:
    feed_dir = Path(feed_dir)
    return json.loads((feed_dir / "layout.json").read_text())


# ------------------------------ unit tests -----------------------------------
if __name__ == "__main__":
    import tempfile

    def assert_eq(a, b, msg=""):
        if a != b:
            raise AssertionError(f"{msg} (got {a!r}, expected {b!r})")

    # --- L2 UF (ws_ts_ns first; ts_off = 0) ---------------------------------
    l2_fields = [
        {"name": "type",     "type": "char"},
        {"name": "side",     "type": "char"},
        {"name": "_",        "type": "_6"},
        {"name": "ws_ts_ns", "type": "uint64"},
        {"name": "ev_ns",    "type": "uint64"},
        {"name": "proc_ns",  "type": "uint64"},
        {"name": "price",    "type": "float64"},
        {"name": "qty",      "type": "float64"},
        {"name": "_",        "type": "_16"},
    ]
    l2_layout = build_layout(l2_fields, ts_col="ws_ts_ns")
    assert_eq(l2_layout["mode"], "UF", "L2 mode")
    assert_eq(l2_layout["fmt"], "<Qcc6xQQdd16x", "L2 fmt")
    assert_eq(l2_layout["record_size"], struct.Struct("<Qcc6xQQdd16x").size, "L2 size")
    assert_eq(l2_layout["ts"]["offset"], 8, "L2 ts_off")
    # total size sanity: 8 + 1 + 1 + 6 + 8 + 8 + 8 + 8 + 16 = 64
    assert_eq(l2_layout["record_size"], 64, "L2 rec_size sanity")

    # --- TRADES UF (ts_col inside; matches <cc14xQQQQdd> ordering) ----------
    trades_fields = [
        {"name": "type",     "type": "char"},
        {"name": "side",     "type": "char"},
        {"name": "_",        "type": "_14"},
        {"name": "trade_id", "type": "uint64"},
        {"name": "ev_ns",    "type": "uint64"},
        {"name": "ws_ts_ns", "type": "uint64"},   # ts_col here
        {"name": "proc_ns",  "type": "uint64"},
        {"name": "price",    "type": "float64"},
        {"name": "size",     "type": "float64"},
    ]
    tr_layout = build_layout(trades_fields, ts_col="ws_ts_ns")
    assert_eq(tr_layout["fmt"], "<cc14xQQQQdd", "TRADES fmt")
    assert_eq(tr_layout["record_size"], struct.Struct("<cc14xQQQQdd").size, "TRADES size")
    # ts offset = 2 (cc) + 14 (pad) + 8 (trade_id) + 8 (ev_ns) = 32
    assert_eq(tr_layout["ts"]["offset"], 32, "TRADES ts_off")
    assert_eq(tr_layout["record_size"], 64, "TRADES rec_size sanity")

    # --- save/load roundtrip -------------------------------------------------
    with tempfile.TemporaryDirectory() as td:
        p_l2 = Path(td) / "CB-L2-BTC-USD"
        save_layout(p_l2, l2_layout)
        rt_l2 = load_layout(p_l2)
        assert_eq(rt_l2["fmt"], l2_layout["fmt"], "RT L2 fmt")
        assert_eq(rt_l2["ts"]["offset"], l2_layout["ts"]["offset"], "RT L2 ts_off")

        p_tr = Path(td) / "CB-TRADES-BTC-USD"
        save_layout(p_tr, tr_layout)
        rt_tr = load_layout(p_tr)
        assert_eq(rt_tr["fmt"], tr_layout["fmt"], "RT TRADES fmt")
        assert_eq(rt_tr["ts"]["offset"], tr_layout["ts"]["offset"], "RT TRADES ts_off")

    print("OK: layout_json UF tests passed (L2 + TRADES)")
