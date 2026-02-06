# core/layout_json.py
from __future__ import annotations

import json
import struct
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# ---- type → (numpy_code, struct_token, size, natural_align) -----------------
# natural_align is capped implicitly by our logic (<= 8) to mirror common ABIs.
_SCALARS: Dict[str, Tuple[str, str, int, int]] = {
    # 1 byte
    "char":   ("|S1", "c", 1, 1),
    "bool":   ("|b1", "?", 1, 1),
    "i8":     ("<i1", "b", 1, 1), "int8":  ("<i1","b",1,1),
    "u8":     ("<u1", "B", 1, 1), "uint8": ("<u1","B",1,1), "byte": ("<u1","B",1,1),
    # 2 bytes
    "i16":    ("<i2", "h", 2, 2), "int16": ("<i2","h",2,2),
    "u16":    ("<u2", "H", 2, 2), "uint16":("<u2","H",2,2),
    "f16":    ("<f2", "e", 2, 2), "float16":("<f2","e",2,2),
    # 4 bytes
    "i32":    ("<i4", "i", 4, 4), "int32": ("<i4","i",4,4),
    "u32":    ("<u4", "I", 4, 4), "uint32":("<u4","I",4,4),
    "f32":    ("<f4", "f", 4, 4), "float32":("<f4","f",4,4),
    # 8 bytes
    "i64":    ("<i8", "q", 8, 8), "int64": ("<i8","q",8,8),
    "u64":    ("<u8", "Q", 8, 8), "uint64":("<u8","Q",8,8),
    "f64":    ("<f8", "d", 8, 8), "float64":("<f8","d",8,8),
    # fixed-length byte strings
    "bytes8":  ("|S8",  "8s",  8, 1),
    "bytes16": ("|S16", "16s", 16, 1),
    "bytes32": ("|S32", "32s", 32, 1),
    "bytes64": ("|S64", "64s", 64, 1),
}

def _pad_token(tok: str) -> Tuple[str, int] | None:
    """
    Support explicit padding tokens like '_6' or '_16' → ('V6' dtype / '6x' struct).
    """
    if tok.startswith("_") and tok[1:].isdigit():
        n = int(tok[1:])
        return f"V{n}", n
    return None

def _dtype_size(code: str) -> int:
    if code.startswith("|S"): return int(code[2:])
    if code.startswith("V"):  return int(code[1:])
    return {
        "<i1":1,"<u1":1,"|b1":1,
        "<i2":2,"<u2":2,"<f2":2,
        "<i4":4,"<u4":4,"<f4":4,
        "<i8":8,"<u8":8,"<f8":8,
    }[code]

def _dtype_to_struct(code: str) -> str:
    if code == "|b1": return "?"
    if code == "<i1": return "b"
    if code == "<u1": return "B"
    if code == "<i2": return "h"
    if code == "<u2": return "H"
    if code == "<f2": return "e"
    if code == "<i4": return "i"
    if code == "<u4": return "I"
    if code == "<f4": return "f"
    if code == "<i8": return "q"
    if code == "<u8": return "Q"
    if code == "<f8": return "d"
    if code.startswith("|S"): return f"{int(code[2:])}s"
    if code.startswith("V"):  return f"{int(code[1:])}x"
    raise ValueError(f"unsupported dtype code: {code}")

def build_layout(
    fields: List[Dict],
    *,
    ts_col: str,
    query_cols: Optional[List[str]] = None,
    endian: str = "<",
    aligned: bool = False,
) -> Dict:
    """
    Build a UF layout dict from your app config.

    Params:
      fields : [{'name': str, 'type': str, ...}, ...]
               types may be scalar (e.g. 'uint64','float64','char','bool',...)
               or padding like '_6'
      ts_col : name of the primary uint64 timestamp column (for indexing, required)
      query_cols : optional list of additional uint64 timestamp columns that can be queried
                   (e.g. ['recv_us', 'proc_us', 'ev_us']). All must be monotonically increasing.
      endian : '<' or '>' (struct endianness)
      aligned: if True, apply C-style alignment (explicit pads inserted so dtype & struct match)

    Returns:
      {
        "mode": "UF",
        "fmt": "<...>",                 # struct format string
        "record_size": int,
        "fields": [{"name","type","offset","size"}, ...],
        "ts_name": str,                 # primary timestamp column
        "ts_offset": int,
        "ts_size": 8,
        "ts_endian": "<" or ">",
        "query_keys": {"col_name": {"offset": int, "size": 8}, ...},  # queryable timestamps
        "dtype": { "names","formats","offsets","itemsize" }  # NumPy explicit-offset spec
        "version": 1
      }
    """
    if endian not in ("<", ">"):
        raise ValueError("endian must be '<' or '>'")

    names:   List[str] = []
    formats: List[str] = []
    offsets: List[int] = []
    out_fields: List[Dict] = []

    off = 0
    max_align = 1
    ts_off: Optional[int] = None
    query_keys: Dict[str, Dict[str, int]] = {}  # {col_name: {offset, size}}
    query_cols_set = set(query_cols or [])

    for f in fields:
        name = f["name"]
        typ  = f["type"]

        # explicit user padding like "_6"
        p = _pad_token(typ)
        if p is not None:
            dcode, n = p
            names.append(name)
            formats.append(dcode)     # 'Vn' (void bytes)
            offsets.append(off)
            out_fields.append({"name": name, "type": typ, "offset": off, "size": n})
            off += n
            continue

        # scalars
        scalar = _SCALARS.get(typ)
        if scalar is None:
            raise ValueError(f"unknown field type: {typ}")
        np_code, st_code, size, align = scalar

        if aligned:
            a = min(align, 8)
            pad = (-off) & (a - 1)
            if pad:
                # inject explicit implicit-pad so dtype & struct remain identical
                names.append(f"_impad{pad}@{off}")
                formats.append(f"V{pad}")
                offsets.append(off)
                out_fields.append({"name": "_", "type": f"_{pad}", "offset": off, "size": pad})
                off += pad
            max_align = max(max_align, a)

        # place the field
        names.append(name)
        formats.append(np_code)
        offsets.append(off)
        out_fields.append({"name": name, "type": typ, "offset": off, "size": size})
        if name == ts_col:
            if typ != "uint64":
                raise ValueError("ts_col must be a uint64 field")
            ts_off = off
        if name in query_cols_set:
            if typ != "uint64":
                raise ValueError(f"query_col '{name}' must be a uint64 field")
            query_keys[name] = {"offset": off, "size": size}
        off += size

    # tail pad to overall alignment if requested
    itemsize = off
    if aligned:
        a = min(max_align, 8)
        tail = (-itemsize) & (a - 1)
        if tail:
            names.append(f"_impad{tail}@{off}")
            formats.append(f"V{tail}")
            offsets.append(off)
            out_fields.append({"name": "_", "type": f"_{tail}", "offset": off, "size": tail})
            itemsize += tail

    if ts_off is None:
        raise ValueError(f"ts_col '{ts_col}' not present in fields[]")
    
    # Validate all query_cols were found
    if query_cols:
        missing = query_cols_set - set(query_keys.keys())
        if missing:
            raise ValueError(f"query_cols not found in fields: {missing}")

    # Build struct fmt by walking the dtype spec (offset gaps → 'x' pads)
    parts: List[str] = []
    cursor = 0
    for code, ofs in zip(formats, offsets):
        if ofs > cursor:
            parts.append(f"{ofs - cursor}x")
            cursor = ofs
        parts.append(_dtype_to_struct(code))
        cursor += _dtype_size(code)
    if cursor < itemsize:
        parts.append(f"{itemsize - cursor}x")
    fmt = endian + "".join(parts)
    if struct.Struct(fmt).size != itemsize:
        raise AssertionError("struct size mismatch with dtype")

    layout = {
        "mode": "UF",
        "fmt": fmt,
        "record_size": itemsize,
        "fields": out_fields,
        "ts_name": ts_col,
        "ts_offset": ts_off,
        "ts_size": 8,
        "ts_endian": endian,
        "dtype": {
            "names": names,
            "formats": formats,
            "offsets": offsets,
            "itemsize": itemsize,
        },
        "version": 1,
    }
    if query_keys:
        layout["query_keys"] = query_keys
    return layout


# ------------------------------ persistence ----------------------------------

def save_layout(feed_dir: Path | str, layout: Dict) -> None:
    feed_dir = Path(feed_dir)
    feed_dir.mkdir(parents=True, exist_ok=True)
    path = feed_dir / "record_format.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(layout, indent=2))
    tmp.replace(path)

def load_layout(feed_dir: Path | str) -> Dict:
    feed_dir = Path(feed_dir)
    return json.loads((feed_dir / "record_format.json").read_text())
