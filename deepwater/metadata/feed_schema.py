from __future__ import annotations

import struct
from pathlib import Path
from typing import Any, Sequence

import msgspec
import numpy as np

SCHEMA_FILENAME = "feed_schema.msgpack"

TYPE_PAD = 0
TYPE_CHAR = 1
TYPE_BOOL = 2
TYPE_I8 = 3
TYPE_U8 = 4
TYPE_I16 = 5
TYPE_U16 = 6
TYPE_F16 = 7
TYPE_I32 = 8
TYPE_U32 = 9
TYPE_F32 = 10
TYPE_I64 = 11
TYPE_U64 = 12
TYPE_F64 = 13
TYPE_BYTES = 14

# type_id -> canonical name, NumPy format, struct token, fixed size, alignment
_TYPE_INFO: dict[int, tuple[str, str, str, int, int]] = {
    TYPE_CHAR: ("char", "|S1", "c", 1, 1),
    TYPE_BOOL: ("bool", "|b1", "?", 1, 1),
    TYPE_I8: ("int8", "<i1", "b", 1, 1),
    TYPE_U8: ("uint8", "<u1", "B", 1, 1),
    TYPE_I16: ("int16", "<i2", "h", 2, 2),
    TYPE_U16: ("uint16", "<u2", "H", 2, 2),
    TYPE_F16: ("float16", "<f2", "e", 2, 2),
    TYPE_I32: ("int32", "<i4", "i", 4, 4),
    TYPE_U32: ("uint32", "<u4", "I", 4, 4),
    TYPE_F32: ("float32", "<f4", "f", 4, 4),
    TYPE_I64: ("int64", "<i8", "q", 8, 8),
    TYPE_U64: ("uint64", "<u8", "Q", 8, 8),
    TYPE_F64: ("float64", "<f8", "d", 8, 8),
}

_ALIASES: dict[str, int] = {
    "char": TYPE_CHAR,
    "bool": TYPE_BOOL,
    "i8": TYPE_I8,
    "int8": TYPE_I8,
    "u8": TYPE_U8,
    "uint8": TYPE_U8,
    "byte": TYPE_U8,
    "i16": TYPE_I16,
    "int16": TYPE_I16,
    "u16": TYPE_U16,
    "uint16": TYPE_U16,
    "f16": TYPE_F16,
    "float16": TYPE_F16,
    "i32": TYPE_I32,
    "int32": TYPE_I32,
    "u32": TYPE_U32,
    "uint32": TYPE_U32,
    "f32": TYPE_F32,
    "float32": TYPE_F32,
    "i64": TYPE_I64,
    "int64": TYPE_I64,
    "u64": TYPE_U64,
    "uint64": TYPE_U64,
    "f64": TYPE_F64,
    "float64": TYPE_F64,
}


class SchemaField(msgspec.Struct, frozen=True, array_like=True):
    name: str
    type_id: int
    offset: int
    size: int

    @property
    def type(self) -> str:
        return type_name(self)

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.type, "offset": self.offset, "size": self.size}


class FeedSchemaData(msgspec.Struct, frozen=True, array_like=True):
    version: int
    endian: str
    record_size: int
    clock_level: int
    fields: tuple[SchemaField, ...]


class FeedSchema:
    __slots__ = (
        "data",
        "version",
        "endian",
        "record_size",
        "clock_level",
        "fields",
        "_dtype",
        "_struct",
        "_field_names",
        "_primary_ts_offset",
    )

    def __init__(self, data: FeedSchemaData):
        self.data = data
        self.version = data.version
        self.endian = data.endian
        self.record_size = data.record_size
        self.clock_level = data.clock_level
        self.fields = data.fields
        self._dtype = _compile_numpy_dtype(data)
        self._struct = _compile_struct(data)
        self._field_names = tuple(field.name for field in data.fields if field.type_id != TYPE_PAD)
        self._primary_ts_offset = data.fields[0].offset if data.clock_level else 0

    @property
    def numpy_dtype(self) -> np.dtype:
        return self._dtype

    @property
    def struct(self) -> struct.Struct:
        return self._struct

    @property
    def fmt(self) -> str:
        return self._struct.format

    @property
    def timestamp_fields(self) -> tuple[SchemaField, ...]:
        return self.fields[: self.clock_level]

    @property
    def primary_ts_offset(self) -> int:
        return self._primary_ts_offset

    @property
    def field_names(self) -> tuple[str, ...]:
        return self._field_names

    def to_msgpack(self) -> bytes:
        return msgspec.msgpack.encode(self.data)

    @classmethod
    def from_msgpack(cls, data: bytes | bytearray | memoryview) -> FeedSchema:
        return cls(msgspec.msgpack.decode(data, type=FeedSchemaData))

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": "UF",
            "fmt": self.fmt,
            "record_size": self.record_size,
            "fields": [field.to_dict() for field in self.fields],
            "clock_level": self.clock_level,
            "dtype": self.dtype_dict(),
            "version": self.version,
        }

    def dtype_dict(self) -> dict[str, Any]:
        fields = [field for field in self.fields if field.type_id != TYPE_PAD]
        return {
            "names": [field.name for field in fields],
            "formats": [_numpy_format(field) for field in fields],
            "offsets": [field.offset for field in fields],
            "itemsize": self.record_size,
        }


def build_schema(
    fields: Sequence[tuple[str, str]],
    *,
    clock_level: int | None,
    aligned: bool = False,
) -> FeedSchema:
    if clock_level is None:
        raise ValueError("clock_level is required (1-3)")
    if clock_level < 1 or clock_level > 3:
        raise ValueError("clock_level must be between 1 and 3")
    if len(fields) < clock_level:
        raise ValueError(f"clock_level={clock_level} requires at least {clock_level} fields for timestamps")

    for i in range(clock_level):
        name, typ = fields[i]
        type_id, size, _align = _parse_type(typ)
        if type_id != TYPE_U64 or size != 8:
            raise ValueError(f"clock_level={clock_level} requires field '{name}' to be uint64 for timestamp")

    out_fields: list[SchemaField] = []
    offset = 0
    max_align = 1

    for name, typ in fields:
        type_id, size, align = _parse_type(typ)

        if aligned and type_id != TYPE_PAD:
            align = min(align, 8)
            pad = (-offset) & (align - 1)
            if pad:
                out_fields.append(SchemaField("_", TYPE_PAD, offset, pad))
                offset += pad
            max_align = max(max_align, align)

        out_fields.append(SchemaField(name, type_id, offset, size))
        offset += size

    record_size = offset
    if aligned:
        align = min(max_align, 8)
        tail = (-record_size) & (align - 1)
        if tail:
            out_fields.append(SchemaField("_", TYPE_PAD, record_size, tail))
            record_size += tail

    data = FeedSchemaData(
        version=1,
        endian="<",
        record_size=record_size,
        clock_level=clock_level,
        fields=tuple(out_fields),
    )
    return FeedSchema(data)


def save_schema(feed_dir: Path | str, schema: FeedSchema) -> None:
    feed_dir = Path(feed_dir)
    feed_dir.mkdir(parents=True, exist_ok=True)
    path = feed_dir / SCHEMA_FILENAME
    tmp = path.with_suffix(".msgpack.tmp")
    tmp.write_bytes(schema.to_msgpack())
    tmp.replace(path)


def load_schema(feed_dir: Path | str) -> FeedSchema:
    return FeedSchema.from_msgpack((Path(feed_dir) / SCHEMA_FILENAME).read_bytes())


def load_feed_schema(base_path: Path | str, feed_name: str) -> FeedSchema:
    return load_schema(Path(base_path) / "data" / feed_name)


def load_record_schema_for_feed(base_path: Path | str, feed_name: str) -> FeedSchema:
    return load_feed_schema(base_path, feed_name)


def schema_to_dict(schema: FeedSchema) -> dict[str, Any]:
    return schema.to_dict()


def type_name(field: SchemaField) -> str:
    if field.type_id == TYPE_PAD:
        return f"_{field.size}"
    if field.type_id == TYPE_BYTES:
        return f"bytes{field.size}"
    return _TYPE_INFO[field.type_id][0]


def _parse_type(typ: str) -> tuple[int, int, int]:
    if typ.startswith("_") and typ[1:].isdigit():
        size = int(typ[1:])
        if size <= 0:
            raise ValueError(f"invalid padding size: {typ}")
        return TYPE_PAD, size, 1
    if typ.startswith("bytes") and typ[5:].isdigit():
        size = int(typ[5:])
        if size <= 0:
            raise ValueError(f"invalid bytes size: {typ}")
        return TYPE_BYTES, size, 1
    type_id = _ALIASES.get(typ)
    if type_id is None:
        raise ValueError(f"unknown field type: {typ}")
    _name, _np_fmt, _struct_fmt, size, align = _TYPE_INFO[type_id]
    return type_id, size, align


def _numpy_format(field: SchemaField) -> str:
    if field.type_id == TYPE_PAD:
        return f"V{field.size}"
    if field.type_id == TYPE_BYTES:
        return f"|S{field.size}"
    return _TYPE_INFO[field.type_id][1]


def _struct_token(field: SchemaField) -> str:
    if field.type_id == TYPE_PAD:
        return f"{field.size}x"
    if field.type_id == TYPE_BYTES:
        return f"{field.size}s"
    return _TYPE_INFO[field.type_id][2]


def _compile_numpy_dtype(data: FeedSchemaData) -> np.dtype:
    names: list[str] = []
    formats: list[str] = []
    offsets: list[int] = []
    seen: set[str] = set()

    for field in data.fields:
        if field.type_id == TYPE_PAD:
            continue
        if field.name in seen:
            raise ValueError(f"duplicate dtype field name: {field.name!r}")
        seen.add(field.name)
        names.append(field.name)
        formats.append(_numpy_format(field))
        offsets.append(field.offset)

    return np.dtype({
        "names": names,
        "formats": formats,
        "offsets": offsets,
        "itemsize": data.record_size,
    })


def _compile_struct(data: FeedSchemaData) -> struct.Struct:
    parts: list[str] = []
    cursor = 0
    for field in data.fields:
        if field.offset > cursor:
            parts.append(f"{field.offset - cursor}x")
            cursor = field.offset
        parts.append(_struct_token(field))
        cursor += field.size

    if cursor < data.record_size:
        parts.append(f"{data.record_size - cursor}x")

    compiled = struct.Struct(data.endian + "".join(parts))
    if compiled.size != data.record_size:
        raise ValueError("struct size mismatch with schema")
    return compiled
