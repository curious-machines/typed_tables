"""Table storage for typed data."""

from __future__ import annotations

import mmap
import os
import struct
from pathlib import Path
from typing import Any

from typed_tables.types import (
    ArrayTypeDefinition,
    CompositeTypeDefinition,
    EnumTypeDefinition,
    EnumValue,
    InterfaceTypeDefinition,
    PrimitiveType,
    PrimitiveTypeDefinition,
    TypeDefinition,
)


class Table:
    """Manages binary storage for a single type."""

    # Initial file size and growth increment
    INITIAL_SIZE = 4096
    GROWTH_FACTOR = 2

    def __init__(self, type_def: TypeDefinition, file_path: Path) -> None:
        self.type_def = type_def
        self.file_path = file_path
        self._record_size = type_def.size_bytes
        self._file: Any = None
        self._mmap: mmap.mmap | None = None
        self._count = 0
        self._capacity = 0  # Number of records that fit in current file

        self._open_or_create()

    def _open_or_create(self) -> None:
        """Open existing file or create new one."""
        if self.file_path.exists():
            self._open_existing()
        else:
            self._create_new()

    def _create_new(self) -> None:
        """Create a new table file."""
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

        # Create file with initial size
        with open(self.file_path, "wb") as f:
            # Header: 8 bytes for count
            f.write(struct.pack("<Q", 0))
            # Pad to initial size
            remaining = self.INITIAL_SIZE - 8
            f.write(b"\x00" * remaining)

        self._open_file()
        self._count = 0
        self._capacity = (self.INITIAL_SIZE - 8) // self._record_size

    def _open_existing(self) -> None:
        """Open an existing table file."""
        self._open_file()
        # Read count from header
        self._mmap.seek(0)  # type: ignore
        self._count = struct.unpack("<Q", self._mmap.read(8))[0]  # type: ignore
        file_size = self._mmap.size()  # type: ignore
        self._capacity = (file_size - 8) // self._record_size

    def _open_file(self) -> None:
        """Open file and create memory map."""
        self._file = open(self.file_path, "r+b")
        self._mmap = mmap.mmap(self._file.fileno(), 0)

    def _grow_file(self) -> None:
        """Grow the file to accommodate more records."""
        if self._mmap is not None:
            self._mmap.close()
        if self._file is not None:
            self._file.close()

        current_size = self.file_path.stat().st_size
        new_size = current_size * self.GROWTH_FACTOR

        with open(self.file_path, "r+b") as f:
            f.seek(new_size - 1)
            f.write(b"\x00")

        self._open_file()
        self._capacity = (new_size - 8) // self._record_size

    def _update_count(self) -> None:
        """Update the count in the file header."""
        self._mmap.seek(0)  # type: ignore
        self._mmap.write(struct.pack("<Q", self._count))  # type: ignore

    def _record_offset(self, index: int) -> int:
        """Get byte offset for a record index."""
        return 8 + index * self._record_size  # 8 bytes for header

    @property
    def count(self) -> int:
        """Return the number of records in the table."""
        return self._count

    def insert(self, value: Any) -> int:
        """Insert a value and return its index."""
        if self._count >= self._capacity:
            self._grow_file()

        index = self._count
        offset = self._record_offset(index)

        data = self._serialize(value)
        self._mmap.seek(offset)  # type: ignore
        self._mmap.write(data)  # type: ignore

        self._count += 1
        self._update_count()
        self._mmap.flush()  # type: ignore

        return index

    def get(self, index: int) -> Any:
        """Get a value by index."""
        if index < 0 or index >= self._count:
            raise IndexError(f"Index {index} out of range [0, {self._count})")

        offset = self._record_offset(index)
        self._mmap.seek(offset)  # type: ignore
        data = self._mmap.read(self._record_size)  # type: ignore

        return self._deserialize(data)

    def update(self, index: int, value: Any) -> None:
        """Update a value at the given index."""
        if index < 0 or index >= self._count:
            raise IndexError(f"Index {index} out of range [0, {self._count})")

        offset = self._record_offset(index)
        data = self._serialize(value)
        self._mmap.seek(offset)  # type: ignore
        self._mmap.write(data)  # type: ignore
        self._mmap.flush()  # type: ignore

    # Deletion marker: all 0xFF bytes (distinguishable from valid data with index 0)
    DELETED_MARKER = b"\xff"

    def delete(self, index: int) -> None:
        """Delete a record at the given index by marking it with 0xFF bytes.

        Note: This is a soft delete that marks the record but preserves indices.
        The record count is not decremented to maintain referential integrity.
        """
        if index < 0 or index >= self._count:
            raise IndexError(f"Index {index} out of range [0, {self._count})")

        offset = self._record_offset(index)
        # Mark record as deleted with 0xFF bytes
        self._mmap.seek(offset)  # type: ignore
        self._mmap.write(self.DELETED_MARKER * self._record_size)  # type: ignore
        self._mmap.flush()  # type: ignore

    def is_deleted(self, index: int) -> bool:
        """Check if a record at the given index has been deleted."""
        if index < 0 or index >= self._count:
            raise IndexError(f"Index {index} out of range [0, {self._count})")

        offset = self._record_offset(index)
        self._mmap.seek(offset)  # type: ignore
        data = self._mmap.read(self._record_size)  # type: ignore
        return data == self.DELETED_MARKER * self._record_size

    def _serialize(self, value: Any) -> bytes:
        """Serialize a value to bytes."""
        if isinstance(self.type_def, PrimitiveTypeDefinition):
            return self._serialize_primitive(value, self.type_def.primitive)
        elif isinstance(self.type_def, ArrayTypeDefinition):
            # For array tables, value should be (start_index, length)
            start_index, length = value
            return struct.pack("<II", start_index, length)
        elif isinstance(self.type_def, CompositeTypeDefinition):
            return self._serialize_composite(value, self.type_def)
        else:
            # Handle alias by resolving to base
            base = self.type_def.resolve_base_type()
            if isinstance(base, PrimitiveTypeDefinition):
                return self._serialize_primitive(value, base.primitive)
            raise TypeError(f"Cannot serialize type: {self.type_def.name}")

    def _serialize_primitive(self, value: Any, primitive: PrimitiveType) -> bytes:
        """Serialize a primitive value."""
        format_map = {
            PrimitiveType.BIT: "<?",
            PrimitiveType.CHARACTER: "<I",  # Unicode code point
            PrimitiveType.UINT8: "<B",
            PrimitiveType.INT8: "<b",
            PrimitiveType.UINT16: "<H",
            PrimitiveType.INT16: "<h",
            PrimitiveType.UINT32: "<I",
            PrimitiveType.INT32: "<i",
            PrimitiveType.UINT64: "<Q",
            PrimitiveType.INT64: "<q",
            PrimitiveType.FLOAT32: "<f",
            PrimitiveType.FLOAT64: "<d",
        }

        if primitive == PrimitiveType.CHARACTER:
            # Convert character to code point
            if isinstance(value, str):
                value = ord(value[0]) if value else 0
            return struct.pack("<I", value)
        elif primitive in (PrimitiveType.UINT128, PrimitiveType.INT128):
            # Handle 128-bit integers
            if isinstance(value, str):
                # Parse UUID-like strings
                value = int(value.replace("-", ""), 16)
            # Pack as two 64-bit values (little-endian)
            low = value & ((1 << 64) - 1)
            high = (value >> 64) & ((1 << 64) - 1)
            return struct.pack("<QQ", low, high)
        else:
            return struct.pack(format_map[primitive], value)

    def _serialize_composite(self, value: Any, type_def: CompositeTypeDefinition) -> bytes:
        """Serialize a composite value.

        Record layout: [null_bitmap] [field0_data] [field1_data] ...

        - Primitive/alias-to-primitive fields: actual value bytes (inline)
        - Array fields: (start_index, length) tuple (8 bytes)
        - Composite ref fields: uint32 index (4 bytes)
        """
        if isinstance(value, (list, tuple)):
            value = {field.name: value[i] for i, field in enumerate(type_def.fields)}
        elif not isinstance(value, dict):
            raise TypeError(f"Expected dict or tuple for composite type, got {type(value)}")

        # Build null bitmap
        bitmap_size = type_def.null_bitmap_size
        bitmap = bytearray(bitmap_size)
        for i, field in enumerate(type_def.fields):
            if value.get(field.name) is None:
                bitmap[i // 8] |= 1 << (i % 8)

        parts = [bytes(bitmap)]

        for i, field in enumerate(type_def.fields):
            field_value = value.get(field.name)
            ref_size = field.type_def.reference_size
            if field_value is None:
                # Null field: write zeroed bytes
                parts.append(b"\x00" * ref_size)
            else:
                parts.append(self._serialize_field_data(field_value, field.type_def))

        return b"".join(parts)

    def _serialize_field_data(self, value: Any, type_def: TypeDefinition) -> bytes:
        """Serialize field data within a composite record.

        - Array fields: (start_index, length) as 8 bytes
        - Composite ref fields: uint32 index as 4 bytes
        - Enum fields: discriminant + payload inline
        - Primitive/alias-to-primitive fields: actual value bytes (inline)
        """
        base = type_def.resolve_base_type()
        if isinstance(base, EnumTypeDefinition):
            return self._serialize_enum_value(value, base)
        elif isinstance(base, InterfaceTypeDefinition):
            # Tagged reference: (type_id, index) → uint16 + uint32 = 6 bytes
            type_id, index = value
            return struct.pack("<HI", type_id, index)
        elif isinstance(base, ArrayTypeDefinition):
            return struct.pack("<II", value[0], value[1])
        elif isinstance(base, CompositeTypeDefinition):
            return struct.pack("<I", value)
        elif isinstance(base, PrimitiveTypeDefinition):
            return self._serialize_primitive(value, base.primitive)
        else:
            raise TypeError(f"Cannot serialize field type: {type_def.name}")

    def _serialize_enum_value(self, value: EnumValue, enum_def: EnumTypeDefinition) -> bytes:
        """Serialize an enum value: discriminant + variant payload + padding."""
        disc_size = enum_def.discriminant_size
        max_payload = enum_def.max_payload_size

        # Serialize discriminant
        if disc_size == 1:
            data = struct.pack("<B", value.discriminant)
        elif disc_size == 2:
            data = struct.pack("<H", value.discriminant)
        else:
            data = struct.pack("<I", value.discriminant)

        # Serialize variant fields
        variant = enum_def.get_variant_by_discriminant(value.discriminant)
        payload = b""
        if variant and variant.fields:
            for f in variant.fields:
                fval = value.fields.get(f.name)
                if fval is None:
                    payload += b"\x00" * f.type_def.reference_size
                else:
                    payload += self._serialize_field_data(fval, f.type_def)

        # Pad to max_payload_size
        payload += b"\x00" * (max_payload - len(payload))
        return data + payload

    def _deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to a value."""
        if isinstance(self.type_def, PrimitiveTypeDefinition):
            return self._deserialize_primitive(data, self.type_def.primitive)
        elif isinstance(self.type_def, ArrayTypeDefinition):
            start_index, length = struct.unpack("<II", data)
            return (start_index, length)
        elif isinstance(self.type_def, CompositeTypeDefinition):
            return self._deserialize_composite(data, self.type_def)
        else:
            base = self.type_def.resolve_base_type()
            if isinstance(base, PrimitiveTypeDefinition):
                return self._deserialize_primitive(data, base.primitive)
            raise TypeError(f"Cannot deserialize type: {self.type_def.name}")

    def _deserialize_primitive(self, data: bytes, primitive: PrimitiveType) -> Any:
        """Deserialize a primitive value."""
        format_map = {
            PrimitiveType.BIT: "<?",
            PrimitiveType.CHARACTER: "<I",
            PrimitiveType.UINT8: "<B",
            PrimitiveType.INT8: "<b",
            PrimitiveType.UINT16: "<H",
            PrimitiveType.INT16: "<h",
            PrimitiveType.UINT32: "<I",
            PrimitiveType.INT32: "<i",
            PrimitiveType.UINT64: "<Q",
            PrimitiveType.INT64: "<q",
            PrimitiveType.FLOAT32: "<f",
            PrimitiveType.FLOAT64: "<d",
        }

        if primitive == PrimitiveType.CHARACTER:
            code_point = struct.unpack("<I", data)[0]
            return chr(code_point) if code_point else "\x00"
        elif primitive in (PrimitiveType.UINT128, PrimitiveType.INT128):
            low, high = struct.unpack("<QQ", data)
            return (high << 64) | low
        else:
            return struct.unpack(format_map[primitive], data)[0]

    def _deserialize_composite(
        self, data: bytes, type_def: CompositeTypeDefinition
    ) -> dict[str, Any]:
        """Deserialize a composite record.

        Returns a dict of field values/references:
        - Primitive fields: actual deserialized value
        - Array fields: (start_index, length) tuple
        - Composite ref fields: uint32 index
        - Null fields: None
        """
        result: dict[str, Any] = {}
        bitmap_size = type_def.null_bitmap_size
        bitmap = data[:bitmap_size]
        offset = bitmap_size

        for i, field in enumerate(type_def.fields):
            field_ref_size = field.type_def.reference_size
            field_data = data[offset : offset + field_ref_size]

            # Check null bitmap
            is_null = bool(bitmap[i // 8] & (1 << (i % 8)))
            if is_null:
                result[field.name] = None
                offset += field_ref_size
                continue

            field_base = field.type_def.resolve_base_type()
            if isinstance(field_base, EnumTypeDefinition):
                result[field.name] = self._deserialize_enum_value(field_data, field_base)
            elif isinstance(field_base, InterfaceTypeDefinition):
                type_id, index = struct.unpack("<HI", field_data)
                result[field.name] = (type_id, index)
            elif isinstance(field_base, ArrayTypeDefinition):
                result[field.name] = struct.unpack("<II", field_data)
            elif isinstance(field_base, CompositeTypeDefinition):
                result[field.name] = struct.unpack("<I", field_data)[0]
            elif isinstance(field_base, PrimitiveTypeDefinition):
                result[field.name] = self._deserialize_primitive(field_data, field_base.primitive)
            else:
                raise TypeError(f"Cannot deserialize field type: {field.type_def.name}")
            offset += field_ref_size

        return result

    def _deserialize_enum_value(self, data: bytes, enum_def: EnumTypeDefinition) -> EnumValue:
        """Deserialize an enum value from bytes."""
        disc_size = enum_def.discriminant_size

        # Read discriminant
        if disc_size == 1:
            disc = struct.unpack("<B", data[:1])[0]
        elif disc_size == 2:
            disc = struct.unpack("<H", data[:2])[0]
        else:
            disc = struct.unpack("<I", data[:4])[0]

        variant = enum_def.get_variant_by_discriminant(disc)
        if variant is None:
            return EnumValue(variant_name="?", discriminant=disc)

        # Deserialize variant fields
        fields: dict[str, Any] = {}
        payload_offset = disc_size
        for f in variant.fields:
            f_size = f.type_def.reference_size
            f_data = data[payload_offset:payload_offset + f_size]
            f_base = f.type_def.resolve_base_type()
            if isinstance(f_base, EnumTypeDefinition):
                fields[f.name] = self._deserialize_enum_value(f_data, f_base)
            elif isinstance(f_base, InterfaceTypeDefinition):
                type_id, index = struct.unpack("<HI", f_data)
                fields[f.name] = (type_id, index)
            elif isinstance(f_base, ArrayTypeDefinition):
                fields[f.name] = struct.unpack("<II", f_data)
            elif isinstance(f_base, CompositeTypeDefinition):
                fields[f.name] = struct.unpack("<I", f_data)[0]
            elif isinstance(f_base, PrimitiveTypeDefinition):
                fields[f.name] = self._deserialize_primitive(f_data, f_base.primitive)
            payload_offset += f_size

        return EnumValue(variant_name=variant.name, discriminant=disc, fields=fields)

    def close(self) -> None:
        """Close the table file."""
        if self._mmap is not None:
            self._mmap.flush()
            self._mmap.close()
            self._mmap = None
        if self._file is not None:
            self._file.close()
            self._file = None

    def __enter__(self) -> Table:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
