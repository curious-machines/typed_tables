"""Storage manager for typed tables."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from typed_tables.array_table import ArrayTable, create_array_table
from typed_tables.table import Table
from typed_tables.types import (
    AliasTypeDefinition,
    ArrayTypeDefinition,
    BooleanTypeDefinition,
    CompositeTypeDefinition,
    EnumTypeDefinition,
    EnumValue,
    EnumVariantDefinition,
    FieldDefinition,
    InterfaceTypeDefinition,
    PrimitiveType,
    PrimitiveTypeDefinition,
    StringTypeDefinition,
    TypeDefinition,
    TypeRegistry,
)

if TYPE_CHECKING:
    pass


class StorageManager:
    """Manages all tables for a schema."""

    METADATA_FILE = "_metadata.json"

    def __init__(self, data_dir: Path, registry: TypeRegistry) -> None:
        """Initialize the storage manager.

        Args:
            data_dir: Directory to store table files.
            registry: Type registry containing all type definitions.
        """
        self.data_dir = data_dir
        self.registry = registry
        self._tables: dict[str, Table] = {}
        self._array_tables: dict[str, ArrayTable] = {}
        self._variant_tables: dict[str, dict[str, Table]] = {}  # enum_name → {variant_name → Table}

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._save_metadata()

    def _save_metadata(self) -> None:
        """Save type metadata to disk."""
        metadata = {
            "types": self._serialize_type_registry(),
        }
        # Persist type_ids for tagged interface references
        if self.registry._type_ids:
            metadata["type_ids"] = self.registry._type_ids
        metadata_path = self.data_dir / self.METADATA_FILE
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    def save_metadata(self) -> None:
        """Public method to save type metadata to disk."""
        self._save_metadata()

    def _serialize_type_registry(self) -> dict[str, Any]:
        """Serialize the type registry to JSON-compatible format."""
        result: dict[str, Any] = {}

        for type_name in self.registry.list_types():
            type_def = self.registry.get(type_name)
            if type_def is None:
                continue

            result[type_name] = self._serialize_type_def(type_def)

        return result

    def _serialize_type_def(self, type_def: TypeDefinition) -> dict[str, Any]:
        """Serialize a single type definition."""
        if isinstance(type_def, BooleanTypeDefinition):
            return {"kind": "boolean"}
        elif isinstance(type_def, PrimitiveTypeDefinition):
            return {
                "kind": "primitive",
                "primitive": type_def.primitive.value,
            }
        elif isinstance(type_def, AliasTypeDefinition):
            return {
                "kind": "alias",
                "base_type": type_def.base_type.name,
            }
        elif isinstance(type_def, StringTypeDefinition):
            return {
                "kind": "string",
                "element_type": type_def.element_type.name,
            }
        elif isinstance(type_def, ArrayTypeDefinition):
            return {
                "kind": "array",
                "element_type": type_def.element_type.name,
            }
        elif isinstance(type_def, EnumTypeDefinition):
            variants = []
            for v in type_def.variants:
                vspec: dict[str, Any] = {"name": v.name, "discriminant": v.discriminant}
                if v.fields:
                    vspec["fields"] = [{"name": f.name, "type": f.type_def.name} for f in v.fields]
                variants.append(vspec)
            result_enum: dict[str, Any] = {
                "kind": "enum",
                "variants": variants,
                "has_explicit_values": type_def.has_explicit_values,
            }
            if type_def.backing_type is not None:
                result_enum["backing_type"] = type_def.backing_type.value
            return result_enum
        elif isinstance(type_def, InterfaceTypeDefinition):
            return {
                "kind": "interface",
                "fields": [self._serialize_field_def(f) for f in type_def.fields],
            }
        elif isinstance(type_def, CompositeTypeDefinition):
            result: dict[str, Any] = {
                "kind": "composite",
                "fields": [self._serialize_field_def(f) for f in type_def.fields],
            }
            if type_def.interfaces:
                result["interfaces"] = type_def.interfaces
            return result
        else:
            return {"kind": "unknown"}

    def _serialize_field_def(self, f: FieldDefinition) -> dict[str, Any]:
        """Serialize a field definition including optional default value."""
        entry: dict[str, Any] = {"name": f.name, "type": f.type_def.name}
        if f.overflow is not None:
            entry["overflow"] = f.overflow
        if f.default_value is not None:
            entry["default"] = self._serialize_default_value(f.default_value, f.type_def)
        return entry

    def _serialize_default_value(self, value: Any, type_def: TypeDefinition) -> Any:
        """Serialize a default value to JSON-compatible format."""
        if value is None:
            return None
        if isinstance(value, EnumValue):
            if value.fields:
                result: dict[str, Any] = {"_variant": value.variant_name}
                result.update(value.fields)
                return result
            return value.variant_name
        base = type_def.resolve_base_type()
        if isinstance(base, PrimitiveTypeDefinition):
            if base.primitive in (PrimitiveType.UINT128, PrimitiveType.INT128):
                return f"0x{value:032x}"
        return value

    def get_table(self, type_name: str) -> Table:
        """Get or create a table for the given type.

        Args:
            type_name: Name of the type.

        Returns:
            Table for the type.
        """
        if type_name in self._tables:
            return self._tables[type_name]

        type_def = self.registry.get_or_raise(type_name)
        base = type_def.resolve_base_type()

        # Interfaces have no .bin tables
        if isinstance(base, InterfaceTypeDefinition):
            raise ValueError(
                f"Interface types have no tables: {type_name}"
            )

        # For array types, we need special handling
        if isinstance(base, ArrayTypeDefinition):
            raise ValueError(
                f"Use get_array_table for array types: {type_name}"
            )

        table = Table(type_def, self.data_dir / f"{type_name}.bin")
        self._tables[type_name] = table
        return table

    def get_array_table_for_type(self, type_def: TypeDefinition) -> ArrayTable:
        """Get or create an array table for the given type definition.

        This handles both direct array types and aliases to array types.

        Args:
            type_def: The type definition (may be an alias to an array type).

        Returns:
            ArrayTable for the type.
        """
        base = type_def.resolve_base_type()

        if not isinstance(base, ArrayTypeDefinition):
            raise ValueError(f"Type '{type_def.name}' does not resolve to an array type")

        # Use the type's own name for the table (preserves alias names)
        type_name = type_def.name

        if type_name in self._array_tables:
            return self._array_tables[type_name]

        # Create array table using the resolved array type definition
        array_table = create_array_table(base, self.data_dir, type_name)
        self._array_tables[type_name] = array_table
        return array_table

    def get_array_table(self, type_name: str) -> ArrayTable:
        """Get or create an array table for the given type.

        Args:
            type_name: Name of the array type (or alias to array type).

        Returns:
            ArrayTable for the type.
        """
        type_def = self.registry.get_or_raise(type_name)
        return self.get_array_table_for_type(type_def)

    def get_table_for_type(self, type_def: TypeDefinition) -> Table | ArrayTable:
        """Get the appropriate table for a type definition.

        Args:
            type_def: The type definition.

        Returns:
            Table or ArrayTable for the type.
        """
        base = type_def.resolve_base_type()

        if isinstance(base, ArrayTypeDefinition):
            return self.get_array_table(type_def.name)
        else:
            return self.get_table(type_def.name)

    def get_variant_table(self, enum_def: EnumTypeDefinition, variant_name: str) -> Table:
        """Get or create a variant table for a Swift-style enum variant.

        Variant tables store the associated value fields for each variant
        in per-variant .bin files inside a folder named after the enum.
        """
        enum_name = enum_def.name
        if enum_name not in self._variant_tables:
            self._variant_tables[enum_name] = {}

        if variant_name in self._variant_tables[enum_name]:
            return self._variant_tables[enum_name][variant_name]

        variant = enum_def.get_variant(variant_name)
        if variant is None:
            raise ValueError(f"Unknown variant '{variant_name}' on enum '{enum_name}'")

        # Create a synthetic CompositeTypeDefinition for the variant's fields
        variant_type = CompositeTypeDefinition(
            name=f"_{enum_name}_{variant_name}",
            fields=list(variant.fields),
        )

        # Create folder and table
        enum_dir = self.data_dir / enum_name
        enum_dir.mkdir(exist_ok=True)
        table = Table(variant_type, enum_dir / f"{variant_name}.bin")
        self._variant_tables[enum_name][variant_name] = table
        return table

    def close(self) -> None:
        """Close all tables."""
        for table in self._tables.values():
            table.close()
        for array_table in self._array_tables.values():
            array_table.close()
        for variant_dict in self._variant_tables.values():
            for table in variant_dict.values():
                table.close()
        self._tables.clear()
        self._array_tables.clear()
        self._variant_tables.clear()

    def __enter__(self) -> StorageManager:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
