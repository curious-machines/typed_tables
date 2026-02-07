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
    CompositeTypeDefinition,
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

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._save_metadata()

    def _save_metadata(self) -> None:
        """Save type metadata to disk."""
        metadata = {
            "types": self._serialize_type_registry(),
        }
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
        if isinstance(type_def, PrimitiveTypeDefinition):
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
        elif isinstance(type_def, CompositeTypeDefinition):
            return {
                "kind": "composite",
                "fields": [
                    {"name": f.name, "type": f.type_def.name} for f in type_def.fields
                ],
            }
        else:
            return {"kind": "unknown"}

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

    def close(self) -> None:
        """Close all tables."""
        for table in self._tables.values():
            table.close()
        for array_table in self._array_tables.values():
            array_table.close()
        self._tables.clear()
        self._array_tables.clear()

    def __enter__(self) -> StorageManager:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
