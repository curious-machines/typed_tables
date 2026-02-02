"""Schema class for managing typed tables."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from typed_tables.instance import InstanceRef
from typed_tables.parsing import TypeParser
from typed_tables.storage import StorageManager
from typed_tables.types import (
    ArrayTypeDefinition,
    CompositeTypeDefinition,
    TypeDefinition,
    TypeRegistry,
)


class Schema:
    """Parsed type definitions with storage management."""

    def __init__(self, registry: TypeRegistry, data_dir: Path) -> None:
        """Initialize a schema.

        Args:
            registry: Type registry with all type definitions.
            data_dir: Directory for storing table files.
        """
        self.registry = registry
        self.storage = StorageManager(data_dir, registry)

    @classmethod
    def parse(cls, type_definitions: str, data_dir: Path | str) -> Schema:
        """Parse type definitions and create a schema.

        Args:
            type_definitions: DSL string defining types.
            data_dir: Directory for storing table files.

        Returns:
            A new Schema instance.
        """
        parser = TypeParser()
        registry = parser.parse(type_definitions)

        if isinstance(data_dir, str):
            data_dir = Path(data_dir)

        return cls(registry, data_dir)

    def get_type(self, name: str) -> TypeDefinition:
        """Get a type definition by name.

        Args:
            name: Name of the type.

        Returns:
            The type definition.

        Raises:
            KeyError: If the type is not found.
        """
        return self.registry.get_or_raise(name)

    def list_types(self) -> list[str]:
        """List all registered type names.

        Returns:
            List of type names.
        """
        return self.registry.list_types()

    def create_instance(self, type_name: str, values: Any) -> InstanceRef:
        """Create an instance of a type and store it.

        Args:
            type_name: Name of the type to instantiate.
            values: Values for the instance (dict or tuple for composites,
                   list for arrays, scalar for primitives).

        Returns:
            InstanceRef pointing to the stored instance.
        """
        type_def = self.registry.get_or_raise(type_name)
        return self._create_instance_for_type(type_def, values)

    def _create_instance_for_type(
        self, type_def: TypeDefinition, values: Any
    ) -> InstanceRef:
        """Create an instance for a specific type definition."""
        base = type_def.resolve_base_type()

        if isinstance(base, ArrayTypeDefinition):
            return self._create_array_instance(type_def, base, values)
        elif isinstance(base, CompositeTypeDefinition):
            return self._create_composite_instance(type_def, base, values)
        else:
            return self._create_primitive_instance(type_def, values)

    def _create_primitive_instance(
        self, type_def: TypeDefinition, value: Any
    ) -> InstanceRef:
        """Create a primitive instance."""
        table = self.storage.get_table(type_def.name)
        index = table.insert(value)
        return InstanceRef(schema=self, type_name=type_def.name, index=index)

    def _create_array_instance(
        self,
        type_def: TypeDefinition,
        array_type: ArrayTypeDefinition,
        values: list[Any],
    ) -> InstanceRef:
        """Create an array instance."""
        array_table = self.storage.get_array_table_for_type(type_def)
        index = array_table.insert(values)
        return InstanceRef(schema=self, type_name=type_def.name, index=index)

    def _create_composite_instance(
        self,
        type_def: TypeDefinition,
        composite_type: CompositeTypeDefinition,
        values: dict[str, Any] | tuple[Any, ...] | list[Any],
    ) -> InstanceRef:
        """Create a composite instance.

        All field values are stored in their respective type tables.
        The composite record stores only references (indices) to those values.
        """
        # Convert tuple/list to dict
        if isinstance(values, (tuple, list)):
            values = {
                field.name: v for field, v in zip(composite_type.fields, values)
            }

        # Process each field value - store in field's type table, get reference
        field_references: dict[str, Any] = {}

        for field in composite_type.fields:
            field_value = values[field.name]
            field_base = field.type_def.resolve_base_type()

            if isinstance(field_base, ArrayTypeDefinition):
                # Store array elements and get index into the array's header table
                instance_ref = self._create_array_instance(
                    field.type_def, field_base, field_value
                )
                field_references[field.name] = instance_ref.index
            elif isinstance(field_base, CompositeTypeDefinition):
                # Store nested composite and get index reference
                instance_ref = self._create_composite_instance(
                    field.type_def, field_base, field_value
                )
                field_references[field.name] = instance_ref.index
            else:
                # Store primitive/alias value in its type's table and get index
                instance_ref = self._create_instance_for_type(field.type_def, field_value)
                field_references[field.name] = instance_ref.index

        # Store the composite record (contains only references)
        table = self.storage.get_table(type_def.name)
        index = table.insert(field_references)
        return InstanceRef(schema=self, type_name=type_def.name, index=index)

    def get_instance(self, type_name: str, index: int) -> InstanceRef:
        """Get an instance reference by type and index.

        Args:
            type_name: Name of the type.
            index: Index in the table.

        Returns:
            InstanceRef for the instance.
        """
        return InstanceRef(schema=self, type_name=type_name, index=index)

    def close(self) -> None:
        """Close all storage resources."""
        self.storage.close()

    def __enter__(self) -> Schema:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
