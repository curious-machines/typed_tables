"""Instance reference for typed table entries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typed_tables.schema import Schema


@dataclass
class InstanceRef:
    """Reference to an instance in a typed table.

    An InstanceRef is a (type_name, index) pair that uniquely identifies
    a value stored in the typed tables system.
    """

    schema: Schema
    type_name: str
    index: int

    def load(self, resolve_references: bool = True) -> Any:
        """Load and return the value from storage.

        Args:
            resolve_references: If True, field references in composites are resolved
                to their actual values. If False, returns raw indices/headers.

        Returns:
            The stored value. For primitives, returns the scalar value.
            For arrays, returns a list of elements.
            For composites, returns a dict of field values.
        """
        type_def = self.schema.registry.get_or_raise(self.type_name)
        base = type_def.resolve_base_type()

        from typed_tables.types import ArrayTypeDefinition, CompositeTypeDefinition

        if isinstance(base, ArrayTypeDefinition):
            array_table = self.schema.storage.get_array_table_for_type(type_def)
            return array_table.get(self.index)
        elif isinstance(base, CompositeTypeDefinition):
            table = self.schema.storage.get_table(self.type_name)
            raw_data = table.get(self.index)

            if resolve_references:
                return self._resolve_field_references(raw_data, base)
            return raw_data
        else:
            table = self.schema.storage.get_table(self.type_name)
            return table.get(self.index)

    def _resolve_field_references(
        self, data: dict[str, Any], composite_type: "CompositeTypeDefinition"
    ) -> dict[str, Any]:
        """Resolve all field references in composite data to actual values.

        Each field in a composite stores a reference to its value in another table.
        This method resolves those references to return the actual values.
        """
        from typed_tables.types import ArrayTypeDefinition, CompositeTypeDefinition

        result = {}
        for field in composite_type.fields:
            field_ref = data[field.name]
            field_base = field.type_def.resolve_base_type()

            if isinstance(field_base, ArrayTypeDefinition):
                # Resolve array reference (start_index, length)
                start_index, length = field_ref
                if length == 0:
                    result[field.name] = []
                else:
                    array_table = self.schema.storage.get_array_table_for_type(field.type_def)
                    elements = [
                        array_table.element_table.get(start_index + i)
                        for i in range(length)
                    ]
                    result[field.name] = elements
            elif isinstance(field_base, CompositeTypeDefinition):
                # Resolve nested composite reference (index)
                nested_table = self.schema.storage.get_table(field.type_def.name)
                nested_raw = nested_table.get(field_ref)
                result[field.name] = self._resolve_field_references(nested_raw, field_base)
            else:
                # Resolve primitive/alias reference (index)
                field_table = self.schema.storage.get_table(field.type_def.name)
                result[field.name] = field_table.get(field_ref)

        return result

    def __repr__(self) -> str:
        return f"InstanceRef({self.type_name!r}, {self.index})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, InstanceRef):
            return NotImplemented
        return self.type_name == other.type_name and self.index == other.index

    def __hash__(self) -> int:
        return hash((self.type_name, self.index))
