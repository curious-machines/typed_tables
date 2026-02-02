"""Type definitions for the typed_tables library."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class PrimitiveType(Enum):
    """Built-in primitive types supported by the type system."""

    BIT = "bit"
    CHARACTER = "character"
    UINT8 = "uint8"
    INT8 = "int8"
    UINT16 = "uint16"
    INT16 = "int16"
    UINT32 = "uint32"
    INT32 = "int32"
    UINT64 = "uint64"
    INT64 = "int64"
    UINT128 = "uint128"
    INT128 = "int128"
    FLOAT32 = "float32"
    FLOAT64 = "float64"

    @property
    def size_bytes(self) -> int:
        """Return the size in bytes for this primitive type."""
        sizes = {
            PrimitiveType.BIT: 1,  # Stored as 1 byte for simplicity
            PrimitiveType.CHARACTER: 4,  # Unicode code point (UTF-32)
            PrimitiveType.UINT8: 1,
            PrimitiveType.INT8: 1,
            PrimitiveType.UINT16: 2,
            PrimitiveType.INT16: 2,
            PrimitiveType.UINT32: 4,
            PrimitiveType.INT32: 4,
            PrimitiveType.UINT64: 8,
            PrimitiveType.INT64: 8,
            PrimitiveType.UINT128: 16,
            PrimitiveType.INT128: 16,
            PrimitiveType.FLOAT32: 4,
            PrimitiveType.FLOAT64: 8,
        }
        return sizes[self]


# Mapping from type name strings to PrimitiveType enum values
PRIMITIVE_TYPE_NAMES: dict[str, PrimitiveType] = {pt.value: pt for pt in PrimitiveType}


# Size of a reference (index) to an entry in a table
REFERENCE_SIZE = 4  # uint32 index


@dataclass
class TypeDefinition:
    """Base class for all type definitions."""

    name: str

    @property
    def size_bytes(self) -> int:
        """Return the size in bytes for storing a value of this type."""
        raise NotImplementedError

    @property
    def reference_size(self) -> int:
        """Return the size in bytes for storing a reference to this type.

        When a composite type has a field of this type, it stores a reference
        (index) to the value in the field type's table, not the value itself.
        For arrays, this is (start_index, length) = 8 bytes.
        For all other types, this is a uint32 index = 4 bytes.
        """
        return REFERENCE_SIZE

    @property
    def is_array(self) -> bool:
        """Return whether this type is an array type."""
        return False

    @property
    def is_primitive(self) -> bool:
        """Return whether this type is a primitive type."""
        return False

    @property
    def is_composite(self) -> bool:
        """Return whether this type is a composite type."""
        return False

    def resolve_base_type(self) -> TypeDefinition:
        """Resolve through aliases to get the underlying type."""
        return self


@dataclass
class PrimitiveTypeDefinition(TypeDefinition):
    """Type definition wrapping a primitive type."""

    primitive: PrimitiveType

    @property
    def size_bytes(self) -> int:
        return self.primitive.size_bytes

    @property
    def is_primitive(self) -> bool:
        return True


@dataclass
class AliasTypeDefinition(TypeDefinition):
    """Type definition for 'define X as Y' aliases."""

    base_type: TypeDefinition

    @property
    def size_bytes(self) -> int:
        return self.base_type.size_bytes

    @property
    def reference_size(self) -> int:
        return self.base_type.reference_size

    @property
    def is_array(self) -> bool:
        return self.base_type.is_array

    def resolve_base_type(self) -> TypeDefinition:
        """Resolve through aliases to get the underlying type."""
        return self.base_type.resolve_base_type()


@dataclass
class ArrayTypeDefinition(TypeDefinition):
    """Type definition for array types (e.g., uint8[])."""

    element_type: TypeDefinition

    # Array header is fixed: uint32 start_index + uint32 length = 8 bytes
    HEADER_SIZE: int = 8

    @property
    def size_bytes(self) -> int:
        """Return the size of the array header (not the contents)."""
        return self.HEADER_SIZE

    @property
    def reference_size(self) -> int:
        """Arrays use an index into the header table as their reference."""
        return REFERENCE_SIZE

    @property
    def is_array(self) -> bool:
        return True


@dataclass
class FieldDefinition:
    """Definition of a field within a composite type."""

    name: str
    type_def: TypeDefinition


@dataclass
class CompositeTypeDefinition(TypeDefinition):
    """Type definition for composite types (structs).

    A composite stores references to values in other tables, not the values
    themselves. Each field's value is stored in its own type's table, and
    the composite record stores indices (or start_index+length for arrays).
    """

    fields: list[FieldDefinition] = field(default_factory=list)

    @property
    def size_bytes(self) -> int:
        """Return the total size of all field references.

        This is the size of a composite record, which stores references
        to field values, not the actual values.
        """
        return sum(f.type_def.reference_size for f in self.fields)

    @property
    def is_composite(self) -> bool:
        return True

    def get_field(self, name: str) -> FieldDefinition | None:
        """Get a field by name."""
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def get_field_offset(self, name: str) -> int:
        """Get the byte offset of a field reference within the composite."""
        offset = 0
        for f in self.fields:
            if f.name == name:
                return offset
            offset += f.type_def.reference_size
        raise KeyError(f"Field '{name}' not found in type '{self.name}'")


class TypeRegistry:
    """Registry of all defined types."""

    def __init__(self) -> None:
        self._types: dict[str, TypeDefinition] = {}
        self._register_primitives()

    def _register_primitives(self) -> None:
        """Register all primitive types."""
        for pt in PrimitiveType:
            self._types[pt.value] = PrimitiveTypeDefinition(name=pt.value, primitive=pt)

    def register(self, type_def: TypeDefinition) -> None:
        """Register a type definition."""
        if type_def.name in self._types:
            raise ValueError(f"Type '{type_def.name}' is already defined")
        self._types[type_def.name] = type_def

    def get(self, name: str) -> TypeDefinition | None:
        """Get a type by name."""
        return self._types.get(name)

    def get_or_raise(self, name: str) -> TypeDefinition:
        """Get a type by name, raising if not found."""
        type_def = self._types.get(name)
        if type_def is None:
            raise KeyError(f"Type '{name}' not found")
        return type_def

    def get_array_type(self, element_type_name: str) -> ArrayTypeDefinition:
        """Get or create an array type for the given element type."""
        array_name = f"{element_type_name}[]"
        existing = self._types.get(array_name)
        if existing is not None:
            if not isinstance(existing, ArrayTypeDefinition):
                raise TypeError(f"Type '{array_name}' exists but is not an array type")
            return existing

        element_type = self.get_or_raise(element_type_name)
        array_type = ArrayTypeDefinition(name=array_name, element_type=element_type)
        self._types[array_name] = array_type
        return array_type

    def list_types(self) -> list[str]:
        """List all registered type names."""
        return list(self._types.keys())

    def __contains__(self, name: str) -> bool:
        return name in self._types
