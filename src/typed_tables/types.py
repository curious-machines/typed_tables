"""Type definitions for the typed_tables library."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

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
    FLOAT16 = "float16"
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
            PrimitiveType.FLOAT16: 2,
            PrimitiveType.FLOAT32: 4,
            PrimitiveType.FLOAT64: 8,
        }
        return sizes[self]


# Mapping from type name strings to PrimitiveType enum values
PRIMITIVE_TYPE_NAMES: dict[str, PrimitiveType] = {pt.value: pt for pt in PrimitiveType}


def type_range(prim: PrimitiveType) -> tuple[int | float, int | float]:
    """Return (min_val, max_val) for a primitive type."""
    ranges: dict[PrimitiveType, tuple[int | float, int | float]] = {
        PrimitiveType.BIT: (0, 1),
        PrimitiveType.CHARACTER: (0, 0x10FFFF),
        PrimitiveType.UINT8: (0, 0xFF),
        PrimitiveType.INT8: (-128, 127),
        PrimitiveType.UINT16: (0, 0xFFFF),
        PrimitiveType.INT16: (-32768, 32767),
        PrimitiveType.UINT32: (0, 0xFFFFFFFF),
        PrimitiveType.INT32: (-(2**31), 2**31 - 1),
        PrimitiveType.UINT64: (0, 2**64 - 1),
        PrimitiveType.INT64: (-(2**63), 2**63 - 1),
        PrimitiveType.UINT128: (0, 2**128 - 1),
        PrimitiveType.INT128: (-(2**127), 2**127 - 1),
        PrimitiveType.FLOAT16: (-65504.0, 65504.0),
        PrimitiveType.FLOAT32: (-3.4028235e+38, 3.4028235e+38),
        PrimitiveType.FLOAT64: (-1.7976931348623157e+308, 1.7976931348623157e+308),
    }
    return ranges[prim]


# Size of a reference (index) to an entry in a table
REFERENCE_SIZE = 4  # uint32 index

# Size of a polymorphic (interface) reference: uint16 type_id + uint32 index
INTERFACE_REFERENCE_SIZE = 6

# Sentinel value: field points to no entry in the referenced table
NULL_REF = 0xFFFFFFFF

# Sentinel for null array references: (start=NULL_REF, length=0)
NULL_ARRAY_REF = (NULL_REF, 0)


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

    @property
    def is_enum(self) -> bool:
        """Return whether this type is an enum type."""
        return False

    @property
    def is_interface(self) -> bool:
        """Return whether this type is an interface type."""
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
    def reference_size(self) -> int:
        """Primitives are stored inline in composites — size equals the value size."""
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
        """Arrays store (start_index, length) inline = 8 bytes."""
        return self.HEADER_SIZE

    @property
    def is_array(self) -> bool:
        return True


@dataclass
class StringTypeDefinition(ArrayTypeDefinition):
    """Built-in string type — stored as character[], displayed as a joined string."""

    pass


def is_string_type(type_def: TypeDefinition) -> bool:
    """Check if a type resolves to the built-in string type."""
    return isinstance(type_def.resolve_base_type(), StringTypeDefinition)


@dataclass
class BigIntTypeDefinition(ArrayTypeDefinition):
    """Built-in bigint type — arbitrary-precision signed integer, stored as uint8[]."""

    pass


@dataclass
class BigUIntTypeDefinition(ArrayTypeDefinition):
    """Built-in biguint type — arbitrary-precision unsigned integer, stored as uint8[]."""

    pass


def is_bigint_type(type_def: TypeDefinition) -> bool:
    """Check if a type resolves to the built-in bigint type."""
    return isinstance(type_def.resolve_base_type(), BigIntTypeDefinition)


def is_biguint_type(type_def: TypeDefinition) -> bool:
    """Check if a type resolves to the built-in biguint type."""
    return isinstance(type_def.resolve_base_type(), BigUIntTypeDefinition)


class BigInt(int):
    """Wrapper to distinguish bigint values from regular ints for decimal display."""

    pass


class BigUInt(int):
    """Wrapper to distinguish biguint values from regular ints for decimal display."""

    pass


@dataclass
class SetTypeDefinition(ArrayTypeDefinition):
    """Set type — stored as array with uniqueness constraint."""

    pass


def is_set_type(type_def: TypeDefinition) -> bool:
    """Check if a type resolves to a set type."""
    return isinstance(type_def.resolve_base_type(), SetTypeDefinition)


@dataclass
class DictionaryTypeDefinition(TypeDefinition):
    """Dictionary type — stored as array of synthetic entry composites."""

    key_type: TypeDefinition
    value_type: TypeDefinition
    entry_type: CompositeTypeDefinition  # synthetic, e.g. Dict_string_int32

    HEADER_SIZE: int = 8  # same as ArrayTypeDefinition

    @property
    def size_bytes(self) -> int:
        return self.HEADER_SIZE

    @property
    def reference_size(self) -> int:
        """Dicts store (start_index, length) inline = 8 bytes."""
        return self.HEADER_SIZE

    @property
    def is_array(self) -> bool:
        return True


def is_dict_type(type_def: TypeDefinition) -> bool:
    """Check if a type resolves to a dictionary type."""
    return isinstance(type_def.resolve_base_type(), DictionaryTypeDefinition)


class SetValue(list):
    """Wrapper to distinguish set values from regular lists in display."""

    pass


@dataclass
class BooleanTypeDefinition(PrimitiveTypeDefinition):
    """Built-in boolean type — stored as bit, displayed as true/false."""

    pass


def is_boolean_type(type_def: TypeDefinition) -> bool:
    """Check if a type resolves to the built-in boolean type."""
    return isinstance(type_def.resolve_base_type(), BooleanTypeDefinition)


@dataclass
class TypedValue:
    """A value with a known primitive type, used for type-checked math expressions."""

    value: int | float
    type_name: str  # "int8", "uint16", "float32", etc.

    def __repr__(self) -> str:
        return f"TypedValue({self.value!r}, {self.type_name!r})"


@dataclass
class FieldDefinition:
    """Definition of a field within a composite type."""

    name: str
    type_def: TypeDefinition
    default_value: Any = None  # None = NULL default (current behavior)
    overflow: str | None = None  # "saturating" or "wrapping"


@dataclass
class CompositeTypeDefinition(TypeDefinition):
    """Type definition for composite types (structs).

    A composite record layout:
      [null_bitmap (ceil(N/8) bytes)] [field0_data] [field1_data] ...

    Primitive fields are stored inline (actual value bytes).
    Array fields store (start_index, length) = 8 bytes.
    Composite ref fields store a uint32 index = 4 bytes.
    """

    fields: list[FieldDefinition] = field(default_factory=list)
    interfaces: list[str] = field(default_factory=list)

    @property
    def null_bitmap_size(self) -> int:
        """Return the number of bytes needed for the null bitmap."""
        if not self.fields:
            return 0
        return (len(self.fields) + 7) // 8

    @property
    def size_bytes(self) -> int:
        """Return the total record size: bitmap + all field data."""
        return self.null_bitmap_size + sum(f.type_def.reference_size for f in self.fields)

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
        """Get the byte offset of a field within the composite record (after bitmap)."""
        offset = self.null_bitmap_size
        for f in self.fields:
            if f.name == name:
                return offset
            offset += f.type_def.reference_size
        raise KeyError(f"Field '{name}' not found in type '{self.name}'")


@dataclass
class InterfaceTypeDefinition(TypeDefinition):
    """Type definition for interface types.

    Interfaces define field contracts but are not instantiable.
    Concrete types implement interfaces via multiple inheritance.
    When used as a field type, stores a tagged reference:
    [uint16 type_id][uint32 index] = 6 bytes.
    """

    fields: list[FieldDefinition] = field(default_factory=list)

    @property
    def null_bitmap_size(self) -> int:
        """Return the number of bytes needed for the null bitmap."""
        if not self.fields:
            return 0
        return (len(self.fields) + 7) // 8

    @property
    def size_bytes(self) -> int:
        """Return the total record size: bitmap + all field data."""
        return self.null_bitmap_size + sum(f.type_def.reference_size for f in self.fields)

    @property
    def reference_size(self) -> int:
        """Interface refs use tagged references: uint16 type_id + uint32 index = 6 bytes."""
        return INTERFACE_REFERENCE_SIZE

    @property
    def is_interface(self) -> bool:
        return True

    def get_field(self, name: str) -> FieldDefinition | None:
        """Get a field by name."""
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def get_field_offset(self, name: str) -> int:
        """Get the byte offset of a field within the interface record (after bitmap)."""
        offset = self.null_bitmap_size
        for f in self.fields:
            if f.name == name:
                return offset
            offset += f.type_def.reference_size
        raise KeyError(f"Field '{name}' not found in type '{self.name}'")


@dataclass
class EnumVariantDefinition:
    """A single variant within an enum type."""

    name: str
    discriminant: int
    fields: list[FieldDefinition] = field(default_factory=list)  # empty for C-style


@dataclass
class EnumValue:
    """Runtime representation of an enum value (returned by deserialization)."""

    variant_name: str
    discriminant: int
    fields: dict[str, Any] = field(default_factory=dict)


@dataclass
class EnumTypeDefinition(TypeDefinition):
    """Enum type definition — covers both C-style and Swift-style."""

    variants: list[EnumVariantDefinition] = field(default_factory=list)
    has_explicit_values: bool = False  # True for C-style with `= N`
    backing_type: PrimitiveType | None = None  # e.g. PrimitiveType.UINT8 for `enum Color : uint8 { ... }`

    @property
    def discriminant_size(self) -> int:
        """Size of discriminant in bytes."""
        max_disc = max(v.discriminant for v in self.variants) if self.variants else 0
        if max_disc <= 0xFF:
            return 1
        if max_disc <= 0xFFFF:
            return 2
        return 4

    @property
    def has_associated_values(self) -> bool:
        """True if any variant has fields (Swift-style)."""
        return any(v.fields for v in self.variants)

    @property
    def size_bytes(self) -> int:
        if self.has_associated_values:
            return self.discriminant_size + REFERENCE_SIZE  # disc + uint32 variant table index
        return self.discriminant_size  # C-style: discriminant only

    @property
    def reference_size(self) -> int:
        """Enums stored inline (like primitives)."""
        return self.size_bytes

    @property
    def is_enum(self) -> bool:
        return True

    def get_variant(self, name: str) -> EnumVariantDefinition | None:
        for v in self.variants:
            if v.name == name:
                return v
        return None

    def get_variant_by_discriminant(self, disc: int) -> EnumVariantDefinition | None:
        for v in self.variants:
            if v.discriminant == disc:
                return v
        return None


def _type_def_to_type_string(td: TypeDefinition) -> str:
    """Convert a TypeDefinition back to its TTQ type string representation."""
    if isinstance(td, SetTypeDefinition):
        return "{" + _type_def_to_type_string(td.element_type) + "}"
    if isinstance(td, DictionaryTypeDefinition):
        return "{" + _type_def_to_type_string(td.key_type) + ": " + _type_def_to_type_string(td.value_type) + "}"
    if isinstance(td, (BigIntTypeDefinition, BigUIntTypeDefinition)):
        return td.name
    if isinstance(td, StringTypeDefinition):
        return td.name
    if isinstance(td, ArrayTypeDefinition):
        return td.name  # e.g. "int32[]"
    return td.name


def _make_dict_entry_type_name(key_td: TypeDefinition, val_td: TypeDefinition) -> str:
    """Generate a dict entry type name like Dict_string_int32."""

    def _td_component(td: TypeDefinition) -> str:
        if isinstance(td, SetTypeDefinition):
            return "Set_" + _td_component(td.element_type)
        if isinstance(td, DictionaryTypeDefinition):
            return "Dict_" + _td_component(td.key_type) + "_" + _td_component(td.value_type)
        if isinstance(td, StringTypeDefinition):
            return td.name  # "string", not "Array_character"
        if isinstance(td, ArrayTypeDefinition):
            return "Array_" + _td_component(td.element_type)
        return td.name

    return "Dict_" + _td_component(key_td) + "_" + _td_component(val_td)


class TypeRegistry:
    """Registry of all defined types."""

    def __init__(self) -> None:
        self._types: dict[str, TypeDefinition] = {}
        self._type_ids: dict[str, int] = {}
        self._next_type_id: int = 1  # 0 reserved for "no type"
        self._register_primitives()

    def _register_primitives(self) -> None:
        """Register all primitive types."""
        for pt in PrimitiveType:
            self._types[pt.value] = PrimitiveTypeDefinition(name=pt.value, primitive=pt)
        # Register built-in string type (stored as character[], displayed as string)
        char_prim = self._types["character"]
        self._types["string"] = StringTypeDefinition(name="string", element_type=char_prim)
        # Register built-in boolean type (stored as bit, displayed as true/false)
        self._types["boolean"] = BooleanTypeDefinition(name="boolean", primitive=PrimitiveType.BIT)
        # Register built-in bigint/biguint types (stored as uint8[], arbitrary precision)
        uint8_prim = self._types["uint8"]
        self._types["bigint"] = BigIntTypeDefinition(name="bigint", element_type=uint8_prim)
        self._types["biguint"] = BigUIntTypeDefinition(name="biguint", element_type=uint8_prim)
        # Register built-in path alias (alias for string)
        self._types["path"] = AliasTypeDefinition(name="path", base_type=self._types["string"])

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

    def get_or_create_set_type(self, element_type: TypeDefinition) -> SetTypeDefinition:
        """Get or create a set type for the given element type."""
        set_name = "{" + _type_def_to_type_string(element_type) + "}"
        existing = self._types.get(set_name)
        if existing is not None:
            if not isinstance(existing, SetTypeDefinition):
                raise TypeError(f"Type '{set_name}' exists but is not a set type")
            return existing

        set_type = SetTypeDefinition(name=set_name, element_type=element_type)
        self._types[set_name] = set_type
        return set_type

    def get_or_create_dict_type(
        self,
        key_type: TypeDefinition,
        value_type: TypeDefinition,
        name: str | None = None,
    ) -> DictionaryTypeDefinition:
        """Get or create a dictionary type for the given key/value types.

        Also creates the synthetic entry composite if needed.
        """
        dict_name = name or ("{" + _type_def_to_type_string(key_type) + ": " + _type_def_to_type_string(value_type) + "}")
        existing = self._types.get(dict_name)
        if existing is not None:
            if not isinstance(existing, DictionaryTypeDefinition):
                raise TypeError(f"Type '{dict_name}' exists but is not a dictionary type")
            return existing

        # Create entry composite
        entry_name = _make_dict_entry_type_name(key_type, value_type)
        entry_type = self._types.get(entry_name)
        if entry_type is None or not isinstance(entry_type, CompositeTypeDefinition):
            entry_type = CompositeTypeDefinition(
                name=entry_name,
                fields=[
                    FieldDefinition(name="key", type_def=key_type),
                    FieldDefinition(name="value", type_def=value_type),
                ],
            )
            self._types[entry_name] = entry_type

        dict_type = DictionaryTypeDefinition(
            name=dict_name,
            key_type=key_type,
            value_type=value_type,
            entry_type=entry_type,
        )
        self._types[dict_name] = dict_type
        return dict_type

    def register_enum_stub(self, name: str) -> EnumTypeDefinition:
        """Pre-register an empty enum for forward-declaration support.

        Idempotent: returns existing stub if name is already an empty enum.
        Raises ValueError if name is registered with a non-empty type.
        """
        existing = self._types.get(name)
        if existing is not None:
            if isinstance(existing, EnumTypeDefinition) and not existing.variants:
                return existing
            raise ValueError(f"Type '{name}' is already defined")
        stub = EnumTypeDefinition(name=name, variants=[])
        self._types[name] = stub
        return stub

    def is_enum_stub(self, name: str) -> bool:
        """Check if a type is registered as an unpopulated enum stub."""
        td = self._types.get(name)
        return isinstance(td, EnumTypeDefinition) and not td.variants

    def register_stub(self, name: str) -> CompositeTypeDefinition:
        """Pre-register an empty composite for forward/self-references.

        Idempotent: returns existing stub if name is already an empty composite.
        Raises ValueError if name is registered with a non-empty type.
        """
        existing = self._types.get(name)
        if existing is not None:
            if isinstance(existing, CompositeTypeDefinition) and not existing.fields:
                return existing
            raise ValueError(f"Type '{name}' is already defined")
        stub = CompositeTypeDefinition(name=name, fields=[])
        self._types[name] = stub
        return stub

    def is_stub(self, name: str) -> bool:
        """Check if a type is registered as an unpopulated stub."""
        td = self._types.get(name)
        return isinstance(td, CompositeTypeDefinition) and not td.fields

    def register_interface_stub(self, name: str) -> "InterfaceTypeDefinition":
        """Pre-register an empty interface for forward-declaration support.

        Idempotent: returns existing stub if name is already an empty interface.
        Raises ValueError if name is registered with a non-empty type.
        """
        existing = self._types.get(name)
        if existing is not None:
            if isinstance(existing, InterfaceTypeDefinition) and not existing.fields:
                return existing
            raise ValueError(f"Type '{name}' is already defined")
        stub = InterfaceTypeDefinition(name=name, fields=[])
        self._types[name] = stub
        return stub

    def is_interface_stub(self, name: str) -> bool:
        """Check if a type is registered as an unpopulated interface stub."""
        td = self._types.get(name)
        return isinstance(td, InterfaceTypeDefinition) and not td.fields

    def find_implementing_types(self, interface_name: str) -> list[tuple[str, "CompositeTypeDefinition"]]:
        """Find all composite types that implement the given interface.

        Returns list of (type_name, composite_def) tuples.
        """
        results: list[tuple[str, CompositeTypeDefinition]] = []
        for name, td in self._types.items():
            if isinstance(td, CompositeTypeDefinition) and interface_name in td.interfaces:
                results.append((name, td))
        return results

    def get_type_id(self, type_name: str) -> int:
        """Get or assign a numeric type ID for a concrete type (for tagged references)."""
        if type_name not in self._type_ids:
            self._type_ids[type_name] = self._next_type_id
            self._next_type_id += 1
        return self._type_ids[type_name]

    def get_type_name_by_id(self, type_id: int) -> str | None:
        """Look up a type name by its numeric ID."""
        for name, tid in self._type_ids.items():
            if tid == type_id:
                return name
        return None

    def list_types(self) -> list[str]:
        """List all registered type names."""
        return list(self._types.keys())

    def find_composites_with_field_type(
        self, type_name: str
    ) -> list[tuple[str, str, "CompositeTypeDefinition"]]:
        """Find all composite types that have a field whose type matches type_name.

        Returns a list of (composite_name, field_name, composite_def) tuples
        for each composite field whose type name matches or whose base resolves
        to the same base as type_name.
        """
        target = self._types.get(type_name)
        if target is None:
            return []
        target_base = target.resolve_base_type()

        results: list[tuple[str, str, CompositeTypeDefinition]] = []
        for name, td in self._types.items():
            if not isinstance(td, CompositeTypeDefinition):
                continue
            for f in td.fields:
                field_base = f.type_def.resolve_base_type()
                # Match if field type name equals target, or both resolve to same base
                if f.type_def.name == type_name or (
                    type(field_base) is type(target_base)
                    and field_base.name == target_base.name
                ):
                    results.append((name, f.name, td))
        return results

    def find_enum_variants_with_field_type(
        self, type_name: str
    ) -> list[tuple[str, str, str, str, "CompositeTypeDefinition"]]:
        """Find composite fields that are enums whose variants contain a given type.

        Returns list of (comp_name, enum_field_name, variant_name, variant_field_name, comp_def)
        for type-based queries that traverse into enum variant payloads.
        """
        target = self._types.get(type_name)
        if target is None:
            return []
        target_base = target.resolve_base_type()

        results: list[tuple[str, str, str, str, CompositeTypeDefinition]] = []
        for name, td in self._types.items():
            if not isinstance(td, CompositeTypeDefinition):
                continue
            for f in td.fields:
                field_base = f.type_def.resolve_base_type()
                if not isinstance(field_base, EnumTypeDefinition):
                    continue
                for variant in field_base.variants:
                    for vf in variant.fields:
                        vf_base = vf.type_def.resolve_base_type()
                        if vf.type_def.name == type_name or (
                            type(vf_base) is type(target_base)
                            and vf_base.name == target_base.name
                        ):
                            results.append((name, f.name, variant.name, vf.name, td))
        return results

    def __contains__(self, name: str) -> bool:
        return name in self._types
