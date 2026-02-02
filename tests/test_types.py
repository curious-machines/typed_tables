"""Tests for the type system."""

import pytest

from typed_tables.types import (
    AliasTypeDefinition,
    ArrayTypeDefinition,
    CompositeTypeDefinition,
    FieldDefinition,
    PrimitiveType,
    PrimitiveTypeDefinition,
    TypeRegistry,
)


class TestPrimitiveType:
    """Tests for PrimitiveType enum."""

    def test_size_bytes(self):
        """Test size_bytes for all primitive types."""
        assert PrimitiveType.BIT.size_bytes == 1
        assert PrimitiveType.CHARACTER.size_bytes == 4
        assert PrimitiveType.UINT8.size_bytes == 1
        assert PrimitiveType.INT8.size_bytes == 1
        assert PrimitiveType.UINT16.size_bytes == 2
        assert PrimitiveType.INT16.size_bytes == 2
        assert PrimitiveType.UINT32.size_bytes == 4
        assert PrimitiveType.INT32.size_bytes == 4
        assert PrimitiveType.UINT64.size_bytes == 8
        assert PrimitiveType.INT64.size_bytes == 8
        assert PrimitiveType.UINT128.size_bytes == 16
        assert PrimitiveType.INT128.size_bytes == 16
        assert PrimitiveType.FLOAT32.size_bytes == 4
        assert PrimitiveType.FLOAT64.size_bytes == 8


class TestPrimitiveTypeDefinition:
    """Tests for PrimitiveTypeDefinition."""

    def test_properties(self):
        """Test type definition properties."""
        type_def = PrimitiveTypeDefinition(name="uint32", primitive=PrimitiveType.UINT32)
        assert type_def.name == "uint32"
        assert type_def.size_bytes == 4
        assert type_def.is_primitive is True
        assert type_def.is_array is False
        assert type_def.is_composite is False


class TestAliasTypeDefinition:
    """Tests for AliasTypeDefinition."""

    def test_alias_to_primitive(self):
        """Test alias pointing to a primitive type."""
        base = PrimitiveTypeDefinition(name="uint128", primitive=PrimitiveType.UINT128)
        alias = AliasTypeDefinition(name="uuid", base_type=base)

        assert alias.name == "uuid"
        assert alias.size_bytes == 16
        assert alias.is_primitive is False
        assert alias.is_array is False
        assert alias.resolve_base_type() is base

    def test_chained_aliases(self):
        """Test alias pointing to another alias."""
        base = PrimitiveTypeDefinition(name="uint128", primitive=PrimitiveType.UINT128)
        alias1 = AliasTypeDefinition(name="uuid", base_type=base)
        alias2 = AliasTypeDefinition(name="my_uuid", base_type=alias1)

        assert alias2.size_bytes == 16
        assert alias2.resolve_base_type() is base


class TestArrayTypeDefinition:
    """Tests for ArrayTypeDefinition."""

    def test_array_of_primitive(self):
        """Test array of primitive type."""
        element = PrimitiveTypeDefinition(name="uint8", primitive=PrimitiveType.UINT8)
        array = ArrayTypeDefinition(name="uint8[]", element_type=element)

        assert array.name == "uint8[]"
        assert array.size_bytes == 8  # Header size
        assert array.is_array is True
        assert array.element_type is element


class TestCompositeTypeDefinition:
    """Tests for CompositeTypeDefinition."""

    def test_simple_composite(self):
        """Test composite with primitive fields."""
        uint32 = PrimitiveTypeDefinition(name="uint32", primitive=PrimitiveType.UINT32)
        uint64 = PrimitiveTypeDefinition(name="uint64", primitive=PrimitiveType.UINT64)

        composite = CompositeTypeDefinition(
            name="Point",
            fields=[
                FieldDefinition(name="x", type_def=uint32),
                FieldDefinition(name="y", type_def=uint32),
                FieldDefinition(name="z", type_def=uint64),
            ],
        )

        assert composite.name == "Point"
        assert composite.size_bytes == 4 + 4 + 8
        assert composite.is_composite is True
        assert len(composite.fields) == 3

    def test_get_field(self):
        """Test getting a field by name."""
        uint32 = PrimitiveTypeDefinition(name="uint32", primitive=PrimitiveType.UINT32)
        composite = CompositeTypeDefinition(
            name="Point",
            fields=[FieldDefinition(name="x", type_def=uint32)],
        )

        field = composite.get_field("x")
        assert field is not None
        assert field.name == "x"
        assert composite.get_field("nonexistent") is None

    def test_get_field_offset(self):
        """Test getting field offset."""
        uint32 = PrimitiveTypeDefinition(name="uint32", primitive=PrimitiveType.UINT32)
        uint64 = PrimitiveTypeDefinition(name="uint64", primitive=PrimitiveType.UINT64)

        composite = CompositeTypeDefinition(
            name="Point",
            fields=[
                FieldDefinition(name="x", type_def=uint32),
                FieldDefinition(name="y", type_def=uint64),
                FieldDefinition(name="z", type_def=uint32),
            ],
        )

        assert composite.get_field_offset("x") == 0
        assert composite.get_field_offset("y") == 4
        assert composite.get_field_offset("z") == 12

        with pytest.raises(KeyError):
            composite.get_field_offset("nonexistent")


class TestTypeRegistry:
    """Tests for TypeRegistry."""

    def test_primitives_registered(self):
        """Test that all primitives are registered by default."""
        registry = TypeRegistry()

        for pt in PrimitiveType:
            assert pt.value in registry
            type_def = registry.get(pt.value)
            assert isinstance(type_def, PrimitiveTypeDefinition)

    def test_register_custom_type(self):
        """Test registering a custom type."""
        registry = TypeRegistry()
        uint128 = registry.get_or_raise("uint128")

        alias = AliasTypeDefinition(name="uuid", base_type=uint128)
        registry.register(alias)

        assert "uuid" in registry
        assert registry.get("uuid") is alias

    def test_duplicate_registration_fails(self):
        """Test that duplicate registration raises."""
        registry = TypeRegistry()
        uint128 = registry.get_or_raise("uint128")

        alias = AliasTypeDefinition(name="uuid", base_type=uint128)
        registry.register(alias)

        with pytest.raises(ValueError):
            registry.register(alias)

    def test_get_array_type(self):
        """Test getting or creating array types."""
        registry = TypeRegistry()

        array1 = registry.get_array_type("uint8")
        assert array1.name == "uint8[]"
        assert array1.is_array is True

        # Should return same instance
        array2 = registry.get_array_type("uint8")
        assert array1 is array2

    def test_list_types(self):
        """Test listing all types."""
        registry = TypeRegistry()
        type_names = registry.list_types()

        assert "uint8" in type_names
        assert "uint128" in type_names
        assert "float64" in type_names
