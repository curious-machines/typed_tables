"""Tests for the type definition parsing via QueryParser."""

import tempfile
from pathlib import Path

import pytest

from typed_tables import Schema
from typed_tables.types import (
    AliasTypeDefinition,
    ArrayTypeDefinition,
    CompositeTypeDefinition,
)


def _parse_types(type_defs: str) -> CompositeTypeDefinition | AliasTypeDefinition | None:
    """Parse type definitions and return the registry via Schema.parse()."""
    with tempfile.TemporaryDirectory() as tmp:
        schema = Schema.parse(type_defs, tmp)
        return schema.registry


class TestTypeDefinitions:
    """Tests for type definition parsing (formerly TestTypeParser)."""

    def test_parse_alias(self):
        """Test parsing an alias definition."""
        registry = _parse_types("alias uuid as uint128")

        assert "uuid" in registry
        type_def = registry.get("uuid")
        assert isinstance(type_def, AliasTypeDefinition)
        assert type_def.base_type.name == "uint128"

    def test_parse_array_alias(self):
        """Test parsing an alias to an array type."""
        registry = _parse_types("alias name as character[]")

        assert "name" in registry
        type_def = registry.get("name")
        assert isinstance(type_def, AliasTypeDefinition)
        assert isinstance(type_def.base_type, ArrayTypeDefinition)

    def test_parse_simple_composite(self):
        """Test parsing a simple composite type."""
        registry = _parse_types("""
        type Point {
            x: uint32,
            y: uint32
        }
        """)

        assert "Point" in registry
        type_def = registry.get("Point")
        assert isinstance(type_def, CompositeTypeDefinition)
        assert len(type_def.fields) == 2
        assert type_def.fields[0].name == "x"
        assert type_def.fields[1].name == "y"

    def test_parse_example(self):
        """Test parsing a typical type definition."""
        registry = _parse_types("""
        alias uuid as uint128

        type Person {
            id: uuid,
            name: string
        }
        """)

        assert "uuid" in registry
        assert "Person" in registry

        person = registry.get("Person")
        assert isinstance(person, CompositeTypeDefinition)
        assert len(person.fields) == 2

        id_field = person.get_field("id")
        assert id_field is not None
        assert id_field.type_def.name == "uuid"

        name_field = person.get_field("name")
        assert name_field is not None
        assert name_field.type_def.name == "string"

    def test_parse_multiple_composites(self):
        """Test parsing multiple composite types."""
        registry = _parse_types("""
        type Point {
            x: float64,
            y: float64
        }

        type Rectangle {
            width: float64,
            height: float64
        }
        """)

        assert "Point" in registry
        assert "Rectangle" in registry

    def test_parse_empty_composite(self):
        """Test parsing an empty composite type."""
        registry = _parse_types("type Empty { }")

        assert "Empty" in registry
        type_def = registry.get("Empty")
        assert isinstance(type_def, CompositeTypeDefinition)
        assert len(type_def.fields) == 0

    def test_undefined_type_error(self):
        """Test error on undefined type reference."""
        with pytest.raises(Exception):
            _parse_types("""
            type Person {
                name: undefined_type
            }
            """)

    def test_syntax_error(self):
        """Test syntax error handling."""
        with pytest.raises(SyntaxError):
            _parse_types("type Person { name: }")

    def test_forward_reference_resolution(self):
        """Test that types can reference types defined later."""
        registry = _parse_types("""
        alias uuid as uint128

        type Person {
            id: uuid
        }
        """)

        assert "Person" in registry

    def test_self_referential_composite(self):
        """Test parser handles self-referential types like Node { children: Node[] }."""
        registry = _parse_types("""
        type Node {
            value: uint8,
            children: Node[]
        }
        """)

        assert "Node" in registry
        node_def = registry.get("Node")
        assert isinstance(node_def, CompositeTypeDefinition)
        assert len(node_def.fields) == 2
        assert node_def.fields[0].name == "value"
        assert node_def.fields[1].name == "children"
        # The children field should be an array of Node
        children_type = node_def.fields[1].type_def
        assert isinstance(children_type, ArrayTypeDefinition)
        assert children_type.element_type is node_def

    def test_mutual_reference_composites(self):
        """Test parser handles mutually referential types A↔B."""
        registry = _parse_types("""
        forward A
        forward B

        type A {
            value: uint8,
            b: B
        }

        type B {
            value: uint8,
            a: A
        }
        """)

        assert "A" in registry
        assert "B" in registry
        a_def = registry.get("A")
        b_def = registry.get("B")
        assert isinstance(a_def, CompositeTypeDefinition)
        assert isinstance(b_def, CompositeTypeDefinition)
        # A.b should reference B and B.a should reference A
        assert a_def.fields[1].type_def is b_def
        assert b_def.fields[1].type_def is a_def
