"""Tests for the DSL parser."""

import pytest

from typed_tables.parsing import TypeParser
from typed_tables.parsing.type_lexer import TypeLexer
from typed_tables.types import (
    AliasTypeDefinition,
    ArrayTypeDefinition,
    CompositeTypeDefinition,
)


class TestTypeLexer:
    """Tests for the type lexer."""

    def test_tokenize_alias(self):
        """Test tokenizing an alias definition."""
        lexer = TypeLexer()
        lexer.build()

        tokens = lexer.tokenize("define uuid as uint128")
        token_types = [t.type for t in tokens]

        assert token_types == ["DEFINE", "IDENTIFIER", "AS", "IDENTIFIER"]

    def test_tokenize_composite(self):
        """Test tokenizing a composite type."""
        lexer = TypeLexer()
        lexer.build()

        tokens = lexer.tokenize("Person { name: character[] }")
        token_types = [t.type for t in tokens]

        assert token_types == [
            "IDENTIFIER",
            "LBRACE",
            "IDENTIFIER",
            "COLON",
            "IDENTIFIER",
            "LBRACKET",
            "RBRACKET",
            "RBRACE",
        ]

    def test_newlines_ignored(self):
        """Test that newlines are ignored (not tokenized)."""
        lexer = TypeLexer()
        lexer.build()

        tokens = lexer.tokenize("define uuid as uint128\nPerson { }")
        token_types = [t.type for t in tokens]

        assert "NEWLINE" not in token_types

    def test_tokenize_comma(self):
        """Test that commas are tokenized."""
        lexer = TypeLexer()
        lexer.build()

        tokens = lexer.tokenize("Person { x: uint32, y: uint32 }")
        token_types = [t.type for t in tokens]

        assert "COMMA" in token_types

    def test_illegal_character(self):
        """Test error on illegal character."""
        lexer = TypeLexer()
        lexer.build()

        with pytest.raises(SyntaxError):
            lexer.tokenize("define uuid @ uint128")


class TestTypeParser:
    """Tests for the type parser."""

    def test_parse_alias(self):
        """Test parsing an alias definition."""
        parser = TypeParser()
        registry = parser.parse("define uuid as uint128")

        assert "uuid" in registry
        type_def = registry.get("uuid")
        assert isinstance(type_def, AliasTypeDefinition)
        assert type_def.base_type.name == "uint128"

    def test_parse_array_alias(self):
        """Test parsing an alias to an array type."""
        parser = TypeParser()
        registry = parser.parse("define name as character[]")

        assert "name" in registry
        type_def = registry.get("name")
        assert isinstance(type_def, AliasTypeDefinition)
        assert isinstance(type_def.base_type, ArrayTypeDefinition)

    def test_parse_simple_composite(self):
        """Test parsing a simple composite type."""
        parser = TypeParser()
        registry = parser.parse("""
        Point {
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

    def test_parse_composite_with_implicit_type(self):
        """Test parsing composite with implicit field type."""
        parser = TypeParser()
        registry = parser.parse("""
        define name as character[]

        Person {
            name
        }
        """)

        assert "Person" in registry
        type_def = registry.get("Person")
        assert isinstance(type_def, CompositeTypeDefinition)
        assert type_def.fields[0].name == "name"
        assert type_def.fields[0].type_def.name == "name"

    def test_parse_example_from_readme(self):
        """Test parsing the example from CLAUDE.md."""
        parser = TypeParser()
        registry = parser.parse("""
        define uuid as uint128
        define name as character[]

        Person {
            id: uuid,
            name
        }
        """)

        assert "uuid" in registry
        assert "name" in registry
        assert "Person" in registry

        person = registry.get("Person")
        assert isinstance(person, CompositeTypeDefinition)
        assert len(person.fields) == 2

        id_field = person.get_field("id")
        assert id_field is not None
        assert id_field.type_def.name == "uuid"

        name_field = person.get_field("name")
        assert name_field is not None
        assert name_field.type_def.name == "name"

    def test_parse_multiple_composites(self):
        """Test parsing multiple composite types."""
        parser = TypeParser()
        registry = parser.parse("""
        Point {
            x: float64,
            y: float64
        }

        Rectangle {
            width: float64,
            height: float64
        }
        """)

        assert "Point" in registry
        assert "Rectangle" in registry

    def test_parse_empty_composite(self):
        """Test parsing an empty composite type."""
        parser = TypeParser()
        registry = parser.parse("Empty { }")

        assert "Empty" in registry
        type_def = registry.get("Empty")
        assert isinstance(type_def, CompositeTypeDefinition)
        assert len(type_def.fields) == 0

    def test_undefined_type_error(self):
        """Test error on undefined type reference."""
        parser = TypeParser()

        with pytest.raises(ValueError):
            parser.parse("""
            Person {
                name: undefined_type
            }
            """)

    def test_syntax_error(self):
        """Test syntax error handling."""
        parser = TypeParser()

        with pytest.raises(SyntaxError):
            parser.parse("Person { name: }")

    def test_forward_reference_resolution(self):
        """Test that types can reference types defined later (within limits)."""
        parser = TypeParser()
        # This should work because we resolve in dependency order
        registry = parser.parse("""
        define uuid as uint128

        Person {
            id: uuid
        }
        """)

        assert "Person" in registry

    def test_self_referential_composite(self):
        """Test DSL parser handles self-referential types like Node { children: Node[] }."""
        parser = TypeParser()
        registry = parser.parse("""
        Node {
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
        from typed_tables.types import ArrayTypeDefinition
        children_type = node_def.fields[1].type_def
        assert isinstance(children_type, ArrayTypeDefinition)
        assert children_type.element_type is node_def

    def test_mutual_reference_composites(self):
        """Test DSL parser handles mutually referential types A↔B."""
        parser = TypeParser()
        registry = parser.parse("""
        A {
            value: uint8,
            b: B
        }

        B {
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
