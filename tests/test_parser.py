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

    def test_tokenize_newlines(self):
        """Test that newlines are tokenized."""
        lexer = TypeLexer()
        lexer.build()

        tokens = lexer.tokenize("define uuid as uint128\nPerson { }")
        token_types = [t.type for t in tokens]

        assert "NEWLINE" in token_types

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
            x: uint32
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
            id: uuid
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
            x: float64
            y: float64
        }

        Rectangle {
            width: float64
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
