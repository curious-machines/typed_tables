"""Tests for the TTQ language server helper functions."""

import pytest
from lsprotocol import types

from typed_tables.lsp.server import (
    _extract_position_from_error,
    _find_user_types,
    _word_at_position,
    lexpos_to_position,
)


# ---------------------------------------------------------------------------
# lexpos_to_position
# ---------------------------------------------------------------------------


class TestLexposToPosition:
    def test_start_of_single_line(self):
        pos = lexpos_to_position("hello", 0)
        assert pos == types.Position(line=0, character=0)

    def test_middle_of_single_line(self):
        pos = lexpos_to_position("hello world", 6)
        assert pos == types.Position(line=0, character=6)

    def test_start_of_second_line(self):
        pos = lexpos_to_position("abc\ndef", 4)
        assert pos == types.Position(line=1, character=0)

    def test_middle_of_second_line(self):
        pos = lexpos_to_position("abc\ndef", 6)
        assert pos == types.Position(line=1, character=2)

    def test_third_line(self):
        source = "line0\nline1\nline2"
        # 'l' of line2 is at offset 12
        pos = lexpos_to_position(source, 12)
        assert pos == types.Position(line=2, character=0)

    def test_end_of_line(self):
        source = "ab\ncd\nef"
        # 'd' is at offset 4
        pos = lexpos_to_position(source, 4)
        assert pos == types.Position(line=1, character=1)

    def test_empty_source_offset_zero(self):
        pos = lexpos_to_position("", 0)
        assert pos == types.Position(line=0, character=0)

    def test_newline_character_itself(self):
        # The newline between "ab" and "cd" is at offset 2
        pos = lexpos_to_position("ab\ncd", 2)
        assert pos == types.Position(line=0, character=2)


# ---------------------------------------------------------------------------
# _extract_position_from_error
# ---------------------------------------------------------------------------


class TestExtractPositionFromError:
    def test_parenthesized_format(self):
        msg = "Syntax error at 'foo' (position 42)"
        assert _extract_position_from_error(msg) == 42

    def test_at_format(self):
        msg = "Illegal character '@' at position 7"
        assert _extract_position_from_error(msg) == 7

    def test_no_position(self):
        msg = "Syntax error at end of input"
        assert _extract_position_from_error(msg) is None

    def test_empty_message(self):
        assert _extract_position_from_error("") is None

    def test_large_position(self):
        msg = "Syntax error at 'x' (position 99999)"
        assert _extract_position_from_error(msg) == 99999


# ---------------------------------------------------------------------------
# _word_at_position
# ---------------------------------------------------------------------------


class TestWordAtPosition:
    def test_simple_word(self):
        assert _word_at_position("from Person select *", 5) == "Person"

    def test_word_start(self):
        assert _word_at_position("hello world", 0) == "hello"

    def test_word_end(self):
        assert _word_at_position("hello world", 4) == "hello"

    def test_underscore_word(self):
        assert _word_at_position("my_var = 5", 2) == "my_var"

    def test_at_space(self):
        assert _word_at_position("hello world", 5) == ""

    def test_empty_line(self):
        assert _word_at_position("", 0) == ""

    def test_out_of_bounds_negative(self):
        assert _word_at_position("hello", -1) == ""

    def test_out_of_bounds_past_end(self):
        assert _word_at_position("hello", 10) == ""

    def test_keyword(self):
        assert _word_at_position("from Person select *", 0) == "from"

    def test_single_char(self):
        assert _word_at_position("a b c", 2) == "b"

    def test_digit_in_word(self):
        assert _word_at_position("uint32 field", 3) == "uint32"


# ---------------------------------------------------------------------------
# _find_user_types
# ---------------------------------------------------------------------------


class TestFindUserTypes:
    def test_create_type(self):
        assert _find_user_types("create type Person { name: string }") == ["Person"]

    def test_create_enum(self):
        assert _find_user_types("create enum Color { red, green }") == ["Color"]

    def test_create_interface(self):
        assert _find_user_types("create interface Drawable { x: float32 }") == ["Drawable"]

    def test_create_alias(self):
        assert _find_user_types("create alias uuid as uint128") == ["uuid"]

    def test_define_as(self):
        assert _find_user_types("define name as character[]") == ["name"]

    def test_multiple(self):
        source = "create type A { x: uint8 }\ncreate type B { y: uint8 }\ncreate enum C { a, b }"
        assert _find_user_types(source) == ["A", "B", "C"]

    def test_empty(self):
        assert _find_user_types("from Person select *") == []
