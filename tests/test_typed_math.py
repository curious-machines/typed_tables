"""Tests for typed math expressions (Phases 1-6)."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.dump import load_registry_from_metadata
from typed_tables.parsing.query_parser import (
    CreateEnumQuery,
    FieldDef,
    QueryParser,
    TypedLiteral,
)
from typed_tables.query_executor import (
    CreateResult,
    DumpResult,
    QueryExecutor,
    QueryResult,
)
from typed_tables.storage import StorageManager
from typed_tables.types import (
    EnumTypeDefinition,
    EnumValue,
    FieldDefinition,
    PrimitiveType,
    PrimitiveTypeDefinition,
    TypedValue,
    TypeRegistry,
    type_range,
)


@pytest.fixture
def db_dir():
    """Create a temporary database directory."""
    tmp = tempfile.mkdtemp()
    yield Path(tmp)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def executor(db_dir):
    """Create a fresh executor with registry and storage."""
    registry = TypeRegistry()
    storage = StorageManager(db_dir, registry)
    return QueryExecutor(storage, registry)


def _run(executor, *stmts):
    """Execute one or more TTQ statements and return the last result."""
    parser = QueryParser()
    result = None
    for stmt in stmts:
        queries = parser.parse_program(stmt)
        for q in queries:
            result = executor.execute(q)
    return result


# ============================================================
# Phase 1: Overflow Policy on Fields
# ============================================================


class TestOverflowParsing:
    @pytest.fixture
    def parser(self):
        return QueryParser()

    def test_parse_saturating_field(self, parser):
        query = parser.parse("type T { x: saturating uint8 }")
        assert len(query.fields) == 1
        assert query.fields[0].name == "x"
        assert query.fields[0].type_name == "uint8"
        assert query.fields[0].overflow == "saturating"

    def test_parse_wrapping_field(self, parser):
        query = parser.parse("type T { x: wrapping uint16 }")
        assert query.fields[0].overflow == "wrapping"

    def test_parse_no_overflow_field(self, parser):
        query = parser.parse("type T { x: uint8 }")
        assert query.fields[0].overflow is None

    def test_parse_overflow_with_default(self, parser):
        query = parser.parse("type T { x: saturating int8 = 10 }")
        assert query.fields[0].overflow == "saturating"
        assert query.fields[0].default_value == 10

    def test_parse_overflow_array_field(self, parser):
        query = parser.parse("type T { x: saturating int8[] }")
        assert query.fields[0].overflow == "saturating"
        assert query.fields[0].type_name == "int8[]"

    def test_parse_multiple_overflow_fields(self, parser):
        query = parser.parse("type T { x: saturating uint8, y: wrapping int16, z: uint32 }")
        assert query.fields[0].overflow == "saturating"
        assert query.fields[1].overflow == "wrapping"
        assert query.fields[2].overflow is None


class TestOverflowExecution:
    def test_create_type_with_overflow(self, executor):
        result = _run(executor, "type Sensor { reading: saturating int8, count: wrapping uint16 }")
        assert isinstance(result, CreateResult)

    def test_overflow_rejected_on_string(self, executor):
        """Overflow modifier on string returns error message."""
        result = _run(executor, "type T { name: saturating string }")
        assert result.message is not None
        assert "overflow" in result.message.lower() or "not allowed" in result.message.lower()

    def test_overflow_rejected_on_composite(self, executor):
        _run(executor, "type Inner { x: uint8 }")
        result = _run(executor, "type T { inner: saturating Inner }")
        assert result.message is not None
        assert "overflow" in result.message.lower() or "not allowed" in result.message.lower()

    def test_overflow_rejected_on_float(self, executor):
        result = _run(executor, "type T { val: saturating float32 }")
        assert result.message is not None
        assert "overflow" in result.message.lower() or "not allowed" in result.message.lower()

    def test_overflow_allowed_on_integer_types(self, executor):
        _run(executor, """type T {
            a: saturating uint8,
            b: wrapping int8,
            c: saturating uint16,
            d: wrapping int16,
            e: saturating uint32,
            f: wrapping int32,
            g: saturating uint64,
            h: wrapping int64
        }""")

    def test_describe_shows_overflow(self, executor):
        _run(executor, "type Sensor { reading: saturating int8, count: wrapping uint16 }")
        result = _run(executor, "describe Sensor")
        assert any(row.get("overflow") == "saturating" for row in result.rows)
        assert any(row.get("overflow") == "wrapping" for row in result.rows)

    def test_overflow_metadata_roundtrip(self, executor, db_dir):
        _run(executor, "type Sensor { reading: saturating int8, count: wrapping uint16 }")

        registry2 = load_registry_from_metadata(db_dir)
        sensor = registry2.get("Sensor")
        reading_field = sensor.fields[0]
        count_field = sensor.fields[1]
        assert reading_field.overflow == "saturating"
        assert count_field.overflow == "wrapping"

    def test_dump_emits_overflow(self, executor):
        _run(executor, "type Sensor { reading: saturating int8, count: wrapping uint16 }")
        result = _run(executor, "dump")
        assert isinstance(result, DumpResult)
        assert "saturating int8" in result.script
        assert "wrapping uint16" in result.script

    def test_dump_roundtrip_overflow(self, executor, db_dir):
        _run(executor, "type Sensor { reading: saturating int8, count: wrapping uint16 }")
        dump_result = _run(executor, "dump")

        tmp2 = tempfile.mkdtemp()
        try:
            registry2 = TypeRegistry()
            storage2 = StorageManager(Path(tmp2), registry2)
            executor2 = QueryExecutor(storage2, registry2)
            _run(executor2, dump_result.script)

            sensor = registry2.get("Sensor")
            assert sensor.fields[0].overflow == "saturating"
            assert sensor.fields[1].overflow == "wrapping"
        finally:
            shutil.rmtree(tmp2, ignore_errors=True)

    def test_interface_overflow_field(self, executor):
        _run(executor, "interface I { x: saturating uint8 }")
        _run(executor, "type T from I { y: uint8 }")
        result = _run(executor, "describe T")
        # describe uses "property" column for field names
        x_row = next(r for r in result.rows if r.get("property") == "x")
        assert x_row.get("overflow") == "saturating"


# ============================================================
# Phase 2: Type-Annotated Literals
# ============================================================


class TestTypedLiteralLexing:
    @pytest.fixture
    def parser(self):
        return QueryParser()

    def test_lex_typed_integer(self, parser):
        queries = parser.parse_program("5i8")
        assert queries

    def test_lex_typed_float(self, parser):
        queries = parser.parse_program("3.14f32")
        assert queries

    def test_lex_typed_integer_hex(self, parser):
        queries = parser.parse_program("0xFFu8")
        assert queries

    def test_lex_typed_integer_binary(self, parser):
        queries = parser.parse_program("0b1010i8")
        assert queries


class TestTypedLiteralParsing:
    @pytest.fixture
    def parser(self):
        return QueryParser()

    def test_parse_typed_integer_literal(self, parser):
        queries = parser.parse_program("5i8")
        assert queries

    def test_parse_various_suffixes(self, parser):
        for suffix in ["i8", "u8", "i16", "u16", "i32", "u32", "i64", "u64", "i128", "u128"]:
            queries = parser.parse_program(f"42{suffix}")
            assert queries, f"Failed to parse 42{suffix}"

    def test_parse_float_suffixes(self, parser):
        for suffix in ["f32", "f64"]:
            queries = parser.parse_program(f"3.14{suffix}")
            assert queries, f"Failed to parse 3.14{suffix}"


class TestTypedLiteralExecution:
    def test_eval_typed_integer(self, executor):
        result = _run(executor, "5i8")
        assert result.rows[0]["5i8"] == 5

    def test_eval_typed_float(self, executor):
        result = _run(executor, "3.14f32")
        assert abs(result.rows[0]["3.14f32"] - 3.14) < 0.01

    def test_eval_typed_integer_hex(self, executor):
        result = _run(executor, "0xFFu8")
        assert result.rows[0]["255u8"] == 255

    def test_eval_typed_integer_binary(self, executor):
        result = _run(executor, "0b1010i8")
        assert result.rows[0]["10i8"] == 10

    def test_typed_literal_in_create(self, executor):
        _run(executor, "type T { x: uint8 }")
        _run(executor, "create T(x=5u8)")
        result = _run(executor, "from T select *")
        assert result.rows[0]["x"] == 5

    def test_typed_literal_in_array(self, executor):
        _run(executor, "type T { vals: int8[] }")
        _run(executor, "create T(vals=[1i8, 2i8, 3i8])")
        result = _run(executor, "from T select *")
        assert result.rows[0]["vals"] == [1, 2, 3]

    def test_negative_typed_literal(self, executor):
        result = _run(executor, "-5i8")
        assert result.rows[0]["-5i8"] == -5


# ============================================================
# Phase 3: Type Conversion Functions
# ============================================================


class TestTypeConversion:
    def test_int16_conversion(self, executor):
        result = _run(executor, "int16(42)")
        assert result.rows[0]["int16(42)"] == 42

    def test_uint8_conversion(self, executor):
        result = _run(executor, "uint8(200)")
        assert result.rows[0]["uint8(200)"] == 200

    def test_uint8_overflow_errors(self, executor):
        with pytest.raises(RuntimeError, match="out of range"):
            _run(executor, "uint8(256)")

    def test_int8_overflow_errors(self, executor):
        with pytest.raises(RuntimeError, match="out of range"):
            _run(executor, "int8(-129)")

    def test_int8_positive_overflow(self, executor):
        with pytest.raises(RuntimeError, match="out of range"):
            _run(executor, "int8(128)")

    def test_float32_from_int(self, executor):
        result = _run(executor, "float32(5)")
        assert result.rows[0]["float32(5)"] == 5.0

    def test_float64_conversion(self, executor):
        result = _run(executor, "float64(42)")
        assert result.rows[0]["float64(42)"] == 42.0

    def test_element_wise_conversion(self, executor):
        result = _run(executor, "int16([1, 2, 3])")
        assert result.rows[0]["int16([1, 2, 3])"] == [1, 2, 3]

    def test_conversion_in_create(self, executor):
        _run(executor, "type T { x: uint8 }")
        _run(executor, "create T(x=uint8(42))")
        result = _run(executor, "from T select *")
        assert result.rows[0]["x"] == 42

    def test_conversion_in_create_composite_ref_path(self, executor):
        """CompositeRef path: int16(42) in instance context."""
        _run(executor, "type T { x: int16 }")
        _run(executor, "create T(x=int16(42))")
        result = _run(executor, "from T select *")
        assert result.rows[0]["x"] == 42

    def test_narrowing_always_errors(self, executor):
        """Narrowing conversion should always error."""
        with pytest.raises(RuntimeError, match="out of range"):
            _run(executor, "int8(200)")


# ============================================================
# Phase 4: TypedValue Propagation and Type Checking
# ============================================================


class TestTypedValuePropagation:
    def test_same_type_addition(self, executor):
        result = _run(executor, "5i8 + 3i8")
        assert result.rows[0]["5i8 + 3i8"] == 8

    def test_same_type_subtraction(self, executor):
        result = _run(executor, "10i16 - 3i16")
        assert result.rows[0]["10i16 - 3i16"] == 7

    def test_same_type_multiplication(self, executor):
        result = _run(executor, "4u8 * 3u8")
        assert result.rows[0]["4u8 * 3u8"] == 12

    def test_type_mismatch_error(self, executor):
        with pytest.raises(RuntimeError, match="[Tt]ype mismatch"):
            _run(executor, "5i8 + 3i16")

    def test_type_mismatch_signed_unsigned(self, executor):
        with pytest.raises(RuntimeError, match="[Tt]ype mismatch"):
            _run(executor, "5i8 + 3u8")

    def test_typed_plus_bare_adopts_type(self, executor):
        """Bare literal adopts the typed side's type."""
        result = _run(executor, "5i8 + 3")
        assert result.rows[0]["5i8 + 3"] == 8

    def test_bare_plus_typed_adopts_type(self, executor):
        result = _run(executor, "3 + 5i8")
        assert result.rows[0]["3 + 5i8"] == 8

    def test_bare_plus_bare_untyped(self, executor):
        """Both bare → existing behavior (arbitrary precision)."""
        result = _run(executor, "5 + 3")
        assert result.rows[0]["5 + 3"] == 8

    def test_conversion_then_addition(self, executor):
        """int16(5i8) + 3 → int16 result."""
        result = _run(executor, "int16(5i8) + 3")
        assert result.rows[0]["int16(5i8) + 3"] == 8

    def test_unary_minus_typed(self, executor):
        result = _run(executor, "-5i8")
        assert result.rows[0]["-5i8"] == -5

    def test_unary_plus_typed(self, executor):
        result = _run(executor, "+5i8")
        assert result.rows[0]["+5i8"] == 5

    def test_chained_operations_same_type(self, executor):
        result = _run(executor, "5i8 + 3i8 - 2i8")
        assert result.rows[0]["5i8 + 3i8 - 2i8"] == 6

    def test_modulo_typed(self, executor):
        result = _run(executor, "10i16 % 3i16")
        assert result.rows[0]["10i16 % 3i16"] == 1


# ============================================================
# Phase 5: Overflow Enforcement
# ============================================================


class TestLiteralFitness:
    def test_literal_in_range(self, executor):
        result = _run(executor, "127i8")
        assert result.rows[0]["127i8"] == 127

    def test_literal_out_of_range(self, executor):
        with pytest.raises(RuntimeError, match="out of range"):
            _run(executor, "128i8")

    def test_literal_negative_out_of_range(self, executor):
        with pytest.raises(RuntimeError, match="out of range"):
            _run(executor, "-129i8")

    def test_literal_uint8_max(self, executor):
        result = _run(executor, "255u8")
        assert result.rows[0]["255u8"] == 255

    def test_literal_uint8_overflow(self, executor):
        with pytest.raises(RuntimeError, match="out of range"):
            _run(executor, "256u8")

    def test_literal_int128_extreme(self, executor):
        result = _run(executor, "0i128")
        assert result.rows[0]["0i128"] == 0


class TestOverflowEnforcement:
    def test_overflow_error_default(self, executor):
        """Without overflow policy, overflow should error."""
        with pytest.raises(RuntimeError, match="[Oo]verflow"):
            _run(executor, "200u8 + 100u8")

    def test_overflow_error_no_policy_subtraction(self, executor):
        with pytest.raises(RuntimeError, match="[Oo]verflow"):
            _run(executor, "0u8 - 1u8")

    def test_enforce_overflow_saturating_unit(self):
        """Direct unit test of _enforce_overflow with saturating policy."""
        result = QueryExecutor._enforce_overflow(300, "uint8", "saturating")
        assert result == 255

    def test_enforce_overflow_saturating_underflow(self):
        result = QueryExecutor._enforce_overflow(-200, "int8", "saturating")
        assert result == -128

    def test_enforce_overflow_wrapping_unit(self):
        """Direct unit test of _enforce_overflow with wrapping policy."""
        result = QueryExecutor._enforce_overflow(300, "uint8", "wrapping")
        # (300 - 0) % 256 + 0 = 44
        assert result == 44

    def test_enforce_overflow_wrapping_signed(self):
        result = QueryExecutor._enforce_overflow(200, "int8", "wrapping")
        # ((200 - (-128)) % 256) + (-128) = (328 % 256) + (-128) = 72 + (-128) = -56
        assert result == -56

    def test_enforce_overflow_error_raises(self):
        with pytest.raises(RuntimeError, match="[Oo]verflow"):
            QueryExecutor._enforce_overflow(300, "uint8", None)

    def test_enforce_overflow_in_range_passthrough(self):
        """Values in range pass through regardless of policy."""
        assert QueryExecutor._enforce_overflow(100, "uint8", None) == 100
        assert QueryExecutor._enforce_overflow(100, "uint8", "saturating") == 100
        assert QueryExecutor._enforce_overflow(100, "uint8", "wrapping") == 100

    def test_enforce_overflow_float_not_enforced(self):
        """Float overflow is not enforced."""
        result = QueryExecutor._enforce_overflow(1e40, "float32", None)
        assert result == 1e40


class TestTypedDivision:
    def test_typed_integer_division_is_floor(self, executor):
        result = _run(executor, "7i8 / 2i8")
        assert result.rows[0]["7i8 / 2i8"] == 3

    def test_typed_integer_division_negative(self, executor):
        result = _run(executor, "-7i8 / 2i8")
        assert result.rows[0]["-7i8 / 2i8"] == -4

    def test_typed_float_division_is_true_division(self, executor):
        result = _run(executor, "7.0f32 / 2.0f32")
        assert abs(result.rows[0]["7.0f32 / 2.0f32"] - 3.5) < 0.01

    def test_untyped_division_is_true_division(self, executor):
        result = _run(executor, "7 / 2")
        assert result.rows[0]["7 / 2"] == 3.5

    def test_typed_floor_divide_operator(self, executor):
        result = _run(executor, "7i8 // 2i8")
        assert result.rows[0]["7i8 // 2i8"] == 3


# ============================================================
# Phase 6: Enum Backing Type and Conversion
# ============================================================


class TestEnumBackingTypeParsing:
    @pytest.fixture
    def parser(self):
        return QueryParser()

    def test_parse_enum_with_backing_type(self, parser):
        query = parser.parse("enum Color : uint8 { red, green, blue }")
        assert isinstance(query, CreateEnumQuery)
        assert query.backing_type == "uint8"

    def test_parse_enum_without_backing_type(self, parser):
        query = parser.parse("enum Color { red, green, blue }")
        assert isinstance(query, CreateEnumQuery)
        assert query.backing_type is None


class TestEnumBackingTypeExecution:
    def test_create_enum_with_backing_type(self, executor):
        result = _run(executor, "enum Color : uint8 { red, green, blue }")
        assert isinstance(result, CreateResult)

    def test_backing_type_must_be_integer(self, executor):
        """Enum backing type with float returns error message."""
        result = _run(executor, "enum Color : float32 { red, green, blue }")
        assert result.message is not None
        assert "integer" in result.message.lower()

    def test_discriminants_must_fit_backing_type(self, executor):
        result = _run(executor, "enum Big : uint8 { x = 300 }")
        assert result.message is not None
        assert "out of range" in result.message.lower() or "300" in result.message

    def test_backing_type_metadata_roundtrip(self, executor, db_dir):
        _run(executor, "enum Color : uint8 { red, green, blue }")

        registry2 = load_registry_from_metadata(db_dir)
        color = registry2.get("Color")
        assert isinstance(color, EnumTypeDefinition)
        assert color.backing_type == PrimitiveType.UINT8

    def test_dump_emits_backing_type(self, executor):
        _run(executor, "enum Color : uint8 { red, green, blue }")
        result = _run(executor, "dump")
        assert isinstance(result, DumpResult)
        assert ": uint8" in result.script


class TestEnumConversion:
    def test_enum_conversion_by_discriminant(self, executor):
        _run(executor, "enum Color { red, green, blue }")
        result = _run(executor, "Color(0)")
        val = list(result.rows[0].values())[0]
        assert isinstance(val, EnumValue)
        assert val.variant_name == "red"
        assert val.discriminant == 0

    def test_enum_conversion_by_name(self, executor):
        _run(executor, 'enum Color { red, green, blue }')
        result = _run(executor, 'Color("red")')
        val = list(result.rows[0].values())[0]
        assert isinstance(val, EnumValue)
        assert val.variant_name == "red"

    def test_enum_conversion_invalid_discriminant(self, executor):
        _run(executor, "enum Color { red, green, blue }")
        with pytest.raises(RuntimeError, match="[Nn]o variant"):
            _run(executor, "Color(5)")

    def test_enum_conversion_invalid_name(self, executor):
        _run(executor, "enum Color { red, green, blue }")
        with pytest.raises(RuntimeError, match="[Nn]o variant"):
            _run(executor, 'Color("purple")')

    def test_enum_in_create_by_discriminant(self, executor):
        _run(executor, "enum Color { red, green, blue }")
        _run(executor, "type Pixel { color: Color }")
        _run(executor, "create Pixel(color=Color(1))")
        result = _run(executor, "from Pixel select *")
        val = result.rows[0]["color"]
        assert isinstance(val, EnumValue)
        assert val.variant_name == "green"

    def test_enum_in_create_by_name(self, executor):
        _run(executor, "enum Color { red, green, blue }")
        _run(executor, "type Pixel { color: Color }")
        _run(executor, 'create Pixel(color=Color("blue"))')
        result = _run(executor, "from Pixel select *")
        val = result.rows[0]["color"]
        assert isinstance(val, EnumValue)
        assert val.variant_name == "blue"


class TestEnumArithmetic:
    def test_enum_discriminant_plus_typed_int(self, executor):
        _run(executor, "enum Color : uint8 { red, green, blue }")
        result = _run(executor, "Color(0) + 1u8")
        assert result.rows[0]["Color(0) + 1u8"] == 1

    def test_enum_discriminant_addition(self, executor):
        _run(executor, "enum Color : uint8 { red, green, blue }")
        result = _run(executor, 'Color("green") + Color("blue")')
        # green=1, blue=2 → 3
        assert result.rows[0]['Color("green") + Color("blue")'] == 3

    def test_enum_without_backing_no_arithmetic(self, executor):
        """Enum without backing type should not convert to TypedValue for arithmetic."""
        _run(executor, "enum Color { red, green, blue }")
        with pytest.raises(Exception):
            _run(executor, "Color(0) + 1u8")


# ============================================================
# Function name casing preservation
# ============================================================


class TestFunctionNameCasing:
    def test_type_name_case_preserved(self, executor):
        """Type names used as function names should preserve case."""
        _run(executor, "enum HttpStatus { ok = 200, not_found = 404 }")
        result = _run(executor, "HttpStatus(200)")
        val = list(result.rows[0].values())[0]
        assert isinstance(val, EnumValue)
        assert val.variant_name == "ok"

    def test_builtin_functions_case_insensitive(self, executor):
        """Built-in functions like abs should still work regardless of case."""
        result = _run(executor, "abs(-5)")
        assert result.rows[0]["abs(-5)"] == 5

        result = _run(executor, "ABS(-5)")
        assert result.rows[0]["ABS(-5)"] == 5


# ============================================================
# type_range() unit tests
# ============================================================


class TestTypeRange:
    def test_uint8_range(self):
        assert type_range(PrimitiveType.UINT8) == (0, 255)

    def test_int8_range(self):
        assert type_range(PrimitiveType.INT8) == (-128, 127)

    def test_uint16_range(self):
        assert type_range(PrimitiveType.UINT16) == (0, 65535)

    def test_int16_range(self):
        assert type_range(PrimitiveType.INT16) == (-32768, 32767)

    def test_uint32_range(self):
        assert type_range(PrimitiveType.UINT32) == (0, 2**32 - 1)

    def test_int32_range(self):
        assert type_range(PrimitiveType.INT32) == (-(2**31), 2**31 - 1)

    def test_uint64_range(self):
        assert type_range(PrimitiveType.UINT64) == (0, 2**64 - 1)

    def test_int64_range(self):
        assert type_range(PrimitiveType.INT64) == (-(2**63), 2**63 - 1)

    def test_uint128_range(self):
        assert type_range(PrimitiveType.UINT128) == (0, 2**128 - 1)

    def test_int128_range(self):
        assert type_range(PrimitiveType.INT128) == (-(2**127), 2**127 - 1)

    def test_float32_returns_range(self):
        """type_range returns a range for floats (approximate bounds)."""
        min_val, max_val = type_range(PrimitiveType.FLOAT32)
        assert min_val < 0
        assert max_val > 0


# ============================================================
# TypedValue unit tests
# ============================================================


class TestTypedValueDataclass:
    def test_typed_value_creation(self):
        tv = TypedValue(value=42, type_name="int8")
        assert tv.value == 42
        assert tv.type_name == "int8"

    def test_typed_value_equality(self):
        tv1 = TypedValue(value=42, type_name="int8")
        tv2 = TypedValue(value=42, type_name="int8")
        assert tv1 == tv2

    def test_typed_value_inequality_type(self):
        tv1 = TypedValue(value=42, type_name="int8")
        tv2 = TypedValue(value=42, type_name="int16")
        assert tv1 != tv2


# ============================================================
# _unwrap_typed unit tests
# ============================================================


class TestUnwrapTyped:
    def test_unwrap_scalar(self):
        assert QueryExecutor._unwrap_typed(TypedValue(42, "int8")) == 42

    def test_unwrap_list(self):
        result = QueryExecutor._unwrap_typed([TypedValue(1, "int8"), TypedValue(2, "int8")])
        assert result == [1, 2]

    def test_unwrap_plain_value(self):
        assert QueryExecutor._unwrap_typed(42) == 42

    def test_unwrap_mixed_list(self):
        result = QueryExecutor._unwrap_typed([TypedValue(1, "int8"), 2, TypedValue(3, "int8")])
        assert result == [1, 2, 3]


# ============================================================
# Integration / cross-phase tests
# ============================================================


class TestCrossPhaseIntegration:
    def test_typed_literal_stored_and_retrieved(self, executor):
        """TypedLiteral strips to raw value for storage, then retrieves correctly."""
        _run(executor, "type T { x: uint8, y: int16 }")
        _run(executor, "create T(x=42u8, y=-100i16)")
        result = _run(executor, "from T select *")
        assert result.rows[0]["x"] == 42
        assert result.rows[0]["y"] == -100

    def test_conversion_in_where_clause(self, executor):
        """Type conversion in WHERE context."""
        _run(executor, "type T { x: uint8 }")
        _run(executor, "create T(x=10)")
        _run(executor, "create T(x=20)")
        result = _run(executor, "from T select * where x > 15")
        assert len(result.rows) == 1
        assert result.rows[0]["x"] == 20

    def test_enum_conversion_in_create_and_select(self, executor):
        """Enum conversion works in both create and select contexts."""
        _run(executor, "enum Color : uint8 { red, green, blue }")
        _run(executor, "type Pixel { color: Color, brightness: uint8 }")
        _run(executor, "create Pixel(color=Color(0), brightness=100)")
        result = _run(executor, "from Pixel select *")
        val = result.rows[0]["color"]
        assert isinstance(val, EnumValue)
        assert val.variant_name == "red"
        assert result.rows[0]["brightness"] == 100

    def test_full_dump_roundtrip_with_all_features(self, executor, db_dir):
        """Dump and reload a database with overflow, enums with backing type."""
        _run(executor, "enum Color : uint8 { red, green, blue }")
        _run(executor, "type Sensor { reading: saturating int8, count: wrapping uint16, color: Color }")
        _run(executor, "create Sensor(reading=100, count=1000, color=.red)")

        dump_result = _run(executor, "dump")

        tmp2 = tempfile.mkdtemp()
        try:
            registry2 = TypeRegistry()
            storage2 = StorageManager(Path(tmp2), registry2)
            executor2 = QueryExecutor(storage2, registry2)
            _run(executor2, dump_result.script)

            # Verify types
            color = registry2.get("Color")
            assert isinstance(color, EnumTypeDefinition)
            assert color.backing_type == PrimitiveType.UINT8

            sensor = registry2.get("Sensor")
            reading_field = next(f for f in sensor.fields if f.name == "reading")
            count_field = next(f for f in sensor.fields if f.name == "count")
            assert reading_field.overflow == "saturating"
            assert count_field.overflow == "wrapping"

            # Verify data
            result = _run(executor2, "from Sensor select *")
            assert len(result.rows) == 1
            assert result.rows[0]["reading"] == 100
            assert result.rows[0]["count"] == 1000
            val = result.rows[0]["color"]
            assert isinstance(val, EnumValue)
            assert val.variant_name == "red"
        finally:
            shutil.rmtree(tmp2, ignore_errors=True)
