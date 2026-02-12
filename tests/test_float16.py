"""Tests for float16 (IEEE 754 half-precision) type support."""

from __future__ import annotations

import shutil
import struct
import tempfile
from pathlib import Path

import pytest

from typed_tables.parsing.query_parser import QueryParser
from typed_tables.query_executor import QueryExecutor
from typed_tables.storage import StorageManager
from typed_tables.types import TypeRegistry


@pytest.fixture
def db_dir():
    tmp = tempfile.mkdtemp()
    yield Path(tmp)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def executor(db_dir):
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


class TestFloat16Basic:
    """Basic float16 type creation and storage."""

    def test_create_type_with_float16_field(self, executor):
        _run(executor, "type Sensor { value: float16 }")
        _run(executor, "create Sensor(value=3.14)")
        result = _run(executor, "from Sensor select *")
        assert len(result.rows) == 1
        val = result.rows[0]["value"]
        assert abs(val - 3.14) < 0.01

    def test_float16_zero(self, executor):
        _run(executor, "type T { x: float16 }")
        _run(executor, "create T(x=0.0)")
        result = _run(executor, "from T select x")
        assert result.rows[0]["x"] == 0.0

    def test_float16_negative(self, executor):
        _run(executor, "type T { x: float16 }")
        _run(executor, "create T(x=-2.5)")
        result = _run(executor, "from T select x")
        assert result.rows[0]["x"] == -2.5

    def test_float16_max_value(self, executor):
        _run(executor, "type T { x: float16 }")
        _run(executor, "create T(x=65504.0)")
        result = _run(executor, "from T select x")
        assert result.rows[0]["x"] == 65504.0

    def test_float16_precision_rounding(self, executor):
        """float16 has limited precision -- values get rounded."""
        _run(executor, "type T { x: float16 }")
        _run(executor, "create T(x=1.001)")
        result = _run(executor, "from T select x")
        val = result.rows[0]["x"]
        expected = struct.unpack("<e", struct.pack("<e", 1.001))[0]
        assert val == expected

    def test_float16_multiple_records(self, executor):
        _run(executor, "type T { x: float16 }")
        _run(executor, "create T(x=1.0)")
        _run(executor, "create T(x=2.0)")
        _run(executor, "create T(x=3.0)")
        result = _run(executor, "from T select x")
        values = [row["x"] for row in result.rows]
        assert values == [1.0, 2.0, 3.0]


class TestFloat16TypedLiterals:
    """Typed literal suffix f16."""

    def test_typed_float_literal(self, executor):
        result = _run(executor, "3.14f16")
        val = result.rows[0][result.columns[0]]
        # Typed literal retains Python float precision; rounding happens on storage
        assert val == 3.14

    def test_typed_integer_to_float16(self, executor):
        """Integer with f16 suffix should produce float16 typed value."""
        result = _run(executor, "5f16")
        assert result.rows[0][result.columns[0]] == 5.0

    def test_float16_arithmetic(self, executor):
        result = _run(executor, "1.0f16 + 2.0f16")
        assert result.rows[0][result.columns[0]] == 3.0

    def test_float16_division_is_true_division(self, executor):
        """float16 / should use true division, not floor division."""
        result = _run(executor, "7.0f16 / 2.0f16")
        assert result.rows[0][result.columns[0]] == 3.5

    def test_float16_literal_subtraction(self, executor):
        result = _run(executor, "10.0f16 - 3.0f16")
        assert result.rows[0][result.columns[0]] == 7.0

    def test_float16_literal_multiplication(self, executor):
        result = _run(executor, "2.5f16 * 4.0f16")
        assert result.rows[0][result.columns[0]] == 10.0


class TestFloat16Conversion:
    """Type conversion functions with float16."""

    def test_float16_cast_from_int(self, executor):
        result = _run(executor, "float16(42)")
        assert result.rows[0][result.columns[0]] == 42.0

    def test_float64_cast_from_float16(self, executor):
        result = _run(executor, "float64(3.14f16)")
        val = result.rows[0][result.columns[0]]
        # TypedValue wraps 3.14 as float64 — precision is Python float
        assert val == 3.14

    def test_float32_cast_from_float16(self, executor):
        result = _run(executor, "float32(2.5f16)")
        assert result.rows[0][result.columns[0]] == 2.5

    def test_float16_cast_from_float32(self, executor):
        result = _run(executor, "float16(2.5f32)")
        assert result.rows[0][result.columns[0]] == 2.5

    def test_float16_array_conversion(self, executor):
        result = _run(executor, "float16([1, 2, 3])")
        values = result.rows[0][result.columns[0]]
        assert values == [1.0, 2.0, 3.0]


class TestFloat16Overflow:
    """Overflow behavior for float16."""

    def test_overflow_policy_saturating_rejected(self, executor):
        result = _run(executor, "type T { x: saturating float16 }")
        assert result.message is not None
        assert "not allowed" in result.message.lower()

    def test_overflow_policy_wrapping_rejected(self, executor):
        result = _run(executor, "type T { x: wrapping float16 }")
        assert result.message is not None
        assert "not allowed" in result.message.lower()


class TestFloat16Array:
    """float16 array fields."""

    def test_float16_array_field(self, executor):
        _run(executor, "type T { values: float16[] }")
        _run(executor, "create T(values=[1.0, 2.5, 3.75])")
        result = _run(executor, "from T select values")
        values = result.rows[0]["values"]
        assert values == [1.0, 2.5, 3.75]

    def test_float16_array_indexing(self, executor):
        _run(executor, "type T { values: float16[] }")
        _run(executor, "create T(values=[1.0, 2.5, 3.75])")
        result = _run(executor, "from T select values[0]")
        assert result.rows[0][result.columns[0]] == 1.0

    def test_float16_array_methods(self, executor):
        _run(executor, "type T { values: float16[] }")
        _run(executor, "create T(values=[3.0, 1.0, 2.0])")
        result = _run(executor, "from T select values.length()")
        assert result.rows[0][result.columns[0]] == 3
        result = _run(executor, "from T select values.min()")
        assert result.rows[0][result.columns[0]] == 1.0
        result = _run(executor, "from T select values.max()")
        assert result.rows[0][result.columns[0]] == 3.0

    def test_float16_empty_array(self, executor):
        _run(executor, "type T { values: float16[] }")
        _run(executor, "create T(values=[])")
        result = _run(executor, "from T select values")
        assert result.rows[0]["values"] == []


class TestFloat16Where:
    """float16 in WHERE clauses."""

    def test_where_comparison(self, executor):
        _run(executor, "type T { x: float16 }")
        _run(executor, "create T(x=1.0)")
        _run(executor, "create T(x=2.0)")
        _run(executor, "create T(x=3.0)")
        result = _run(executor, "from T select x where x > 1.5")
        values = [row["x"] for row in result.rows]
        assert values == [2.0, 3.0]

    def test_where_equality(self, executor):
        _run(executor, "type T { x: float16 }")
        _run(executor, "create T(x=2.5)")
        _run(executor, "create T(x=3.5)")
        result = _run(executor, "from T select x where x = 2.5")
        assert len(result.rows) == 1
        assert result.rows[0]["x"] == 2.5


class TestFloat16DumpRestore:
    """Dump and restore roundtrip for float16."""

    def test_dump_restore(self, db_dir):
        registry = TypeRegistry()
        storage = StorageManager(db_dir, registry)
        executor = QueryExecutor(storage, registry)
        _run(executor, "type T { x: float16 }")
        _run(executor, "create T(x=3.14)")
        dump_result = _run(executor, "dump")
        script = dump_result.script
        assert "float16" in script

        # Restore into a new directory
        restore_dir = db_dir.parent / (db_dir.name + "_restored")
        try:
            registry2 = TypeRegistry()
            storage2 = StorageManager(restore_dir, registry2)
            executor2 = QueryExecutor(storage2, registry2)
            for line in script.strip().split("\n"):
                line = line.strip()
                if line and not line.startswith("--"):
                    _run(executor2, line)
            result = _run(executor2, "from T select x")
            val = result.rows[0]["x"]
            expected = struct.unpack("<e", struct.pack("<e", 3.14))[0]
            assert val == expected
        finally:
            shutil.rmtree(restore_dir, ignore_errors=True)

    def test_describe_shows_float16(self, executor):
        _run(executor, "type T { x: float16 }")
        result = _run(executor, "describe T")
        found = any(
            "float16" in str(v)
            for row in result.rows
            for v in row.values()
        )
        assert found


class TestFloat16Alias:
    """Aliases to float16."""

    def test_alias_to_float16(self, executor):
        _run(executor, "alias half = float16")
        _run(executor, "type T { x: half }")
        _run(executor, "create T(x=2.5)")
        result = _run(executor, "from T select x")
        assert result.rows[0]["x"] == 2.5

    def test_float16_default_value(self, executor):
        _run(executor, "type T { x: float16 = 1.5 }")
        _run(executor, "create T()")
        result = _run(executor, "from T select x")
        assert result.rows[0]["x"] == 1.5


class TestFloat16Update:
    """UPDATE with float16 fields."""

    def test_update_float16_field(self, executor):
        _run(executor, "type T { x: float16 }")
        _run(executor, "$t = create T(x=1.0)")
        _run(executor, "update $t set x=2.5")
        result = _run(executor, "from T select x")
        assert result.rows[0]["x"] == 2.5


class TestFloat16Null:
    """NULL handling for float16 fields."""

    def test_float16_null_default(self, executor):
        _run(executor, "type T { x: float16, y: float16 }")
        _run(executor, "create T(x=1.0)")
        result = _run(executor, "from T select y")
        assert result.rows[0]["y"] is None

    def test_float16_set_to_null(self, executor):
        _run(executor, "type T { x: float16 }")
        _run(executor, "$t = create T(x=1.0)")
        _run(executor, "update $t set x=null")
        result = _run(executor, "from T select x")
        assert result.rows[0]["x"] is None
