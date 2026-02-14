"""Tests for bigint and biguint types."""

import os
import shutil
import tempfile

import pytest

from typed_tables.query_executor import QueryExecutor, QueryResult
from typed_tables.storage import StorageManager
from typed_tables.types import (
    BigInt,
    BigIntTypeDefinition,
    BigUInt,
    BigUIntTypeDefinition,
    TypeRegistry,
    is_bigint_type,
    is_biguint_type,
)


@pytest.fixture
def executor():
    """Create a QueryExecutor with a temp data directory."""
    data_dir = tempfile.mkdtemp()
    registry = TypeRegistry()
    storage = StorageManager(data_dir=__import__("pathlib").Path(data_dir), registry=registry)
    exec_ = QueryExecutor(registry=registry, storage=storage)
    yield exec_
    storage.close()
    shutil.rmtree(data_dir, ignore_errors=True)


def run(executor, query):
    """Execute a query and return the result."""
    from typed_tables.parsing.query_parser import QueryParser
    parser = QueryParser()
    statements = parser.parse_program(query)
    result = None
    for stmt in statements:
        result = executor.execute(stmt)
    return result


# --- Type system tests ---

class TestTypeSystem:
    def test_bigint_registered(self):
        registry = TypeRegistry()
        td = registry.get("bigint")
        assert td is not None
        assert isinstance(td, BigIntTypeDefinition)

    def test_biguint_registered(self):
        registry = TypeRegistry()
        td = registry.get("biguint")
        assert td is not None
        assert isinstance(td, BigUIntTypeDefinition)

    def test_bigint_is_array_subclass(self):
        registry = TypeRegistry()
        td = registry.get("bigint")
        assert td.is_array  # Inherits from ArrayTypeDefinition

    def test_bigint_reference_size(self):
        registry = TypeRegistry()
        td = registry.get("bigint")
        assert td.reference_size == 8  # (start_index, length)

    def test_is_bigint_type_helper(self):
        registry = TypeRegistry()
        assert is_bigint_type(registry.get("bigint"))
        assert not is_bigint_type(registry.get("biguint"))
        assert not is_bigint_type(registry.get("string"))
        assert not is_bigint_type(registry.get("uint8"))

    def test_is_biguint_type_helper(self):
        registry = TypeRegistry()
        assert is_biguint_type(registry.get("biguint"))
        assert not is_biguint_type(registry.get("bigint"))
        assert not is_biguint_type(registry.get("string"))

    def test_bigint_element_type_is_uint8(self):
        registry = TypeRegistry()
        td = registry.get("bigint")
        assert td.element_type.name == "uint8"


# --- Type creation tests ---

class TestTypeCreation:
    def test_create_type_with_bigint(self, executor):
        result = run(executor, 'type Data { big: bigint }')
        assert result is not None

    def test_create_type_with_biguint(self, executor):
        result = run(executor, 'type Data { pos: biguint }')
        assert result is not None

    def test_create_type_with_both(self, executor):
        result = run(executor, 'type Data { big: bigint, pos: biguint }')
        assert result is not None

    def test_describe_bigint(self, executor):
        run(executor, 'type Data { big: bigint, pos: biguint }')
        result = run(executor, 'describe Data')
        assert result is not None
        # First row is (type) = Composite, then the fields
        field_rows = [r for r in result.rows if r["property"] not in ("(type)",)]
        assert len(field_rows) == 2
        assert field_rows[0]["type"] == "bigint"
        assert field_rows[1]["type"] == "biguint"


# --- Instance creation tests ---

class TestInstanceCreation:
    def test_create_bigint_zero(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=0)')
        result = run(executor, 'from Data select big')
        assert result.rows[0]["big"] == 0

    def test_create_bigint_positive(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=42)')
        result = run(executor, 'from Data select big')
        assert result.rows[0]["big"] == 42

    def test_create_bigint_negative(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=-100)')
        result = run(executor, 'from Data select big')
        assert result.rows[0]["big"] == -100

    def test_create_bigint_large_positive(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=12345678901234567890)')
        result = run(executor, 'from Data select big')
        val = result.rows[0]["big"]
        assert int(val) == 12345678901234567890

    def test_create_bigint_large_negative(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=-99999999999999999999)')
        result = run(executor, 'from Data select big')
        val = result.rows[0]["big"]
        assert int(val) == -99999999999999999999

    def test_create_biguint_zero(self, executor):
        run(executor, 'type Data { pos: biguint }')
        run(executor, 'create Data(pos=0)')
        result = run(executor, 'from Data select pos')
        assert result.rows[0]["pos"] == 0

    def test_create_biguint_large(self, executor):
        run(executor, 'type Data { pos: biguint }')
        run(executor, 'create Data(pos=12345678901234567890)')
        result = run(executor, 'from Data select pos')
        val = result.rows[0]["pos"]
        assert int(val) == 12345678901234567890

    def test_biguint_rejects_negative(self, executor):
        run(executor, 'type Data { pos: biguint }')
        result = run(executor, 'create Data(pos=-1)')
        assert "negative" in result.message.lower()

    def test_create_bigint_null(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data()')
        result = run(executor, 'from Data select big')
        assert result.rows[0]["big"] is None

    def test_create_multiple_records(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=-1)')
        run(executor, 'create Data(big=0)')
        run(executor, 'create Data(big=1)')
        run(executor, 'create Data(big=999999999999)')
        result = run(executor, 'from Data select big')
        assert len(result.rows) == 4
        vals = [int(r["big"]) for r in result.rows]
        assert vals == [-1, 0, 1, 999999999999]


# --- BigInt/BigUInt display wrapper tests ---

class TestDisplayWrappers:
    def test_bigint_is_int_subclass(self):
        v = BigInt(42)
        assert isinstance(v, int)
        assert isinstance(v, BigInt)
        assert v == 42

    def test_biguint_is_int_subclass(self):
        v = BigUInt(42)
        assert isinstance(v, int)
        assert isinstance(v, BigUInt)
        assert v == 42

    def test_bigint_negative(self):
        v = BigInt(-100)
        assert v == -100
        assert str(int(v)) == "-100"

    def test_bigint_large(self):
        v = BigInt(10**50)
        assert v == 10**50

    def test_select_returns_bigint_wrapper(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=42)')
        result = run(executor, 'from Data select big')
        val = result.rows[0]["big"]
        assert isinstance(val, BigInt)

    def test_select_returns_biguint_wrapper(self, executor):
        run(executor, 'type Data { pos: biguint }')
        run(executor, 'create Data(pos=42)')
        result = run(executor, 'from Data select pos')
        val = result.rows[0]["pos"]
        assert isinstance(val, BigUInt)


# --- Conversion function tests ---

class TestConversionFunctions:
    def test_bigint_conversion(self, executor):
        result = run(executor, 'bigint(42)')
        val = result.rows[0][result.columns[0]]
        assert isinstance(val, BigInt)
        assert int(val) == 42

    def test_biguint_conversion(self, executor):
        result = run(executor, 'biguint(42)')
        val = result.rows[0][result.columns[0]]
        assert isinstance(val, BigUInt)
        assert int(val) == 42

    def test_bigint_from_negative(self, executor):
        result = run(executor, 'bigint(-100)')
        val = result.rows[0][result.columns[0]]
        assert isinstance(val, BigInt)
        assert int(val) == -100

    def test_biguint_rejects_negative(self, executor):
        with pytest.raises(Exception, match="negative"):
            run(executor, 'biguint(-1)')

    def test_bigint_in_create(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=bigint(42))')
        result = run(executor, 'from Data select big')
        assert int(result.rows[0]["big"]) == 42

    def test_biguint_in_create(self, executor):
        run(executor, 'type Data { pos: biguint }')
        run(executor, 'create Data(pos=biguint(100))')
        result = run(executor, 'from Data select pos')
        assert int(result.rows[0]["pos"]) == 100


# --- WHERE clause tests ---

class TestWhereClause:
    def test_where_equals(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=10)')
        run(executor, 'create Data(big=20)')
        run(executor, 'create Data(big=30)')
        result = run(executor, 'from Data select big where big = 20')
        assert len(result.rows) == 1
        assert int(result.rows[0]["big"]) == 20

    def test_where_greater_than(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=10)')
        run(executor, 'create Data(big=20)')
        run(executor, 'create Data(big=30)')
        result = run(executor, 'from Data select big where big > 15')
        assert len(result.rows) == 2

    def test_where_negative(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=-10)')
        run(executor, 'create Data(big=0)')
        run(executor, 'create Data(big=10)')
        result = run(executor, 'from Data select big where big < 0')
        assert len(result.rows) == 1
        assert int(result.rows[0]["big"]) == -10


# --- Default value tests ---

class TestDefaults:
    def test_bigint_default_zero(self, executor):
        run(executor, 'type Data { big: bigint = 0 }')
        run(executor, 'create Data()')
        result = run(executor, 'from Data select big')
        assert int(result.rows[0]["big"]) == 0

    def test_biguint_default(self, executor):
        run(executor, 'type Data { pos: biguint = 42 }')
        run(executor, 'create Data()')
        result = run(executor, 'from Data select pos')
        assert int(result.rows[0]["pos"]) == 42

    def test_bigint_default_negative(self, executor):
        run(executor, 'type Data { big: bigint = -5 }')
        run(executor, 'create Data()')
        result = run(executor, 'from Data select big')
        assert int(result.rows[0]["big"]) == -5

    def test_bigint_default_override(self, executor):
        run(executor, 'type Data { big: bigint = 0 }')
        run(executor, 'create Data(big=100)')
        result = run(executor, 'from Data select big')
        assert int(result.rows[0]["big"]) == 100


# --- Update tests ---

class TestUpdate:
    def test_update_bigint(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, '$d = create Data(big=10)')
        run(executor, 'update $d set big=20')
        result = run(executor, 'from Data select big')
        assert int(result.rows[0]["big"]) == 20

    def test_update_bigint_to_negative(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, '$d = create Data(big=10)')
        run(executor, 'update $d set big=-999')
        result = run(executor, 'from Data select big')
        assert int(result.rows[0]["big"]) == -999

    def test_update_biguint_rejects_negative(self, executor):
        run(executor, 'type Data { pos: biguint }')
        run(executor, '$d = create Data(pos=10)')
        result = run(executor, 'update $d set pos=-1')
        # Should fail with an error message
        assert "negative" in result.message.lower()

    def test_update_bigint_to_null(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, '$d = create Data(big=10)')
        run(executor, 'update $d set big=null')
        result = run(executor, 'from Data select big')
        assert result.rows[0]["big"] is None


# --- Dump/restore tests ---

class TestDump:
    def test_dump_bigint(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=42)')
        result = run(executor, 'dump')
        assert "42" in result.script

    def test_dump_bigint_negative(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=-100)')
        result = run(executor, 'dump')
        assert "-100" in result.script

    def test_dump_bigint_large(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=12345678901234567890)')
        result = run(executor, 'dump')
        assert "12345678901234567890" in result.script

    def test_dump_biguint(self, executor):
        run(executor, 'type Data { pos: biguint }')
        run(executor, 'create Data(pos=42)')
        result = run(executor, 'dump')
        assert "42" in result.script

    def test_dump_roundtrip(self, executor):
        run(executor, 'type Data { big: bigint, pos: biguint }')
        run(executor, 'create Data(big=-42, pos=12345678901234567890)')
        result = run(executor, 'dump')
        script = result.script

        # Create a new executor and execute the dump script
        data_dir2 = tempfile.mkdtemp()
        registry2 = TypeRegistry()
        storage2 = StorageManager(data_dir=__import__("pathlib").Path(data_dir2), registry=registry2)
        exec2 = QueryExecutor(registry=registry2, storage=storage2)
        try:
            for line in script.strip().split('\n'):
                line = line.strip()
                if line and not line.startswith('--'):
                    run_exec(exec2, line)
            result2 = run_exec(exec2, 'from Data select big, pos')
            assert int(result2.rows[0]["big"]) == -42
            assert int(result2.rows[0]["pos"]) == 12345678901234567890
        finally:
            storage2.close()
            shutil.rmtree(data_dir2, ignore_errors=True)

    def test_dump_yaml(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=42)')
        result = run(executor, 'dump yaml')
        assert "42" in result.script

    def test_dump_json(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=42)')
        result = run(executor, 'dump json')
        assert "42" in result.script

    def test_dump_xml(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=42)')
        result = run(executor, 'dump xml')
        assert "42" in result.script


def run_exec(executor, query):
    """Execute a query on a given executor."""
    from typed_tables.parsing.query_parser import QueryParser
    parser = QueryParser()
    statements = parser.parse_program(query)
    result = None
    for stmt in statements:
        result = executor.execute(stmt)
    return result


# --- Alias tests ---

class TestAliases:
    def test_alias_to_bigint(self, executor):
        run(executor, 'alias mybig = bigint')
        run(executor, 'type Data { val: mybig }')
        run(executor, 'create Data(val=42)')
        result = run(executor, 'from Data select val')
        assert int(result.rows[0]["val"]) == 42

    def test_alias_to_biguint(self, executor):
        run(executor, 'alias myuint = biguint')
        run(executor, 'type Data { val: myuint }')
        run(executor, 'create Data(val=999)')
        result = run(executor, 'from Data select val')
        assert int(result.rows[0]["val"]) == 999

    def test_alias_preserves_signedness(self, executor):
        run(executor, 'alias mybig = bigint')
        run(executor, 'type Data { val: mybig }')
        run(executor, 'create Data(val=-50)')
        result = run(executor, 'from Data select val')
        assert int(result.rows[0]["val"]) == -50


# --- Compact tests ---

class TestCompact:
    def test_compact_bigint(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=10)')
        run(executor, 'create Data(big=20)')
        run(executor, 'create Data(big=30)')
        run(executor, 'delete Data where big = 20')

        output_dir = tempfile.mkdtemp()
        shutil.rmtree(output_dir)  # compact needs non-existent target

        result = run(executor, f'compact > "{output_dir}"')
        assert "2" in result.message  # 3 -> 2 records

        # Verify compacted data
        from pathlib import Path
        from typed_tables.dump import load_registry_from_metadata
        registry2 = load_registry_from_metadata(Path(output_dir))
        storage2 = StorageManager(data_dir=Path(output_dir), registry=registry2)
        exec2 = QueryExecutor(registry=registry2, storage=storage2)
        try:
            result2 = run_exec(exec2, 'from Data select big')
            vals = [int(r["big"]) for r in result2.rows]
            assert sorted(vals) == [10, 30]
        finally:
            storage2.close()
            shutil.rmtree(output_dir, ignore_errors=True)


# --- Byte encoding edge cases ---

class TestByteEncoding:
    def test_zero_encoding(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=0)')
        result = run(executor, 'from Data select big')
        assert result.rows[0]["big"] == 0

    def test_one_encoding(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=1)')
        result = run(executor, 'from Data select big')
        assert result.rows[0]["big"] == 1

    def test_minus_one_encoding(self, executor):
        run(executor, 'type Data { big: bigint }')
        run(executor, 'create Data(big=-1)')
        result = run(executor, 'from Data select big')
        assert result.rows[0]["big"] == -1

    def test_max_uint64_boundary(self, executor):
        """Test a value that exceeds uint64 range."""
        val = 2**64
        run(executor, 'type Data { big: bigint }')
        run(executor, f'create Data(big={val})')
        result = run(executor, 'from Data select big')
        assert int(result.rows[0]["big"]) == val

    def test_very_large_number(self, executor):
        """Test a 100+ digit number."""
        val = 10**100
        run(executor, 'type Data { big: bigint }')
        run(executor, f'create Data(big={val})')
        result = run(executor, 'from Data select big')
        assert int(result.rows[0]["big"]) == val

    def test_biguint_max_uint128_boundary(self, executor):
        val = 2**128 - 1
        run(executor, 'type Data { pos: biguint }')
        run(executor, f'create Data(pos={val})')
        result = run(executor, 'from Data select pos')
        assert int(result.rows[0]["pos"]) == val

    def test_biguint_beyond_uint128(self, executor):
        val = 2**256
        run(executor, 'type Data { pos: biguint }')
        run(executor, f'create Data(pos={val})')
        result = run(executor, 'from Data select pos')
        assert int(result.rows[0]["pos"]) == val

    def test_bigint_min_int128_boundary(self, executor):
        val = -(2**127)
        run(executor, 'type Data { big: bigint }')
        run(executor, f'create Data(big={val})')
        result = run(executor, 'from Data select big')
        assert int(result.rows[0]["big"]) == val


# --- Mixed type fields ---

class TestMixedFields:
    def test_bigint_with_other_fields(self, executor):
        run(executor, 'type Data { name: string, big: bigint, count: uint32 }')
        run(executor, 'create Data(name="test", big=42, count=10)')
        result = run(executor, 'from Data select *')
        assert result.rows[0]["name"] == "test"
        assert int(result.rows[0]["big"]) == 42
        assert result.rows[0]["count"] == 10

    def test_bigint_and_biguint_together(self, executor):
        run(executor, 'type Data { signed: bigint, unsigned: biguint }')
        run(executor, 'create Data(signed=-42, unsigned=42)')
        result = run(executor, 'from Data select *')
        assert int(result.rows[0]["signed"]) == -42
        assert int(result.rows[0]["unsigned"]) == 42


# --- REPL display tests ---

class TestReplDisplay:
    def test_format_bigint(self):
        from typed_tables.repl import format_value
        assert format_value(BigInt(42)) == "42"
        assert format_value(BigInt(-100)) == "-100"
        assert format_value(BigInt(0)) == "0"

    def test_format_biguint(self):
        from typed_tables.repl import format_value
        assert format_value(BigUInt(42)) == "42"
        assert format_value(BigUInt(0)) == "0"

    def test_format_large_bigint_not_hex(self):
        """Large BigInt should display as decimal, not hex."""
        from typed_tables.repl import format_value
        val = BigInt(12345678901234567890)
        result = format_value(val)
        assert result == "12345678901234567890"
        assert "0x" not in result

    def test_format_large_biguint_not_hex(self):
        """Large BigUInt should display as decimal, not hex."""
        from typed_tables.repl import format_value
        val = BigUInt(12345678901234567890)
        result = format_value(val)
        assert result == "12345678901234567890"
        assert "0x" not in result


# --- Eval expression tests ---

class TestEvalExpressions:
    def test_eval_bigint(self, executor):
        result = run(executor, 'bigint(100)')
        val = result.rows[0][result.columns[0]]
        assert isinstance(val, BigInt)
        assert int(val) == 100

    def test_eval_biguint(self, executor):
        result = run(executor, 'biguint(100)')
        val = result.rows[0][result.columns[0]]
        assert isinstance(val, BigUInt)
        assert int(val) == 100

    def test_eval_bigint_large_not_hex(self, executor):
        """BigInt eval results should not be hex-formatted."""
        result = run(executor, 'bigint(12345678901234567890)')
        val = result.rows[0][result.columns[0]]
        # The value should be kept as BigInt, not converted to hex string
        assert isinstance(val, BigInt)
        assert int(val) == 12345678901234567890
