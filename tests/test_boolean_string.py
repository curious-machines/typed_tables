"""Tests for boolean type and string() cast."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.parsing.query_parser import QueryParser
from typed_tables.query_executor import (
    CreateResult,
    DumpResult,
    QueryExecutor,
    QueryResult,
)
from typed_tables.storage import StorageManager
from typed_tables.types import (
    BooleanTypeDefinition,
    PrimitiveType,
    TypeRegistry,
    is_boolean_type,
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


# --- Boolean type definition ---

class TestBooleanTypeDefinition:
    def test_boolean_registered(self):
        """boolean type is registered in TypeRegistry."""
        registry = TypeRegistry()
        type_def = registry.get("boolean")
        assert type_def is not None
        assert isinstance(type_def, BooleanTypeDefinition)
        assert type_def.primitive == PrimitiveType.BIT

    def test_is_boolean_type(self):
        """is_boolean_type works for boolean and aliases."""
        registry = TypeRegistry()
        assert is_boolean_type(registry.get("boolean"))
        assert not is_boolean_type(registry.get("bit"))
        assert not is_boolean_type(registry.get("uint8"))
        assert not is_boolean_type(registry.get("string"))

    def test_boolean_reference_size(self):
        """boolean has same storage as bit (1 byte inline)."""
        registry = TypeRegistry()
        bool_def = registry.get("boolean")
        bit_def = registry.get("bit")
        assert bool_def.reference_size == bit_def.reference_size
        assert bool_def.reference_size == 1


# --- Boolean parsing ---

class TestBooleanParsing:
    def test_true_false_literals(self, executor):
        """true/false parse as 1/0."""
        result = _run(executor, "true")
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == 1

        result = _run(executor, "false")
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == 0

    def test_true_false_in_array(self, executor):
        """true/false work in array literals."""
        result = _run(executor, "[true, false, true]")
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == [1, 0, 1]


# --- Boolean field create/select ---

class TestBooleanField:
    def test_create_and_select(self, executor):
        """Create a type with boolean field and select it back."""
        _run(executor, 'type Toggle { active: boolean, label: string }')
        _run(executor, 'create Toggle(active=true, label="on")')
        _run(executor, 'create Toggle(active=false, label="off")')

        result = _run(executor, 'from Toggle select *')
        assert isinstance(result, QueryResult)
        assert len(result.rows) == 2
        assert result.rows[0]["active"] is True
        assert result.rows[1]["active"] is False

    def test_boolean_where(self, executor):
        """Filter by boolean field."""
        _run(executor, 'type Flag { name: string, enabled: boolean }')
        _run(executor, 'create Flag(name="a", enabled=true)')
        _run(executor, 'create Flag(name="b", enabled=false)')
        _run(executor, 'create Flag(name="c", enabled=true)')

        result = _run(executor, 'from Flag select name where enabled = true')
        assert isinstance(result, QueryResult)
        assert len(result.rows) == 2
        assert result.rows[0]["name"] == "a"
        assert result.rows[1]["name"] == "c"

    def test_boolean_default_value(self, executor):
        """Boolean field with default value."""
        _run(executor, 'type Setting { key: string, enabled: boolean = true }')
        _run(executor, 'create Setting(key="test")')

        result = _run(executor, 'from Setting select *')
        assert isinstance(result, QueryResult)
        assert result.rows[0]["enabled"] is True

    def test_boolean_null(self, executor):
        """Boolean field defaults to null when omitted (no default)."""
        _run(executor, 'type Toggle { active: boolean, label: string }')
        _run(executor, 'create Toggle(label="test")')

        result = _run(executor, 'from Toggle select *')
        assert isinstance(result, QueryResult)
        assert result.rows[0]["active"] is None

    def test_boolean_update(self, executor):
        """Update a boolean field."""
        _run(executor, 'type Toggle { active: boolean, label: string }')
        result = _run(executor, '$t = create Toggle(active=true, label="test")')
        _run(executor, 'update $t set active=false')

        result = _run(executor, 'from Toggle select *')
        assert isinstance(result, QueryResult)
        assert result.rows[0]["active"] is False

    def test_boolean_array(self, executor):
        """boolean[] field."""
        _run(executor, 'type Flags { values: boolean[] }')
        _run(executor, 'create Flags(values=[true, false, true])')

        result = _run(executor, 'from Flags select *')
        assert isinstance(result, QueryResult)
        # Array elements are stored as bit (0/1), not bool
        assert result.rows[0]["values"] == [1, 0, 1]


# --- Boolean in describe ---

class TestBooleanDescribe:
    def test_describe_shows_boolean(self, executor):
        """describe shows boolean type name."""
        _run(executor, 'type Toggle { active: boolean }')
        result = _run(executor, 'describe Toggle')
        assert isinstance(result, QueryResult)
        assert any(row["type"] == "boolean" for row in result.rows)


# --- Boolean dump ---

class TestBooleanDump:
    def test_dump_ttq(self, executor):
        """TTQ dump outputs true/false."""
        _run(executor, 'type Toggle { active: boolean }')
        _run(executor, 'create Toggle(active=true)')
        _run(executor, 'create Toggle(active=false)')

        result = _run(executor, 'dump')
        assert isinstance(result, DumpResult)
        assert "true" in result.script
        assert "false" in result.script

    def test_dump_roundtrip(self, executor, db_dir):
        """Dump and restore boolean values."""
        _run(executor, 'type Toggle { active: boolean, label: string }')
        _run(executor, 'create Toggle(active=true, label="on")')
        _run(executor, 'create Toggle(active=false, label="off")')

        result = _run(executor, 'dump')
        assert isinstance(result, DumpResult)

        # Create new executor and restore
        registry2 = TypeRegistry()
        db_dir2 = db_dir / "restored"
        storage2 = StorageManager(db_dir2, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        for line in result.script.strip().split("\n"):
            line = line.strip()
            if line and not line.startswith("--"):
                _run(executor2, line)

        result2 = _run(executor2, 'from Toggle select *')
        assert isinstance(result2, QueryResult)
        assert len(result2.rows) == 2
        assert result2.rows[0]["active"] is True
        assert result2.rows[1]["active"] is False

    def test_dump_yaml(self, executor):
        """YAML dump outputs true/false."""
        _run(executor, 'type Toggle { active: boolean }')
        _run(executor, 'create Toggle(active=true)')

        result = _run(executor, 'dump yaml')
        assert isinstance(result, DumpResult)
        assert "true" in result.script

    def test_dump_json(self, executor):
        """JSON dump outputs native booleans."""
        _run(executor, 'type Toggle { active: boolean }')
        _run(executor, 'create Toggle(active=true)')
        _run(executor, 'create Toggle(active=false)')

        result = _run(executor, 'dump json')
        assert isinstance(result, DumpResult)
        import json
        data = json.loads(result.script)
        records = data["Toggle"]
        assert records[0]["active"] is True
        assert records[1]["active"] is False

    def test_dump_default_value(self, executor):
        """Dump preserves boolean default values."""
        _run(executor, 'type Toggle { active: boolean = false }')
        result = _run(executor, 'dump')
        assert isinstance(result, DumpResult)
        assert "= false" in result.script


# --- Boolean metadata persistence ---

class TestBooleanMetadata:
    def test_metadata_roundtrip(self, executor, db_dir):
        """Boolean type survives metadata save/load."""
        _run(executor, 'type Toggle { active: boolean }')
        _run(executor, 'create Toggle(active=true)')

        # Load from metadata
        from typed_tables.dump import load_registry_from_metadata
        registry2 = load_registry_from_metadata(db_dir)
        bool_type = registry2.get("boolean")
        assert isinstance(bool_type, BooleanTypeDefinition)


# --- Boolean classify ---

class TestBooleanClassify:
    def test_graph_boolean(self, executor):
        """graph includes boolean type when used by a composite."""
        _run(executor, 'type Toggle { active: boolean }')
        result = _run(executor, 'graph all')
        assert isinstance(result, QueryResult)
        # TTGE graph all shows field edges with source/label/target
        assert any(
            row["source"] == "Toggle" and row["label"] == "active" and row["target"] == "boolean"
            for row in result.rows
        )


# --- boolean() cast ---

class TestBooleanCast:
    def test_boolean_0(self, executor):
        """boolean(0) returns false."""
        result = _run(executor, 'boolean(0)')
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] is False

    def test_boolean_1(self, executor):
        """boolean(1) returns true."""
        result = _run(executor, 'boolean(1)')
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] is True

    def test_boolean_invalid(self, executor):
        """boolean(2) raises error."""
        with pytest.raises(RuntimeError, match="requires 0 or 1"):
            _run(executor, 'boolean(2)')


# --- string() cast ---

class TestStringCast:
    def test_string_int(self, executor):
        """string(42) returns '42'."""
        result = _run(executor, 'string(42)')
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == "42"

    def test_string_float(self, executor):
        """string(3.14) returns '3.14'."""
        result = _run(executor, 'string(3.14)')
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == "3.14"

    def test_string_string(self, executor):
        """string("hello") returns 'hello' (identity)."""
        result = _run(executor, 'string("hello")')
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == "hello"

    def test_string_array(self, executor):
        """string([1, 2, 3]) returns ['1', '2', '3']."""
        result = _run(executor, 'string([1, 2, 3])')
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == ["1", "2", "3"]

    def test_string_typed_value(self, executor):
        """string(5i8) returns '5'."""
        result = _run(executor, 'string(5i8)')
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == "5"

    def test_string_negative(self, executor):
        """string(-7) returns '-7'."""
        result = _run(executor, 'string(-7)')
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == "-7"

    def test_string_enum(self, executor):
        """string(Color.red) returns 'red'."""
        _run(executor, 'enum Color { red, green, blue }')
        result = _run(executor, 'string(Color("red"))')
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == "red"

    def test_string_bool_true(self, executor):
        """string(boolean(1)) returns 'true'."""
        result = _run(executor, 'string(boolean(1))')
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == "true"

    def test_string_bool_false(self, executor):
        """string(boolean(0)) returns 'false'."""
        result = _run(executor, 'string(boolean(0))')
        assert isinstance(result, QueryResult)
        assert result.rows[0][result.columns[0]] == "false"
