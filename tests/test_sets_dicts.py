"""Tests for sets and dictionaries — Phase 1 & 2: Type System, Parser, Metadata, Storage, Instance Creation, Selection, Dump."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from typed_tables.dump import load_registry_from_metadata
from typed_tables.parsing.query_parser import (
    ArrayTypeSpec,
    DictEntry,
    DictLiteral,
    DictTypeSpec,
    EmptyBraces,
    FieldDef,
    QueryParser,
    SetLiteral,
    SetTypeSpec,
)
from typed_tables.query_executor import QueryExecutor, QueryResult, CreateResult, DumpResult, UpdateResult
from typed_tables.storage import StorageManager
from typed_tables.types import (
    ArrayTypeDefinition,
    CompositeTypeDefinition,
    DictionaryTypeDefinition,
    FieldDefinition,
    SetTypeDefinition,
    SetValue,
    TypeRegistry,
    _make_dict_entry_type_name,
    _type_def_to_type_string,
    is_dict_type,
    is_set_type,
)


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary database directory and executor."""
    db_dir = tmp_path / "test_db"
    db_dir.mkdir()
    registry = TypeRegistry()
    storage = StorageManager(db_dir, registry)
    executor = QueryExecutor(storage, registry)
    return executor, db_dir, registry, storage


def _run(executor: QueryExecutor, query: str):
    """Run a query and return the result."""
    parser = QueryParser()
    parsed = parser.parse(query)
    return executor.execute(parsed)


def _run_all(executor: QueryExecutor, script: str):
    """Run multiple queries."""
    parser = QueryParser()
    stmts = parser.parse_program(script)
    results = []
    for stmt in stmts:
        results.append(executor.execute(stmt))
    return results


# ==============================================================
# Parser tests
# ==============================================================

class TestParser:
    """Test that the parser produces correct TypeSpec AST nodes."""

    def test_parse_set_field(self):
        parser = QueryParser()
        stmts = parser.parse_program('type Student { tags: {string} }')
        assert len(stmts) == 1
        q = stmts[0]
        assert q.name == "Student"
        assert len(q.fields) == 1
        f = q.fields[0]
        assert f.name == "tags"
        assert isinstance(f.type_name, SetTypeSpec)
        assert f.type_name.element_type == "string"

    def test_parse_dict_field(self):
        parser = QueryParser()
        stmts = parser.parse_program('type Student { scores: {string: float64} }')
        assert len(stmts) == 1
        q = stmts[0]
        f = q.fields[0]
        assert f.name == "scores"
        assert isinstance(f.type_name, DictTypeSpec)
        assert f.type_name.key_type == "string"
        assert f.type_name.value_type == "float64"

    def test_parse_prefix_array(self):
        parser = QueryParser()
        stmts = parser.parse_program('type X { data: [int32] }')
        assert len(stmts) == 1
        f = stmts[0].fields[0]
        assert isinstance(f.type_name, ArrayTypeSpec)
        assert f.type_name.element_type == "int32"

    def test_parse_postfix_array_still_works(self):
        parser = QueryParser()
        stmts = parser.parse_program('type X { data: int32[] }')
        assert len(stmts) == 1
        f = stmts[0].fields[0]
        assert f.type_name == "int32[]"

    def test_parse_nested_set_in_dict(self):
        parser = QueryParser()
        stmts = parser.parse_program('type X { data: {string: {int32}} }')
        f = stmts[0].fields[0]
        assert isinstance(f.type_name, DictTypeSpec)
        assert f.type_name.key_type == "string"
        assert isinstance(f.type_name.value_type, SetTypeSpec)
        assert f.type_name.value_type.element_type == "int32"

    def test_parse_array_of_sets(self):
        parser = QueryParser()
        stmts = parser.parse_program('type X { data: [{string}] }')
        f = stmts[0].fields[0]
        assert isinstance(f.type_name, ArrayTypeSpec)
        assert isinstance(f.type_name.element_type, SetTypeSpec)
        assert f.type_name.element_type.element_type == "string"

    def test_parse_dict_with_array_value(self):
        parser = QueryParser()
        stmts = parser.parse_program('type X { data: {string: [int32]} }')
        f = stmts[0].fields[0]
        assert isinstance(f.type_name, DictTypeSpec)
        assert f.type_name.key_type == "string"
        assert isinstance(f.type_name.value_type, ArrayTypeSpec)
        assert f.type_name.value_type.element_type == "int32"

    def test_parse_alias_to_set(self):
        parser = QueryParser()
        stmts = parser.parse_program('alias Tags = {string}')
        q = stmts[0]
        assert q.name == "Tags"
        assert isinstance(q.base_type, SetTypeSpec)
        assert q.base_type.element_type == "string"

    def test_parse_alias_to_dict(self):
        parser = QueryParser()
        stmts = parser.parse_program('alias Scores = {string: float64}')
        q = stmts[0]
        assert q.name == "Scores"
        assert isinstance(q.base_type, DictTypeSpec)
        assert q.base_type.key_type == "string"
        assert q.base_type.value_type == "float64"

    def test_parse_alias_to_prefix_array(self):
        parser = QueryParser()
        stmts = parser.parse_program('alias Ints = [int32]')
        q = stmts[0]
        assert q.name == "Ints"
        assert isinstance(q.base_type, ArrayTypeSpec)
        assert q.base_type.element_type == "int32"

    def test_parse_multiple_fields(self):
        parser = QueryParser()
        stmts = parser.parse_program("""
            type Student {
                name: string,
                tags: {string},
                scores: {string: float64}
            }
        """)
        q = stmts[0]
        assert len(q.fields) == 3
        assert q.fields[0].type_name == "string"
        assert isinstance(q.fields[1].type_name, SetTypeSpec)
        assert isinstance(q.fields[2].type_name, DictTypeSpec)


# ==============================================================
# Type system tests
# ==============================================================

class TestTypeSystem:
    """Test SetTypeDefinition and DictionaryTypeDefinition."""

    def test_set_type_creation(self):
        registry = TypeRegistry()
        string_td = registry.get("string")
        set_td = registry.get_or_create_set_type(string_td)
        assert isinstance(set_td, SetTypeDefinition)
        assert set_td.name == "{string}"
        assert set_td.element_type is string_td
        assert set_td.reference_size == 8
        assert set_td.is_array

    def test_set_type_reuse(self):
        registry = TypeRegistry()
        string_td = registry.get("string")
        set1 = registry.get_or_create_set_type(string_td)
        set2 = registry.get_or_create_set_type(string_td)
        assert set1 is set2

    def test_dict_type_creation(self):
        registry = TypeRegistry()
        string_td = registry.get("string")
        float_td = registry.get("float64")
        dict_td = registry.get_or_create_dict_type(string_td, float_td)
        assert isinstance(dict_td, DictionaryTypeDefinition)
        assert dict_td.name == "{string: float64}"
        assert dict_td.key_type is string_td
        assert dict_td.value_type is float_td
        assert dict_td.reference_size == 8
        assert dict_td.is_array

    def test_dict_entry_type_creation(self):
        registry = TypeRegistry()
        string_td = registry.get("string")
        float_td = registry.get("float64")
        dict_td = registry.get_or_create_dict_type(string_td, float_td)
        entry = dict_td.entry_type
        assert isinstance(entry, CompositeTypeDefinition)
        assert entry.name == "Dict_string_float64"
        assert len(entry.fields) == 2
        assert entry.fields[0].name == "key"
        assert entry.fields[0].type_def is string_td
        assert entry.fields[1].name == "value"
        assert entry.fields[1].type_def is float_td

    def test_dict_type_reuse(self):
        registry = TypeRegistry()
        string_td = registry.get("string")
        float_td = registry.get("float64")
        dict1 = registry.get_or_create_dict_type(string_td, float_td)
        dict2 = registry.get_or_create_dict_type(string_td, float_td)
        assert dict1 is dict2

    def test_is_set_type(self):
        registry = TypeRegistry()
        string_td = registry.get("string")
        set_td = registry.get_or_create_set_type(string_td)
        assert is_set_type(set_td)
        assert not is_set_type(string_td)

    def test_is_dict_type(self):
        registry = TypeRegistry()
        string_td = registry.get("string")
        float_td = registry.get("float64")
        dict_td = registry.get_or_create_dict_type(string_td, float_td)
        assert is_dict_type(dict_td)
        assert not is_dict_type(string_td)

    def test_type_def_to_type_string(self):
        registry = TypeRegistry()
        string_td = registry.get("string")
        float_td = registry.get("float64")
        int_td = registry.get("int32")

        set_td = registry.get_or_create_set_type(string_td)
        assert _type_def_to_type_string(set_td) == "{string}"

        dict_td = registry.get_or_create_dict_type(string_td, float_td)
        assert _type_def_to_type_string(dict_td) == "{string: float64}"

        assert _type_def_to_type_string(string_td) == "string"
        assert _type_def_to_type_string(int_td) == "int32"

    def test_make_dict_entry_type_name(self):
        registry = TypeRegistry()
        string_td = registry.get("string")
        int_td = registry.get("int32")
        float_td = registry.get("float64")

        assert _make_dict_entry_type_name(string_td, int_td) == "Dict_string_int32"
        assert _make_dict_entry_type_name(string_td, float_td) == "Dict_string_float64"

    def test_make_dict_entry_type_name_nested(self):
        registry = TypeRegistry()
        string_td = registry.get("string")
        int_td = registry.get("int32")
        int_arr = registry.get_array_type("int32")
        int_set = registry.get_or_create_set_type(int_td)

        assert _make_dict_entry_type_name(string_td, int_arr) == "Dict_string_Array_int32"
        assert _make_dict_entry_type_name(string_td, int_set) == "Dict_string_Set_int32"


# ==============================================================
# Executor type creation tests
# ==============================================================

class TestExecutorTypeCreation:
    """Test creating types with set and dict fields via the executor."""

    def test_create_type_with_set_field(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'type Student { name: string, tags: {string} }')
        assert isinstance(result, CreateResult)
        assert "Created type" in result.message

        td = registry.get("Student")
        assert isinstance(td, CompositeTypeDefinition)
        tags_field = td.get_field("tags")
        assert tags_field is not None
        assert isinstance(tags_field.type_def, SetTypeDefinition)
        assert tags_field.type_def.name == "{string}"

    def test_create_type_with_dict_field(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'type Student { name: string, scores: {string: float64} }')
        assert isinstance(result, CreateResult)
        assert "Created type" in result.message

        td = registry.get("Student")
        scores_field = td.get_field("scores")
        assert isinstance(scores_field.type_def, DictionaryTypeDefinition)
        assert scores_field.type_def.name == "{string: float64}"

        # Entry type should be registered
        entry = registry.get("Dict_string_float64")
        assert isinstance(entry, CompositeTypeDefinition)
        assert len(entry.fields) == 2

    def test_create_type_with_prefix_array(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'type X { data: [int32] }')
        assert isinstance(result, CreateResult)
        td = registry.get("X")
        data_field = td.get_field("data")
        assert isinstance(data_field.type_def, ArrayTypeDefinition)
        assert data_field.type_def.element_type.name == "int32"

    def test_create_type_nested_dict_array(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'type Matrix { data: {string: [int32]} }')
        assert isinstance(result, CreateResult)
        td = registry.get("Matrix")
        data_field = td.get_field("data")
        assert isinstance(data_field.type_def, DictionaryTypeDefinition)
        assert isinstance(data_field.type_def.value_type, ArrayTypeDefinition)
        assert data_field.type_def.value_type.element_type.name == "int32"

    def test_create_type_array_of_sets(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'type Flags { active: [{string}] }')
        assert isinstance(result, CreateResult)
        td = registry.get("Flags")
        active_field = td.get_field("active")
        assert isinstance(active_field.type_def, ArrayTypeDefinition)
        inner = active_field.type_def.element_type
        assert isinstance(inner, SetTypeDefinition)
        assert inner.element_type.name == "string"

    def test_create_alias_to_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'alias Tags = {string}')
        assert isinstance(result, CreateResult)
        assert "Created alias" in result.message
        td = registry.get("Tags")
        assert td is not None
        assert is_set_type(td)

    def test_create_alias_to_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'alias Scores = {string: float64}')
        assert isinstance(result, CreateResult)
        td = registry.get("Scores")
        assert td is not None
        assert is_dict_type(td)

    def test_create_alias_to_prefix_array(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'alias Ints = [int32]')
        assert isinstance(result, CreateResult)
        td = registry.get("Ints")
        assert td is not None
        base = td.resolve_base_type()
        assert isinstance(base, ArrayTypeDefinition)
        assert base.element_type.name == "int32"

    def test_create_interface_with_set_field(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'interface Taggable { tags: {string} }')
        assert isinstance(result, CreateResult)
        td = registry.get("Taggable")
        tags_field = td.get_field("tags")
        assert isinstance(tags_field.type_def, SetTypeDefinition)

    def test_unknown_type_in_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'type X { data: {Nonexistent} }')
        assert "Unknown type" in result.message

    def test_unknown_type_in_dict_key(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'type X { data: {Nonexistent: int32} }')
        assert "Unknown type" in result.message

    def test_unknown_type_in_dict_value(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        result = _run(executor, 'type X { data: {string: Nonexistent} }')
        assert "Unknown type" in result.message

    def test_enum_with_set_field(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, """
            enum Shape {
                none,
                labeled(name: string, tags: {string})
            }
        """)
        # Check it was created successfully
        td = registry.get("Shape")
        labeled = td.get_variant("labeled")
        assert labeled is not None
        tags_f = next(f for f in labeled.fields if f.name == "tags")
        assert isinstance(tags_f.type_def, SetTypeDefinition)


# ==============================================================
# Metadata roundtrip tests
# ==============================================================

class TestMetadataRoundtrip:
    """Test that set/dict types survive save → load cycles."""

    def test_set_metadata_roundtrip(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { name: string, tags: {string} }')
        storage.save_metadata()

        # Load from scratch
        reg2 = load_registry_from_metadata(db_dir)
        td = reg2.get("Student")
        assert isinstance(td, CompositeTypeDefinition)
        tags_field = td.get_field("tags")
        assert isinstance(tags_field.type_def, SetTypeDefinition)
        assert tags_field.type_def.element_type.name == "string"

    def test_dict_metadata_roundtrip(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { name: string, scores: {string: float64} }')
        storage.save_metadata()

        reg2 = load_registry_from_metadata(db_dir)
        td = reg2.get("Student")
        assert isinstance(td, CompositeTypeDefinition)
        scores_field = td.get_field("scores")
        assert isinstance(scores_field.type_def, DictionaryTypeDefinition)
        assert scores_field.type_def.key_type.name == "string"
        assert scores_field.type_def.value_type.name == "float64"

        # Entry type should be registered
        entry = reg2.get("Dict_string_float64")
        assert isinstance(entry, CompositeTypeDefinition)

    def test_prefix_array_metadata_roundtrip(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type X { data: [int32] }')
        storage.save_metadata()

        reg2 = load_registry_from_metadata(db_dir)
        td = reg2.get("X")
        data_field = td.get_field("data")
        assert isinstance(data_field.type_def, ArrayTypeDefinition)
        assert data_field.type_def.element_type.name == "int32"

    def test_nested_dict_metadata_roundtrip(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Matrix { data: {string: [int32]} }')
        storage.save_metadata()

        reg2 = load_registry_from_metadata(db_dir)
        td = reg2.get("Matrix")
        data_field = td.get_field("data")
        assert isinstance(data_field.type_def, DictionaryTypeDefinition)
        vt = data_field.type_def.value_type
        assert isinstance(vt, ArrayTypeDefinition)
        assert vt.element_type.name == "int32"

    def test_metadata_json_structure(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { tags: {string}, scores: {string: float64} }')
        storage.save_metadata()

        with open(db_dir / "_metadata.json") as f:
            meta = json.load(f)

        types = meta["types"]
        # Set type should be serialized
        set_spec = types.get("{string}")
        assert set_spec is not None
        assert set_spec["kind"] == "set"
        assert set_spec["element_type"] == "string"

        # Dict type should be serialized
        dict_spec = types.get("{string: float64}")
        assert dict_spec is not None
        assert dict_spec["kind"] == "dictionary"
        assert dict_spec["key_type"] == "string"
        assert dict_spec["value_type"] == "float64"

        # Entry composite should be serialized
        entry_spec = types.get("Dict_string_float64")
        assert entry_spec is not None
        assert entry_spec["kind"] == "composite"


# ==============================================================
# Describe tests
# ==============================================================

class TestDescribe:
    """Test describe output for set and dict types."""

    def test_describe_set_type(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { tags: {string} }')
        result = _run(executor, 'describe Student')
        assert isinstance(result, QueryResult)
        # Should have a tags field with type {string}
        tags_row = next(r for r in result.rows if r.get("property") == "tags")
        assert tags_row["type"] == "{string}"

    def test_describe_dict_type(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { scores: {string: float64} }')
        result = _run(executor, 'describe Student')
        scores_row = next(r for r in result.rows if r.get("property") == "scores")
        assert scores_row["type"] == "{string: float64}"

    def test_describe_set_directly(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { tags: {string} }')
        result = _run(executor, 'describe "{string}"')
        assert isinstance(result, QueryResult)
        # Should show Set type info and element_type
        type_row = next(r for r in result.rows if r["property"] == "(type)")
        assert type_row["type"] == "Set"
        elem_row = next(r for r in result.rows if r["property"] == "(element_type)")
        assert elem_row["type"] == "string"

    def test_describe_dict_directly(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { scores: {string: float64} }')
        result = _run(executor, 'describe "{string: float64}"')
        type_row = next(r for r in result.rows if r["property"] == "(type)")
        assert type_row["type"] == "Dictionary"
        key_row = next(r for r in result.rows if r["property"] == "(key_type)")
        assert key_row["type"] == "string"
        val_row = next(r for r in result.rows if r["property"] == "(value_type)")
        assert val_row["type"] == "float64"


# ==============================================================
# Show types tests
# ==============================================================

class TestShowTypes:
    """Test show types includes set/dict types."""

    def test_show_types_includes_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { name: string, tags: {string} }')
        # Create the .bin file so Student shows up in show types
        storage.get_table("Student")
        result = _run(executor, 'show types')
        type_names = [r["type"] for r in result.rows]
        assert "Student" in type_names
        assert "{string}" in type_names

    def test_show_types_includes_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { name: string, scores: {string: float64} }')
        # Create .bin files so types show up in show types
        storage.get_table("Student")
        storage.get_table("Dict_string_float64")
        result = _run(executor, 'show types')
        type_names = [r["type"] for r in result.rows]
        assert "Student" in type_names
        assert "{string: float64}" in type_names
        # Entry composite is visible when it has a .bin file
        assert "Dict_string_float64" in type_names

    def test_show_types_set_kind(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { tags: {string} }')
        result = _run(executor, 'show types')
        set_row = next(r for r in result.rows if r["type"] == "{string}")
        assert set_row["kind"] == "Set"

    def test_show_types_dict_kind(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { scores: {string: float64} }')
        result = _run(executor, 'show types')
        dict_row = next(r for r in result.rows if r["type"] == "{string: float64}")
        assert dict_row["kind"] == "Dictionary"


# ==============================================================
# Graph tests
# ==============================================================

class TestGraph:
    """Test graph includes set/dict edges."""

    def test_graph_set_edges(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { name: string, tags: {string} }')
        result = _run(executor, 'graph')
        edges = result.rows
        # Student → {string} edge
        assert any(e["source"] == "Student" and e["target"] == "{string}" for e in edges)
        # {string} → string edge
        assert any(e["source"] == "{string}" and e["target"] == "string" for e in edges)

    def test_graph_dict_edges(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { scores: {string: float64} }')
        result = _run(executor, 'graph')
        edges = result.rows
        # Student → {string: float64} edge
        assert any(e["source"] == "Student" and e["target"] == "{string: float64}" for e in edges)

    def test_graph_dict_type_edges(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        _run(executor, 'type Student { scores: {string: float64} }')
        result = _run(executor, 'graph')
        edges = result.rows
        # {string: float64} → string edge
        assert any(e["source"] == "{string: float64}" and e["target"] == "string" and e["field"] == "{key}" for e in edges)
        # {string: float64} → float64 edge
        assert any(e["source"] == "{string: float64}" and e["target"] == "float64" and e["field"] == "{value}" for e in edges)
        # {string: float64} → Dict_string_float64 edge
        assert any(e["source"] == "{string: float64}" and e["target"] == "Dict_string_float64" and e["field"] == "(entry)" for e in edges)


# ==============================================================
# Phase 2: Parser literal tests
# ==============================================================

class TestParserLiterals:
    """Test parsing of set/dict literal syntax."""

    def test_parse_set_literal(self):
        parser = QueryParser()
        stmts = parser.parse_program('create X(tags={"a", "b", "c"})')
        fv = stmts[0].fields[0]
        assert isinstance(fv.value, SetLiteral)
        assert fv.value.elements == ["a", "b", "c"]

    def test_parse_dict_literal(self):
        parser = QueryParser()
        stmts = parser.parse_program('create X(scores={"math": 92.5, "eng": 88.0})')
        fv = stmts[0].fields[0]
        assert isinstance(fv.value, DictLiteral)
        assert len(fv.value.entries) == 2
        assert fv.value.entries[0].key == "math"
        assert fv.value.entries[0].value == 92.5

    def test_parse_empty_braces(self):
        parser = QueryParser()
        stmts = parser.parse_program('create X(data={})')
        fv = stmts[0].fields[0]
        assert isinstance(fv.value, EmptyBraces)

    def test_parse_empty_set(self):
        parser = QueryParser()
        stmts = parser.parse_program('create X(data={,})')
        fv = stmts[0].fields[0]
        assert isinstance(fv.value, SetLiteral)
        assert fv.value.elements == []

    def test_parse_empty_dict(self):
        parser = QueryParser()
        stmts = parser.parse_program('create X(data={:})')
        fv = stmts[0].fields[0]
        assert isinstance(fv.value, DictLiteral)
        assert fv.value.entries == []

    def test_parse_set_with_integers(self):
        parser = QueryParser()
        stmts = parser.parse_program('create X(nums={1, 2, 3})')
        fv = stmts[0].fields[0]
        assert isinstance(fv.value, SetLiteral)
        assert fv.value.elements == [1, 2, 3]

    def test_parse_dict_with_int_keys(self):
        parser = QueryParser()
        stmts = parser.parse_program('create X(data={1: "one", 2: "two"})')
        fv = stmts[0].fields[0]
        assert isinstance(fv.value, DictLiteral)
        assert fv.value.entries[0].key == 1
        assert fv.value.entries[0].value == "one"

    def test_parse_set_trailing_comma(self):
        parser = QueryParser()
        stmts = parser.parse_program('create X(tags={"a", "b",})')
        fv = stmts[0].fields[0]
        assert isinstance(fv.value, SetLiteral)
        assert fv.value.elements == ["a", "b"]

    def test_parse_dict_trailing_comma(self):
        parser = QueryParser()
        stmts = parser.parse_program('create X(data={"a": 1, "b": 2,})')
        fv = stmts[0].fields[0]
        assert isinstance(fv.value, DictLiteral)
        assert len(fv.value.entries) == 2

    def test_parse_set_single_element(self):
        parser = QueryParser()
        stmts = parser.parse_program('create X(tags={"only"})')
        fv = stmts[0].fields[0]
        assert isinstance(fv.value, SetLiteral)
        assert fv.value.elements == ["only"]

    def test_parse_set_negative_numbers(self):
        parser = QueryParser()
        stmts = parser.parse_program('create X(nums={-1, -2, 3})')
        fv = stmts[0].fields[0]
        assert isinstance(fv.value, SetLiteral)
        assert fv.value.elements == [-1, -2, 3]


# ==============================================================
# Phase 2: Instance creation tests
# ==============================================================

class TestInstanceCreation:
    """Test creating instances with set and dict fields."""

    def test_create_with_string_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { name: string, tags: {string} }
            create Student(name="Alice", tags={"math", "science"})
        ''')
        assert results[-1].index == 0

    def test_create_with_int_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Data { nums: {int32} }
            create Data(nums={10, 20, 30})
        ''')
        assert results[-1].index == 0

    def test_create_with_float_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Measures { values: {float64} }
            create Measures(values={1.5, 2.5, 3.5})
        ''')
        assert results[-1].index == 0

    def test_create_with_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { name: string, scores: {string: float64} }
            create Student(name="Alice", scores={"midterm": 92.5, "final": 88.0})
        ''')
        assert results[-1].index == 0

    def test_create_with_int_key_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Lookup { data: {int32: string} }
            create Lookup(data={1: "one", 2: "two"})
        ''')
        assert results[-1].index == 0

    def test_create_with_empty_set_comma(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {string} }
            create X(tags={,})
        ''')
        assert results[-1].index == 0

    def test_create_with_empty_dict_colon(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={:})
        ''')
        assert results[-1].index == 0

    def test_create_with_empty_braces_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {string} }
            create X(tags={})
        ''')
        assert results[-1].index == 0

    def test_create_with_empty_braces_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={})
        ''')
        assert results[-1].index == 0

    def test_create_null_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {string} }
            create X()
        ''')
        assert results[-1].index == 0

    def test_create_null_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X()
        ''')
        assert results[-1].index == 0

    def test_duplicate_set_element_error(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={1, 2, 1})
        ''')
        assert "Duplicate element" in results[-1].message

    def test_duplicate_dict_key_error(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0, "a": 2.0})
        ''')
        assert "Duplicate key" in results[-1].message

    def test_multiple_instances(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { name: string, tags: {string} }
            create Student(name="Alice", tags={"math", "science"})
            create Student(name="Bob", tags={"art", "music", "drama"})
        ''')
        assert results[-1].index == 1


# ==============================================================
# Phase 2: SELECT tests
# ==============================================================

class TestSelect:
    """Test selecting records with set and dict fields."""

    def test_select_string_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { name: string, tags: {string} }
            create Student(name="Alice", tags={"math", "science"})
            from Student select *
        ''')
        rows = results[-1].rows
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
        assert isinstance(rows[0]["tags"], SetValue)
        assert list(rows[0]["tags"]) == ["math", "science"]

    def test_select_int_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Data { nums: {int32} }
            create Data(nums={10, 20, 30})
            from Data select *
        ''')
        rows = results[-1].rows
        assert isinstance(rows[0]["nums"], SetValue)
        assert list(rows[0]["nums"]) == [10, 20, 30]

    def test_select_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { name: string, scores: {string: float64} }
            create Student(name="Alice", scores={"midterm": 92.5, "final": 88.0})
            from Student select *
        ''')
        rows = results[-1].rows
        assert rows[0]["scores"] == {"midterm": 92.5, "final": 88.0}

    def test_select_empty_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {string} }
            create X(tags={,})
            from X select *
        ''')
        rows = results[-1].rows
        assert isinstance(rows[0]["tags"], SetValue)
        assert len(rows[0]["tags"]) == 0

    def test_select_empty_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={:})
            from X select *
        ''')
        rows = results[-1].rows
        assert rows[0]["scores"] == {}

    def test_select_null_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {string} }
            create X()
            from X select *
        ''')
        rows = results[-1].rows
        assert rows[0]["tags"] is None

    def test_select_null_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X()
            from X select *
        ''')
        rows = results[-1].rows
        assert rows[0]["scores"] is None

    def test_select_dict_with_int_keys(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Lookup { data: {int32: string} }
            create Lookup(data={1: "one", 2: "two"})
            from Lookup select *
        ''')
        rows = results[-1].rows
        assert rows[0]["data"] == {1: "one", 2: "two"}

    def test_select_multiple_records(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { name: string, tags: {string} }
            create Student(name="Alice", tags={"math"})
            create Student(name="Bob", tags={"art", "music"})
            from Student select *
        ''')
        rows = results[-1].rows
        assert len(rows) == 2
        assert list(rows[0]["tags"]) == ["math"]
        assert list(rows[1]["tags"]) == ["art", "music"]

    def test_select_mixed_fields(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { name: string, age: uint8, tags: {string}, scores: {string: float64} }
            create Student(name="Alice", age=20, tags={"math"}, scores={"midterm": 92.5})
            from Student select *
        ''')
        rows = results[-1].rows
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 20
        assert isinstance(rows[0]["tags"], SetValue)
        assert isinstance(rows[0]["scores"], dict)


# ==============================================================
# Phase 2: UPDATE tests
# ==============================================================

class TestUpdate:
    """Test updating records with set and dict fields."""

    def test_update_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { name: string, tags: {string} }
            $s = create Student(name="Alice", tags={"math"})
            update $s set tags={"math", "science", "art"}
            from Student select *
        ''')
        rows = results[-1].rows
        assert list(rows[0]["tags"]) == ["math", "science", "art"]

    def test_update_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { name: string, scores: {string: float64} }
            $s = create Student(name="Alice", scores={"midterm": 92.5})
            update $s set scores={"midterm": 95.0, "final": 88.0}
            from Student select *
        ''')
        rows = results[-1].rows
        assert rows[0]["scores"] == {"midterm": 95.0, "final": 88.0}

    def test_update_set_to_empty(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {string} }
            $x = create X(tags={"a", "b"})
            update $x set tags={,}
            from X select *
        ''')
        rows = results[-1].rows
        assert len(rows[0]["tags"]) == 0

    def test_update_dict_to_empty(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            $x = create X(scores={"a": 1.0})
            update $x set scores={:}
            from X select *
        ''')
        rows = results[-1].rows
        assert rows[0]["scores"] == {}

    def test_update_set_to_null(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {string} }
            $x = create X(tags={"a"})
            update $x set tags=null
            from X select *
        ''')
        rows = results[-1].rows
        assert rows[0]["tags"] is None

    def test_update_dict_to_null(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            $x = create X(scores={"a": 1.0})
            update $x set scores=null
            from X select *
        ''')
        rows = results[-1].rows
        assert rows[0]["scores"] is None

    def test_update_set_duplicate_error(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            $x = create X(tags={1})
            update $x set tags={1, 2, 1}
        ''')
        assert "Duplicate element" in results[-1].message

    def test_update_dict_duplicate_key_error(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            $x = create X(scores={"a": 1.0})
            update $x set scores={"a": 1.0, "a": 2.0}
        ''')
        assert "Duplicate key" in results[-1].message

    def test_bulk_update_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { label: string, tags: {int32} }
            create X(label="a", tags={1})
            create X(label="b", tags={2})
            update X set tags={99}
            from X select *
        ''')
        rows = results[-1].rows
        assert list(rows[0]["tags"]) == [99]
        assert list(rows[1]["tags"]) == [99]


# ==============================================================
# Phase 2: Dump tests
# ==============================================================

class TestDump:
    """Test dump format for set and dict fields."""

    def test_dump_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { name: string, tags: {string} }
            create Student(name="Alice", tags={"math", "science"})
            dump
        ''')
        dump = results[-1]
        assert isinstance(dump, DumpResult)
        assert '{"math", "science"}' in dump.script

    def test_dump_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { scores: {string: float64} }
            create Student(scores={"midterm": 92.5, "final": 88.0})
            dump
        ''')
        dump = results[-1]
        assert '{"midterm": 92.5, "final": 88.0}' in dump.script

    def test_dump_empty_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {string} }
            create X(tags={,})
            dump
        ''')
        dump = results[-1]
        assert '{,}' in dump.script

    def test_dump_empty_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={:})
            dump
        ''')
        dump = results[-1]
        assert '{:}' in dump.script

    def test_dump_int_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Data { nums: {int32} }
            create Data(nums={10, 20, 30})
            dump
        ''')
        dump = results[-1]
        assert '{10, 20, 30}' in dump.script

    def test_dump_type_definition_includes_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { tags: {string} }
            dump
        ''')
        dump = results[-1]
        assert 'tags: {string}' in dump.script

    def test_dump_type_definition_includes_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { scores: {string: float64} }
            dump
        ''')
        dump = results[-1]
        assert 'scores: {string: float64}' in dump.script

    def test_dump_no_dict_entry_type(self, tmp_db):
        """Dict entry types should not appear in dump output."""
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { scores: {string: float64} }
            create Student(scores={"a": 1.0})
            dump
        ''')
        dump = results[-1]
        assert 'Dict_string_float64' not in dump.script

    def test_dump_roundtrip(self, tmp_db):
        """Dump output should be re-parseable and produce the same data."""
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type Student { name: string, tags: {string}, scores: {string: float64} }
            create Student(name="Alice", tags={"math", "science"}, scores={"midterm": 92.5, "final": 88.0})
            dump
        ''')
        dump_script = results[-1].script

        # Re-execute in fresh db
        import tempfile
        d2 = Path(tempfile.mkdtemp())
        reg2 = TypeRegistry()
        storage2 = StorageManager(d2, reg2)
        executor2 = QueryExecutor(storage2, reg2)
        results2 = _run_all(executor2, dump_script)

        # Select and verify
        result = _run(executor2, 'from Student select *')
        rows = result.rows
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
        assert list(rows[0]["tags"]) == ["math", "science"]
        assert rows[0]["scores"] == {"midterm": 92.5, "final": 88.0}

        import shutil
        shutil.rmtree(d2)

    def test_dump_null_set_dict(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {string}, scores: {string: float64} }
            create X()
            dump
        ''')
        dump = results[-1]
        # NULL fields should be omitted (default behavior)
        assert 'create X()' in dump.script or 'tags=null' in dump.script


# ==============================================================
# Phase 3: Set projection method tests
# ==============================================================

class TestSetProjectionMethods:
    """Test set-specific projection methods (read-only, in SELECT)."""

    def test_set_length(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={10, 20, 30})
            from X select tags.length()
        ''')
        rows = results[-1].rows
        assert rows[0]["tags.length()"] == 3

    def test_set_isEmpty_false(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={1})
            from X select tags.isEmpty()
        ''')
        assert results[-1].rows[0]["tags.isEmpty()"] == False

    def test_set_isEmpty_true(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={,})
            from X select tags.isEmpty()
        ''')
        assert results[-1].rows[0]["tags.isEmpty()"] == True

    def test_set_contains_true(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={10, 20, 30})
            from X select tags.contains(20)
        ''')
        assert results[-1].rows[0]["tags.contains(20)"] == True

    def test_set_contains_false(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={10, 20, 30})
            from X select tags.contains(99)
        ''')
        assert results[-1].rows[0]["tags.contains(99)"] == False

    def test_set_add_new(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={1, 2})
            from X select tags.add(3)
        ''')
        val = results[-1].rows[0]["tags.add(3)"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 2, 3]

    def test_set_add_duplicate(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={1, 2})
            from X select tags.add(2)
        ''')
        val = results[-1].rows[0]["tags.add(2)"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 2]

    def test_set_union(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={1, 2, 3})
            from X select tags.union({3, 4, 5})
        ''')
        val = results[-1].rows[0]["tags.union({3, 4, 5})"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 2, 3, 4, 5]

    def test_set_intersect(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={1, 2, 3})
            from X select tags.intersect({2, 3, 4})
        ''')
        val = results[-1].rows[0]["tags.intersect({2, 3, 4})"]
        assert isinstance(val, SetValue)
        assert list(val) == [2, 3]

    def test_set_difference(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={1, 2, 3})
            from X select tags.difference({2, 4})
        ''')
        val = results[-1].rows[0]["tags.difference({2, 4})"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 3]

    def test_set_symmetric_difference(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={1, 2, 3})
            from X select tags.symmetric_difference({2, 3, 4})
        ''')
        val = results[-1].rows[0]["tags.symmetric_difference({2, 3, 4})"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 4]

    def test_set_sort_preserves_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={3, 1, 2})
            from X select tags.sort()
        ''')
        val = results[-1].rows[0]["tags.sort()"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 2, 3]

    def test_set_reverse_preserves_set(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={1, 2, 3})
            from X select tags.reverse()
        ''')
        val = results[-1].rows[0]["tags.reverse()"]
        assert isinstance(val, SetValue)
        assert list(val) == [3, 2, 1]

    def test_set_chaining(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            create X(tags={3, 1, 2})
            from X select tags.add(5).sort()
        ''')
        val = results[-1].rows[0]["tags.add(5).sort()"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 2, 3, 5]


# ==============================================================
# Phase 3: Dict projection method tests
# ==============================================================

class TestDictProjectionMethods:
    """Test dict-specific projection methods (read-only, in SELECT)."""

    def test_dict_length(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0, "b": 2.0})
            from X select scores.length()
        ''')
        assert results[-1].rows[0]["scores.length()"] == 2

    def test_dict_isEmpty_false(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0})
            from X select scores.isEmpty()
        ''')
        assert results[-1].rows[0]["scores.isEmpty()"] == False

    def test_dict_isEmpty_true(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={:})
            from X select scores.isEmpty()
        ''')
        assert results[-1].rows[0]["scores.isEmpty()"] == True

    def test_dict_contains_key(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0, "b": 2.0})
            from X select scores.contains("a")
        ''')
        assert results[-1].rows[0]['scores.contains("a")'] == True

    def test_dict_contains_missing(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0})
            from X select scores.contains("z")
        ''')
        assert results[-1].rows[0]['scores.contains("z")'] == False

    def test_dict_hasKey(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0, "b": 2.0})
            from X select scores.hasKey("a")
        ''')
        assert results[-1].rows[0]['scores.hasKey("a")'] == True

    def test_dict_hasKey_missing(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0})
            from X select scores.hasKey("z")
        ''')
        assert results[-1].rows[0]['scores.hasKey("z")'] == False

    def test_dict_keys(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0, "b": 2.0})
            from X select scores.keys()
        ''')
        val = results[-1].rows[0]["scores.keys()"]
        assert isinstance(val, SetValue)
        assert list(val) == ["a", "b"]

    def test_dict_values(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0, "b": 2.0})
            from X select scores.values()
        ''')
        val = results[-1].rows[0]["scores.values()"]
        assert val == [1.0, 2.0]

    def test_dict_entries(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0, "b": 2.0})
            from X select scores.entries()
        ''')
        val = results[-1].rows[0]["scores.entries()"]
        assert val == [{"key": "a", "value": 1.0}, {"key": "b", "value": 2.0}]

    def test_dict_remove_projection(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0, "b": 2.0})
            from X select scores.remove("a")
        ''')
        val = results[-1].rows[0]['scores.remove("a")']
        assert val == {"b": 2.0}

    def test_dict_keys_length_chain(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0, "b": 2.0, "c": 3.0})
            from X select scores.keys().length()
        ''')
        assert results[-1].rows[0]["scores.keys().length()"] == 3


# ==============================================================
# Phase 3: Dict bracket access tests
# ==============================================================

class TestDictBracketAccess:
    """Test dict bracket access scores["midterm"]."""

    def test_dict_bracket_access(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"midterm": 92.5, "final": 88.0})
            from X select scores["midterm"]
        ''')
        assert results[-1].rows[0]['scores["midterm"]'] == 92.5

    def test_dict_bracket_missing_key(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"midterm": 92.5})
            from X select scores["nonexistent"]
        ''')
        assert results[-1].rows[0]['scores["nonexistent"]'] is None

    def test_dict_bracket_column_name(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"midterm": 92.5})
            from X select scores["midterm"]
        ''')
        assert 'scores["midterm"]' in results[-1].columns


# ==============================================================
# Phase 3: Set mutation tests (UPDATE)
# ==============================================================

class TestSetMutations:
    """Test set mutations via UPDATE statements."""

    def test_set_add_mutation(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            $x = create X(tags={1, 2})
            update $x set tags.add(3)
            from X select *
        ''')
        val = results[-1].rows[0]["tags"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 2, 3]

    def test_set_add_duplicate_noop(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            $x = create X(tags={1, 2})
            update $x set tags.add(2)
            from X select *
        ''')
        val = results[-1].rows[0]["tags"]
        assert list(val) == [1, 2]

    def test_set_union_mutation(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            $x = create X(tags={1, 2})
            update $x set tags.union({2, 3, 4})
            from X select *
        ''')
        val = results[-1].rows[0]["tags"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 2, 3, 4]

    def test_set_intersect_mutation(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            $x = create X(tags={1, 2, 3})
            update $x set tags.intersect({2, 3, 4})
            from X select *
        ''')
        val = results[-1].rows[0]["tags"]
        assert isinstance(val, SetValue)
        assert list(val) == [2, 3]

    def test_set_difference_mutation(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            $x = create X(tags={1, 2, 3})
            update $x set tags.difference({2, 4})
            from X select *
        ''')
        val = results[-1].rows[0]["tags"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 3]

    def test_set_symmetric_difference_mutation(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            $x = create X(tags={1, 2, 3})
            update $x set tags.symmetric_difference({2, 3, 4})
            from X select *
        ''')
        val = results[-1].rows[0]["tags"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 4]

    def test_set_add_to_null(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            $x = create X()
            update $x set tags.add(5)
            from X select *
        ''')
        val = results[-1].rows[0]["tags"]
        assert isinstance(val, SetValue)
        assert list(val) == [5]

    def test_set_add_string(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {string} }
            $x = create X(tags={"hello"})
            update $x set tags.add("world")
            from X select *
        ''')
        val = results[-1].rows[0]["tags"]
        assert isinstance(val, SetValue)
        assert list(val) == ["hello", "world"]


# ==============================================================
# Phase 3: Dict mutation tests (UPDATE)
# ==============================================================

class TestDictMutations:
    """Test dict mutations via UPDATE statements."""

    def test_dict_remove_mutation(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            $x = create X(scores={"a": 1.0, "b": 2.0})
            update $x set scores.remove("a")
            from X select *
        ''')
        assert results[-1].rows[0]["scores"] == {"b": 2.0}

    def test_dict_remove_missing_noop(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            $x = create X(scores={"a": 1.0})
            update $x set scores.remove("z")
            from X select *
        ''')
        assert results[-1].rows[0]["scores"] == {"a": 1.0}


# ==============================================================
# Phase 3: WHERE condition tests
# ==============================================================

class TestWhereConditions:
    """Test set/dict methods in WHERE clauses."""

    def test_where_set_contains(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { name: string, tags: {int32} }
            create X(name="a", tags={1, 2, 3})
            create X(name="b", tags={4, 5, 6})
            from X select name where tags.contains(2)
        ''')
        assert len(results[-1].rows) == 1
        assert results[-1].rows[0]["name"] == "a"

    def test_where_dict_hasKey(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { name: string, scores: {string: float64} }
            create X(name="a", scores={"math": 90.0})
            create X(name="b", scores={"eng": 80.0})
            from X select name where scores.hasKey("math")
        ''')
        assert len(results[-1].rows) == 1
        assert results[-1].rows[0]["name"] == "a"

    def test_where_set_length(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { name: string, tags: {int32} }
            create X(name="a", tags={1, 2, 3})
            create X(name="b", tags={1})
            from X select name where tags.length() > 2
        ''')
        assert len(results[-1].rows) == 1
        assert results[-1].rows[0]["name"] == "a"

    def test_where_dict_length(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { name: string, scores: {string: float64} }
            create X(name="a", scores={"math": 90.0, "eng": 85.0})
            create X(name="b", scores={"math": 90.0})
            from X select name where scores.length() = 2
        ''')
        assert len(results[-1].rows) == 1
        assert results[-1].rows[0]["name"] == "a"


# ==============================================================
# Phase 3: Chain mutation tests
# ==============================================================

class TestChainMutations:
    """Test chained operations on sets and dicts."""

    def test_set_chain_add_sort(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { tags: {int32} }
            $x = create X(tags={3, 1})
            update $x set tags = tags.add(2).sort()
            from X select *
        ''')
        val = results[-1].rows[0]["tags"]
        assert isinstance(val, SetValue)
        assert list(val) == [1, 2, 3]

    def test_dict_chain_remove_in_select(self, tmp_db):
        executor, db_dir, registry, storage = tmp_db
        results = _run_all(executor, '''
            type X { scores: {string: float64} }
            create X(scores={"a": 1.0, "b": 2.0, "c": 3.0})
            from X select scores.remove("a").length()
        ''')
        assert results[-1].rows[0]['scores.remove("a").length()'] == 2
