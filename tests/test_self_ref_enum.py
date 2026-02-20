"""Tests for self-referential enum support."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from typed_tables.query_executor import QueryExecutor, QueryResult, CreateResult, DumpResult
from typed_tables.storage import StorageManager
from typed_tables.types import (
    EnumTypeDefinition,
    TypeRegistry,
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


def _exec(executor, query: str):
    """Parse and execute a TTQ query, return the result."""
    from typed_tables.parsing.query_parser import QueryParser
    parser = QueryParser()
    try:
        stmts = parser.parse(query)
        if not isinstance(stmts, list):
            stmts = [stmts]
    except SyntaxError:
        stmts = parser.parse_program(query)
    results = []
    for stmt in stmts:
        results.append(executor.execute(stmt))
    return results[-1] if len(results) == 1 else results


# ──────────────────────────────────────────────────
# Type definition tests
# ──────────────────────────────────────────────────

class TestSelfReferentialEnumDefinition:
    """Test that self-referential enums can be defined."""

    def test_enum_with_self_ref_array(self, tmp_db):
        """enum JV { arr(elements: JV[]) } should work."""
        executor, db_dir, registry, storage = tmp_db
        result = _exec(executor, 'enum JV { null_val, arr(elements: JV[]) }')
        assert isinstance(result, CreateResult)
        assert "Created enum" in result.message
        td = registry.get("JV")
        assert isinstance(td, EnumTypeDefinition)
        assert len(td.variants) == 2

    def test_enum_with_self_ref_dict(self, tmp_db):
        """enum JV { obj(entries: {string: JV}) } should work."""
        executor, db_dir, registry, storage = tmp_db
        result = _exec(executor, 'enum JV { null_val, obj(entries: {string: JV}) }')
        assert isinstance(result, CreateResult)
        assert "Created enum" in result.message
        td = registry.get("JV")
        assert isinstance(td, EnumTypeDefinition)
        assert len(td.variants) == 2

    def test_enum_with_direct_self_ref(self, tmp_db):
        """enum Tree { leaf, node(left: Tree, right: Tree) } should work."""
        executor, db_dir, registry, storage = tmp_db
        result = _exec(executor, 'enum Tree { leaf, node(left: Tree, right: Tree) }')
        assert isinstance(result, CreateResult)
        assert "Created enum" in result.message
        td = registry.get("Tree")
        assert isinstance(td, EnumTypeDefinition)
        assert len(td.variants) == 2
        node_variant = [v for v in td.variants if v.name == "node"][0]
        assert len(node_variant.fields) == 2

    def test_full_json_value_enum(self, tmp_db):
        """The motivating use case: a full JSON value enum."""
        executor, db_dir, registry, storage = tmp_db
        result = _exec(executor, '''
            enum JsonValue {
                null_val,
                bool_val(value: boolean),
                number(value: float64),
                str_val(value: string),
                array(elements: JsonValue[]),
                object(entries: {string: JsonValue})
            }
        ''')
        assert isinstance(result, CreateResult)
        assert "Created enum" in result.message
        assert "6 variant" in result.message

    def test_enum_already_exists_error(self, tmp_db):
        """Defining the same enum twice should fail."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, 'enum Color { red, green, blue }')
        result = _exec(executor, 'enum Color { a, b }')
        assert "already exists" in result.message


# ──────────────────────────────────────────────────
# Forward declaration + enum tests
# ──────────────────────────────────────────────────

class TestForwardDeclaredEnum:
    """Test forward declaration followed by enum definition."""

    def test_forward_then_enum(self, tmp_db):
        """forward enum JV; type Doc { root: JV }; enum JV { ... }"""
        executor, db_dir, registry, storage = tmp_db
        results = _exec(executor, '''
            forward enum JV
            type Doc { root: JV }
            enum JV { null_val, num(value: float64), arr(elements: JV[]) }
        ''')
        # All three should succeed
        td = registry.get("JV")
        assert isinstance(td, EnumTypeDefinition)
        assert len(td.variants) == 3
        doc_td = registry.get("Doc")
        assert doc_td is not None
        # Doc's root field should reference the JV enum
        root_field = [f for f in doc_td.fields if f.name == "root"][0]
        assert root_field.type_def is td

    def test_forward_enum_metadata_roundtrip(self, tmp_db):
        """Forward-declared enum should save/load metadata correctly."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            forward enum JV
            type Doc { root: JV }
            enum JV { null_val, num(value: float64) }
        ''')
        storage.save_metadata()

        # Reload from metadata
        from typed_tables.dump import load_registry_from_metadata
        registry2 = load_registry_from_metadata(db_dir)
        jv2 = registry2.get("JV")
        assert isinstance(jv2, EnumTypeDefinition)
        assert len(jv2.variants) == 2


# ──────────────────────────────────────────────────
# Instance creation tests
# ──────────────────────────────────────────────────

class TestSelfRefEnumInstances:
    """Test creating instances with self-referential enum values."""

    def test_create_bare_variant(self, tmp_db):
        """Create an instance using a bare variant of a self-ref enum."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum Tree { leaf, node(left: Tree, right: Tree) }
            type Forest { tree: Tree }
        ''')
        result = _exec(executor, 'create Forest(tree=.leaf)')
        assert isinstance(result, CreateResult)
        assert "Created" in result.message

    def test_create_with_nested_self_ref(self, tmp_db):
        """Create a tree with nested self-referential enum values."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum Tree { leaf, node(left: Tree, right: Tree) }
            type Forest { tree: Tree }
        ''')
        result = _exec(executor, '''
            create Forest(tree=.node(left=.leaf, right=.node(left=.leaf, right=.leaf)))
        ''')
        assert isinstance(result, CreateResult)
        assert "Created" in result.message

    def test_create_json_value_instances(self, tmp_db):
        """Create instances of a JSON value enum."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum JsonValue {
                null_val,
                bool_val(value: boolean),
                number(value: float64),
                str_val(value: string),
                array(elements: JsonValue[]),
                object(entries: {string: JsonValue})
            }
            type JsonDoc { name: string, root: JsonValue }
        ''')
        # Create simple values
        result = _exec(executor, 'create JsonDoc(name="null", root=.null_val)')
        assert isinstance(result, CreateResult)
        assert "Created" in result.message
        result = _exec(executor, 'create JsonDoc(name="bool", root=.bool_val(value=true))')
        assert "Created" in result.message
        result = _exec(executor, 'create JsonDoc(name="num", root=.number(value=42.0))')
        assert "Created" in result.message
        result = _exec(executor, 'create JsonDoc(name="str", root=.str_val(value="hello"))')
        assert "Created" in result.message


# ──────────────────────────────────────────────────
# Selection / query tests
# ──────────────────────────────────────────────────

class TestSelfRefEnumQueries:
    """Test selecting data from self-referential enum types."""

    def test_select_from_type_with_self_ref_enum(self, tmp_db):
        """Select * from a type containing a self-ref enum field."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum Tree { leaf, node(left: Tree, right: Tree) }
            type Forest { name: string, tree: Tree }
            create Forest(name="small", tree=.leaf)
            create Forest(name="big", tree=.node(left=.leaf, right=.leaf))
        ''')
        result = _exec(executor, 'from Forest select *')
        assert isinstance(result, QueryResult)
        assert len(result.rows) == 2

    def test_select_from_enum_overview(self, tmp_db):
        """from Tree select * should show variant overview."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum Tree { leaf, node(left: Tree, right: Tree) }
            type Forest { tree: Tree }
            create Forest(tree=.leaf)
            create Forest(tree=.node(left=.leaf, right=.leaf))
        ''')
        result = _exec(executor, 'from Tree select *')
        assert isinstance(result, QueryResult)

    def test_select_from_enum_variant(self, tmp_db):
        """from Tree.node select * should show variant fields."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum Tree { leaf, node(left: Tree, right: Tree) }
            type Forest { tree: Tree }
            create Forest(tree=.node(left=.leaf, right=.leaf))
        ''')
        result = _exec(executor, 'from Tree.node select *')
        assert isinstance(result, QueryResult)
        assert len(result.rows) >= 1


# ──────────────────────────────────────────────────
# Dump roundtrip tests
# ──────────────────────────────────────────────────

class TestSelfRefEnumDump:
    """Test dump and restore of self-referential enum data."""

    def test_dump_self_ref_enum_type(self, tmp_db):
        """dump should produce valid TTQ for self-ref enum types."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum Tree { leaf, node(left: Tree, right: Tree) }
            type Forest { name: string, tree: Tree }
            create Forest(name="t", tree=.node(left=.leaf, right=.leaf))
        ''')
        result = _exec(executor, 'dump')
        assert isinstance(result, DumpResult)
        dump_text = result.script
        assert "enum Tree" in dump_text
        assert "node" in dump_text

    def test_dump_roundtrip(self, tmp_db):
        """Dumped TTQ should be re-executable."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum Tree { leaf, node(left: Tree, right: Tree) }
            type Forest { name: string, tree: Tree }
            create Forest(name="t1", tree=.leaf)
            create Forest(name="t2", tree=.node(left=.leaf, right=.leaf))
        ''')
        result = _exec(executor, 'dump')
        dump_text = result.script

        # Re-execute in a fresh database
        db_dir2 = tmp_db[1].parent / "test_db2"
        db_dir2.mkdir()
        registry2 = TypeRegistry()
        storage2 = StorageManager(db_dir2, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        _exec(executor2, dump_text)
        result2 = _exec(executor2, 'from Forest select *')
        assert isinstance(result2, QueryResult)
        assert len(result2.rows) == 2

    def test_dump_json_value_roundtrip(self, tmp_db):
        """JSON value enum dump roundtrip with simple values."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum JsonValue {
                null_val,
                bool_val(value: boolean),
                number(value: float64),
                str_val(value: string),
                array(elements: JsonValue[]),
                object(entries: {string: JsonValue})
            }
            type JsonDoc { name: string, root: JsonValue }
            create JsonDoc(name="test", root=.number(value=42.0))
            create JsonDoc(name="bool", root=.bool_val(value=true))
        ''')
        result = _exec(executor, 'dump')
        dump_text = result.script

        # Re-execute
        db_dir2 = tmp_db[1].parent / "test_db2"
        db_dir2.mkdir()
        registry2 = TypeRegistry()
        storage2 = StorageManager(db_dir2, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        _exec(executor2, dump_text)
        result2 = _exec(executor2, 'from JsonDoc select *')
        assert isinstance(result2, QueryResult)
        assert len(result2.rows) == 2


# ──────────────────────────────────────────────────
# Describe tests
# ──────────────────────────────────────────────────

class TestSelfRefEnumDescribe:
    """Test describe on self-referential enums."""

    def test_describe_self_ref_enum(self, tmp_db):
        """describe Tree should work for a self-ref enum."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, 'enum Tree { leaf, node(left: Tree, right: Tree) }')
        result = _exec(executor, 'describe Tree')
        assert isinstance(result, QueryResult)

    def test_describe_self_ref_variant(self, tmp_db):
        """describe Tree.node should show the variant fields."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, 'enum Tree { leaf, node(left: Tree, right: Tree) }')
        result = _exec(executor, 'describe Tree.node')
        assert isinstance(result, QueryResult)
