"""Tests for show references and dump graph features."""

import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.parsing.query_parser import (
    DumpGraphQuery,
    QueryParser,
    ShowReferencesQuery,
)
from typed_tables.query_executor import DumpResult, QueryExecutor, QueryResult
from typed_tables.storage import StorageManager
from typed_tables.types import TypeRegistry


@pytest.fixture
def tmp_data_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def executor(tmp_data_dir):
    registry = TypeRegistry()
    storage = StorageManager(tmp_data_dir, registry)
    return QueryExecutor(storage, registry)


@pytest.fixture
def parser():
    p = QueryParser()
    p.build(debug=False, write_tables=False)
    return p


def _run(executor, parser, query_str):
    queries = parser.parse_program(query_str)
    result = None
    for q in queries:
        result = executor.execute(q)
    return result


def _edges(result):
    """Extract edges as set of (source, kind, field, target) tuples."""
    return {(e["source"], e["kind"], e["field"], e["target"]) for e in result.rows}


# ---- Parser tests ----


class TestParseShowReferences:
    def test_parse_show_references(self, parser):
        q = parser.parse("show references")
        assert isinstance(q, ShowReferencesQuery)
        assert q.type_name is None

    def test_parse_show_references_type(self, parser):
        q = parser.parse("show references Person")
        assert isinstance(q, ShowReferencesQuery)
        assert q.type_name == "Person"

    def test_parse_show_references_sort_by(self, parser):
        q = parser.parse("show references sort by source")
        assert isinstance(q, ShowReferencesQuery)
        assert q.sort_by == ["source"]

    def test_parse_show_references_type_sort_by(self, parser):
        q = parser.parse("show references Person sort by kind, field")
        assert isinstance(q, ShowReferencesQuery)
        assert q.type_name == "Person"
        assert q.sort_by == ["kind", "field"]

    def test_parse_dump_graph(self, parser):
        q = parser.parse("dump graph")
        assert isinstance(q, DumpGraphQuery)
        assert q.output_file is None

    def test_parse_dump_graph_to(self, parser):
        q = parser.parse('dump graph to "types.dot"')
        assert isinstance(q, DumpGraphQuery)
        assert q.output_file == "types.dot"


# ---- Show references tests ----


class TestShowReferences:
    def _setup_schema(self, executor, parser):
        """Set up a schema with aliases, enums, interfaces, and composites."""
        _run(executor, parser, 'alias myid as uint128')
        _run(executor, parser, 'enum Color { red, green, blue }')
        _run(executor, parser, 'enum Shape { none, circle(r: float32), rect(w: float32, h: float32) }')
        _run(executor, parser, 'interface Named { name: string }')
        _run(executor, parser, 'type Person from Named { id: myid, age: uint8, color: Color, shape: Shape }')

    def test_show_references_all(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'show references')
        assert isinstance(result, QueryResult)
        assert result.columns == ["kind", "source", "field", "target"]
        edges = _edges(result)
        # Alias edge
        assert ("myid", "Alias", "(alias)", "uint128") in edges
        # Named interface edge
        assert ("Named", "Interface", "name", "string") in edges
        # Person composite edges
        assert ("Person", "Composite", "id", "myid") in edges
        assert ("Person", "Composite", "age", "uint8") in edges
        assert ("Person", "Composite", "color", "Color") in edges
        assert ("Person", "Composite", "shape", "Shape") in edges
        assert ("Person", "Composite", "name", "string") in edges

    def test_show_references_specific_type(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'show references Person')
        edges = _edges(result)
        # Should include outgoing edges from Person
        assert ("Person", "Composite", "id", "myid") in edges
        assert ("Person", "Composite", "name", "string") in edges
        # Should NOT include edges unrelated to Person
        assert ("myid", "Alias", "(alias)", "uint128") not in edges

    def test_show_references_alias_edges(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'show references myid')
        edges = _edges(result)
        # myid → uint128 (alias edge)
        assert ("myid", "Alias", "(alias)", "uint128") in edges
        # Person → myid (Person references myid as a field)
        assert ("Person", "Composite", "id", "myid") in edges

    def test_show_references_enum_edges(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'show references Shape')
        edges = _edges(result)
        # Shape enum should have edges for associated value types
        assert ("Shape", "Enum", "circle.r", "float32") in edges
        assert ("Shape", "Enum", "rect.w", "float32") in edges
        assert ("Shape", "Enum", "rect.h", "float32") in edges
        # Person → Shape
        assert ("Person", "Composite", "shape", "Shape") in edges

    def test_show_references_array_edges(self, executor, parser):
        _run(executor, parser, 'type Sensor { name: string, readings: int8[] }')
        result = _run(executor, parser, 'show references')
        edges = _edges(result)
        # Sensor → string (name field)
        assert ("Sensor", "Composite", "name", "string") in edges
        # Sensor → int8[] (readings field)
        assert ("Sensor", "Composite", "readings", "int8[]") in edges
        # int8[] → int8 (array element edge)
        assert ("int8[]", "Array", "[]", "int8") in edges

    def test_show_references_array_type_filter_includes_referrers(self, executor, parser):
        """Filtering by element type also shows who references the array type."""
        _run(executor, parser, 'type Point { x: float32, y: float32 }')
        _run(executor, parser, 'type Polyline { points: Point[] }')
        _run(executor, parser, 'type Polygon { points: Point[] }')
        result = _run(executor, parser, 'show references Point')
        edges = _edges(result)
        # Direct edges from Point
        assert ("Point", "Composite", "x", "float32") in edges
        assert ("Point", "Composite", "y", "float32") in edges
        # Array element edge
        assert ("Point[]", "Array", "[]", "Point") in edges
        # Referrers through array type
        assert ("Polyline", "Composite", "points", "Point[]") in edges
        assert ("Polygon", "Composite", "points", "Point[]") in edges

    def test_show_references_primitives_as_targets(self, executor, parser):
        _run(executor, parser, 'type Point { x: float32, y: float32 }')
        result = _run(executor, parser, 'show references')
        edges = _edges(result)
        assert ("Point", "Composite", "x", "float32") in edges
        assert ("Point", "Composite", "y", "float32") in edges

    def test_show_references_empty(self, executor, parser):
        """No user types = only built-in edges (e.g. string → character)."""
        result = _run(executor, parser, 'show references')
        edges = _edges(result)
        assert ("string", "String", "[]", "character") in edges
        # No user-defined type edges
        user_sources = {e["source"] for e in result.rows} - {"string"}
        assert len(user_sources) == 0

    def test_show_references_nonexistent_type(self, executor, parser):
        """Filtering on a non-existent type returns empty."""
        result = _run(executor, parser, 'show references Nonexistent')
        assert result.rows == []

    def test_show_references_default_sort(self, executor, parser):
        """Default sort is by target, then source."""
        _run(executor, parser, 'type A { x: uint8 }')
        _run(executor, parser, 'type B { x: uint16 }')
        result = _run(executor, parser, 'show references')
        targets = [e["target"] for e in result.rows if e["source"] in ("A", "B")]
        assert targets == sorted(targets)

    def test_show_references_sort_by_source(self, executor, parser):
        """Explicit sort by source."""
        _run(executor, parser, 'type Zz { x: uint8 }')
        _run(executor, parser, 'type Aa { x: uint8 }')
        result = _run(executor, parser, 'show references sort by source')
        sources = [e["source"] for e in result.rows if e["source"] in ("Aa", "Zz")]
        assert sources == ["Aa", "Zz"]

    def test_show_types_sort_by_kind(self, executor, parser):
        _run(executor, parser, 'enum Color { red, green }')
        _run(executor, parser, 'type Point { x: float32 }')
        result = _run(executor, parser, 'show types sort by kind')
        kinds = [r["kind"] for r in result.rows]
        assert kinds == sorted(kinds)

    def test_describe_sort_by_type(self, executor, parser):
        _run(executor, parser, 'type Thing { name: string, age: uint8, score: float32 }')
        result = _run(executor, parser, 'describe Thing sort by type')
        # Field rows sorted by type name
        field_rows = [r for r in result.rows if not r["property"].startswith("(")]
        types = [r["type"] for r in field_rows]
        assert types == sorted(types)


# ---- Dump graph tests ----


class TestDumpGraph:
    def _setup_schema(self, executor, parser):
        _run(executor, parser, 'alias myid as uint128')
        _run(executor, parser, 'type Person { id: myid, name: string, age: uint8 }')

    def test_dump_graph_ttq(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'dump graph')
        assert isinstance(result, DumpResult)
        script = result.script
        assert "type TypeNode" in script
        assert "type Edge" in script
        assert 'kind="Composite"' in script
        assert 'kind="Alias"' in script
        assert 'kind="Primitive"' in script
        assert 'name="Person"' in script
        assert 'field_name="name"' in script

    def test_dump_graph_dot(self, executor, parser, tmp_data_dir):
        self._setup_schema(executor, parser)
        result = executor.execute(DumpGraphQuery(output_file=str(tmp_data_dir / "types.dot")))
        assert isinstance(result, DumpResult)
        script = result.script
        assert "digraph types {" in script
        assert "rankdir=LR;" in script
        assert '"Person"' in script
        assert '"Person" -> "myid"' in script
        assert '"Person" -> "string"' in script

    def test_dump_graph_to_file_dot(self, executor, parser, tmp_data_dir):
        self._setup_schema(executor, parser)
        out_path = str(tmp_data_dir / "types.dot")
        result = executor.execute(DumpGraphQuery(output_file=out_path))
        assert isinstance(result, DumpResult)
        assert result.output_file == out_path
        assert "digraph" in result.script

    def test_dump_graph_to_file_ttq(self, executor, parser, tmp_data_dir):
        self._setup_schema(executor, parser)
        out_path = str(tmp_data_dir / "types.ttq")
        result = executor.execute(DumpGraphQuery(output_file=out_path))
        assert isinstance(result, DumpResult)
        assert result.output_file == out_path
        assert "type TypeNode" in result.script

    def test_dump_graph_no_extension(self, executor, parser, tmp_data_dir):
        self._setup_schema(executor, parser)
        out_path = str(tmp_data_dir / "types")
        result = executor.execute(DumpGraphQuery(output_file=out_path))
        assert isinstance(result, DumpResult)
        # Should have appended .ttq
        assert result.output_file == out_path + ".ttq"
        assert "type TypeNode" in result.script

    def test_dump_graph_dot_node_styles(self, executor, parser):
        _run(executor, parser, 'enum Color { red, green, blue }')
        _run(executor, parser, 'alias name as string')
        _run(executor, parser, 'type Person { name: name, color: Color }')
        result = executor.execute(DumpGraphQuery(output_file="/dev/null/types.dot"))
        script = result.script
        # Check different node styles
        assert 'shape=box' in script  # Composite/Enum
        assert 'shape=ellipse' in script  # Primitive/String

    def test_dump_graph_empty(self, executor, parser):
        """Empty schema produces minimal TTQ output."""
        result = _run(executor, parser, 'dump graph')
        assert isinstance(result, DumpResult)
        assert "type TypeNode" in result.script
        assert "type Edge" in result.script
