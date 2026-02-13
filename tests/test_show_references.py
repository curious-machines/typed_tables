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
        _run(executor, parser, 'alias myid = uint128')
        _run(executor, parser, 'enum Color { red, green, blue }')
        _run(executor, parser, 'enum Shape { none, circle(r: float32), rect(w: float32, h: float32) }')
        _run(executor, parser, 'interface Labelled { name: string }')
        _run(executor, parser, 'type Person from Labelled { id: myid, age: uint8, color: Color, shape: Shape }')

    def test_show_references_all(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'show references')
        assert isinstance(result, QueryResult)
        assert result.columns == ["kind", "source", "field", "target"]
        edges = _edges(result)
        # Alias edge
        assert ("myid", "Alias", "(alias)", "uint128") in edges
        # Labelled interface edge — name belongs to Labelled, not Person
        assert ("Labelled", "Interface", "name", "string") in edges
        # Person composite edges (own fields only, not inherited name)
        assert ("Person", "Composite", "id", "myid") in edges
        assert ("Person", "Composite", "age", "uint8") in edges
        assert ("Person", "Composite", "color", "Color") in edges
        assert ("Person", "Composite", "shape", "Shape") in edges
        assert ("Person", "Composite", "(implements)", "Labelled") in edges
        # Inherited field appears under the interface, not Person
        assert ("Person", "Composite", "name", "string") not in edges

    def test_show_references_specific_type(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'show references Person')
        edges = _edges(result)
        # Should include outgoing edges from Person (own fields)
        assert ("Person", "Composite", "id", "myid") in edges
        # Inherited name appears via Labelled (filter expansion)
        assert ("Labelled", "Interface", "name", "string") in edges
        assert ("Person", "Composite", "(implements)", "Labelled") in edges
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
        _run(executor, parser, 'alias myid = uint128')
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
        _run(executor, parser, 'alias name = string')
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

    def test_dump_graph_filter_type(self, executor, parser):
        """dump graph <type> filters to edges involving that type."""
        _run(executor, parser, 'type Point { x: float32, y: float32 }')
        _run(executor, parser, 'type Line { start: Point, end: Point }')
        _run(executor, parser, 'type Color { r: uint8, g: uint8, b: uint8 }')
        result = _run(executor, parser, 'dump graph Point')
        assert isinstance(result, DumpResult)
        script = result.script
        # Point-related nodes should be present
        assert 'name="Point"' in script
        assert 'name="Line"' in script
        # Color should NOT be in the filtered graph
        assert 'name="Color"' not in script

    def test_dump_graph_filter_type_to_dot(self, executor, parser, tmp_data_dir):
        """dump graph <type> to "file.dot" filters and outputs DOT."""
        _run(executor, parser, 'type Point { x: float32, y: float32 }')
        _run(executor, parser, 'type Line { start: Point }')
        out_path = str(tmp_data_dir / "graph.dot")
        result = executor.execute(DumpGraphQuery(type_name="Point", output_file=out_path))
        assert isinstance(result, DumpResult)
        assert "digraph" in result.script
        assert '"Point"' in result.script
        assert '"Line"' in result.script

    def test_parse_dump_graph_type(self, parser):
        q = parser.parse("dump graph Person")
        assert isinstance(q, DumpGraphQuery)
        assert q.type_name == "Person"
        assert q.output_file is None

    def test_parse_dump_graph_type_to(self, parser):
        q = parser.parse('dump graph Person to "types.dot"')
        assert isinstance(q, DumpGraphQuery)
        assert q.type_name == "Person"
        assert q.output_file == "types.dot"


# ---- Inheritance edge tests ----


class TestInheritanceEdges:
    def test_extends_edge(self, executor, parser):
        """Concrete parent produces an (extends) edge."""
        _run(executor, parser, 'type Person { name: string, age: uint8 }')
        _run(executor, parser, 'type Employee from Person { department: string }')
        result = _run(executor, parser, 'show references')
        edges = _edges(result)
        assert ("Employee", "Composite", "(extends)", "Person") in edges

    def test_implements_edge(self, executor, parser):
        """Interface parent produces an (implements) edge."""
        _run(executor, parser, 'interface Drawable { color: string }')
        _run(executor, parser, 'type Widget from Drawable { label: string }')
        result = _run(executor, parser, 'show references')
        edges = _edges(result)
        assert ("Widget", "Composite", "(implements)", "Drawable") in edges

    def test_extends_and_implements(self, executor, parser):
        """Both concrete and interface parents produce edges."""
        _run(executor, parser, 'interface Labelled { name: string }')
        _run(executor, parser, 'type Base { id: uint32 }')
        _run(executor, parser, 'type Derived from Base, Labelled { extra: float32 }')
        result = _run(executor, parser, 'show references')
        edges = _edges(result)
        assert ("Derived", "Composite", "(extends)", "Base") in edges
        assert ("Derived", "Composite", "(implements)", "Labelled") in edges

    def test_extends_edge_in_dump_graph(self, executor, parser):
        """Inheritance edges appear in dump graph output."""
        _run(executor, parser, 'type Person { name: string }')
        _run(executor, parser, 'type Employee from Person { dept: string }')
        result = _run(executor, parser, 'dump graph')
        assert isinstance(result, DumpResult)
        assert "(extends)" in result.script

    def test_filter_by_parent_shows_extends(self, executor, parser):
        """Filtering show references by parent type shows extends edge."""
        _run(executor, parser, 'type Person { name: string }')
        _run(executor, parser, 'type Employee from Person { dept: string }')
        result = _run(executor, parser, 'show references Person')
        edges = _edges(result)
        assert ("Employee", "Composite", "(extends)", "Person") in edges

    def test_inherited_fields_on_interface_not_composite(self, executor, parser):
        """Inherited fields appear under the interface, not the composite."""
        _run(executor, parser, 'interface Styled { fill: string, stroke: string }')
        _run(executor, parser, 'type Circle from Styled { cx: float32, cy: float32, r: float32 }')
        result = _run(executor, parser, 'show references')
        edges = _edges(result)
        # Own fields on Circle
        assert ("Circle", "Composite", "cx", "float32") in edges
        assert ("Circle", "Composite", "cy", "float32") in edges
        assert ("Circle", "Composite", "r", "float32") in edges
        # Inherited fields on Styled, not Circle
        assert ("Styled", "Interface", "fill", "string") in edges
        assert ("Styled", "Interface", "stroke", "string") in edges
        assert ("Circle", "Composite", "fill", "string") not in edges
        assert ("Circle", "Composite", "stroke", "string") not in edges

    def test_filter_expands_to_interface_edges(self, executor, parser):
        """Filtering by a type also shows outgoing edges from its interfaces."""
        _run(executor, parser, 'interface Styled { fill: string }')
        _run(executor, parser, 'type Circle from Styled { r: float32 }')
        _run(executor, parser, 'type Square from Styled { side: float32 }')
        result = _run(executor, parser, 'show references Circle')
        edges = _edges(result)
        # Circle's own edges
        assert ("Circle", "Composite", "r", "float32") in edges
        assert ("Circle", "Composite", "(implements)", "Styled") in edges
        # Styled's edges included via expansion
        assert ("Styled", "Interface", "fill", "string") in edges
        # Square's edges NOT included (not related to Circle)
        assert ("Square", "Composite", "(implements)", "Styled") not in edges
        assert ("Square", "Composite", "side", "float32") not in edges

    def test_filter_expands_to_parent_edges(self, executor, parser):
        """Filtering by a type also shows outgoing edges from its concrete parent."""
        _run(executor, parser, 'type Person { name: string, age: uint8 }')
        _run(executor, parser, 'type Employee from Person { dept: string }')
        result = _run(executor, parser, 'show references Employee')
        edges = _edges(result)
        # Employee's own edges
        assert ("Employee", "Composite", "dept", "string") in edges
        assert ("Employee", "Composite", "(extends)", "Person") in edges
        # Person's edges included via expansion
        assert ("Person", "Composite", "name", "string") in edges
        assert ("Person", "Composite", "age", "uint8") in edges

    def test_dump_graph_filter_expands_to_interface(self, executor, parser):
        """dump graph <type> also includes interface field edges."""
        _run(executor, parser, 'interface Styled { fill: string }')
        _run(executor, parser, 'type Circle from Styled { r: float32 }')
        result = _run(executor, parser, 'dump graph Circle')
        assert isinstance(result, DumpResult)
        script = result.script
        assert 'name="Circle"' in script
        assert 'name="Styled"' in script
        assert 'name="string"' in script


# ---- Parent tracking tests ----


class TestParentTracking:
    def test_parent_set_on_creation(self, executor, parser):
        """parent field is set when creating a type with a concrete parent."""
        _run(executor, parser, 'type Person { name: string }')
        _run(executor, parser, 'type Employee from Person { dept: string }')
        emp_def = executor.registry.get("Employee")
        assert emp_def.parent == "Person"

    def test_parent_none_for_interface_only(self, executor, parser):
        """parent is None when type only inherits from interfaces."""
        _run(executor, parser, 'interface Labelled { name: string }')
        _run(executor, parser, 'type Widget from Labelled { x: uint8 }')
        widget_def = executor.registry.get("Widget")
        assert widget_def.parent is None

    def test_parent_none_for_no_inheritance(self, executor, parser):
        """parent is None when type has no parents."""
        _run(executor, parser, 'type Point { x: float32, y: float32 }')
        point_def = executor.registry.get("Point")
        assert point_def.parent is None

    def test_parent_persists_in_metadata(self, executor, parser, tmp_data_dir):
        """parent field round-trips through metadata save/load."""
        from typed_tables.dump import load_registry_from_metadata
        _run(executor, parser, 'type Person { name: string }')
        _run(executor, parser, 'type Employee from Person { dept: string }')
        # Save and reload from metadata
        executor.storage.save_metadata()
        registry2 = load_registry_from_metadata(tmp_data_dir)
        emp_def = registry2.get("Employee")
        assert emp_def.parent == "Person"

    def test_describe_shows_parent(self, executor, parser):
        """describe shows a (parent) row for types with concrete parents."""
        _run(executor, parser, 'type Person { name: string }')
        _run(executor, parser, 'type Employee from Person { dept: string }')
        result = _run(executor, parser, 'describe Employee')
        parent_rows = [r for r in result.rows if r["property"] == "(parent)"]
        assert len(parent_rows) == 1
        assert parent_rows[0]["type"] == "Person"

    def test_describe_no_parent_row_without_parent(self, executor, parser):
        """describe does not show (parent) row for types without concrete parent."""
        _run(executor, parser, 'type Point { x: float32 }')
        result = _run(executor, parser, 'describe Point')
        parent_rows = [r for r in result.rows if r["property"] == "(parent)"]
        assert len(parent_rows) == 0

    def test_dump_roundtrip_with_parent(self, executor, parser, tmp_data_dir):
        """dump produces 'from Parent' clause for types with concrete parents."""
        _run(executor, parser, 'type Person { name: string, age: uint8 }')
        _run(executor, parser, 'type Employee from Person { dept: string }')
        result = _run(executor, parser, 'dump')
        assert isinstance(result, DumpResult)
        # The dump script should contain "from Person" in the Employee type def
        assert "from Person" in result.script

    def test_dump_roundtrip_parent_and_interface(self, executor, parser, tmp_data_dir):
        """dump produces correct from clause with both parent and interface."""
        _run(executor, parser, 'interface Labelled { name: string }')
        _run(executor, parser, 'type Base { id: uint32 }')
        _run(executor, parser, 'type Derived from Base, Labelled { extra: float32 }')
        result = _run(executor, parser, 'dump')
        assert isinstance(result, DumpResult)
        # Should have "from Base, Labelled" in the output
        assert "from Base, Labelled" in result.script
