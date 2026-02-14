"""Tests for the unified graph command."""

import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.parsing.query_parser import (
    CreateInterfaceQuery,
    GraphFilter,
    GraphQuery,
    QueryParser,
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


class TestParseGraph:
    def test_parse_graph(self, parser):
        q = parser.parse("graph")
        assert isinstance(q, GraphQuery)
        assert q.focus_type is None

    def test_parse_graph_type(self, parser):
        q = parser.parse("graph Person")
        assert isinstance(q, GraphQuery)
        assert q.focus_type == "Person"

    def test_parse_graph_sort_by(self, parser):
        q = parser.parse("graph sort by source")
        assert isinstance(q, GraphQuery)
        assert q.sort_by == ["source"]

    def test_parse_graph_type_sort_by(self, parser):
        q = parser.parse("graph Person sort by kind, field")
        assert isinstance(q, GraphQuery)
        assert q.focus_type == "Person"
        assert q.sort_by == ["kind", "field"]

    def test_parse_graph_to(self, parser):
        q = parser.parse('graph > "types.dot"')
        assert isinstance(q, GraphQuery)
        assert q.output_file == "types.dot"

    def test_parse_graph_type_to(self, parser):
        q = parser.parse('graph Person > "types.dot"')
        assert isinstance(q, GraphQuery)
        assert q.focus_type == "Person"
        assert q.output_file == "types.dot"


# ---- Graph table output tests ----


class TestGraphTable:
    def _setup_schema(self, executor, parser):
        """Set up a schema with aliases, enums, interfaces, and composites."""
        _run(executor, parser, 'alias myid = uint128')
        _run(executor, parser, 'enum Color { red, green, blue }')
        _run(executor, parser, 'enum Shape { none, circle(r: float32), rect(w: float32, h: float32) }')
        _run(executor, parser, 'interface Labelled { name: string }')
        _run(executor, parser, 'type Person from Labelled { id: myid, age: uint8, color: Color, shape: Shape }')

    def test_graph_all(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'graph')
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

    def test_graph_specific_type(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'graph Person')
        edges = _edges(result)
        # Should include outgoing edges from Person (own fields)
        assert ("Person", "Composite", "id", "myid") in edges
        # Inherited name appears via Labelled (filter expansion)
        assert ("Labelled", "Interface", "name", "string") in edges
        assert ("Person", "Composite", "(implements)", "Labelled") in edges
        # Unlimited depth: aliases are expanded
        assert ("myid", "Alias", "(alias)", "uint128") in edges

    def test_graph_alias_edges(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'graph myid')
        edges = _edges(result)
        # myid → uint128 (alias edge)
        assert ("myid", "Alias", "(alias)", "uint128") in edges
        # Person → myid (Person references myid as a field)
        assert ("Person", "Composite", "id", "myid") in edges

    def test_graph_enum_edges(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'graph Shape')
        edges = _edges(result)
        # Shape enum should have edges for associated value types
        assert ("Shape", "Enum", "circle.r", "float32") in edges
        assert ("Shape", "Enum", "rect.w", "float32") in edges
        assert ("Shape", "Enum", "rect.h", "float32") in edges
        # Person → Shape
        assert ("Person", "Composite", "shape", "Shape") in edges

    def test_graph_array_edges(self, executor, parser):
        _run(executor, parser, 'type Sensor { name: string, readings: int8[] }')
        result = _run(executor, parser, 'graph')
        edges = _edges(result)
        # Sensor → string (name field)
        assert ("Sensor", "Composite", "name", "string") in edges
        # Sensor → int8[] (readings field)
        assert ("Sensor", "Composite", "readings", "int8[]") in edges
        # int8[] → int8 (array element edge)
        assert ("int8[]", "Array", "[]", "int8") in edges

    def test_graph_array_type_filter_includes_referrers(self, executor, parser):
        """Filtering by element type also shows who references the array type."""
        _run(executor, parser, 'type Point { x: float32, y: float32 }')
        _run(executor, parser, 'type Polyline { points: Point[] }')
        _run(executor, parser, 'type Polygon { points: Point[] }')
        result = _run(executor, parser, 'graph Point')
        edges = _edges(result)
        # Direct edges from Point
        assert ("Point", "Composite", "x", "float32") in edges
        assert ("Point", "Composite", "y", "float32") in edges
        # Array element edge
        assert ("Point[]", "Array", "[]", "Point") in edges
        # Referrers through array type
        assert ("Polyline", "Composite", "points", "Point[]") in edges
        assert ("Polygon", "Composite", "points", "Point[]") in edges

    def test_graph_primitives_as_targets(self, executor, parser):
        _run(executor, parser, 'type Point { x: float32, y: float32 }')
        result = _run(executor, parser, 'graph')
        edges = _edges(result)
        assert ("Point", "Composite", "x", "float32") in edges
        assert ("Point", "Composite", "y", "float32") in edges

    def test_graph_empty(self, executor, parser):
        """No user types = only built-in edges (e.g. string → character)."""
        result = _run(executor, parser, 'graph')
        edges = _edges(result)
        assert ("string", "String", "[]", "character") in edges
        # No user-defined type edges
        user_sources = {e["source"] for e in result.rows} - {"string"}
        assert len(user_sources) == 0

    def test_graph_nonexistent_type(self, executor, parser):
        """Filtering on a non-existent type returns empty."""
        result = _run(executor, parser, 'graph Nonexistent')
        assert result.rows == []

    def test_graph_default_sort(self, executor, parser):
        """Default sort is by target, then source."""
        _run(executor, parser, 'type A { x: uint8 }')
        _run(executor, parser, 'type B { x: uint16 }')
        result = _run(executor, parser, 'graph')
        targets = [e["target"] for e in result.rows if e["source"] in ("A", "B")]
        assert targets == sorted(targets)

    def test_graph_sort_by_source(self, executor, parser):
        """Explicit sort by source."""
        _run(executor, parser, 'type Zz { x: uint8 }')
        _run(executor, parser, 'type Aa { x: uint8 }')
        result = _run(executor, parser, 'graph sort by source')
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


# ---- Graph file output tests ----


class TestGraphFileOutput:
    def _setup_schema(self, executor, parser):
        _run(executor, parser, 'alias myid = uint128')
        _run(executor, parser, 'type Person { id: myid, name: string, age: uint8 }')

    def test_graph_ttq(self, executor, parser):
        self._setup_schema(executor, parser)
        result = _run(executor, parser, 'graph > "types.ttq"')
        assert isinstance(result, DumpResult)
        script = result.script
        assert "enum NodeRole" in script
        assert "type TypeNode" in script
        assert "role: NodeRole" in script
        assert "type Edge" in script
        assert 'kind="Composite"' in script
        assert 'kind="Alias"' in script
        assert 'kind="Primitive"' in script
        assert 'name="Person"' in script
        assert 'field_name="name"' in script
        # Without focus type, all nodes should be context or leaf
        assert "role=.focus" not in script

    def test_graph_dot(self, executor, parser, tmp_data_dir):
        self._setup_schema(executor, parser)
        result = executor.execute(GraphQuery(output_file=str(tmp_data_dir / "types.dot")))
        assert isinstance(result, DumpResult)
        script = result.script
        assert "digraph types {" in script
        assert "rankdir=LR;" in script
        assert '"Person"' in script
        assert '"Person" -> "myid"' in script
        assert '"Person" -> "string"' in script

    def test_graph_to_file_dot(self, executor, parser, tmp_data_dir):
        self._setup_schema(executor, parser)
        out_path = str(tmp_data_dir / "types.dot")
        result = executor.execute(GraphQuery(output_file=out_path))
        assert isinstance(result, DumpResult)
        assert result.output_file == out_path
        assert "digraph" in result.script

    def test_graph_to_file_ttq(self, executor, parser, tmp_data_dir):
        self._setup_schema(executor, parser)
        out_path = str(tmp_data_dir / "types.ttq")
        result = executor.execute(GraphQuery(output_file=out_path))
        assert isinstance(result, DumpResult)
        assert result.output_file == out_path
        assert "type TypeNode" in result.script

    def test_graph_no_extension(self, executor, parser, tmp_data_dir):
        self._setup_schema(executor, parser)
        out_path = str(tmp_data_dir / "types")
        result = executor.execute(GraphQuery(output_file=out_path))
        assert isinstance(result, DumpResult)
        # Should have appended .ttq
        assert result.output_file == out_path + ".ttq"
        assert "type TypeNode" in result.script

    def test_graph_dot_node_styles(self, executor, parser):
        _run(executor, parser, 'enum Color { red, green, blue }')
        _run(executor, parser, 'alias name = string')
        _run(executor, parser, 'type Person { name: name, color: Color }')
        result = executor.execute(GraphQuery(output_file="/dev/null/types.dot"))
        script = result.script
        # Check different node styles
        assert 'shape=box' in script  # Composite/Enum
        assert 'shape=ellipse' in script  # Primitive/String

    def test_graph_empty_ttq(self, executor, parser):
        """Empty schema produces minimal TTQ output."""
        result = executor.execute(GraphQuery(output_file="out.ttq"))
        assert isinstance(result, DumpResult)
        assert "type TypeNode" in result.script
        assert "type Edge" in result.script

    def test_graph_filter_type(self, executor, parser):
        """graph <type> filters to edges involving that type."""
        _run(executor, parser, 'type Point { x: float32, y: float32 }')
        _run(executor, parser, 'type Line { start: Point, end: Point }')
        _run(executor, parser, 'type Color { r: uint8, g: uint8, b: uint8 }')
        result = executor.execute(GraphQuery(focus_type="Point", output_file="out.ttq"))
        assert isinstance(result, DumpResult)
        script = result.script
        # Point-related nodes should be present
        assert 'name="Point"' in script
        assert 'name="Line"' in script
        # Color should NOT be in the filtered graph
        assert 'name="Color"' not in script

    def test_graph_filter_type_to_dot(self, executor, parser, tmp_data_dir):
        """graph <type> > "file.dot" filters and outputs DOT."""
        _run(executor, parser, 'type Point { x: float32, y: float32 }')
        _run(executor, parser, 'type Line { start: Point }')
        out_path = str(tmp_data_dir / "graph.dot")
        result = executor.execute(GraphQuery(focus_type="Point", output_file=out_path))
        assert isinstance(result, DumpResult)
        assert "digraph" in result.script
        assert '"Point"' in result.script
        assert '"Line"' in result.script


# ---- Inheritance edge tests ----


class TestInheritanceEdges:
    def test_extends_edge(self, executor, parser):
        """Concrete parent produces an (extends) edge."""
        _run(executor, parser, 'type Person { name: string, age: uint8 }')
        _run(executor, parser, 'type Employee from Person { department: string }')
        result = _run(executor, parser, 'graph')
        edges = _edges(result)
        assert ("Employee", "Composite", "(extends)", "Person") in edges

    def test_implements_edge(self, executor, parser):
        """Interface parent produces an (implements) edge."""
        _run(executor, parser, 'interface Drawable { color: string }')
        _run(executor, parser, 'type Widget from Drawable { label: string }')
        result = _run(executor, parser, 'graph')
        edges = _edges(result)
        assert ("Widget", "Composite", "(implements)", "Drawable") in edges

    def test_extends_and_implements(self, executor, parser):
        """Both concrete and interface parents produce edges."""
        _run(executor, parser, 'interface Labelled { name: string }')
        _run(executor, parser, 'type Base { id: uint32 }')
        _run(executor, parser, 'type Derived from Base, Labelled { extra: float32 }')
        result = _run(executor, parser, 'graph')
        edges = _edges(result)
        assert ("Derived", "Composite", "(extends)", "Base") in edges
        assert ("Derived", "Composite", "(implements)", "Labelled") in edges

    def test_extends_edge_in_graph_output(self, executor, parser):
        """Inheritance edges appear in graph file output."""
        _run(executor, parser, 'type Person { name: string }')
        _run(executor, parser, 'type Employee from Person { dept: string }')
        result = executor.execute(GraphQuery(output_file="out.ttq"))
        assert isinstance(result, DumpResult)
        assert "(extends)" in result.script

    def test_filter_by_parent_shows_extends(self, executor, parser):
        """Filtering graph by parent type shows extends edge."""
        _run(executor, parser, 'type Person { name: string }')
        _run(executor, parser, 'type Employee from Person { dept: string }')
        result = _run(executor, parser, 'graph Person')
        edges = _edges(result)
        assert ("Employee", "Composite", "(extends)", "Person") in edges

    def test_inherited_fields_on_interface_not_composite(self, executor, parser):
        """Inherited fields appear under the interface, not the composite."""
        _run(executor, parser, 'interface Styled { fill: string, stroke: string }')
        _run(executor, parser, 'type Circle from Styled { cx: float32, cy: float32, r: float32 }')
        result = _run(executor, parser, 'graph')
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
        result = _run(executor, parser, 'graph Circle')
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
        result = _run(executor, parser, 'graph Employee')
        edges = _edges(result)
        # Employee's own edges
        assert ("Employee", "Composite", "dept", "string") in edges
        assert ("Employee", "Composite", "(extends)", "Person") in edges
        # Person's edges included via expansion
        assert ("Person", "Composite", "name", "string") in edges
        assert ("Person", "Composite", "age", "uint8") in edges

    def test_graph_filter_expands_to_interface(self, executor, parser):
        """graph <type> also includes interface field edges."""
        _run(executor, parser, 'interface Styled { fill: string }')
        _run(executor, parser, 'type Circle from Styled { r: float32 }')
        result = executor.execute(GraphQuery(focus_type="Circle", output_file="out.ttq"))
        assert isinstance(result, DumpResult)
        script = result.script
        assert 'name="Circle"' in script
        assert 'name="Styled"' in script
        assert 'name="string"' in script

    def test_recursive_expansion_deep_chain(self, executor, parser):
        """Filter expansion follows the full inheritance chain, not just one level."""
        _run(executor, parser, 'type A { x: uint8 }')
        _run(executor, parser, 'type B from A { y: uint16 }')
        _run(executor, parser, 'type C from B { z: uint32 }')
        result = _run(executor, parser, 'graph C')
        edges = _edges(result)
        # C's own
        assert ("C", "Composite", "z", "uint32") in edges
        assert ("C", "Composite", "(extends)", "B") in edges
        # B's (one level)
        assert ("B", "Composite", "y", "uint16") in edges
        assert ("B", "Composite", "(extends)", "A") in edges
        # A's (two levels deep)
        assert ("A", "Composite", "x", "uint8") in edges

    def test_recursive_expansion_mixed_chain(self, executor, parser):
        """Recursive expansion through concrete parent that implements interface."""
        _run(executor, parser, 'interface Labelled { name: string }')
        _run(executor, parser, 'type Person from Labelled { age: uint8 }')
        _run(executor, parser, 'type Employee from Person { dept: string }')
        result = _run(executor, parser, 'graph Employee')
        edges = _edges(result)
        # Employee's own
        assert ("Employee", "Composite", "dept", "string") in edges
        assert ("Employee", "Composite", "(extends)", "Person") in edges
        # Person's (via extends)
        assert ("Person", "Composite", "age", "uint8") in edges
        assert ("Person", "Composite", "(implements)", "Labelled") in edges
        # Labelled's (via Person's implements)
        assert ("Labelled", "Interface", "name", "string") in edges


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


# ---- Interface Inheritance tests ----


class TestInterfaceInheritanceParser:
    """Test parser produces correct CreateInterfaceQuery with parents."""

    def test_parse_interface_from_single_parent(self, parser):
        q = parser.parse("interface B from A { y: uint8 }")
        assert isinstance(q, CreateInterfaceQuery)
        assert q.name == "B"
        assert q.parents == ["A"]
        assert len(q.fields) == 1
        assert q.fields[0].name == "y"

    def test_parse_interface_from_multiple_parents(self, parser):
        q = parser.parse("interface C from A, B { z: uint8 }")
        assert isinstance(q, CreateInterfaceQuery)
        assert q.name == "C"
        assert q.parents == ["A", "B"]
        assert len(q.fields) == 1

    def test_parse_interface_from_parent_no_fields(self, parser):
        q = parser.parse("interface B from A")
        assert isinstance(q, CreateInterfaceQuery)
        assert q.name == "B"
        assert q.parents == ["A"]
        assert q.fields == []

    def test_parse_interface_no_parents(self, parser):
        q = parser.parse("interface A { x: uint8 }")
        assert isinstance(q, CreateInterfaceQuery)
        assert q.parents == []


class TestInterfaceInheritanceExecution:
    """Test interface inheritance execution."""

    def test_single_parent_merges_fields(self, executor, parser):
        _run(executor, parser, "interface A { x: uint8 }")
        result = _run(executor, parser, "interface B from A { y: uint16 }")
        assert "Created interface" in result.message
        # B should have both x and y
        desc = _run(executor, parser, "describe B")
        props = [r["property"] for r in desc.rows]
        assert "x" in props
        assert "y" in props

    def test_multi_parent_merges_fields(self, executor, parser):
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B { y: uint16 }")
        result = _run(executor, parser, "interface C from A, B { z: float32 }")
        assert "Created interface" in result.message
        desc = _run(executor, parser, "describe C")
        props = [r["property"] for r in desc.rows]
        assert "x" in props
        assert "y" in props
        assert "z" in props

    def test_diamond_merge_same_type_ok(self, executor, parser):
        """Diamond: A has x, B from A, C from A, D from B, C — x merges."""
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B from A { y: uint16 }")
        _run(executor, parser, "interface C from A { z: float32 }")
        result = _run(executor, parser, "interface D from B, C { w: string }")
        assert "Created interface" in result.message
        desc = _run(executor, parser, "describe D")
        props = [r["property"] for r in desc.rows]
        assert "x" in props
        assert "y" in props
        assert "z" in props
        assert "w" in props

    def test_diamond_merge_conflict_error(self, executor, parser):
        """Same field name with different types → error."""
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B { x: uint16 }")
        result = _run(executor, parser, "interface C from A, B { }")
        assert "Field conflict" in result.message

    def test_non_interface_parent_error(self, executor, parser):
        """Cannot inherit from a composite type."""
        _run(executor, parser, "type Foo { x: uint8 }")
        result = _run(executor, parser, "interface Bar from Foo { y: uint8 }")
        assert "Interfaces can only inherit from other interfaces" in result.message

    def test_unknown_parent_error(self, executor, parser):
        result = _run(executor, parser, "interface Bar from Unknown { y: uint8 }")
        assert "Unknown parent type" in result.message

    def test_self_inheritance_error(self, executor, parser):
        result = _run(executor, parser, "interface A from A { x: uint8 }")
        assert "Circular inheritance" in result.message

    def test_inherit_from_parent_no_own_fields(self, executor, parser):
        """interface B from A — inherits all fields, adds none."""
        _run(executor, parser, "interface A { x: uint8, y: uint16 }")
        result = _run(executor, parser, "interface B from A")
        assert "Created interface" in result.message
        desc = _run(executor, parser, "describe B")
        props = [r["property"] for r in desc.rows]
        assert "x" in props
        assert "y" in props


class TestInterfaceInheritanceDescribe:
    """Test describe shows (extends) rows for parent interfaces."""

    def test_describe_shows_extends(self, executor, parser):
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B from A { y: uint16 }")
        desc = _run(executor, parser, "describe B")
        extends_rows = [r for r in desc.rows if r["property"] == "(extends)"]
        assert len(extends_rows) == 1
        assert extends_rows[0]["type"] == "A"

    def test_describe_shows_multiple_extends(self, executor, parser):
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B { y: uint16 }")
        _run(executor, parser, "interface C from A, B { z: float32 }")
        desc = _run(executor, parser, "describe C")
        extends_rows = [r for r in desc.rows if r["property"] == "(extends)"]
        assert len(extends_rows) == 2
        extends_types = {r["type"] for r in extends_rows}
        assert extends_types == {"A", "B"}


class TestInterfaceInheritanceReferences:
    """Test graph for interface inheritance."""

    def test_graph_extends_edge(self, executor, parser):
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B from A { y: uint16 }")
        result = _run(executor, parser, "graph B")
        edges = _edges(result)
        # B→A with (extends)
        assert ("B", "Interface", "(extends)", "A") in edges
        # B's own field y→uint16
        assert ("B", "Interface", "y", "uint16") in edges
        # x should NOT appear under B (it's inherited)
        assert not any(e for e in edges if e[0] == "B" and e[2] == "x")

    def test_graph_inherited_fields_on_parent(self, executor, parser):
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B from A { y: uint16 }")
        result = _run(executor, parser, "graph A")
        edges = _edges(result)
        # A has x→uint8
        assert ("A", "Interface", "x", "uint8") in edges

    def test_graph_chain(self, executor, parser):
        """C from B from A — should recursively expand."""
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B from A { y: uint16 }")
        _run(executor, parser, "interface C from B { z: float32 }")
        result = _run(executor, parser, "graph C")
        edges = _edges(result)
        # C→B extends
        assert ("C", "Interface", "(extends)", "B") in edges
        # C's own field z→float32
        assert ("C", "Interface", "z", "float32") in edges
        # x and y should NOT appear under C
        assert not any(e for e in edges if e[0] == "C" and e[2] == "x")
        assert not any(e for e in edges if e[0] == "C" and e[2] == "y")


class TestInterfaceInheritanceDump:
    """Test dump round-trips interface inheritance."""

    def test_dump_includes_from_clause(self, executor, parser):
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B from A { y: uint16 }")
        result = _run(executor, parser, "dump")
        assert isinstance(result, DumpResult)
        assert "interface B from A" in result.script
        # B's dump should not include x (inherited)
        # Find the B interface line
        for line in result.script.split("\n"):
            if "interface B" in line:
                assert "x:" not in line
                assert "y:" in line
                break

    def test_dump_multi_parent_from_clause(self, executor, parser):
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B { y: uint16 }")
        _run(executor, parser, "interface C from A, B { z: float32 }")
        result = _run(executor, parser, "dump")
        assert isinstance(result, DumpResult)
        assert "interface C from A, B" in result.script

    def test_dump_roundtrip(self, tmp_data_dir, parser):
        """Dump and re-execute should produce identical types."""
        registry = TypeRegistry()
        storage = StorageManager(tmp_data_dir, registry)
        executor = QueryExecutor(storage, registry)
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B from A { y: uint16 }")
        _run(executor, parser, "type Widget from B { w: float32 }")
        result = _run(executor, parser, "dump")

        # Re-execute the dump in a fresh environment
        import tempfile
        tmp2 = Path(tempfile.mkdtemp())
        try:
            registry2 = TypeRegistry()
            storage2 = StorageManager(tmp2, registry2)
            executor2 = QueryExecutor(storage2, registry2)
            for q in parser.parse_program(result.script):
                executor2.execute(q)

            # Verify B has correct parents
            desc = _run(executor2, parser, "describe B")
            props = [r["property"] for r in desc.rows]
            assert "x" in props
            assert "y" in props
            extends = [r for r in desc.rows if r["property"] == "(extends)"]
            assert len(extends) == 1
            assert extends[0]["type"] == "A"
        finally:
            shutil.rmtree(tmp2, ignore_errors=True)

    def test_dump_dependency_order(self, executor, parser):
        """Parent interfaces emitted before children."""
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B from A { y: uint16 }")
        result = _run(executor, parser, "dump")
        assert isinstance(result, DumpResult)
        lines = result.script.split("\n")
        a_idx = next(i for i, l in enumerate(lines) if "interface A" in l)
        b_idx = next(i for i, l in enumerate(lines) if "interface B" in l)
        assert a_idx < b_idx


class TestInterfaceInheritanceMetadata:
    """Test metadata persistence."""

    def test_metadata_survives_save_load(self, executor, parser, tmp_data_dir):
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.types import InterfaceTypeDefinition
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B from A { y: uint16 }")

        # Save and reload from metadata
        executor.storage.save_metadata()
        registry2 = load_registry_from_metadata(tmp_data_dir)
        b_def = registry2.get("B")
        assert isinstance(b_def, InterfaceTypeDefinition)
        assert b_def.interfaces == ["A"]
        assert len(b_def.fields) == 2
        field_names = [f.name for f in b_def.fields]
        assert "x" in field_names
        assert "y" in field_names


class TestInterfaceInheritanceWithComposites:
    """Test composites implementing inherited interfaces."""

    def test_composite_from_inherited_interface(self, executor, parser):
        """A composite implementing an interface that itself inherits from another."""
        _run(executor, parser, "interface A { x: uint8 }")
        _run(executor, parser, "interface B from A { y: uint16 }")
        result = _run(executor, parser, "type Widget from B { w: float32 }")
        assert "Created type" in result.message
        desc = _run(executor, parser, "describe Widget")
        props = [r["property"] for r in desc.rows]
        assert "x" in props
        assert "y" in props
        assert "w" in props


# ---- Phase 2: View Modes ----


class TestViewModeParser:
    """Test parsing of view mode syntax."""

    def test_parse_structure(self, parser):
        q = parser.parse("graph structure")
        assert isinstance(q, GraphQuery)
        assert q.view_mode == "structure"
        assert q.focus_type is None

    def test_parse_type_structure(self, parser):
        q = parser.parse("graph Boss structure")
        assert isinstance(q, GraphQuery)
        assert q.focus_type == "Boss"
        assert q.view_mode == "structure"

    def test_parse_declared(self, parser):
        q = parser.parse("graph Boss declared")
        assert isinstance(q, GraphQuery)
        assert q.focus_type == "Boss"
        assert q.view_mode == "declared"
        assert q.field_centric is False

    def test_parse_declared_fields(self, parser):
        q = parser.parse("graph Boss declared fields")
        assert isinstance(q, GraphQuery)
        assert q.view_mode == "declared"
        assert q.field_centric is True

    def test_parse_declared_fields_without_types(self, parser):
        q = parser.parse("graph Boss declared fields without types")
        assert isinstance(q, GraphQuery)
        assert q.view_mode == "declared"
        assert q.field_centric is True
        assert q.without_types is True

    def test_parse_stored(self, parser):
        q = parser.parse("graph Boss stored")
        assert isinstance(q, GraphQuery)
        assert q.view_mode == "stored"

    def test_parse_stored_fields(self, parser):
        q = parser.parse("graph Boss stored fields")
        assert isinstance(q, GraphQuery)
        assert q.view_mode == "stored"
        assert q.field_centric is True

    def test_parse_stored_origin(self, parser):
        q = parser.parse("graph Boss stored origin")
        assert isinstance(q, GraphQuery)
        assert q.view_mode == "stored"
        assert q.show_origin is True
        assert q.field_centric is False

    def test_parse_stored_fields_origin(self, parser):
        q = parser.parse("graph Boss stored fields origin")
        assert isinstance(q, GraphQuery)
        assert q.view_mode == "stored"
        assert q.field_centric is True
        assert q.show_origin is True

    def test_parse_stored_fields_without_types(self, parser):
        q = parser.parse("graph Boss stored fields without types")
        assert isinstance(q, GraphQuery)
        assert q.field_centric is True
        assert q.without_types is True

    def test_parse_stored_fields_origin_without_types(self, parser):
        q = parser.parse("graph Boss stored fields origin without types")
        assert isinstance(q, GraphQuery)
        assert q.field_centric is True
        assert q.show_origin is True
        assert q.without_types is True

    def test_parse_structure_to_file(self, parser):
        q = parser.parse('graph Boss structure > "boss.dot"')
        assert isinstance(q, GraphQuery)
        assert q.view_mode == "structure"
        assert q.output_file == "boss.dot"

    def test_parse_stored_fields_sort(self, parser):
        q = parser.parse("graph Boss stored fields sort by field")
        assert isinstance(q, GraphQuery)
        assert q.view_mode == "stored"
        assert q.field_centric is True
        assert q.sort_by == ["field"]


def _setup_boss_schema(executor, parser):
    """Set up a rich schema for view mode testing."""
    _run(executor, parser, 'interface Identifiable { id: uint32 }')
    _run(executor, parser, 'interface Labelled { name: string }')
    _run(executor, parser, 'interface Positioned { x: float32, y: float32 }')
    _run(executor, parser, 'interface Entity from Identifiable, Labelled { }')
    _run(executor, parser, 'type Creature from Entity, Positioned { hp: int16, speed: float32 }')
    _run(executor, parser, 'type NPC from Creature { dialogue: string }')
    _run(executor, parser, 'type Boss from NPC { phase: uint8 }')


class TestStructureView:
    """Test structure view mode."""

    def test_structure_no_focus(self, executor, parser):
        """Structure without focus shows all inheritance edges."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph structure')
        assert isinstance(result, QueryResult)
        edges = _edges(result)
        # Should have extends/implements edges only
        assert all(e[2] in ("(extends)", "(implements)") for e in edges)
        # Specific edges
        assert ("Boss", "Composite", "(extends)", "NPC") in edges
        assert ("NPC", "Composite", "(extends)", "Creature") in edges
        assert ("Creature", "Composite", "(implements)", "Entity") in edges
        assert ("Entity", "Interface", "(extends)", "Identifiable") in edges

    def test_structure_with_focus(self, executor, parser):
        """Structure with focus shows only reachable inheritance edges."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss structure')
        edges = _edges(result)
        # Should follow Boss → NPC → Creature → Entity → Identifiable/Labelled, Positioned
        assert ("Boss", "Composite", "(extends)", "NPC") in edges
        assert ("NPC", "Composite", "(extends)", "Creature") in edges
        # No field edges
        assert not any(e[2] not in ("(extends)", "(implements)") for e in edges)

    def test_structure_excludes_field_edges(self, executor, parser):
        """Structure mode never includes field→type edges."""
        _run(executor, parser, 'type Point { x: float32, y: float32 }')
        _run(executor, parser, 'type ColorPoint from Point { color: string }')
        result = _run(executor, parser, 'graph structure')
        edges = _edges(result)
        assert not any(e[2] in ("x", "y", "color") for e in edges)
        assert ("ColorPoint", "Composite", "(extends)", "Point") in edges

    def test_structure_to_dot(self, executor, parser):
        _setup_boss_schema(executor, parser)
        result = executor.execute(GraphQuery(view_mode="structure", output_file="out.dot"))
        assert isinstance(result, DumpResult)
        assert "digraph" in result.script
        assert "style=dashed" in result.script


class TestDeclaredView:
    """Test declared view mode."""

    def test_declared_requires_focus(self, executor, parser):
        """Declared without focus type raises an error."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph declared')
        assert "requires a focus type" in result.message

    def test_declared_own_fields(self, executor, parser):
        """Declared shows only fields defined by the type itself."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss declared')
        assert isinstance(result, QueryResult)
        fields = {e["field"] for e in result.rows}
        # Boss only declares phase
        assert fields == {"phase"}

    def test_declared_creature_fields(self, executor, parser):
        """Creature declares hp and speed (Entity/Positioned fields are inherited)."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Creature declared')
        fields = {e["field"] for e in result.rows}
        assert fields == {"hp", "speed"}

    def test_declared_field_centric(self, executor, parser):
        """Declared fields view returns field-centric rows."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Creature declared fields')
        assert isinstance(result, QueryResult)
        assert result.columns == ["field", "type"]
        field_names = {r["field"] for r in result.rows}
        assert field_names == {"hp", "speed"}
        # Check types
        type_map = {r["field"]: r["type"] for r in result.rows}
        assert type_map["hp"] == "int16"
        assert type_map["speed"] == "float32"

    def test_declared_fields_without_types(self, executor, parser):
        """Declared fields without types returns only field names."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Creature declared fields without types')
        assert result.columns == ["field"]
        assert all("type" not in r for r in result.rows)
        field_names = {r["field"] for r in result.rows}
        assert field_names == {"hp", "speed"}


class TestStoredView:
    """Test stored view mode."""

    def test_stored_requires_focus(self, executor, parser):
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph stored')
        assert "requires a focus type" in result.message

    def test_stored_all_fields(self, executor, parser):
        """Stored shows all fields on the record (inherited + own)."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss stored')
        fields = {e["field"] for e in result.rows}
        # Boss should have all inherited + own fields
        assert "phase" in fields  # own
        assert "dialogue" in fields  # from NPC
        assert "hp" in fields  # from Creature
        assert "speed" in fields  # from Creature
        assert "id" in fields  # from Identifiable via Entity
        assert "name" in fields  # from Labelled via Entity
        assert "x" in fields  # from Positioned
        assert "y" in fields  # from Positioned

    def test_stored_field_centric(self, executor, parser):
        """Stored fields view returns field-centric rows."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss stored fields')
        assert result.columns == ["field", "type"]
        field_names = {r["field"] for r in result.rows}
        assert "phase" in field_names
        assert "id" in field_names

    def test_stored_origin(self, executor, parser):
        """Stored origin shows where each field comes from."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss stored origin')
        assert "origin" in result.columns
        origin_map = {r["field"]: r["origin"] for r in result.rows}
        assert origin_map["phase"] == "Boss"
        assert origin_map["dialogue"] == "NPC"
        assert origin_map["hp"] == "Creature"
        assert origin_map["speed"] == "Creature"
        assert origin_map["id"] == "Identifiable"
        assert origin_map["name"] == "Labelled"
        assert origin_map["x"] == "Positioned"
        assert origin_map["y"] == "Positioned"

    def test_stored_fields_origin(self, executor, parser):
        """Stored fields origin combines field-centric with origin."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss stored fields origin')
        assert result.columns == ["field", "type", "origin"]
        assert any(r["field"] == "phase" and r["origin"] == "Boss" for r in result.rows)
        assert any(r["field"] == "id" and r["origin"] == "Identifiable" for r in result.rows)

    def test_stored_fields_without_types(self, executor, parser):
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss stored fields without types')
        assert result.columns == ["field"]
        field_names = {r["field"] for r in result.rows}
        assert "phase" in field_names
        assert "id" in field_names

    def test_stored_fields_origin_without_types(self, executor, parser):
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss stored fields origin without types')
        assert result.columns == ["field", "origin"]
        assert any(r["field"] == "phase" and r["origin"] == "Boss" for r in result.rows)

    def test_stored_to_dot(self, executor, parser):
        _setup_boss_schema(executor, parser)
        result = executor.execute(GraphQuery(
            focus_type="Boss", view_mode="stored", output_file="out.dot"))
        assert isinstance(result, DumpResult)
        assert "digraph" in result.script

    def test_stored_field_centric_to_dot(self, executor, parser):
        _setup_boss_schema(executor, parser)
        result = executor.execute(GraphQuery(
            focus_type="Boss", view_mode="stored", field_centric=True,
            output_file="out.dot"))
        assert isinstance(result, DumpResult)
        assert "digraph" in result.script
        assert "phase" in result.script


class TestViewModeValidation:
    """Test error cases for view mode constraints."""

    def test_origin_requires_stored(self, executor, parser):
        _setup_boss_schema(executor, parser)
        result = executor.execute(GraphQuery(
            focus_type="Boss", view_mode="declared", show_origin=True))
        assert "only valid with 'stored'" in result.message

    def test_without_types_requires_fields(self, executor, parser):
        _setup_boss_schema(executor, parser)
        result = executor.execute(GraphQuery(
            focus_type="Boss", view_mode="stored", without_types=True))
        assert "requires 'fields'" in result.message

    def test_declared_unknown_type(self, executor, parser):
        result = _run(executor, parser, 'graph Nonexistent declared')
        assert result.rows == []

    def test_stored_unknown_type(self, executor, parser):
        result = _run(executor, parser, 'graph Nonexistent stored')
        assert result.rows == []


# ---- Phase 3: Depth Control ----


class TestDepthParser:
    def test_parse_depth(self, parser):
        q = parser.parse("graph Boss depth 2")
        assert isinstance(q, GraphQuery)
        assert q.focus_type == "Boss"
        assert q.depth == 2

    def test_parse_depth_zero(self, parser):
        q = parser.parse("graph Boss depth 0")
        assert isinstance(q, GraphQuery)
        assert q.depth == 0

    def test_parse_depth_to_file(self, parser):
        q = parser.parse('graph Boss depth 1 > "out.dot"')
        assert isinstance(q, GraphQuery)
        assert q.depth == 1
        assert q.output_file == "out.dot"

    def test_parse_depth_sort(self, parser):
        q = parser.parse("graph Boss depth 2 sort by source")
        assert isinstance(q, GraphQuery)
        assert q.depth == 2
        assert q.sort_by == ["source"]

    def test_parse_structure_depth(self, parser):
        q = parser.parse("graph Boss structure depth 1")
        assert isinstance(q, GraphQuery)
        assert q.view_mode == "structure"
        assert q.depth == 1


class TestDepthControl:
    def test_depth_zero_focus_only(self, executor, parser):
        """depth 0 = focus node only, no edges."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss depth 0')
        edges = _edges(result)
        assert len(edges) == 0
        # Table output shows a message with the focus type name
        assert result.message is not None
        assert "Boss" in result.message

    def test_depth_zero_dot_shows_focus_node(self, executor, parser, tmp_data_dir):
        """depth 0 DOT output includes the focus node."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, f'graph Boss depth 0 > "{tmp_data_dir}/d0.dot"')
        assert isinstance(result, DumpResult)
        assert '"Boss"' in result.script
        # No edges in the DOT output
        assert "->" not in result.script

    def test_depth_one_direct_edges(self, executor, parser):
        """depth 1 = direct edges from focus only."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss depth 1')
        edges = _edges(result)
        sources = {e[0] for e in edges}
        # Boss's own edges only
        assert "Boss" in sources
        # No parent expansion yet
        assert "NPC" not in sources
        assert "Creature" not in sources

    def test_depth_two(self, executor, parser):
        """depth 2 = focus edges + 1 level of parent expansion."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss depth 2')
        edges = _edges(result)
        sources = {e[0] for e in edges}
        assert "Boss" in sources
        assert "NPC" in sources
        # Not yet at depth 3
        assert "Creature" not in sources

    def test_depth_unlimited_default(self, executor, parser):
        """No depth = unlimited expansion."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss')
        edges = _edges(result)
        sources = {e[0] for e in edges}
        # Should include everything in the chain
        assert "Boss" in sources
        assert "NPC" in sources
        assert "Creature" in sources
        assert "Entity" in sources
        assert "Identifiable" in sources

    def test_depth_structure(self, executor, parser):
        """depth works with structure view."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss structure depth 1')
        edges = _edges(result)
        # Only Boss → NPC extends edge
        assert ("Boss", "Composite", "(extends)", "NPC") in edges
        # NPC's own extends should NOT be here (depth 1 = only immediate)
        assert ("NPC", "Composite", "(extends)", "Creature") not in edges

    def test_depth_error_with_declared(self, executor, parser):
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss declared depth 1')
        assert "cannot be used with" in result.message

    def test_depth_error_with_stored(self, executor, parser):
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss stored depth 1')
        assert "cannot be used with" in result.message


# ==== Phase 4: Filter tests ====

class TestFilterParser:
    """Test parsing of showing/excluding filter syntax."""

    def test_parse_showing_type(self, parser):
        r = parser.parse('graph showing type Person')
        assert isinstance(r, GraphQuery)
        assert len(r.showing) == 1
        assert r.showing[0].dimension == "type"
        assert r.showing[0].values == ["Person"]

    def test_parse_excluding_field(self, parser):
        r = parser.parse('graph excluding field name')
        assert isinstance(r, GraphQuery)
        assert len(r.excluding) == 1
        assert r.excluding[0].dimension == "field"
        assert r.excluding[0].values == ["name"]

    def test_parse_showing_kind(self, parser):
        r = parser.parse('graph showing kind Composite')
        assert isinstance(r, GraphQuery)
        assert r.showing[0].dimension == "kind"
        assert r.showing[0].values == ["Composite"]

    def test_parse_showing_kind_interface(self, parser):
        """Kind filter with reserved keyword as value."""
        r = parser.parse('graph showing kind interface')
        assert r.showing[0].dimension == "kind"
        assert r.showing[0].values == ["interface"]

    def test_parse_multi_value_brackets(self, parser):
        r = parser.parse('graph showing type [Person, Employee]')
        assert r.showing[0].values == ["Person", "Employee"]

    def test_parse_multi_filter(self, parser):
        """Multiple filter dimensions in one showing clause."""
        r = parser.parse('graph showing type Person field name')
        assert len(r.showing) == 2
        assert r.showing[0].dimension == "type"
        assert r.showing[1].dimension == "field"

    def test_parse_showing_and_excluding(self, parser):
        r = parser.parse('graph showing type uint32 excluding kind Primitive')
        assert len(r.showing) == 1
        assert len(r.excluding) == 1
        assert r.showing[0].dimension == "type"
        assert r.excluding[0].dimension == "kind"

    def test_parse_depth_with_filter(self, parser):
        r = parser.parse('graph Boss depth 2 showing type uint32')
        assert r.depth == 2
        assert len(r.showing) == 1

    def test_parse_filter_with_sort(self, parser):
        r = parser.parse('graph showing kind Composite sort by source')
        assert r.showing[0].dimension == "kind"
        assert r.sort_by == ["source"]

    def test_parse_filter_with_to(self, parser):
        r = parser.parse('graph showing type Person > "out.dot"')
        assert r.showing[0].dimension == "type"
        assert r.output_file == "out.dot"

    def test_parse_focus_with_filter(self, parser):
        r = parser.parse('graph Boss showing type uint8')
        assert r.focus_type == "Boss"
        assert r.showing[0].values == ["uint8"]

    def test_parse_view_mode_with_filter(self, parser):
        r = parser.parse('graph Boss structure showing type NPC')
        assert r.view_mode == "structure"
        assert r.showing[0].values == ["NPC"]


class TestShowingFilter:
    """Test showing filter execution."""

    def test_showing_type(self, executor, parser):
        """showing type keeps edges pointing to that type."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss showing type uint8')
        edges = _edges(result)
        # Boss → uint8 (phase) should be present
        assert ("Boss", "Composite", "phase", "uint8") in edges
        # Other field edges should be absent
        field_edges = [e for e in edges if not e[2].startswith("(")]
        assert all(e[3] == "uint8" for e in field_edges)

    def test_showing_type_with_structural_path(self, executor, parser):
        """showing type includes structural edges connecting to the source."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss showing type int16')
        edges = _edges(result)
        # Creature → int16 (hp) should be present
        assert ("Creature", "Composite", "hp", "int16") in edges
        # Structural path: Boss → NPC → Creature should be present
        assert ("Boss", "Composite", "(extends)", "NPC") in edges
        assert ("NPC", "Composite", "(extends)", "Creature") in edges

    def test_showing_type_multiple_values(self, executor, parser):
        """showing type with bracket list."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss showing type [uint8, int16]')
        edges = _edges(result)
        field_edges = [e for e in edges if not e[2].startswith("(")]
        targets = {e[3] for e in field_edges}
        assert targets == {"uint8", "int16"}

    def test_showing_field(self, executor, parser):
        """showing field keeps edges with matching field name."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss showing field phase')
        edges = _edges(result)
        field_edges = [e for e in edges if not e[2].startswith("(")]
        assert len(field_edges) == 1
        assert field_edges[0] == ("Boss", "Composite", "phase", "uint8")

    def test_showing_field_inherited(self, executor, parser):
        """showing field works for inherited fields."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss showing field hp')
        edges = _edges(result)
        field_edges = [e for e in edges if not e[2].startswith("(")]
        assert len(field_edges) == 1
        assert field_edges[0] == ("Creature", "Composite", "hp", "int16")
        # Structural path should exist
        assert ("Boss", "Composite", "(extends)", "NPC") in edges
        assert ("NPC", "Composite", "(extends)", "Creature") in edges

    def test_showing_kind(self, executor, parser):
        """showing kind keeps edges where source matches kind."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss showing kind Interface')
        edges = _edges(result)
        # All kept edges should have Interface kind (except structural path)
        non_structural = [e for e in edges if not e[2].startswith("(")]
        assert all(e[1] == "Interface" for e in non_structural)
        # Interface sources should be present
        sources = {e[0] for e in non_structural}
        assert "Identifiable" in sources or "Labelled" in sources or "Positioned" in sources

    def test_showing_kind_case_insensitive(self, executor, parser):
        """Kind filter matches case-insensitively."""
        _setup_boss_schema(executor, parser)
        result1 = _run(executor, parser, 'graph Boss showing kind interface')
        result2 = _run(executor, parser, 'graph Boss showing kind Interface')
        edges1 = _edges(result1)
        edges2 = _edges(result2)
        assert edges1 == edges2

    def test_showing_no_match(self, executor, parser):
        """showing with no matching edges returns empty."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss showing type float64')
        assert len(result.rows) == 0

    def test_showing_with_structure_view(self, executor, parser):
        """showing works with structure view mode."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss structure showing type NPC')
        edges = _edges(result)
        # Only structural edges pointing to NPC
        assert ("Boss", "Composite", "(extends)", "NPC") in edges
        # Other structural edges should be absent
        assert ("NPC", "Composite", "(extends)", "Creature") not in edges


class TestExcludingFilter:
    """Test excluding filter execution."""

    def test_excluding_type(self, executor, parser):
        """excluding type removes edges pointing to that type."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss excluding type string')
        edges = _edges(result)
        # No string targets
        assert all(e[3] != "string" for e in edges)
        # Other edges should still be present
        assert ("Boss", "Composite", "phase", "uint8") in edges

    def test_excluding_field(self, executor, parser):
        """excluding field removes edges with that field name."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss excluding field dialogue')
        edges = _edges(result)
        assert all(e[2] != "dialogue" for e in edges)
        # Other edges intact
        assert ("Boss", "Composite", "phase", "uint8") in edges

    def test_excluding_kind(self, executor, parser):
        """excluding kind removes edges where source matches kind."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss excluding kind Interface')
        edges = _edges(result)
        non_structural = [e for e in edges if not e[2].startswith("(")]
        assert all(e[1] != "Interface" for e in non_structural)

    def test_excluding_multiple_values(self, executor, parser):
        """excluding with bracket list."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss excluding type [string, float32]')
        edges = _edges(result)
        targets = {e[3] for e in edges if not e[2].startswith("(")}
        assert "string" not in targets
        assert "float32" not in targets


class TestCombinedFilters:
    """Test combined showing + excluding filters."""

    def test_showing_then_excluding(self, executor, parser):
        """showing narrows, then excluding removes from the narrowed set."""
        _setup_boss_schema(executor, parser)
        # Show float32 type, then exclude the speed field
        result = _run(executor, parser, 'graph Boss showing type float32 excluding field speed')
        edges = _edges(result)
        field_edges = [e for e in edges if not e[2].startswith("(")]
        # Only x and y from Positioned (speed excluded)
        fields = {e[2] for e in field_edges}
        assert "speed" not in fields
        assert "x" in fields or "y" in fields
        assert all(e[3] == "float32" for e in field_edges)

    def test_showing_type_excluding_field(self, executor, parser):
        """Show type then exclude specific field."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss showing type float32 excluding field speed')
        edges = _edges(result)
        field_edges = [e for e in edges if not e[2].startswith("(")]
        # float32 edges remain, but not speed
        assert all(e[2] != "speed" for e in field_edges)
        # x and y from Positioned should remain
        fields = {e[2] for e in field_edges}
        assert "x" in fields or "y" in fields

    def test_filter_with_depth(self, executor, parser):
        """Filters work together with depth."""
        _setup_boss_schema(executor, parser)
        # depth 1 = Boss + NPC, then show only uint8
        result = _run(executor, parser, 'graph Boss depth 1 showing type uint8')
        edges = _edges(result)
        field_edges = [e for e in edges if not e[2].startswith("(")]
        assert len(field_edges) == 1
        assert field_edges[0] == ("Boss", "Composite", "phase", "uint8")


# ==== Phase 6: Styling and Titles ====

class TestTitleStyleParser:
    """Test parsing of title and style clauses."""

    def test_parse_title(self, parser):
        r = parser.parse('graph > "out.dot" title "Boss Schema"')
        assert isinstance(r, GraphQuery)
        assert r.title == "Boss Schema"
        assert r.output_file == "out.dot"

    def test_parse_style(self, parser):
        r = parser.parse('graph > "out.dot" style "styles.txt"')
        assert r.style_file == "styles.txt"
        assert r.output_file == "out.dot"

    def test_parse_title_and_style(self, parser):
        r = parser.parse('graph > "out.dot" title "My Graph" style "s.txt"')
        assert r.title == "My Graph"
        assert r.style_file == "s.txt"

    def test_parse_style_then_title(self, parser):
        r = parser.parse('graph > "out.dot" style "s.txt" title "My Graph"')
        assert r.title == "My Graph"
        assert r.style_file == "s.txt"

    def test_parse_title_no_style(self, parser):
        r = parser.parse('graph Boss > "out.dot" title "Boss"')
        assert r.focus_type == "Boss"
        assert r.title == "Boss"
        assert r.style_file is None

    def test_parse_with_filters(self, parser):
        r = parser.parse('graph Boss showing type uint8 > "out.dot" title "Focused"')
        assert r.title == "Focused"
        assert len(r.showing) == 1


class TestTitleOutput:
    """Test title in DOT and TTQ output."""

    def test_dot_title(self, executor, parser, tmp_data_dir):
        """Title appears as label in DOT output."""
        _run(executor, parser, 'type Foo { x: uint32 }')
        result = _run(executor, parser, 'graph > "out.dot" title "My Schema"')
        assert isinstance(result, DumpResult)
        assert 'label="My Schema"' in result.script
        assert "labelloc=t" in result.script

    def test_dot_default_title(self, executor, parser, tmp_data_dir):
        """Default title when not explicitly specified."""
        _run(executor, parser, 'type Foo { x: uint32 }')
        result = _run(executor, parser, 'graph > "out.dot"')
        assert isinstance(result, DumpResult)
        assert "labelloc=t" in result.script
        assert 'label="graph"' in result.script

    def test_ttq_title(self, executor, parser, tmp_data_dir):
        """Title appears as comment in TTQ output."""
        _run(executor, parser, 'type Foo { x: uint32 }')
        result = _run(executor, parser, 'graph > "out.ttq" title "My Schema"')
        assert isinstance(result, DumpResult)
        assert result.script.startswith("-- My Schema")

    def test_ttq_default_comment(self, executor, parser, tmp_data_dir):
        """Default TTQ comment derived from query when no explicit title."""
        _run(executor, parser, 'type Foo { x: uint32 }')
        result = _run(executor, parser, 'graph > "out.ttq"')
        assert isinstance(result, DumpResult)
        assert result.script.startswith("-- graph")


class TestStyleOutput:
    """Test style file application in DOT output."""

    def test_style_direction(self, executor, parser, tmp_data_dir):
        """Style file can override graph direction."""
        style_path = tmp_data_dir / "styles.txt"
        style_path.write_text('{"direction": "TB"}')
        _run(executor, parser, 'type Foo { x: uint32 }')
        result = _run(executor, parser, f'graph > "out.dot" style "{style_path}"')
        assert isinstance(result, DumpResult)
        assert "rankdir=TB" in result.script

    def test_style_kind_color(self, executor, parser, tmp_data_dir):
        """Style file can override kind colors."""
        style_path = tmp_data_dir / "styles.txt"
        style_path.write_text('{"composite.color": "#FF0000"}')
        _run(executor, parser, 'type Foo { x: uint32 }')
        result = _run(executor, parser, f'graph > "out.dot" style "{style_path}"')
        assert isinstance(result, DumpResult)
        assert "#FF0000" in result.script

    def test_style_focus_color(self, executor, parser, tmp_data_dir):
        """Style file can set focus node color."""
        style_path = tmp_data_dir / "styles.txt"
        style_path.write_text('{"focus.color": "#00FF00"}')
        _run(executor, parser, 'type Foo { x: uint32 }')
        result = _run(executor, parser, f'graph Foo > "out.dot" style "{style_path}"')
        assert isinstance(result, DumpResult)
        assert "#00FF00" in result.script

    def test_style_comments_ignored(self, executor, parser, tmp_data_dir):
        """Comments in style files are ignored."""
        style_path = tmp_data_dir / "styles.txt"
        style_path.write_text('-- This is a comment\n{"direction": "TB"}')
        _run(executor, parser, 'type Foo { x: uint32 }')
        result = _run(executor, parser, f'graph > "out.dot" style "{style_path}"')
        assert isinstance(result, DumpResult)
        assert "rankdir=TB" in result.script

    def test_style_missing_file(self, executor, parser, tmp_data_dir):
        """Missing style file is silently ignored."""
        _run(executor, parser, 'type Foo { x: uint32 }')
        result = _run(executor, parser, 'graph > "out.dot" style "nonexistent.txt"')
        assert isinstance(result, DumpResult)
        # Should still produce valid DOT with defaults
        assert "digraph types" in result.script

    def test_title_and_style_combined(self, executor, parser, tmp_data_dir):
        """Title and style work together."""
        style_path = tmp_data_dir / "styles.txt"
        style_path.write_text('{"direction": "TB"}')
        _run(executor, parser, 'type Foo { x: uint32 }')
        result = _run(executor, parser, f'graph > "out.dot" title "My Graph" style "{style_path}"')
        assert isinstance(result, DumpResult)
        assert 'label="My Graph"' in result.script
        assert "rankdir=TB" in result.script


# ==== Phase 5: Path-To Queries ====

class TestPathToParser:
    """Test parsing of path-to syntax."""

    def test_parse_path_to_single(self, parser):
        r = parser.parse('graph Boss to NPC')
        assert isinstance(r, GraphQuery)
        assert r.focus_type == "Boss"
        assert r.path_to == ["NPC"]

    def test_parse_path_to_multiple(self, parser):
        r = parser.parse('graph Boss to [NPC, Creature]')
        assert r.path_to == ["NPC", "Creature"]

    def test_parse_path_to_with_output(self, parser):
        r = parser.parse('graph Boss to NPC > "out.dot"')
        assert r.path_to == ["NPC"]
        assert r.output_file == "out.dot"

    def test_parse_path_to_with_sort(self, parser):
        r = parser.parse('graph Boss to NPC sort by source')
        assert r.path_to == ["NPC"]
        assert r.sort_by == ["source"]


class TestPathToExecution:
    """Test path-to query execution."""

    def test_path_to_immediate_parent(self, executor, parser):
        """Path from Boss to NPC (one step) + NPC target expansion."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss to NPC')
        edges = _edges(result)
        # Linear path edge
        assert ("Boss", "Composite", "(extends)", "NPC") in edges
        # Target expansion: NPC's own field + its transitive closure
        assert ("NPC", "Composite", "dialogue", "string") in edges
        assert ("NPC", "Composite", "(extends)", "Creature") in edges
        # Creature's fields are also expanded through NPC
        assert ("Creature", "Composite", "speed", "float32") in edges
        assert len(edges) > 1  # path + target expansion

    def test_path_to_grandparent(self, executor, parser):
        """Path from Boss to Creature (two steps) + Creature target expansion."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss to Creature')
        edges = _edges(result)
        # Linear path edges
        assert ("Boss", "Composite", "(extends)", "NPC") in edges
        assert ("NPC", "Composite", "(extends)", "Creature") in edges
        # Target expansion: Creature's own fields + transitive closure
        assert ("Creature", "Composite", "speed", "float32") in edges
        assert ("Creature", "Composite", "hp", "int16") in edges
        assert ("Creature", "Composite", "(implements)", "Entity") in edges
        assert len(edges) > 2  # path + target expansion

    def test_path_to_interface(self, executor, parser):
        """Path from Boss to Entity (through Creature implements) + Entity target expansion."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss to Entity')
        edges = _edges(result)
        # Boss → NPC → Creature → Entity (linear path)
        assert ("Boss", "Composite", "(extends)", "NPC") in edges
        assert ("NPC", "Composite", "(extends)", "Creature") in edges
        assert ("Creature", "Composite", "(implements)", "Entity") in edges
        # Target expansion: Entity's transitive closure
        assert ("Entity", "Interface", "(extends)", "Identifiable") in edges
        assert ("Entity", "Interface", "(extends)", "Labelled") in edges
        assert ("Identifiable", "Interface", "id", "uint32") in edges
        assert len(edges) > 3  # path + target expansion

    def test_path_to_multiple_targets(self, executor, parser):
        """Path to multiple targets merges paths."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss to [NPC, Entity]')
        edges = _edges(result)
        # Should include path to NPC (1 edge) and path to Entity (3 edges, shared prefix)
        assert ("Boss", "Composite", "(extends)", "NPC") in edges
        assert ("NPC", "Composite", "(extends)", "Creature") in edges
        assert ("Creature", "Composite", "(implements)", "Entity") in edges

    def test_path_to_deep_interface(self, executor, parser):
        """Path to a deeply inherited interface."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss to Identifiable')
        edges = _edges(result)
        # Boss → NPC → Creature → Entity → Identifiable
        assert ("Boss", "Composite", "(extends)", "NPC") in edges
        assert ("Entity", "Interface", "(extends)", "Identifiable") in edges

    def test_path_to_unknown_target(self, executor, parser):
        """Error for unknown target type."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss to Unknown')
        assert "Unknown type" in result.message

    def test_path_to_no_path(self, executor, parser):
        """Error when no inheritance path exists."""
        _setup_boss_schema(executor, parser)
        _run(executor, parser, 'type Unrelated { x: uint32 }')
        result = _run(executor, parser, 'graph Boss to Unrelated')
        assert "No inheritance path" in result.message

    def test_path_to_requires_focus(self, executor, parser):
        """Path-to without focus type is an error."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph to NPC')
        assert "requires a focus type" in result.message

    def test_path_to_dot_output(self, executor, parser, tmp_data_dir):
        """Path-to with DOT file output."""
        _setup_boss_schema(executor, parser)
        result = _run(executor, parser, 'graph Boss to Creature > "path.dot"')
        assert isinstance(result, DumpResult)
        assert "Boss" in result.script
        assert "NPC" in result.script
        assert "Creature" in result.script
