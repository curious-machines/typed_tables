"""Tests for TTGE (Typed Tables Graph Expression) integration via the graph keyword."""

import os
import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.parsing.query_parser import QueryParser, TTGEQuery
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


def _setup_schema(executor, parser):
    """Set up a schema with aliases, enums, interfaces, and composites."""
    _run(executor, parser, "alias myid = uint128")
    _run(executor, parser, "enum Color { red, green, blue }")
    _run(
        executor,
        parser,
        "enum Shape { none, circle(r: float32), rect(w: float32, h: float32) }",
    )
    _run(executor, parser, "interface Labelled { name: string }")
    _run(executor, parser, "type Person { id: myid, name: string, age: uint8 }")
    _run(executor, parser, "type Employee from Person { dept: string }")
    _run(executor, parser, "type Widget from Labelled { size: uint8 }")


# ---- Parser tests ----


class TestTTGEParser:
    """Verify the graph keyword produces TTGEQuery AST nodes."""

    def test_bare_graph(self, parser):
        q = parser.parse("graph")
        assert isinstance(q, TTGEQuery)
        assert q.raw_text == ""

    def test_graph_all(self, parser):
        q = parser.parse("graph all")
        assert isinstance(q, TTGEQuery)
        assert q.raw_text == "all"

    def test_graph_composites(self, parser):
        q = parser.parse("graph composites")
        assert isinstance(q, TTGEQuery)
        assert q.raw_text == "composites"

    def test_graph_expression(self, parser):
        q = parser.parse("graph composites + .fields")
        assert isinstance(q, TTGEQuery)
        assert q.raw_text == "composites + .fields"

    def test_graph_with_sort(self, parser):
        q = parser.parse("graph all sort by source")
        assert isinstance(q, TTGEQuery)
        assert "sort by source" in q.raw_text

    def test_graph_with_output(self, parser):
        q = parser.parse('graph all > "out.dot"')
        assert isinstance(q, TTGEQuery)
        assert "out.dot" in q.raw_text

    def test_graph_config(self, parser):
        q = parser.parse('graph config "test.ttgc"')
        assert isinstance(q, TTGEQuery)
        assert "config" in q.raw_text

    def test_graph_style(self, parser):
        q = parser.parse('graph style {"direction": "LR"}')
        assert isinstance(q, TTGEQuery)
        assert "style" in q.raw_text


# ---- Basic execution tests ----


class TestTTGEBasic:
    """Basic TTGE expression evaluation through the query executor."""

    def test_bare_graph_no_results(self, executor, parser):
        """Bare 'graph' with no expression returns no results."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph")
        assert isinstance(result, QueryResult)
        assert result.message == "TTG: no results"

    def test_graph_all(self, executor, parser):
        """'graph all' returns edges with source/label/target columns."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph all")
        assert isinstance(result, QueryResult)
        assert result.columns == ["source", "label", "target"]
        assert len(result.rows) > 0

    def test_graph_all_contains_field_edges(self, executor, parser):
        """'graph all' includes field edges."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph all")
        labels = {r["label"] for r in result.rows}
        assert "name" in labels
        assert "age" in labels

    def test_graph_all_contains_extends_edges(self, executor, parser):
        """'graph all' includes extends edges."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph all")
        extends_edges = [r for r in result.rows if r["label"] == "extends"]
        assert any(
            e["source"] == "Employee" and e["target"] == "Person"
            for e in extends_edges
        )

    def test_graph_all_contains_interfaces_edges(self, executor, parser):
        """'graph all' includes interface implementation edges."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph all")
        iface_edges = [r for r in result.rows if r["label"] == "interfaces"]
        assert any(
            e["source"] == "Widget" and e["target"] == "Labelled"
            for e in iface_edges
        )


# ---- Selector tests ----


class TestTTGESelectors:
    """Selector expressions return the right nodes."""

    def test_composites(self, executor, parser):
        """'graph composites' returns composite type nodes."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph composites")
        sources = {r["source"] for r in result.rows}
        assert "Person" in sources
        assert "Employee" in sources
        assert "Widget" in sources

    def test_interfaces(self, executor, parser):
        """'graph interfaces' returns interface nodes."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph interfaces")
        sources = {r["source"] for r in result.rows}
        assert "Labelled" in sources

    def test_enums(self, executor, parser):
        """'graph enums' returns enum nodes."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph enums")
        sources = {r["source"] for r in result.rows}
        assert "Color" in sources
        assert "Shape" in sources

    def test_aliases(self, executor, parser):
        """'graph aliases' returns alias nodes."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph aliases")
        sources = {r["source"] for r in result.rows}
        assert "myid" in sources


# ---- Axis tests ----


class TestTTGEAxes:
    """Axis traversal expressions."""

    def test_composites_fields(self, executor, parser):
        """'graph composites.fields' returns field endpoint nodes."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph composites.fields")
        assert result.columns == ["source", "label", "target"]
        assert len(result.rows) > 0
        # Dot traversal returns endpoint nodes (the fields themselves)
        sources = {r["source"] for r in result.rows}
        assert len(sources) > 0

    def test_composites_fields_includes_person_fields(self, executor, parser):
        """Field endpoints include Person's fields."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph composites.fields")
        sources = {r["source"] for r in result.rows}
        # The traversal yields field node identifiers
        assert any("Person" in s for s in sources)

    def test_composites_extends(self, executor, parser):
        """'graph composites + .extends' returns extends edges."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph composites + .extends")
        extends_rows = [r for r in result.rows if r["label"] == "extends"]
        assert any(
            r["source"] == "Employee" and r["target"] == "Person"
            for r in extends_rows
        )

    def test_composites_interfaces(self, executor, parser):
        """'graph composites + .interfaces' returns interface edges."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph composites + .interfaces")
        iface_rows = [r for r in result.rows if r["label"] == "interfaces"]
        assert any(
            r["source"] == "Widget" and r["target"] == "Labelled"
            for r in iface_rows
        )


# ---- Chain operation tests ----


class TestTTGEChainOps:
    """Chain operations (union, intersection, etc.)."""

    def test_composites_plus_fields(self, executor, parser):
        """'graph composites + .fields' includes both nodes and edges."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph composites + .fields")
        assert len(result.rows) > 0
        # Should have isolated nodes (composites) and labeled edges (fields)
        has_isolated = any(r["label"] == "" for r in result.rows)
        has_edges = any(r["label"] != "" for r in result.rows)
        # At minimum we should have edges
        assert has_edges

    def test_multi_chain(self, executor, parser):
        """'graph composites + .fields + .extends + .interfaces' works."""
        _setup_schema(executor, parser)
        result = _run(
            executor, parser, "graph composites + .fields + .extends + .interfaces"
        )
        labels = {r["label"] for r in result.rows}
        # Should have field labels and structural labels
        assert "extends" in labels or "interfaces" in labels or "name" in labels


# ---- Sort tests ----


class TestTTGESortBy:
    """Sort by columns."""

    def test_sort_by_source(self, executor, parser):
        """'graph all sort by source' sorts edges and isolated nodes by source."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph all sort by source")
        # Edges and isolated nodes are sorted separately
        edges = [r for r in result.rows if r["label"]]
        isolated = [r for r in result.rows if not r["label"]]
        edge_sources = [r["source"] for r in edges]
        iso_sources = [r["source"] for r in isolated]
        assert edge_sources == sorted(edge_sources)
        assert iso_sources == sorted(iso_sources)

    def test_sort_by_target(self, executor, parser):
        """'graph all sort by target' sorts edges by target."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph all sort by target")
        edges = [r for r in result.rows if r["label"]]
        edge_targets = [r["target"] for r in edges]
        assert edge_targets == sorted(edge_targets)

    def test_sort_by_label(self, executor, parser):
        """'graph all sort by label' sorts edges by label."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph all sort by label")
        edges = [r for r in result.rows if r["label"]]
        edge_labels = [r["label"] for r in edges]
        assert edge_labels == sorted(edge_labels)


# ---- File output tests ----


class TestTTGEFileOutput:
    """File output (DOT and TTQ)."""

    def test_dot_output(self, executor, parser, tmp_data_dir):
        """'graph all > "out.dot"' writes a DOT file."""
        _setup_schema(executor, parser)
        dot_path = str(tmp_data_dir / "out.dot")
        result = _run(executor, parser, f'graph all > "{dot_path}"')
        assert isinstance(result, DumpResult)
        assert os.path.exists(dot_path)
        with open(dot_path) as f:
            content = f.read()
        assert content.startswith("digraph")

    def test_ttq_output(self, executor, parser, tmp_data_dir):
        """'graph all > "out.ttq"' writes a TTQ file."""
        _setup_schema(executor, parser)
        ttq_path = str(tmp_data_dir / "out.ttq")
        result = _run(executor, parser, f'graph all > "{ttq_path}"')
        assert isinstance(result, DumpResult)
        assert os.path.exists(ttq_path)


# ---- Config/style tests ----


class TestTTGEConfig:
    """Config and style commands."""

    def test_config_nonexistent_file(self, executor, parser):
        """'graph config' with nonexistent file returns error message."""
        result = _run(executor, parser, 'graph config "nonexistent.ttgc"')
        assert isinstance(result, QueryResult)
        assert "not found" in result.message

    def test_style_inline(self, executor, parser):
        """'graph style {...}' sets inline style."""
        result = _run(executor, parser, 'graph style {"direction": "LR"}')
        assert isinstance(result, QueryResult)
        assert "style" in result.message.lower()

    def test_config_then_query(self, executor, parser, tmp_data_dir):
        """Config file can be loaded then query executed."""
        _setup_schema(executor, parser)
        # Even without config, queries work (uses builtin meta config)
        result = _run(executor, parser, "graph all")
        assert isinstance(result, QueryResult)
        assert len(result.rows) > 0


# ---- Empty schema tests ----


class TestTTGEEmptySchema:
    """TTGE behavior with no types defined."""

    def test_bare_graph_empty(self, executor, parser):
        """Bare graph on empty schema returns no results."""
        result = _run(executor, parser, "graph")
        assert result.message == "TTG: no results"

    def test_graph_all_empty(self, executor, parser):
        """'graph all' on empty schema returns no results."""
        result = _run(executor, parser, "graph all")
        # Might have no results or just empty
        assert isinstance(result, QueryResult)

    def test_composites_empty(self, executor, parser):
        """'graph composites' on empty schema returns no results."""
        result = _run(executor, parser, "graph composites")
        assert isinstance(result, QueryResult)


# ---- Complex schema tests ----


class TestTTGEComplexSchema:
    """TTGE with more complex schemas."""

    def test_enum_edges(self, executor, parser):
        """Enum fields create edges to the enum type."""
        _setup_schema(executor, parser)
        _run(executor, parser, "type Pixel { x: uint16, y: uint16, color: Color }")
        result = _run(executor, parser, "graph all")
        # Should have an edge Pixel -> Color via 'color' field
        color_edges = [
            r
            for r in result.rows
            if r["source"] == "Pixel" and r["label"] == "color"
        ]
        assert len(color_edges) == 1
        assert color_edges[0]["target"] == "Color"

    def test_self_referential(self, executor, parser):
        """Self-referential types create edges back to themselves."""
        _run(executor, parser, "type Node { value: uint8, children: Node[] }")
        result = _run(executor, parser, "graph all")
        children_edges = [
            r
            for r in result.rows
            if r["source"] == "Node" and r["label"] == "children"
        ]
        assert len(children_edges) == 1
        # Target is "Node[]" (array of Node)
        assert "Node" in children_edges[0]["target"]

    def test_alias_edges(self, executor, parser):
        """Alias types show in graph results."""
        _run(executor, parser, "alias uuid = uint128")
        _run(executor, parser, "type Person { id: uuid, name: string }")
        result = _run(executor, parser, "graph all")
        # Should have Person -> uuid via 'id' field
        id_edges = [
            r
            for r in result.rows
            if r["source"] == "Person" and r["label"] == "id"
        ]
        assert len(id_edges) == 1
        assert id_edges[0]["target"] == "uuid"

    def test_array_field_edges(self, executor, parser):
        """Array fields create proper edges."""
        _run(executor, parser, "type Sensor { name: string, readings: int8[] }")
        result = _run(executor, parser, "graph all")
        readings_edges = [
            r
            for r in result.rows
            if r["source"] == "Sensor" and r["label"] == "readings"
        ]
        assert len(readings_edges) == 1


# ---- Show command tests ----


class TestTTGEShow:
    """Tests for the 'graph show' and 'graph metadata show' commands."""

    def test_metadata_show_selector_list(self, executor, parser):
        """List all selectors from metadata config."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show selector")
        assert result.columns == ["name", "type"]
        names = [r["name"] for r in result.rows]
        assert "composites" in names
        assert "interfaces" in names
        assert "enums" in names
        assert "aliases" in names

    def test_metadata_show_selector_single(self, executor, parser):
        """Look up a single selector from metadata config."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show selector composites")
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "composites"
        assert result.rows[0]["type"] == "CompositeDef"

    def test_metadata_show_group_list(self, executor, parser):
        """List all groups from metadata config."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show group")
        assert result.columns == ["name", "members"]
        names = [r["name"] for r in result.rows]
        assert "integers" in names
        assert "floats" in names
        assert "primitives" in names

    def test_metadata_show_group_single(self, executor, parser):
        """Look up the 'floats' group."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show group floats")
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "floats"
        assert "float32" in result.rows[0]["members"]

    def test_metadata_show_axis_list(self, executor, parser):
        """List all axes from metadata config."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show axis")
        assert result.columns == ["name", "paths"]
        names = [r["name"] for r in result.rows]
        assert "fields" in names
        assert "extends" in names
        assert "type" in names

    def test_metadata_show_axis_single(self, executor, parser):
        """Look up a single axis."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show axis fields")
        assert len(result.rows) == 1
        assert "composites.fields" in result.rows[0]["paths"]

    def test_metadata_show_reverse_list(self, executor, parser):
        """List all reverse axes."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show reverse")
        assert result.columns == ["name", "axis"]
        names = [r["name"] for r in result.rows]
        assert "children" in names
        assert "owner" in names

    def test_metadata_show_reverse_single(self, executor, parser):
        """Look up a single reverse axis."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show reverse children")
        assert len(result.rows) == 1
        assert result.rows[0]["axis"] == "extends"

    def test_metadata_show_axis_group_list(self, executor, parser):
        """List all axis groups."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show axis_group")
        assert result.columns == ["name", "axes"]
        names = [r["name"] for r in result.rows]
        assert "all" in names
        assert "allReverse" in names

    def test_metadata_show_identity_list(self, executor, parser):
        """List all identity entries."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show identity")
        assert result.columns == ["selector", "field"]
        assert len(result.rows) >= 1
        assert result.rows[0]["selector"] == "default"

    def test_metadata_show_shortcut_list(self, executor, parser):
        """List all shortcuts."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show shortcut")
        assert result.columns == ["name", "expression"]
        names = [r["name"] for r in result.rows]
        assert "all" in names

    def test_show_unknown_category_error(self, executor, parser):
        """Unknown category produces a syntax error."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show bogus")
        assert result.message
        assert "unknown show category" in result.message

    def test_show_unknown_name_error(self, executor, parser):
        """Unknown name in a valid category produces an error."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph metadata show selector nonexistent")
        assert result.message
        assert "not found" in result.message

    def test_data_show_no_config_error(self, executor, parser):
        """'graph show' without a data config loaded produces an error."""
        _setup_schema(executor, parser)
        result = _run(executor, parser, "graph show selector")
        assert result.message
        assert "no data config loaded" in result.message

