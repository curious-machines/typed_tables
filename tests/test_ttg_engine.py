"""Tests for the TTG engine — config, style, session state, expression evaluation."""

import os
import tempfile

import pytest

from typed_tables.ttg.engine import TTGEngine
from typed_tables.ttg.types import FileResult, GraphConfig, GraphResult
from typed_tables.parsing.query_parser import QueryParser
from typed_tables.query_executor import QueryExecutor, QueryResult


@pytest.fixture
def engine():
    """Create a TTG engine with no backing storage (for config/style tests)."""
    return TTGEngine(storage=None, registry=None)


@pytest.fixture
def db_engine(tmp_path):
    """Create a TTG engine backed by a real database with test types."""
    from typed_tables.types import TypeRegistry
    from typed_tables.storage import StorageManager

    registry = TypeRegistry()
    storage = StorageManager(tmp_path / "data", registry)

    # Set up types via executor
    parser = QueryParser()
    parser.build(debug=False, write_tables=False)
    executor = QueryExecutor(storage, registry)

    def run(query_str):
        for q in parser.parse_program(query_str):
            executor.execute(q)

    run("""
        interface Entity { name: string }
        interface Sizeable { width: float32, height: float32 }
        type Person from Entity { age: uint8 }
        type Employee from Person { department: string, title: string }
        type Team { name: string, members: Employee[] }
    """)

    engine = TTGEngine(storage, registry)
    return engine


class TestBuiltinMetaConfig:
    def test_meta_config_loaded(self, engine):
        """Built-in meta-schema config should be loaded on init."""
        assert engine._meta_config is not None
        assert isinstance(engine._meta_config, GraphConfig)

    def test_meta_config_selectors(self, engine):
        cfg = engine._meta_config
        assert "composites" in cfg.selectors
        assert cfg.selectors["composites"] == "CompositeDef"
        assert "uint8" in cfg.selectors
        assert cfg.selectors["fraction"] == "FractionDef"

    def test_meta_config_groups(self, engine):
        cfg = engine._meta_config
        assert "types" in cfg.groups
        assert "composites" in cfg.groups["types"]
        assert "all" in cfg.groups

    def test_meta_config_axes(self, engine):
        cfg = engine._meta_config
        assert "fields" in cfg.axes
        assert "composites.fields" in cfg.axes["fields"]
        assert "type" in cfg.axes
        assert cfg.axes["type"] == ["fields.type"]

    def test_meta_config_reverses(self, engine):
        cfg = engine._meta_config
        assert "children" in cfg.reverses
        assert cfg.reverses["children"] == "extends"

    def test_meta_config_axis_groups(self, engine):
        cfg = engine._meta_config
        assert "all" in cfg.axis_groups
        assert "fields" in cfg.axis_groups["all"]

    def test_meta_config_identity(self, engine):
        cfg = engine._meta_config
        assert cfg.identity["default"] == "name"

    def test_meta_config_shortcuts(self, engine):
        cfg = engine._meta_config
        assert "all" in cfg.shortcuts


class TestConfigLoading:
    def test_load_config_from_file(self, engine):
        """Load a .ttgc config file."""
        ttgc_path = os.path.join(
            os.path.dirname(__file__), "..", "scratch", "schemas", "meta-schema.ttgc"
        )
        if not os.path.exists(ttgc_path):
            pytest.skip("meta-schema.ttgc not found")

        result = engine.execute(f'config "{ttgc_path}"')
        assert "loaded config" in result
        assert engine._data_config is not None
        assert "composites" in engine._data_config.selectors

    def test_config_file_not_found(self, engine):
        with pytest.raises(FileNotFoundError, match="config file not found"):
            engine.execute('config "nonexistent.ttgc"')

    def test_meta_config_from_file(self, engine):
        """meta config loads into meta context."""
        ttgc_path = os.path.join(
            os.path.dirname(__file__), "..", "scratch", "schemas", "meta-schema.ttgc"
        )
        if not os.path.exists(ttgc_path):
            pytest.skip("meta-schema.ttgc not found")

        old_meta = engine._meta_config
        result = engine.execute(f'meta config "{ttgc_path}"')
        assert "loaded meta config" in result
        assert engine._meta_config is not None
        # Should have replaced the old meta config
        assert engine._meta_config is not old_meta


class TestStyleLoading:
    def test_inline_style(self, engine):
        result = engine.execute('style {"direction": "TB", "composite.color": "#FF0000"}')
        assert "style updated" in result
        assert engine._data_style["direction"] == "TB"
        assert engine._data_style["composite.color"] == "#FF0000"

    def test_style_file(self, engine):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ttgs", delete=False) as f:
            f.write('-- Test style\n{"direction": "LR", "title": "Test"}\n')
            style_path = f.name

        try:
            result = engine.execute(f'style "{style_path}"')
            assert "style updated" in result
            assert engine._data_style["direction"] == "LR"
            assert engine._data_style["title"] == "Test"
        finally:
            os.unlink(style_path)

    def test_style_file_with_inline_override(self, engine):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ttgs", delete=False) as f:
            f.write('{"direction": "LR"}\n')
            style_path = f.name

        try:
            result = engine.execute(f'style "{style_path}" {{"direction": "TB"}}')
            assert "style updated" in result
            # Inline overrides file
            assert engine._data_style["direction"] == "TB"
        finally:
            os.unlink(style_path)

    def test_style_file_not_found(self, engine):
        with pytest.raises(FileNotFoundError, match="style file not found"):
            engine.execute('style "nonexistent.ttgs"')

    def test_meta_style_inline(self, engine):
        result = engine.execute('meta style {"direction": "TB"}')
        assert "meta style updated" in result
        assert engine._meta_style["direction"] == "TB"


class TestResetSession:
    def test_reset_clears_data(self, engine):
        engine._data_config = GraphConfig()
        engine._data_style = {"foo": "bar"}
        engine.reset_session()
        assert engine._data_config is None
        assert engine._data_style == {}
        # Meta config should be reloaded
        assert engine._meta_config is not None
        assert "composites" in engine._meta_config.selectors


class TestExprStubRequiresConfig:
    def test_data_expr_without_config_errors(self, engine):
        """Expression evaluation errors when no data config is loaded."""
        with pytest.raises(RuntimeError, match="no config loaded for data context"):
            engine.execute("composites")

    def test_metadata_expr_uses_builtin_config(self, engine):
        """meta expressions use the built-in meta-schema config."""
        result = engine.execute("meta composites")
        assert isinstance(result, GraphResult)


class TestScriptExecution:
    def test_execute_script(self, engine):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ttg", delete=False) as f:
            f.write('style {"direction": "LR"}\n')
            script_path = f.name

        try:
            result = engine.execute(f'execute "{script_path}"')
            assert "executed" in result
            assert engine._data_style["direction"] == "LR"
        finally:
            os.unlink(script_path)

    def test_execute_auto_extension(self, engine):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".ttg", delete=False, dir=tempfile.gettempdir()
        ) as f:
            f.write('style {"direction": "TB"}\n')
            full_path = f.name
            base_path = full_path[:-4]  # strip .ttg

        try:
            result = engine.execute(f'execute "{base_path}"')
            assert "executed" in result
        finally:
            os.unlink(full_path)

    def test_execute_cycle_detection(self, engine):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ttg", delete=False) as f:
            # Script that tries to execute itself
            f.write(f'execute "{f.name}"\n')
            script_path = f.name

        try:
            with pytest.raises(RuntimeError, match="cycle"):
                engine.execute(f'execute "{script_path}"')
        finally:
            os.unlink(script_path)

    def test_execute_not_found(self, engine):
        with pytest.raises(FileNotFoundError, match="script not found"):
            engine.execute('execute "nonexistent.ttg"')

    def test_execute_relative_path(self, engine):
        """Scripts resolve paths relative to their own directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a style file and a script that references it
            style_path = os.path.join(tmpdir, "my.ttgs")
            with open(style_path, "w") as f:
                f.write('{"direction": "LR"}\n')

            script_path = os.path.join(tmpdir, "setup.ttg")
            with open(script_path, "w") as f:
                f.write('style "my.ttgs"\n')

            result = engine.execute(f'execute "{script_path}"')
            assert "executed" in result
            assert engine._data_style["direction"] == "LR"

    def test_execute_multi_statement_script(self, engine):
        """Scripts can contain multiple statements."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ttg", delete=False) as f:
            f.write('-- Setup script\n')
            f.write('style {"direction": "TB"}\n')
            f.write('style {"title": "My Graph"}\n')
            script_path = f.name

        try:
            result = engine.execute(f'execute "{script_path}"')
            assert "executed" in result
            assert engine._data_style["direction"] == "TB"
            assert engine._data_style["title"] == "My Graph"
        finally:
            os.unlink(script_path)

    def test_execute_nested_scripts(self, engine):
        """Scripts can execute other scripts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            inner_path = os.path.join(tmpdir, "inner.ttg")
            with open(inner_path, "w") as f:
                f.write('style {"title": "Nested"}\n')

            outer_path = os.path.join(tmpdir, "outer.ttg")
            with open(outer_path, "w") as f:
                f.write('style {"direction": "TB"}\n')
                f.write(f'execute "inner.ttg"\n')

            result = engine.execute(f'execute "{outer_path}"')
            assert "executed" in result
            assert engine._data_style["direction"] == "TB"
            assert engine._data_style["title"] == "Nested"

    def test_execute_via_ttq_graph(self, db_engine, tmp_path):
        """Test graph execute via TTQ parser."""
        script_path = str(tmp_path / "setup.ttg")
        with open(script_path, "w") as f:
            f.write('meta style {"title": "Via TTQ"}\n')

        executor = QueryExecutor(db_engine.storage, db_engine.registry)
        parser = QueryParser()
        parser.build(debug=False, write_tables=False)

        q = parser.parse(f'graph execute "{script_path}"')
        result = executor.execute(q)
        assert isinstance(result, QueryResult)


# ---- Expression evaluation tests (metadata context) ----


class TestMetadataSelectors:
    def test_composites_selector(self, db_engine):
        """meta composites returns all composite types."""
        result = db_engine.execute("meta composites")
        assert isinstance(result, GraphResult)
        assert "Person" in result.isolated_nodes
        assert "Employee" in result.isolated_nodes
        assert "Team" in result.isolated_nodes

    def test_interfaces_selector(self, db_engine):
        result = db_engine.execute("meta interfaces")
        assert isinstance(result, GraphResult)
        assert "Entity" in result.isolated_nodes
        assert "Sizeable" in result.isolated_nodes

    def test_composites_name_filter(self, db_engine):
        """meta composites{name=Person} returns only Person."""
        result = db_engine.execute("meta composites{name=Person}")
        assert isinstance(result, GraphResult)
        assert "Person" in result.isolated_nodes
        assert "Employee" not in result.isolated_nodes

    def test_composites_name_or(self, db_engine):
        result = db_engine.execute("meta composites{name=Person|Team}")
        assert isinstance(result, GraphResult)
        nodes = set(result.isolated_nodes)
        assert "Person" in nodes
        assert "Team" in nodes
        assert "Employee" not in nodes

    def test_composites_name_negated(self, db_engine):
        result = db_engine.execute("meta composites{name=!Person}")
        assert isinstance(result, GraphResult)
        nodes = set(result.isolated_nodes)
        assert "Person" not in nodes
        assert "Employee" in nodes
        assert "Team" in nodes


class TestMetadataAxes:
    def test_composites_plus_fields(self, db_engine):
        """composites + .fields adds field nodes with edges."""
        result = db_engine.execute("meta composites{name=Person} + .fields")
        assert isinstance(result, GraphResult)
        assert len(result.edges) > 0
        person_edges = [e for e in result.edges if e.source == "Person"]
        assert len(person_edges) > 0

    def test_composites_plus_extends(self, db_engine):
        """composites{name=Employee} + .extends shows inheritance."""
        result = db_engine.execute("meta composites{name=Employee} + .extends")
        assert isinstance(result, GraphResult)
        extend_edges = [e for e in result.edges if e.source == "Employee"]
        assert any(e.target == "Person" for e in extend_edges)

    def test_composites_plus_interfaces(self, db_engine):
        """composites{name=Person} + .interfaces shows interface impl."""
        result = db_engine.execute("meta composites{name=Person} + .interfaces")
        assert isinstance(result, GraphResult)
        iface_edges = [e for e in result.edges if e.source == "Person"]
        assert any(e.target == "Entity" for e in iface_edges)

    def test_dot_chaining_accumulates(self, db_engine):
        """composites{name=Person}.fields accumulates (dot = +)."""
        result = db_engine.execute("meta composites{name=Person}.fields")
        assert isinstance(result, GraphResult)
        # Dot is now accumulate: Person AND its field nodes are in the result
        all_nodes = {e.source for e in result.edges} | {e.target for e in result.edges}
        assert "Person" in all_nodes  # Source node is kept
        assert any("Person." in n for n in all_nodes)  # Field nodes added

    def test_slash_chaining_pipe(self, db_engine):
        """composites{name=Person}/fields navigates — only fields remain."""
        result = db_engine.execute("meta composites{name=Person}/fields")
        assert isinstance(result, GraphResult)
        all_items = set(result.isolated_nodes)
        assert "Person" not in all_items
        assert len(result.isolated_nodes) > 0

    def test_chain_subtract_selector(self, db_engine):
        """composites - composites{name=Person} removes Person."""
        result = db_engine.execute(
            "meta composites - composites{name=Person}"
        )
        assert isinstance(result, GraphResult)
        nodes = set(result.isolated_nodes)
        assert "Person" not in nodes
        assert "Employee" in nodes


class TestMetadataSetOperators:
    def test_union(self, db_engine):
        result = db_engine.execute(
            "meta composites{name=Person} | interfaces{name=Sizeable}"
        )
        assert isinstance(result, GraphResult)
        nodes = set(result.isolated_nodes)
        assert "Person" in nodes
        assert "Sizeable" in nodes

    def test_set_literal(self, db_engine):
        result = db_engine.execute(
            "meta {composites{name=Person}, interfaces{name=Entity}}"
        )
        assert isinstance(result, GraphResult)
        nodes = set(result.isolated_nodes)
        assert "Person" in nodes
        assert "Entity" in nodes


class TestMetadataCompactForm:
    def test_fields_edge_result(self, db_engine):
        """composites{name=Person} + .fields{edge=.name, result=.type}"""
        result = db_engine.execute(
            "meta composites{name=Person} + .fields{edge=.name, result=.type}"
        )
        assert isinstance(result, GraphResult)
        edges = result.edges
        assert len(edges) > 0
        person_edges = [e for e in edges if e.source == "Person"]
        assert len(person_edges) > 0
        labels = {e.label for e in person_edges}
        assert "name" in labels or "age" in labels


class TestMetadataDepth:
    def test_extends_depth_inf(self, db_engine):
        """composites{name=Employee} + .extends{depth=inf} follows full chain."""
        result = db_engine.execute(
            "meta composites{name=Employee} + .extends{depth=inf}"
        )
        assert isinstance(result, GraphResult)
        all_nodes = {e.source for e in result.edges} | {e.target for e in result.edges}
        all_nodes |= set(result.isolated_nodes)
        assert "Person" in all_nodes

    def test_depth_zero(self, db_engine):
        """depth=0 is a no-op."""
        result = db_engine.execute(
            "meta composites{name=Person} + .fields{depth=0}"
        )
        assert isinstance(result, GraphResult)
        field_edges = [e for e in result.edges if e.label == "fields"]
        assert len(field_edges) == 0


class TestNodeKinds:
    """Test that node_kinds are populated in GraphResult."""

    def test_composites_have_kinds(self, db_engine):
        result = db_engine.execute("meta composites")
        assert isinstance(result, GraphResult)
        for node in result.isolated_nodes:
            assert node in result.node_kinds
            assert result.node_kinds[node] == "composites"

    def test_interfaces_have_kinds(self, db_engine):
        result = db_engine.execute("meta interfaces")
        assert isinstance(result, GraphResult)
        for node in result.isolated_nodes:
            assert node in result.node_kinds
            assert result.node_kinds[node] == "interfaces"

    def test_mixed_kinds(self, db_engine):
        result = db_engine.execute(
            "meta composites{name=Person} | interfaces{name=Entity}"
        )
        assert isinstance(result, GraphResult)
        assert result.node_kinds.get("Person") == "composites"
        assert result.node_kinds.get("Entity") == "interfaces"

    def test_edge_node_kinds(self, db_engine):
        """Nodes from edge targets also get kinds."""
        result = db_engine.execute(
            "meta composites{name=Person} + .fields"
        )
        assert isinstance(result, GraphResult)
        # Person should be composites, fields should be fields
        assert result.node_kinds.get("Person") == "composites"
        field_nodes = {e.target for e in result.edges if e.source == "Person"}
        for fn in field_nodes:
            assert fn in result.node_kinds


class TestSortBy:
    """Test sort by clause."""

    def test_sort_by_source(self, db_engine):
        result = db_engine.execute(
            "meta composites + .fields sort by source"
        )
        assert isinstance(result, GraphResult)
        sources = [e.source for e in result.edges]
        assert sources == sorted(sources)

    def test_sort_by_target(self, db_engine):
        result = db_engine.execute(
            "meta composites + .fields sort by target"
        )
        assert isinstance(result, GraphResult)
        targets = [e.target for e in result.edges]
        assert targets == sorted(targets)

    def test_sort_by_source_then_label(self, db_engine):
        result = db_engine.execute(
            "meta composites + .fields + .extends sort by source, label"
        )
        assert isinstance(result, GraphResult)
        keys = [(e.source, e.label) for e in result.edges]
        assert keys == sorted(keys)


class TestDotOutput:
    """Test DOT file output."""

    def test_dot_output_basic(self, db_engine, tmp_path):
        dot_path = str(tmp_path / "test.dot")
        result = db_engine.execute(
            f'meta composites > "{dot_path}"'
        )
        assert isinstance(result, FileResult)
        assert result.path == dot_path

        content = open(dot_path).read()
        assert content.startswith("digraph types {")
        assert "rankdir=LR;" in content
        assert '"Person"' in content
        assert content.strip().endswith("}")

    def test_dot_output_with_edges(self, db_engine, tmp_path):
        dot_path = str(tmp_path / "test.dot")
        db_engine.execute(
            f'meta composites{{name=Employee}} + .extends{{edge="extends"}} > "{dot_path}"'
        )
        content = open(dot_path).read()
        assert "Employee" in content
        assert "Person" in content
        assert "style=dashed" in content  # extends edges use dashed style

    def test_dot_output_with_style(self, db_engine, tmp_path):
        db_engine.execute('meta style {"direction": "TB", "title": "My Schema"}')
        dot_path = str(tmp_path / "test.dot")
        db_engine.execute(
            f'meta composites > "{dot_path}"'
        )
        content = open(dot_path).read()
        assert "rankdir=TB;" in content
        assert 'label="My Schema"' in content

    def test_dot_auto_extension(self, db_engine, tmp_path):
        base_path = str(tmp_path / "output")
        result = db_engine.execute(
            f'meta composites > "{base_path}"'
        )
        assert isinstance(result, FileResult)
        assert result.path.endswith(".dot")

    def test_dot_field_label_edges(self, db_engine, tmp_path):
        dot_path = str(tmp_path / "test.dot")
        db_engine.execute(
            f'meta composites{{name=Person}} + .fields{{edge=.name, result=.type}} > "{dot_path}"'
        )
        content = open(dot_path).read()
        assert 'label="name"' in content or 'label="age"' in content


class TestTtqOutput:
    """Test TTQ file output."""

    def test_ttq_output_basic(self, db_engine, tmp_path):
        ttq_path = str(tmp_path / "test.ttq")
        result = db_engine.execute(
            f'meta composites > "{ttq_path}"'
        )
        assert isinstance(result, FileResult)
        assert result.path == ttq_path

        content = open(ttq_path).read()
        assert "enum NodeRole" in content
        assert "type TypeNode" in content
        assert "type Edge" in content
        assert 'create TypeNode(name="Person"' in content

    def test_ttq_output_with_edges(self, db_engine, tmp_path):
        ttq_path = str(tmp_path / "test.ttq")
        db_engine.execute(
            f'meta composites{{name=Employee}} + .extends > "{ttq_path}"'
        )
        content = open(ttq_path).read()
        assert "create Edge(" in content
        assert 'name="Employee"' in content
        assert 'name="Person"' in content


class TestIntegrationViaTTQ:
    """Test TTG via the TTQ parser (graph command delegation)."""

    def test_graph_meta_composites(self, db_engine):
        """Test via TTQ: graph meta composites."""
        executor = QueryExecutor(db_engine.storage, db_engine.registry)
        parser = QueryParser()
        parser.build(debug=False, write_tables=False)

        q = parser.parse("graph meta composites")
        result = executor.execute(q)
        assert isinstance(result, QueryResult)
        if result.rows:
            assert "source" in result.columns
