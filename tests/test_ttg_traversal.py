"""Tests for TTG provider traversal through enums, dicts, and multi-segment paths."""

import pytest

from typed_tables.types import TypeRegistry
from typed_tables.storage import StorageManager
from typed_tables.parsing.query_parser import QueryParser
from typed_tables.query_executor import QueryExecutor
from typed_tables.ttg.provider import DatabaseProvider, EdgeInfo
from typed_tables.ttg.types import GraphConfig
from typed_tables.ttg.ttgc_parser import TTGCParser


@pytest.fixture
def parser():
    p = QueryParser()
    p.build(debug=False, write_tables=False)
    return p


def setup_db(tmp_path, parser, schema_ttq, data_ttq=""):
    """Set up a database with the given schema and data, return (storage, registry)."""
    registry = TypeRegistry()
    storage = StorageManager(tmp_path / "data", registry)
    executor = QueryExecutor(storage, registry)

    def run(query_str):
        for q in parser.parse_program(query_str):
            executor.execute(q)

    run(schema_ttq)
    if data_ttq:
        run(data_ttq)
    return storage, registry, executor


def make_config(ttgc_text):
    """Parse a TTGC config string."""
    p = TTGCParser()
    p.build()
    return p.parse(ttgc_text)


# ---- TTGC Parser: multi-segment dotted paths ----

class TestTTGCMultiSegmentPaths:
    def test_two_segment_path(self):
        config = make_config("""
            selector { docs: Doc }
            axis { field1: docs.root }
            identity { default: name }
        """)
        assert config.axes["field1"] == ["docs.root"]

    def test_three_segment_path(self):
        config = make_config("""
            selector { docs: Doc }
            axis { field1: docs.root.object }
            identity { default: name }
        """)
        assert config.axes["field1"] == ["docs.root.object"]

    def test_four_segment_path(self):
        config = make_config("""
            selector { docs: Doc }
            axis { field1: docs.root.object.entries }
            identity { default: name }
        """)
        assert config.axes["field1"] == ["docs.root.object.entries"]

    def test_six_segment_path(self):
        config = make_config("""
            selector { docs: Doc }
            axis { field1: docs.root.object.entries.value.array }
            identity { default: name }
        """)
        assert config.axes["field1"] == ["docs.root.object.entries.value.array"]

    def test_multi_segment_path_list(self):
        config = make_config("""
            selector { docs: Doc, entries: Entry }
            axis { field1: [docs.root.object, entries.value.array.elements] }
            identity { default: name }
        """)
        assert config.axes["field1"] == ["docs.root.object", "entries.value.array.elements"]


# ---- Enum traversal ----

class TestEnumTraversal:
    def test_2seg_enum_field_follows_all_variants(self, tmp_path, parser):
        """A 2-segment path ending at an enum field follows all variants to find nodes."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            forward type Node
            enum Value {
                none,
                leaf(data: uint8),
                ref(target: Node)
            }
            type Node { name: string, child: Value }
        """, """
            create Node(name="A", child=.ref(target=Node(name="B", child=.none)))
        """)

        config = make_config("""
            selector { nodes: Node }
            axis { child_edge: nodes.child }
            identity { default: name }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("child_edge")
        # A.child is .ref(target=Node("B")) — should follow through to B
        assert len(edges) == 1
        assert edges[0].source_id == "A"
        assert edges[0].target_id == "B"

    def test_3seg_enum_variant_filter(self, tmp_path, parser):
        """A 3-segment path with variant name filters to that variant only."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            forward type Node
            enum Value {
                none,
                ref(target: Node)
            }
            type Node { name: string, child: Value }
        """, """
            create Node(name="A", child=.ref(target=Node(name="B", child=.none)))
            create Node(name="C", child=.none)
        """)

        config = make_config("""
            selector { nodes: Node }
            axis { refs: nodes.child.ref }
            identity { default: name }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("refs")
        # Only A has .ref variant, C has .none
        sources = {e.source_id for e in edges}
        assert "A" in sources
        assert "C" not in sources

    def test_4seg_enum_variant_field(self, tmp_path, parser):
        """A 4-segment path navigates variant name then field."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            forward type Node
            enum Value {
                none,
                ref(target: Node)
            }
            type Node { name: string, child: Value }
        """, """
            create Node(name="A", child=.ref(target=Node(name="B", child=.none)))
        """)

        config = make_config("""
            selector { nodes: Node }
            axis { ref_targets: nodes.child.ref.target }
            identity { default: name }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("ref_targets")
        assert len(edges) == 1
        assert edges[0].source_id == "A"
        assert edges[0].target_id == "B"

    def test_cstyle_enum_no_traversal(self, tmp_path, parser):
        """C-style enums produce no edges (no associated values)."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            enum Color { red, green, blue }
            type Pixel { name: string, color: Color }
        """, """
            create Pixel(name="p1", color=.red)
        """)

        config = make_config("""
            selector { pixels: Pixel }
            axis { colors: pixels.color }
            identity { default: name }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("colors")
        assert len(edges) == 0

    def test_bare_variant_skipped(self, tmp_path, parser):
        """Bare (no-field) variant records are safely skipped."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            forward type Node
            enum Value {
                none,
                ref(target: Node)
            }
            type Node { name: string, child: Value }
        """, """
            create Node(name="X", child=.none)
        """)

        config = make_config("""
            selector { nodes: Node }
            axis { child_edge: nodes.child }
            identity { default: name }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("child_edge")
        assert len(edges) == 0

    def test_null_enum_field_skipped(self, tmp_path, parser):
        """Null enum fields produce no edges."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            forward type Node
            enum Value {
                none,
                ref(target: Node)
            }
            type Node { name: string, child: Value }
        """, """
            create Node(name="X")
        """)

        config = make_config("""
            selector { nodes: Node }
            axis { child_edge: nodes.child }
            identity { default: name }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("child_edge")
        assert len(edges) == 0


# ---- Dict traversal ----

class TestDictTraversal:
    def test_dict_field_reaches_entry_nodes(self, tmp_path, parser):
        """Dict field traversal reaches entry composite nodes."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            type Item { name: string, tags: {string: uint8} }
        """, """
            create Item(name="A", tags={"x": 1, "y": 2})
        """)

        config = make_config("""
            selector { items: Item, entries: Dict_string_uint8 }
            axis { item_tags: items.tags }
            identity { default: name, entries: key }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("item_tags")
        targets = {e.target_id for e in edges}
        assert "x" in targets
        assert "y" in targets

    def test_dict_value_traversal(self, tmp_path, parser):
        """Path through dict.value reaches the value's composite."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            type Target { name: string }
            type Container { name: string, map: {string: Target} }
        """, """
            create Container(name="C", map={"first": Target(name="T1"), "second": Target(name="T2")})
        """)

        config = make_config("""
            selector { containers: Container, targets: Target }
            axis { map_values: containers.map.value }
            identity { default: name }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("map_values")
        targets = {e.target_id for e in edges}
        assert "T1" in targets
        assert "T2" in targets


# ---- Combined enum + dict traversal ----

class TestEnumDictCombined:
    def test_enum_then_dict(self, tmp_path, parser):
        """Navigate enum variant → dict field → entry nodes."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            enum JV {
                null_val,
                object(entries: {string: JV})
            }
            type Doc { name: string, root: JV }
        """, """
            create Doc(name="d1", root=.object(entries={"a": .null_val, "b": .null_val}))
        """)

        config = make_config("""
            selector { docs: Doc, entries: Dict_string_JV }
            axis { obj_entries: docs.root.object.entries }
            identity { default: name, entries: key }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("obj_entries")
        targets = {e.target_id for e in edges}
        assert "a" in targets
        assert "b" in targets

    def test_json_like_schema(self, tmp_path, parser):
        """Full JSON-like schema: doc → enum → dict → recursive enum."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            enum JsonValue {
                null_val,
                number(value: float64),
                str_val(value: string),
                object(entries: {string: JsonValue}),
                array(elements: JsonValue[])
            }
            type JsonDocument { name: string, root: JsonValue }
        """, """
            create JsonDocument(
                name="config",
                root=.object(entries={
                    "host": .str_val(value="localhost"),
                    "port": .number(value=8080)
                })
            )
        """)

        config = make_config("""
            selector { documents: JsonDocument, entries: Dict_string_JsonValue }
            axis { object_entries: documents.root.object.entries }
            identity { default: name, entries: key }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("object_entries")
        targets = {e.target_id for e in edges}
        assert "host" in targets
        assert "port" in targets
        assert len(edges) == 2  # two edges from "config"
        assert all(e.source_id == "config" for e in edges)

    def test_nested_dict_via_entry_value(self, tmp_path, parser):
        """Traverse entries.value → another enum object → entries (nested objects)."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            enum JsonValue {
                null_val,
                number(value: float64),
                object(entries: {string: JsonValue})
            }
            type JsonDocument { name: string, root: JsonValue }
        """, """
            create JsonDocument(
                name="config",
                root=.object(entries={
                    "db": .object(entries={
                        "host": .null_val,
                        "port": .null_val
                    })
                })
            )
        """)

        config = make_config("""
            selector {
                documents: JsonDocument,
                entries: Dict_string_JsonValue
            }
            axis {
                object_entries: documents.root.object.entries,
                nested_objects: entries.value.object.entries
            }
            identity { default: name, entries: key }
        """)

        provider = DatabaseProvider(storage, registry, config)

        # First level: config → db entry
        obj_edges = provider.get_all_edges_for_axis("object_entries")
        assert len(obj_edges) == 1
        assert obj_edges[0].source_id == "config"
        assert obj_edges[0].target_id == "db"

        # Second level: db entry → host, port entries
        nested_edges = provider.get_all_edges_for_axis("nested_objects")
        nested_targets = {e.target_id for e in nested_edges}
        assert "host" in nested_targets
        assert "port" in nested_targets
        nested_sources = {e.source_id for e in nested_edges}
        assert "db" in nested_sources


# ---- Composite ref traversal with remaining segments ----

class TestCompositeRefTraversal:
    def test_composite_ref_then_field(self, tmp_path, parser):
        """Path through a composite ref field continues to next field."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            type Address { name: string }
            type Person { name: string, address: Address }
        """, """
            create Person(name="Alice", address=Address(name="Home"))
        """)

        config = make_config("""
            selector { people: Person, addresses: Address }
            axis { home: people.address }
            identity { default: name }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("home")
        assert len(edges) == 1
        assert edges[0].source_id == "Alice"
        assert edges[0].target_id == "Home"


# ---- Array field traversal ----

class TestArrayTraversal:
    def test_array_elements_to_nodes(self, tmp_path, parser):
        """2-segment array path resolves elements to selector nodes."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            type Task { name: string }
            type Project { name: string, tasks: Task[] }
        """, """
            create Project(name="P", tasks=[Task(name="T1"), Task(name="T2")])
        """)

        config = make_config("""
            selector { projects: Project, tasks: Task }
            axis { has_tasks: projects.tasks }
            identity { default: name }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("has_tasks")
        targets = {e.target_id for e in edges}
        assert "T1" in targets
        assert "T2" in targets


# ---- Existing 2-segment backward compatibility ----

class TestBackwardCompatibility:
    def test_simple_composite_ref_2seg(self, tmp_path, parser):
        """Original 2-segment composite ref still works."""
        storage, registry, executor = setup_db(tmp_path, parser, """
            type Parent { name: string }
            type Child { name: string, parent: Parent }
        """, """
            create Child(name="C", parent=Parent(name="P"))
        """)

        config = make_config("""
            selector { children: Child, parents: Parent }
            axis { parent_of: children.parent }
            identity { default: name }
        """)

        provider = DatabaseProvider(storage, registry, config)
        edges = provider.get_all_edges_for_axis("parent_of")
        assert len(edges) == 1
        assert edges[0].source_id == "C"
        assert edges[0].target_id == "P"
