"""Tests for interface support with multiple inheritance."""

import tempfile
import shutil
from pathlib import Path

import pytest

from typed_tables.types import (
    CompositeTypeDefinition,
    FieldDefinition,
    InterfaceTypeDefinition,
    PrimitiveTypeDefinition,
    TypeRegistry,
)
from typed_tables.parsing.query_parser import (
    CreateAliasQuery,
    CreateEnumQuery,
    CreateInstanceQuery,
    CreateInterfaceQuery,
    CreateTypeQuery,
    EnumVariantSpec,
    FieldDef,
    FieldValue,
    QueryParser,
    ShowTypesQuery,
)
from typed_tables.storage import StorageManager
from typed_tables.query_executor import QueryExecutor, QueryResult, CreateResult


@pytest.fixture
def tmp_data_dir():
    """Create a temp directory for test data."""
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def executor(tmp_data_dir):
    """Create a QueryExecutor with fresh registry and storage."""
    registry = TypeRegistry()
    storage = StorageManager(tmp_data_dir, registry)
    return QueryExecutor(storage, registry)


@pytest.fixture
def parser():
    """Create a fresh QueryParser."""
    p = QueryParser()
    p.build(debug=False, write_tables=False)
    return p


class TestInterfaceTypeDefinition:
    """Tests for InterfaceTypeDefinition class."""

    def test_create_interface_type(self):
        registry = TypeRegistry()
        stub = registry.register_interface_stub("Styled")
        assert isinstance(stub, InterfaceTypeDefinition)
        assert stub.name == "Styled"
        assert stub.fields == []
        assert stub.is_interface is True
        assert stub.is_composite is False

    def test_interface_reference_size(self):
        registry = TypeRegistry()
        stub = registry.register_interface_stub("Styled")
        assert stub.reference_size == 6  # uint16 type_id + uint32 index

    def test_interface_with_fields(self):
        registry = TypeRegistry()
        stub = registry.register_interface_stub("Styled")
        string_type = registry.get("string")
        stub.fields = [
            FieldDefinition(name="fill", type_def=string_type),
            FieldDefinition(name="stroke", type_def=string_type),
        ]
        assert len(stub.fields) == 2
        assert stub.null_bitmap_size == 1
        assert stub.size_bytes > 0

    def test_register_interface_stub_idempotent(self):
        registry = TypeRegistry()
        stub1 = registry.register_interface_stub("Styled")
        stub2 = registry.register_interface_stub("Styled")
        assert stub1 is stub2

    def test_register_interface_stub_conflict(self):
        registry = TypeRegistry()
        registry.register_stub("Foo")  # composite stub
        with pytest.raises(ValueError, match="already defined"):
            registry.register_interface_stub("Foo")

    def test_is_interface_stub(self):
        registry = TypeRegistry()
        registry.register_interface_stub("Styled")
        assert registry.is_interface_stub("Styled") is True
        assert registry.is_interface_stub("Nonexistent") is False

    def test_find_implementing_types(self):
        registry = TypeRegistry()
        stub = registry.register_interface_stub("Styled")
        string_type = registry.get("string")
        stub.fields = [FieldDefinition(name="fill", type_def=string_type)]

        comp = registry.register_stub("Rect")
        comp.fields = [
            FieldDefinition(name="fill", type_def=string_type),
            FieldDefinition(name="width", type_def=registry.get("float32")),
        ]
        comp.interfaces = ["Styled"]

        results = registry.find_implementing_types("Styled")
        assert len(results) == 1
        assert results[0][0] == "Rect"

    def test_get_type_id(self):
        registry = TypeRegistry()
        id1 = registry.get_type_id("Rect")
        id2 = registry.get_type_id("Circle")
        id3 = registry.get_type_id("Rect")
        assert id1 == id3  # Same type gets same ID
        assert id1 != id2  # Different types get different IDs
        assert id1 >= 1  # IDs start from 1

    def test_get_type_name_by_id(self):
        registry = TypeRegistry()
        id1 = registry.get_type_id("Rect")
        assert registry.get_type_name_by_id(id1) == "Rect"
        assert registry.get_type_name_by_id(9999) is None


class TestInterfaceParser:
    """Tests for parsing interface-related queries."""

    def test_parse_create_interface(self, parser):
        queries = parser.parse_program('interface Styled { fill: string, stroke: string }')
        assert len(queries) == 1
        q = queries[0]
        assert isinstance(q, CreateInterfaceQuery)
        assert q.name == "Styled"
        assert len(q.fields) == 2
        assert q.fields[0].name == "fill"
        assert q.fields[0].type_name == "string"

    def test_parse_create_interface_empty(self, parser):
        queries = parser.parse_program('interface Marker')
        assert len(queries) == 1
        q = queries[0]
        assert isinstance(q, CreateInterfaceQuery)
        assert q.name == "Marker"
        assert q.fields == []

    def test_parse_create_type_from_single_parent(self, parser):
        queries = parser.parse_program('type Rect from Styled { width: float32 }')
        assert len(queries) == 1
        q = queries[0]
        assert isinstance(q, CreateTypeQuery)
        assert q.parents == ["Styled"]

    def test_parse_create_type_from_multiple_parents(self, parser):
        queries = parser.parse_program('type Rect from Styled, Positioned { width: float32 }')
        assert len(queries) == 1
        q = queries[0]
        assert isinstance(q, CreateTypeQuery)
        assert q.parents == ["Styled", "Positioned"]

    def test_parse_create_type_from_multiple_no_fields(self, parser):
        queries = parser.parse_program('type Rect from Styled, Positioned')
        assert len(queries) == 1
        q = queries[0]
        assert isinstance(q, CreateTypeQuery)
        assert q.parents == ["Styled", "Positioned"]
        assert q.fields == []

    def test_interface_keyword_reserved(self, parser):
        """The word 'interface' should be a reserved keyword."""
        queries = parser.parse_program('interface Foo { x: uint8 }')
        assert isinstance(queries[0], CreateInterfaceQuery)


class TestInterfaceExecution:
    """Tests for executing interface queries."""

    def test_create_interface(self, executor):
        result = executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="fill", type_name="string"), FieldDef(name="stroke", type_name="string")],
        ))
        assert isinstance(result, CreateResult)
        assert "Created interface" in result.message

        # Verify the interface is in the registry
        iface = executor.registry.get("Styled")
        assert isinstance(iface, InterfaceTypeDefinition)
        assert len(iface.fields) == 2

    def test_create_type_from_interface(self, executor):
        # Create interface first
        executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="fill", type_name="string"), FieldDef(name="stroke", type_name="string")],
        ))

        # Create type implementing interface
        result = executor.execute(CreateTypeQuery(
            name="Rect",
            fields=[FieldDef(name="width", type_name="float32"), FieldDef(name="height", type_name="float32")],
            parents=["Styled"],
        ))
        assert isinstance(result, CreateResult)
        assert "Created type" in result.message

        rect = executor.registry.get("Rect")
        assert isinstance(rect, CompositeTypeDefinition)
        assert len(rect.fields) == 4  # fill, stroke, width, height
        assert rect.interfaces == ["Styled"]
        assert rect.fields[0].name == "fill"
        assert rect.fields[1].name == "stroke"
        assert rect.fields[2].name == "width"
        assert rect.fields[3].name == "height"

    def test_create_type_from_multiple_interfaces(self, executor):
        executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="fill", type_name="string")],
        ))
        executor.execute(CreateInterfaceQuery(
            name="Positioned",
            fields=[FieldDef(name="x", type_name="float32"), FieldDef(name="y", type_name="float32")],
        ))

        result = executor.execute(CreateTypeQuery(
            name="Rect",
            fields=[FieldDef(name="width", type_name="float32")],
            parents=["Styled", "Positioned"],
        ))
        assert "Created type" in result.message

        rect = executor.registry.get("Rect")
        assert len(rect.fields) == 4  # fill, x, y, width
        assert set(rect.interfaces) == {"Styled", "Positioned"}

    def test_diamond_inheritance_merge(self, executor):
        """Same field from two interfaces should merge if same type."""
        executor.execute(CreateInterfaceQuery(
            name="HasName",
            fields=[FieldDef(name="name", type_name="string")],
        ))
        executor.execute(CreateInterfaceQuery(
            name="HasLabel",
            fields=[FieldDef(name="name", type_name="string")],
        ))

        result = executor.execute(CreateTypeQuery(
            name="Widget",
            fields=[FieldDef(name="value", type_name="uint8")],
            parents=["HasName", "HasLabel"],
        ))
        assert "Created type" in result.message

        widget = executor.registry.get("Widget")
        # "name" should appear only once (merged)
        field_names = [f.name for f in widget.fields]
        assert field_names.count("name") == 1
        assert "value" in field_names

    def test_field_conflict_detection(self, executor):
        """Same field name with different types should error."""
        executor.execute(CreateInterfaceQuery(
            name="HasId",
            fields=[FieldDef(name="id", type_name="uint32")],
        ))
        executor.execute(CreateInterfaceQuery(
            name="HasUuid",
            fields=[FieldDef(name="id", type_name="string")],
        ))

        result = executor.execute(CreateTypeQuery(
            name="Conflict",
            fields=[],
            parents=["HasId", "HasUuid"],
        ))
        assert "Field conflict" in result.message

    def test_cannot_instantiate_interface(self, executor):
        """Creating an instance of an interface should fail."""
        from typed_tables.parsing.query_parser import CreateInstanceQuery
        executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="fill", type_name="string")],
        ))

        result = executor.execute(CreateInstanceQuery(type_name="Styled", fields=[]))
        assert "Cannot create instance of interface" in result.message

    def test_polymorphic_query(self, executor):
        """from Interface select * should fan out across implementing types."""
        from typed_tables.parsing.query_parser import (
            CreateInstanceQuery, FieldValue, SelectQuery, SelectField,
        )

        executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="fill", type_name="string")],
        ))
        executor.execute(CreateTypeQuery(
            name="Rect",
            fields=[FieldDef(name="width", type_name="float32")],
            parents=["Styled"],
        ))
        executor.execute(CreateTypeQuery(
            name="Circle",
            fields=[FieldDef(name="r", type_name="float32")],
            parents=["Styled"],
        ))

        # Create instances
        executor.execute(CreateInstanceQuery(
            type_name="Rect",
            fields=[FieldValue(name="fill", value="red"), FieldValue(name="width", value=100.0)],
        ))
        executor.execute(CreateInstanceQuery(
            type_name="Circle",
            fields=[FieldValue(name="fill", value="blue"), FieldValue(name="r", value=25.0)],
        ))

        # Query the interface
        result = executor.execute(SelectQuery(
            table="Styled",
            fields=[SelectField(name="*")],
        ))
        assert len(result.rows) == 2
        assert "_type" in result.columns
        assert "fill" in result.columns

        # Check that both implementing types are represented
        types_seen = {r["_type"] for r in result.rows}
        assert types_seen == {"Rect", "Circle"}

        # Check field values
        fills = {r["fill"] for r in result.rows}
        assert fills == {"red", "blue"}

    def test_show_types_includes_interfaces(self, executor):
        from typed_tables.parsing.query_parser import ShowTypesQuery

        executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="fill", type_name="string")],
        ))
        executor.execute(CreateTypeQuery(
            name="Rect",
            fields=[],
            parents=["Styled"],
        ))

        result = executor.execute(ShowTypesQuery())
        type_names = [r["type"] for r in result.rows]
        assert "Styled" in type_names
        styled_row = next(r for r in result.rows if r["type"] == "Styled")
        assert styled_row["kind"] == "Interface"

    def test_describe_interface(self, executor):
        from typed_tables.parsing.query_parser import DescribeQuery, CreateInstanceQuery, FieldValue

        executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="fill", type_name="string")],
        ))
        executor.execute(CreateTypeQuery(
            name="Rect",
            fields=[FieldDef(name="width", type_name="float32")],
            parents=["Styled"],
        ))

        result = executor.execute(DescribeQuery(table="Styled"))
        props = [r["property"] for r in result.rows]
        assert "(type)" in props
        assert "fill" in props
        assert "(implements)" in props

        # Check implementing type listed
        impl_rows = [r for r in result.rows if r["property"] == "(implements)"]
        assert any(r["type"] == "Rect" for r in impl_rows)

    def test_describe_composite_shows_interfaces(self, executor):
        from typed_tables.parsing.query_parser import DescribeQuery

        executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="fill", type_name="string")],
        ))
        executor.execute(CreateTypeQuery(
            name="Rect",
            fields=[FieldDef(name="width", type_name="float32")],
            parents=["Styled"],
        ))

        result = executor.execute(DescribeQuery(table="Rect"))
        props = [r["property"] for r in result.rows]
        assert "(interface)" in props
        iface_rows = [r for r in result.rows if r["property"] == "(interface)"]
        assert any(r["type"] == "Styled" for r in iface_rows)


class TestInterfaceDump:
    """Tests for dump roundtrip with interfaces."""

    def test_dump_includes_interfaces(self, executor):
        from typed_tables.parsing.query_parser import (
            CreateInstanceQuery, FieldValue, DumpQuery,
        )

        executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="fill", type_name="string")],
        ))
        executor.execute(CreateTypeQuery(
            name="Rect",
            fields=[FieldDef(name="width", type_name="float32")],
            parents=["Styled"],
        ))
        executor.execute(CreateInstanceQuery(
            type_name="Rect",
            fields=[FieldValue(name="fill", value="red"), FieldValue(name="width", value=100.0)],
        ))

        result = executor.execute(DumpQuery())
        assert "interface Styled" in result.script
        assert "type Rect from Styled" in result.script

    def test_dump_roundtrip(self, executor, parser, tmp_data_dir):
        """Dump then re-execute should produce same data."""
        from typed_tables.parsing.query_parser import (
            CreateInstanceQuery, FieldValue, DumpQuery, SelectQuery, SelectField,
        )

        executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="fill", type_name="string")],
        ))
        executor.execute(CreateTypeQuery(
            name="Rect",
            fields=[FieldDef(name="width", type_name="float32")],
            parents=["Styled"],
        ))
        executor.execute(CreateInstanceQuery(
            type_name="Rect",
            fields=[FieldValue(name="fill", value="red"), FieldValue(name="width", value=100.0)],
        ))

        # Dump
        dump_result = executor.execute(DumpQuery())
        script = dump_result.script

        # Re-execute in a new database
        new_dir = tmp_data_dir / "roundtrip"
        registry2 = TypeRegistry()
        storage2 = StorageManager(new_dir, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        for stmt in parser.parse_program(script):
            executor2.execute(stmt)

        # Verify same structure
        styled = registry2.get("Styled")
        assert isinstance(styled, InterfaceTypeDefinition)
        assert len(styled.fields) == 1

        rect = registry2.get("Rect")
        assert isinstance(rect, CompositeTypeDefinition)
        assert rect.interfaces == ["Styled"]

        # Verify data
        result = executor2.execute(SelectQuery(
            table="Rect", fields=[SelectField(name="*")],
        ))
        assert len(result.rows) == 1
        assert result.rows[0]["fill"] == "red"

        storage2.close()


class TestInterfaceMetadata:
    """Tests for metadata persistence of interfaces."""

    def test_metadata_roundtrip(self, executor, tmp_data_dir):
        """Interfaces should survive metadata save/load."""
        from typed_tables.dump import load_registry_from_metadata

        executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="fill", type_name="string")],
        ))
        executor.execute(CreateTypeQuery(
            name="Rect",
            fields=[FieldDef(name="width", type_name="float32")],
            parents=["Styled"],
        ))

        # Load metadata in a fresh registry
        registry2 = load_registry_from_metadata(tmp_data_dir)

        styled = registry2.get("Styled")
        assert isinstance(styled, InterfaceTypeDefinition)
        assert len(styled.fields) == 1
        assert styled.fields[0].name == "fill"

        rect = registry2.get("Rect")
        assert isinstance(rect, CompositeTypeDefinition)
        assert rect.interfaces == ["Styled"]
        assert len(rect.fields) == 2  # fill + width


class TestPolymorphicQueryFiltering:
    """Tests for WHERE filtering on polymorphic interface queries."""

    def test_where_filter_on_interface(self, executor):
        from typed_tables.parsing.query_parser import (
            CreateInstanceQuery, FieldValue, SelectQuery, SelectField,
            Condition,
        )

        executor.execute(CreateInterfaceQuery(
            name="Named",
            fields=[FieldDef(name="name", type_name="string")],
        ))
        executor.execute(CreateTypeQuery(
            name="Dog",
            fields=[FieldDef(name="breed", type_name="string")],
            parents=["Named"],
        ))
        executor.execute(CreateTypeQuery(
            name="Cat",
            fields=[FieldDef(name="color", type_name="string")],
            parents=["Named"],
        ))

        executor.execute(CreateInstanceQuery(
            type_name="Dog",
            fields=[FieldValue(name="name", value="Rex"), FieldValue(name="breed", value="Lab")],
        ))
        executor.execute(CreateInstanceQuery(
            type_name="Cat",
            fields=[FieldValue(name="name", value="Whiskers"), FieldValue(name="color", value="orange")],
        ))
        executor.execute(CreateInstanceQuery(
            type_name="Dog",
            fields=[FieldValue(name="name", value="Buddy"), FieldValue(name="breed", value="Poodle")],
        ))

        # Filter by name
        result = executor.execute(SelectQuery(
            table="Named",
            fields=[SelectField(name="*")],
            where=Condition(field="name", operator="eq", value="Rex"),
        ))
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "Rex"
        assert result.rows[0]["_type"] == "Dog"

    def test_select_specific_fields_from_interface(self, executor):
        from typed_tables.parsing.query_parser import (
            CreateInstanceQuery, FieldValue, SelectQuery, SelectField,
        )

        executor.execute(CreateInterfaceQuery(
            name="Sized",
            fields=[FieldDef(name="width", type_name="float32"), FieldDef(name="height", type_name="float32")],
        ))
        executor.execute(CreateTypeQuery(
            name="Box",
            fields=[FieldDef(name="depth", type_name="float32")],
            parents=["Sized"],
        ))
        executor.execute(CreateInstanceQuery(
            type_name="Box",
            fields=[FieldValue(name="width", value=10.0), FieldValue(name="height", value=20.0), FieldValue(name="depth", value=5.0)],
        ))

        result = executor.execute(SelectQuery(
            table="Sized",
            fields=[SelectField(name="width"), SelectField(name="height")],
        ))
        assert len(result.rows) == 1
        assert "width" in result.columns
        assert "height" in result.columns


class TestShowFiltered:
    """Tests for filtered show commands."""

    def _setup_types(self, executor):
        """Set up a mix of composites, interfaces, enums, and aliases."""
        executor.execute(CreateInterfaceQuery(
            name="Styled",
            fields=[FieldDef(name="color", type_name="string")],
        ))
        executor.execute(CreateEnumQuery(
            name="Color",
            variants=[
                EnumVariantSpec(name="red"),
                EnumVariantSpec(name="green"),
                EnumVariantSpec(name="blue"),
            ],
        ))
        executor.execute(CreateAliasQuery(name="age", base_type="uint8"))
        executor.execute(CreateTypeQuery(
            name="Person",
            fields=[
                FieldDef(name="name", type_name="string"),
                FieldDef(name="age", type_name="age"),
            ],
        ))
        executor.execute(CreateInstanceQuery(
            type_name="Person",
            fields=[FieldValue(name="name", value="Alice"), FieldValue(name="age", value=30)],
        ))

    def test_show_interfaces(self, executor):
        self._setup_types(executor)
        result = executor.execute(ShowTypesQuery(filter="interfaces"))
        kinds = {row["kind"] for row in result.rows}
        assert kinds == {"Interface"}
        names = {row["type"] for row in result.rows}
        assert "Styled" in names

    def test_show_composites(self, executor):
        self._setup_types(executor)
        result = executor.execute(ShowTypesQuery(filter="composites"))
        kinds = {row["kind"] for row in result.rows}
        assert kinds == {"Composite"}
        names = {row["type"] for row in result.rows}
        assert "Person" in names

    def test_show_enums(self, executor):
        self._setup_types(executor)
        result = executor.execute(ShowTypesQuery(filter="enums"))
        kinds = {row["kind"] for row in result.rows}
        assert kinds == {"Enum"}
        names = {row["type"] for row in result.rows}
        assert "Color" in names
        # Variant count as count
        color_row = [r for r in result.rows if r["type"] == "Color"][0]
        assert color_row["count"] == 3

    def test_show_primitives(self, executor):
        self._setup_types(executor)
        result = executor.execute(ShowTypesQuery(filter="primitives"))
        kinds = {row["kind"] for row in result.rows}
        assert kinds == {"Primitive"}
        names = {row["type"] for row in result.rows}
        # uint8 is referenced via the 'age' alias, character is referenced via string fields
        assert "uint8" in names
        assert "character" in names
        # Built-in primitives NOT referenced should be absent
        assert "float64" not in names

    def test_show_aliases(self, executor):
        self._setup_types(executor)
        result = executor.execute(ShowTypesQuery(filter="aliases"))
        kinds = {row["kind"] for row in result.rows}
        assert kinds == {"Alias"}
        names = {row["type"] for row in result.rows}
        assert "age" in names

    def test_show_types_all_kinds(self, executor):
        self._setup_types(executor)
        result = executor.execute(ShowTypesQuery())
        kinds = {row["kind"] for row in result.rows}
        assert "Composite" in kinds
        assert "Interface" in kinds
        assert "Enum" in kinds
        assert "Primitive" in kinds
        assert "Alias" in kinds

    def test_show_interfaces_parsed(self, parser):
        result = parser.parse("show interfaces")
        assert isinstance(result, ShowTypesQuery)
        assert result.filter == "interfaces"

    def test_show_composites_parsed(self, parser):
        result = parser.parse("show composites")
        assert isinstance(result, ShowTypesQuery)
        assert result.filter == "composites"

    def test_show_enums_parsed(self, parser):
        result = parser.parse("show enums")
        assert isinstance(result, ShowTypesQuery)
        assert result.filter == "enums"

    def test_show_primitives_parsed(self, parser):
        result = parser.parse("show primitives")
        assert isinstance(result, ShowTypesQuery)
        assert result.filter == "primitives"

    def test_show_aliases_parsed(self, parser):
        result = parser.parse("show aliases")
        assert isinstance(result, ShowTypesQuery)
        assert result.filter == "aliases"
