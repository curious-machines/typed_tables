"""Tests for enum type support."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.parsing.query_parser import (
    CreateEnumQuery,
    EnumValueExpr,
    QueryParser,
)
from typed_tables.query_executor import CreateResult, QueryExecutor, QueryResult
from typed_tables.storage import StorageManager
from typed_tables.types import (
    EnumTypeDefinition,
    EnumValue,
    EnumVariantDefinition,
    FieldDefinition,
    PrimitiveTypeDefinition,
    TypeRegistry,
)


# ---- Type system tests ----


class TestEnumTypeDefinition:
    def test_c_style_discriminant_size_uint8(self):
        """C-style enum with small discriminants uses uint8."""
        variants = [
            EnumVariantDefinition(name="red", discriminant=0),
            EnumVariantDefinition(name="green", discriminant=1),
            EnumVariantDefinition(name="blue", discriminant=2),
        ]
        enum_def = EnumTypeDefinition(name="Color", variants=variants)
        assert enum_def.discriminant_size == 1
        assert enum_def.max_payload_size == 0
        assert enum_def.size_bytes == 1
        assert enum_def.reference_size == 1

    def test_c_style_discriminant_size_uint16(self):
        """C-style enum with large discriminants uses uint16."""
        variants = [
            EnumVariantDefinition(name="ok", discriminant=200),
            EnumVariantDefinition(name="not_found", discriminant=404),
        ]
        enum_def = EnumTypeDefinition(name="HttpStatus", variants=variants, has_explicit_values=True)
        assert enum_def.discriminant_size == 2
        assert enum_def.size_bytes == 2

    def test_swift_style_payload_size(self):
        """Swift-style enum payload is padded to largest variant."""
        registry = TypeRegistry()
        float32_type = registry.get_or_raise("float32")

        variants = [
            EnumVariantDefinition(name="none", discriminant=0),
            EnumVariantDefinition(
                name="circle",
                discriminant=1,
                fields=[
                    FieldDefinition(name="cx", type_def=float32_type),
                    FieldDefinition(name="cy", type_def=float32_type),
                    FieldDefinition(name="r", type_def=float32_type),
                ],
            ),
            EnumVariantDefinition(
                name="line",
                discriminant=2,
                fields=[
                    FieldDefinition(name="x1", type_def=float32_type),
                    FieldDefinition(name="y1", type_def=float32_type),
                    FieldDefinition(name="x2", type_def=float32_type),
                    FieldDefinition(name="y2", type_def=float32_type),
                ],
            ),
        ]
        enum_def = EnumTypeDefinition(name="Shape", variants=variants)
        assert enum_def.discriminant_size == 1
        assert enum_def.max_payload_size == 16  # 4 * float32 = 16
        assert enum_def.size_bytes == 17  # 1 + 16

    def test_get_variant(self):
        variants = [
            EnumVariantDefinition(name="a", discriminant=0),
            EnumVariantDefinition(name="b", discriminant=1),
        ]
        enum_def = EnumTypeDefinition(name="Test", variants=variants)
        assert enum_def.get_variant("a").name == "a"
        assert enum_def.get_variant("b").discriminant == 1
        assert enum_def.get_variant("c") is None

    def test_get_variant_by_discriminant(self):
        variants = [
            EnumVariantDefinition(name="ok", discriminant=200),
            EnumVariantDefinition(name="err", discriminant=500),
        ]
        enum_def = EnumTypeDefinition(name="Status", variants=variants, has_explicit_values=True)
        assert enum_def.get_variant_by_discriminant(200).name == "ok"
        assert enum_def.get_variant_by_discriminant(500).name == "err"
        assert enum_def.get_variant_by_discriminant(404) is None

    def test_is_enum(self):
        enum_def = EnumTypeDefinition(name="Test", variants=[])
        assert enum_def.is_enum is True
        assert enum_def.is_composite is False
        assert enum_def.is_array is False
        assert enum_def.is_primitive is False


class TestTypeRegistryEnum:
    def test_register_enum_stub(self):
        registry = TypeRegistry()
        stub = registry.register_enum_stub("Color")
        assert isinstance(stub, EnumTypeDefinition)
        assert stub.variants == []
        assert registry.is_enum_stub("Color")

    def test_register_enum_stub_idempotent(self):
        registry = TypeRegistry()
        stub1 = registry.register_enum_stub("Color")
        stub2 = registry.register_enum_stub("Color")
        assert stub1 is stub2

    def test_register_enum_stub_conflict(self):
        registry = TypeRegistry()
        registry.register_enum_stub("Color")
        # Populate the stub
        stub = registry.get("Color")
        stub.variants = [EnumVariantDefinition(name="red", discriminant=0)]
        with pytest.raises(ValueError):
            registry.register_enum_stub("Color")


# ---- Parser tests ----


class TestEnumParsing:
    @pytest.fixture
    def parser(self):
        return QueryParser()

    def test_parse_c_style_enum(self, parser):
        query = parser.parse("create enum Color { red, green, blue }")
        assert isinstance(query, CreateEnumQuery)
        assert query.name == "Color"
        assert len(query.variants) == 3
        assert query.variants[0].name == "red"
        assert query.variants[1].name == "green"
        assert query.variants[2].name == "blue"

    def test_parse_c_style_explicit_values(self, parser):
        query = parser.parse("create enum HttpStatus { ok = 200, not_found = 404 }")
        assert isinstance(query, CreateEnumQuery)
        assert query.variants[0].explicit_value == 200
        assert query.variants[1].explicit_value == 404

    def test_parse_swift_style_enum(self, parser):
        query = parser.parse(
            "create enum Shape { none, circle(cx: float32, cy: float32, r: float32) }"
        )
        assert isinstance(query, CreateEnumQuery)
        assert len(query.variants) == 2
        assert query.variants[0].name == "none"
        assert query.variants[0].fields is None
        assert query.variants[1].name == "circle"
        assert len(query.variants[1].fields) == 3
        assert query.variants[1].fields[0].name == "cx"
        assert query.variants[1].fields[0].type_name == "float32"

    def test_parse_trailing_comma(self, parser):
        query = parser.parse("create enum Color { red, green, blue, }")
        assert isinstance(query, CreateEnumQuery)
        assert len(query.variants) == 3

    def test_parse_enum_value_bare(self, parser):
        """Parse enum value expression in instance creation."""
        query = parser.parse("create Pixel(x=0, y=0, color=Color.red)")
        field_values = {fv.name: fv.value for fv in query.fields}
        assert isinstance(field_values["color"], EnumValueExpr)
        assert field_values["color"].enum_name == "Color"
        assert field_values["color"].variant_name == "red"

    def test_parse_enum_value_with_args(self, parser):
        """Parse enum value expression with associated values."""
        query = parser.parse("create Canvas(bg=Shape.circle(cx=50.0, cy=50.0, r=25.0))")
        field_values = {fv.name: fv.value for fv in query.fields}
        ev = field_values["bg"]
        assert isinstance(ev, EnumValueExpr)
        assert ev.enum_name == "Shape"
        assert ev.variant_name == "circle"
        assert len(ev.args) == 3

    def test_parse_from_variant(self, parser):
        """Parse 'from Shape.circle select *'."""
        query = parser.parse("from Shape.circle select *")
        assert query.table == "Shape"
        assert query.variant == "circle"

    def test_parse_describe_variant(self, parser):
        query = parser.parse("describe Shape.circle")
        assert query.table == "Shape.circle"

    def test_parse_enum_shorthand_bare(self, parser):
        """Parse shorthand enum value: .red instead of Color.red."""
        query = parser.parse("create Pixel(x=0, y=0, color=.red)")
        field_values = {fv.name: fv.value for fv in query.fields}
        assert isinstance(field_values["color"], EnumValueExpr)
        assert field_values["color"].enum_name is None
        assert field_values["color"].variant_name == "red"

    def test_parse_enum_shorthand_with_args(self, parser):
        """Parse shorthand enum value with associated values."""
        query = parser.parse("create Canvas(bg=.circle(cx=50.0, cy=50.0, r=25.0))")
        field_values = {fv.name: fv.value for fv in query.fields}
        ev = field_values["bg"]
        assert isinstance(ev, EnumValueExpr)
        assert ev.enum_name is None
        assert ev.variant_name == "circle"
        assert len(ev.args) == 3

    def test_parse_enum_shorthand_empty_args(self, parser):
        """Parse shorthand enum value with empty parens."""
        query = parser.parse("create Canvas(bg=.none())")
        field_values = {fv.name: fv.value for fv in query.fields}
        ev = field_values["bg"]
        assert isinstance(ev, EnumValueExpr)
        assert ev.enum_name is None
        assert ev.variant_name == "none"
        assert ev.args == []


# ---- Type DSL parser tests ----


class TestTypeDSLEnum:
    def test_type_dsl_c_style_enum(self):
        from typed_tables.parsing.type_parser import TypeParser

        parser = TypeParser()
        registry = parser.parse("enum Color { red, green, blue }")
        color = registry.get("Color")
        assert isinstance(color, EnumTypeDefinition)
        assert len(color.variants) == 3
        assert color.variants[0].name == "red"
        assert color.variants[0].discriminant == 0
        assert color.variants[2].discriminant == 2

    def test_type_dsl_explicit_values(self):
        from typed_tables.parsing.type_parser import TypeParser

        parser = TypeParser()
        registry = parser.parse("enum Status { ok = 200, not_found = 404, error = 500 }")
        status = registry.get("Status")
        assert isinstance(status, EnumTypeDefinition)
        assert status.has_explicit_values is True
        assert status.get_variant("ok").discriminant == 200

    def test_type_dsl_swift_style(self):
        from typed_tables.parsing.type_parser import TypeParser

        parser = TypeParser()
        registry = parser.parse(
            "enum Shape { none, circle(cx: float32, cy: float32, r: float32) }"
        )
        shape = registry.get("Shape")
        assert isinstance(shape, EnumTypeDefinition)
        assert len(shape.variants) == 2
        circle = shape.get_variant("circle")
        assert len(circle.fields) == 3
        assert circle.fields[0].name == "cx"

    def test_type_dsl_enum_mixed_reject(self):
        from typed_tables.parsing.type_parser import TypeParser

        parser = TypeParser()
        with pytest.raises(ValueError, match="cannot coexist"):
            parser.parse("enum Bad { a = 1, b(x: uint8) }")


# ---- Integration tests ----


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


class TestEnumExecution:
    def test_create_c_style_enum(self, executor):
        parser = QueryParser()
        query = parser.parse("create enum Color { red, green, blue }")
        result = executor.execute(query)
        assert isinstance(result, CreateResult)
        assert "Created enum" in result.message

        # Verify type is registered
        color = executor.registry.get("Color")
        assert isinstance(color, EnumTypeDefinition)
        assert len(color.variants) == 3

    def test_create_enum_with_explicit_values(self, executor):
        parser = QueryParser()
        query = parser.parse("create enum HttpStatus { ok = 200, not_found = 404, error = 500 }")
        result = executor.execute(query)
        assert "Created enum" in result.message

        hs = executor.registry.get("HttpStatus")
        assert hs.get_variant("ok").discriminant == 200
        assert hs.get_variant("not_found").discriminant == 404

    def test_create_swift_style_enum(self, executor):
        parser = QueryParser()
        query = parser.parse(
            "create enum Shape { none, line(x1: float32, y1: float32, x2: float32, y2: float32), circle(cx: float32, cy: float32, r: float32) }"
        )
        result = executor.execute(query)
        assert "Created enum" in result.message
        assert "3 variant" in result.message

    def test_reject_mixed_enum(self, executor):
        parser = QueryParser()
        query = parser.parse("create enum Bad { a = 1, b(x: uint8) }")
        result = executor.execute(query)
        assert "cannot coexist" in result.message

    def test_create_instance_with_c_style_enum(self, executor):
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
            create type Pixel { x: uint16, y: uint16, color: Color }
            create Pixel(x=0, y=0, color=Color.red)
        """)
        for stmt in stmts:
            result = executor.execute(stmt)

        assert isinstance(result, CreateResult)
        assert result.index == 0

        # Read back
        query = parser.parse("from Pixel select *")
        result = executor.execute(query)
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row["x"] == 0
        assert isinstance(row["color"], EnumValue)
        assert row["color"].variant_name == "red"
        assert row["color"].discriminant == 0

    def test_create_instance_with_swift_style_enum(self, executor):
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Shape { none, circle(cx: float32, cy: float32, r: float32) }
            create type Canvas { name: string, bg: Shape }
            create Canvas(name="test", bg=Shape.circle(cx=50.0, cy=50.0, r=25.0))
        """)
        for stmt in stmts:
            result = executor.execute(stmt)

        query = parser.parse("from Canvas select *")
        result = executor.execute(query)
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row["name"] == "test"
        ev = row["bg"]
        assert isinstance(ev, EnumValue)
        assert ev.variant_name == "circle"
        assert abs(ev.fields["cx"] - 50.0) < 0.001
        assert abs(ev.fields["r"] - 25.0) < 0.001

    def test_enum_null_field(self, executor):
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
            create type Pixel { x: uint16, y: uint16, color: Color }
            create Pixel(x=0, y=0)
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("from Pixel select *")
        result = executor.execute(query)
        assert result.rows[0]["color"] is None

    def test_type_based_query_enum(self, executor):
        """from Color select * should scan composites and find enum values."""
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
            create type Pixel { x: uint16, y: uint16, color: Color }
            create Pixel(x=0, y=0, color=Color.red)
            create Pixel(x=1, y=0, color=Color.blue)
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("from Color select *")
        result = executor.execute(query)
        assert len(result.rows) == 2
        assert result.rows[0]["_variant"] == "red"
        assert result.rows[1]["_variant"] == "blue"

    def test_variant_query(self, executor):
        """from Shape.circle select * should filter to circle variant with fields as columns."""
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Shape { none, circle(cx: float32, cy: float32, r: float32) }
            create type Canvas { name: string, bg: Shape, fg: Shape }
            create Canvas(name="test", bg=Shape.none, fg=Shape.circle(cx=50.0, cy=50.0, r=25.0))
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("from Shape.circle select *")
        result = executor.execute(query)
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row["_source"] == "Canvas"
        assert row["_field"] == "fg"
        assert abs(row["cx"] - 50.0) < 0.001
        assert abs(row["r"] - 25.0) < 0.001

    def test_variant_query_with_where(self, executor):
        """from Shape.circle select * where r > 20 should filter."""
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Shape { none, circle(cx: float32, cy: float32, r: float32) }
            create type Canvas { name: string, bg: Shape }
            create Canvas(name="big", bg=Shape.circle(cx=0.0, cy=0.0, r=50.0))
            create Canvas(name="small", bg=Shape.circle(cx=0.0, cy=0.0, r=5.0))
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("from Shape.circle select * where r > 20.0")
        result = executor.execute(query)
        assert len(result.rows) == 1
        assert result.rows[0]["_source"] == "Canvas"

    def test_describe_enum(self, executor):
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("describe Color")
        result = executor.execute(query)
        assert len(result.rows) >= 4  # type + 3 variants
        # Check type row
        assert result.rows[0]["property"] == "(type)"
        assert "Enum" in result.rows[0]["type"]

    def test_describe_enum_variant(self, executor):
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Shape { none, circle(cx: float32, cy: float32, r: float32) }
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("describe Shape.circle")
        result = executor.execute(query)
        assert len(result.rows) == 4  # variant header + 3 fields
        assert result.rows[1]["property"] == "cx"

    def test_dump_with_enum(self, executor):
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
            create type Pixel { x: uint16, y: uint16, color: Color }
            create Pixel(x=0, y=0, color=Color.red)
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("dump")
        result = executor.execute(query)
        assert "create enum Color" in result.script
        assert "Color.red" in result.script

    def test_dump_with_swift_style_enum(self, executor):
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Shape { none, circle(cx: float32, cy: float32, r: float32) }
            create type Canvas { name: string, bg: Shape }
            create Canvas(name="test", bg=Shape.circle(cx=50.0, cy=50.0, r=25.0))
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("dump")
        result = executor.execute(query)
        assert "create enum Shape" in result.script
        assert "Shape.circle" in result.script

    def test_dump_explicit_values(self, executor):
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum HttpStatus { ok = 200, not_found = 404 }
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("dump")
        result = executor.execute(query)
        assert "ok = 200" in result.script
        assert "not_found = 404" in result.script

    def test_metadata_roundtrip(self, executor, db_dir):
        """Enum types should survive metadata save/load."""
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
            create enum Shape { none, circle(cx: float32, cy: float32, r: float32) }
        """)
        for stmt in stmts:
            executor.execute(stmt)

        # Load from metadata
        from typed_tables.dump import load_registry_from_metadata
        registry2 = load_registry_from_metadata(db_dir)

        color = registry2.get("Color")
        assert isinstance(color, EnumTypeDefinition)
        assert len(color.variants) == 3

        shape = registry2.get("Shape")
        assert isinstance(shape, EnumTypeDefinition)
        circle = shape.get_variant("circle")
        assert len(circle.fields) == 3

    def test_enum_where_disallowed_on_overview(self, executor):
        """WHERE should not be allowed on enum overview queries."""
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
            create type Pixel { x: uint16, y: uint16, color: Color }
            create Pixel(x=0, y=0, color=Color.red)
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("from Color select * where value = \"red\"")
        result = executor.execute(query)
        assert "WHERE not supported" in result.message

    def test_update_enum_field(self, executor):
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
            create type Pixel { x: uint16, y: uint16, color: Color }
            $p = create Pixel(x=0, y=0, color=Color.red)
            update $p set color=Color.blue
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("from Pixel select *")
        result = executor.execute(query)
        assert result.rows[0]["color"].variant_name == "blue"

    def test_multiple_enum_fields(self, executor):
        """Composite with two enum fields of different types."""
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
            create enum Shape { none, circle(r: float32) }
            create type Widget { color: Color, shape: Shape }
            create Widget(color=Color.green, shape=Shape.circle(r=10.0))
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("from Widget select *")
        result = executor.execute(query)
        row = result.rows[0]
        assert row["color"].variant_name == "green"
        assert row["shape"].variant_name == "circle"
        assert abs(row["shape"].fields["r"] - 10.0) < 0.001

    def test_enum_shorthand_c_style(self, executor):
        """Shorthand .variant syntax for C-style enums."""
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
            create type Pixel { x: uint16, y: uint16, color: Color }
            create Pixel(x=0, y=0, color=.red)
            create Pixel(x=1, y=0, color=.blue)
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("from Pixel select *")
        result = executor.execute(query)
        assert len(result.rows) == 2
        assert result.rows[0]["color"].variant_name == "red"
        assert result.rows[1]["color"].variant_name == "blue"

    def test_enum_shorthand_swift_style(self, executor):
        """Shorthand .variant(args) syntax for Swift-style enums."""
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Shape { none, circle(cx: float32, cy: float32, r: float32) }
            create type Canvas { name: string, bg: Shape }
            create Canvas(name="test", bg=.circle(cx=50.0, cy=50.0, r=25.0))
            create Canvas(name="empty", bg=.none)
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("from Canvas select *")
        result = executor.execute(query)
        assert len(result.rows) == 2
        assert result.rows[0]["bg"].variant_name == "circle"
        assert abs(result.rows[0]["bg"].fields["cx"] - 50.0) < 0.001
        assert result.rows[1]["bg"].variant_name == "none"

    def test_enum_shorthand_mixed_with_qualified(self, executor):
        """Both shorthand and fully-qualified forms work in the same program."""
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
            create type Pixel { x: uint16, y: uint16, color: Color }
            create Pixel(x=0, y=0, color=Color.red)
            create Pixel(x=1, y=0, color=.blue)
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("from Pixel select *")
        result = executor.execute(query)
        assert result.rows[0]["color"].variant_name == "red"
        assert result.rows[1]["color"].variant_name == "blue"

    def test_enum_shorthand_bad_variant(self, executor):
        """Shorthand with unknown variant name returns error."""
        parser = QueryParser()
        stmts = parser.parse_program("""
            create enum Color { red, green, blue }
            create type Pixel { x: uint16, y: uint16, color: Color }
        """)
        for stmt in stmts:
            executor.execute(stmt)

        query = parser.parse("create Pixel(x=0, y=0, color=.purple)")
        result = executor.execute(query)
        assert "Unknown variant 'purple'" in result.message
