"""Tests for default values on type fields."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.dump import load_registry_from_metadata
from typed_tables.parsing.query_parser import FieldDef, QueryParser
from typed_tables.query_executor import CreateResult, DumpResult, QueryExecutor, QueryResult
from typed_tables.storage import StorageManager
from typed_tables.types import (
    EnumTypeDefinition,
    EnumValue,
    FieldDefinition,
    TypeRegistry,
)


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


class TestDefaultValueParsing:
    @pytest.fixture
    def parser(self):
        return QueryParser()

    def test_parse_field_with_integer_default(self, parser):
        query = parser.parse("create type T { x: uint8 = 42 }")
        assert len(query.fields) == 1
        assert query.fields[0].name == "x"
        assert query.fields[0].type_name == "uint8"
        assert query.fields[0].default_value == 42

    def test_parse_field_with_string_default(self, parser):
        query = parser.parse('create type T { name: string = "hello" }')
        assert query.fields[0].default_value == "hello"

    def test_parse_field_with_float_default(self, parser):
        query = parser.parse("create type T { x: float32 = 3.14 }")
        assert query.fields[0].default_value == 3.14

    def test_parse_field_with_null_default(self, parser):
        from typed_tables.parsing.query_parser import NullValue
        query = parser.parse("create type T { x: uint8 = null }")
        assert isinstance(query.fields[0].default_value, NullValue)

    def test_parse_field_with_enum_shorthand_default(self, parser):
        from typed_tables.parsing.query_parser import EnumValueExpr
        query = parser.parse("create type T { color: Color = .red }")
        assert isinstance(query.fields[0].default_value, EnumValueExpr)
        assert query.fields[0].default_value.variant_name == "red"
        assert query.fields[0].default_value.enum_name is None

    def test_parse_field_with_enum_qualified_default(self, parser):
        from typed_tables.parsing.query_parser import EnumValueExpr
        query = parser.parse("create type T { color: Color = Color.red }")
        assert isinstance(query.fields[0].default_value, EnumValueExpr)
        assert query.fields[0].default_value.variant_name == "red"
        assert query.fields[0].default_value.enum_name == "Color"

    def test_parse_field_with_array_default(self, parser):
        query = parser.parse("create type T { data: int8[] = [1, 2, 3] }")
        assert query.fields[0].type_name == "int8[]"
        assert query.fields[0].default_value == [1, 2, 3]

    def test_parse_field_without_default(self, parser):
        query = parser.parse("create type T { x: uint8 }")
        assert query.fields[0].default_value is None

    def test_parse_mixed_fields(self, parser):
        query = parser.parse('create type T { x: uint8 = 0, name: string, active: uint8 = 1 }')
        assert query.fields[0].default_value == 0
        assert query.fields[1].default_value is None
        assert query.fields[2].default_value == 1

    def test_parse_interface_with_defaults(self, parser):
        query = parser.parse("create interface Positioned { x: float32 = 0.0, y: float32 = 0.0 }")
        assert query.fields[0].default_value == 0.0
        assert query.fields[1].default_value == 0.0

    def test_parse_enum_shorthand_with_args_default(self, parser):
        from typed_tables.parsing.query_parser import EnumValueExpr
        query = parser.parse("create type T { bg: Shape = .circle(cx=0, cy=0, r=1) }")
        default = query.fields[0].default_value
        assert isinstance(default, EnumValueExpr)
        assert default.variant_name == "circle"
        assert len(default.args) == 3


class TestDefaultValueExecution:
    def _run(self, executor, *stmts):
        parser = QueryParser()
        result = None
        for stmt in stmts:
            queries = parser.parse_program(stmt)
            for q in queries:
                result = executor.execute(q)
        return result

    def test_primitive_default_uint8(self, executor):
        self._run(executor, "create type T { x: uint8 = 42, y: uint8 }")
        self._run(executor, "create T()")

        result = self._run(executor, "from T select *")
        assert len(result.rows) == 1
        assert result.rows[0]["x"] == 42
        assert result.rows[0]["y"] is None

    def test_primitive_default_float32(self, executor):
        self._run(executor, "create type T { val: float32 = 3.14 }")
        self._run(executor, "create T()")

        result = self._run(executor, "from T select *")
        assert abs(result.rows[0]["val"] - 3.14) < 0.01

    def test_string_default(self, executor):
        self._run(executor, 'create type T { status: string = "active" }')
        self._run(executor, "create T()")

        result = self._run(executor, "from T select *")
        assert result.rows[0]["status"] == "active"

    def test_no_default_gives_null(self, executor):
        """Backward compat: fields without default still get NULL."""
        self._run(executor, "create type T { x: uint8 }")
        self._run(executor, "create T()")

        result = self._run(executor, "from T select *")
        assert result.rows[0]["x"] is None

    def test_explicit_value_overrides_default(self, executor):
        self._run(executor, "create type T { x: uint8 = 42 }")
        self._run(executor, "create T(x=99)")

        result = self._run(executor, "from T select *")
        assert result.rows[0]["x"] == 99

    def test_explicit_null_overrides_default(self, executor):
        self._run(executor, "create type T { x: uint8 = 42 }")
        self._run(executor, "create T(x=null)")

        result = self._run(executor, "from T select *")
        assert result.rows[0]["x"] is None

    def test_enum_c_style_default_shorthand(self, executor):
        self._run(executor,
            "create enum Color { red, green, blue }",
            "create type Pixel { x: uint16, y: uint16, color: Color = .red }",
            "create Pixel(x=0, y=0)",
        )

        result = self._run(executor, "from Pixel select *")
        assert result.rows[0]["color"].variant_name == "red"

    def test_enum_c_style_default_qualified(self, executor):
        self._run(executor,
            "create enum Color { red, green, blue }",
            "create type Pixel { x: uint16, y: uint16, color: Color = Color.green }",
            "create Pixel(x=0, y=0)",
        )

        result = self._run(executor, "from Pixel select *")
        assert result.rows[0]["color"].variant_name == "green"

    def test_enum_swift_style_default(self, executor):
        self._run(executor,
            "create enum Shape { none, circle(cx: float32, cy: float32, r: float32) }",
            "create type Canvas { name: string, bg: Shape = .circle(cx=0, cy=0, r=1) }",
            'create Canvas(name="test")',
        )

        result = self._run(executor, "from Canvas select *")
        bg = result.rows[0]["bg"]
        assert isinstance(bg, EnumValue)
        assert bg.variant_name == "circle"
        assert bg.fields["cx"] == 0
        assert bg.fields["r"] == 1

    def test_array_default(self, executor):
        self._run(executor,
            "create type Sensor { name: string, readings: int8[] = [0, 0, 0] }",
            'create Sensor(name="temp")',
        )

        result = self._run(executor, "from Sensor select *")
        assert result.rows[0]["readings"] == [0, 0, 0]

    def test_reject_function_call_default(self, executor):
        result = self._run(executor, "create type T { id: uint128 = uuid() }")
        assert "Invalid default" in result.message or "FunctionCall" in result.message

    def test_reject_inline_instance_default(self, executor):
        self._run(executor, "create type Inner { v: uint8 }")
        result = self._run(executor, "create type T { inner: Inner = Inner(v=1) }")
        assert "Invalid default" in result.message or "InlineInstance" in result.message

    def test_reject_composite_ref_default(self, executor):
        self._run(executor, "create type Inner { v: uint8 }")
        result = self._run(executor, "create type T { inner: Inner = Inner(0) }")
        assert "Invalid default" in result.message or "CompositeRef" in result.message


class TestDefaultValueMetadataRoundtrip:
    def _run(self, executor, *stmts):
        parser = QueryParser()
        result = None
        for stmt in stmts:
            queries = parser.parse_program(stmt)
            for q in queries:
                result = executor.execute(q)
        return result

    def test_metadata_roundtrip_primitive_default(self, db_dir):
        """Defaults survive save + reload from metadata."""
        registry = TypeRegistry()
        storage = StorageManager(db_dir, registry)
        executor = QueryExecutor(storage, registry)

        self._run(executor, "create type T { x: uint8 = 42, name: string = \"hello\" }")
        storage.close()

        # Reload from metadata
        registry2 = load_registry_from_metadata(db_dir)
        t = registry2.get("T")
        assert t is not None
        x_field = t.get_field("x")
        assert x_field.default_value == 42
        name_field = t.get_field("name")
        assert name_field.default_value == "hello"

    def test_metadata_roundtrip_enum_default(self, db_dir):
        """Enum defaults survive save + reload."""
        registry = TypeRegistry()
        storage = StorageManager(db_dir, registry)
        executor = QueryExecutor(storage, registry)

        self._run(executor,
            "create enum Color { red, green, blue }",
            "create type Pixel { color: Color = .red }",
        )
        storage.close()

        registry2 = load_registry_from_metadata(db_dir)
        pixel = registry2.get("Pixel")
        color_field = pixel.get_field("color")
        assert isinstance(color_field.default_value, EnumValue)
        assert color_field.default_value.variant_name == "red"

    def test_metadata_roundtrip_swift_enum_default(self, db_dir):
        """Swift-style enum defaults survive save + reload."""
        registry = TypeRegistry()
        storage = StorageManager(db_dir, registry)
        executor = QueryExecutor(storage, registry)

        self._run(executor,
            "create enum Shape { none, circle(cx: float32, cy: float32, r: float32) }",
            "create type Canvas { bg: Shape = .circle(cx=0, cy=0, r=1) }",
        )
        storage.close()

        registry2 = load_registry_from_metadata(db_dir)
        canvas = registry2.get("Canvas")
        bg_field = canvas.get_field("bg")
        assert isinstance(bg_field.default_value, EnumValue)
        assert bg_field.default_value.variant_name == "circle"
        assert bg_field.default_value.fields["r"] == 1

    def test_metadata_roundtrip_no_default(self, db_dir):
        """Fields without defaults remain None after reload."""
        registry = TypeRegistry()
        storage = StorageManager(db_dir, registry)
        executor = QueryExecutor(storage, registry)

        self._run(executor, "create type T { x: uint8 }")
        storage.close()

        registry2 = load_registry_from_metadata(db_dir)
        t = registry2.get("T")
        assert t.get_field("x").default_value is None

    def test_metadata_roundtrip_array_default(self, db_dir):
        """Array defaults survive save + reload."""
        registry = TypeRegistry()
        storage = StorageManager(db_dir, registry)
        executor = QueryExecutor(storage, registry)

        self._run(executor, "create type T { data: int8[] = [1, 2, 3] }")
        storage.close()

        registry2 = load_registry_from_metadata(db_dir)
        t = registry2.get("T")
        assert t.get_field("data").default_value == [1, 2, 3]

    def test_metadata_roundtrip_interface_default(self, db_dir):
        """Interface field defaults survive save + reload."""
        registry = TypeRegistry()
        storage = StorageManager(db_dir, registry)
        executor = QueryExecutor(storage, registry)

        self._run(executor, "create interface Pos { x: float32 = 0.0, y: float32 = 0.0 }")
        storage.close()

        registry2 = load_registry_from_metadata(db_dir)
        pos = registry2.get("Pos")
        assert pos.get_field("x").default_value == 0.0
        assert pos.get_field("y").default_value == 0.0


class TestDefaultValueDumpRoundtrip:
    def _run(self, executor, *stmts):
        parser = QueryParser()
        result = None
        for stmt in stmts:
            queries = parser.parse_program(stmt)
            for q in queries:
                result = executor.execute(q)
        return result

    def test_dump_roundtrip_with_defaults(self, db_dir):
        """Dump produces TTQ that can recreate the type with defaults."""
        registry = TypeRegistry()
        storage = StorageManager(db_dir, registry)
        executor = QueryExecutor(storage, registry)

        self._run(executor,
            "create type T { x: uint8 = 42, name: string = \"hello\" }",
            "create T()",
        )

        # Dump
        dump_result = self._run(executor, "dump")
        assert isinstance(dump_result, DumpResult)
        dump_text = dump_result.script

        # Create new db, replay dump
        db_dir2 = db_dir / "replay"
        registry2 = TypeRegistry()
        storage2 = StorageManager(db_dir2, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        parser = QueryParser()
        for q in parser.parse_program(dump_text):
            executor2.execute(q)

        t = registry2.get("T")
        assert t.get_field("x").default_value == 42
        assert t.get_field("name").default_value == "hello"
        storage2.close()


class TestDefaultValueDescribe:
    def _run(self, executor, *stmts):
        parser = QueryParser()
        result = None
        for stmt in stmts:
            queries = parser.parse_program(stmt)
            for q in queries:
                result = executor.execute(q)
        return result

    def test_describe_shows_defaults(self, executor):
        self._run(executor, 'create type T { x: uint8 = 42, name: string = "hello", y: uint8 }')

        result = self._run(executor, "describe T")
        assert "default" in result.columns

        # Find field rows (skip the (type) header row)
        field_rows = [r for r in result.rows if r["property"] not in ("(type)",)]
        x_row = next(r for r in field_rows if r["property"] == "x")
        assert x_row["default"] == "42"

        name_row = next(r for r in field_rows if r["property"] == "name")
        assert name_row["default"] == '"hello"'

        y_row = next(r for r in field_rows if r["property"] == "y")
        assert y_row["default"] == "NULL"

    def test_describe_interface_shows_defaults(self, executor):
        self._run(executor, "create interface Pos { x: float32 = 0.0, y: float32 = 0.0 }")

        result = self._run(executor, "describe Pos")
        assert "default" in result.columns
        x_row = next(r for r in result.rows if r["property"] == "x")
        assert "0.0" in x_row["default"]


class TestInterfaceDefaultInheritance:
    def _run(self, executor, *stmts):
        parser = QueryParser()
        result = None
        for stmt in stmts:
            queries = parser.parse_program(stmt)
            for q in queries:
                result = executor.execute(q)
        return result

    def test_interface_defaults_inherited_by_composite(self, executor):
        """Composites that implement an interface inherit field defaults."""
        self._run(executor,
            "create interface Pos { x: float32 = 0.0, y: float32 = 0.0 }",
            "create type Point from Pos { label: string }",
            'create Point(label="origin")',
        )

        result = self._run(executor, "from Point select *")
        row = result.rows[0]
        assert abs(row["x"] - 0.0) < 0.001
        assert abs(row["y"] - 0.0) < 0.001
        assert row["label"] == "origin"

    def test_interface_defaults_used_when_omitted(self, executor):
        """Interface defaults used when fields omitted during creation."""
        self._run(executor,
            "create interface Pos { x: float32 = 0.0, y: float32 = 0.0 }",
            "create type Point from Pos { label: string }",
            "create Point()",
        )

        result = self._run(executor, "from Point select *")
        row = result.rows[0]
        assert abs(row["x"] - 0.0) < 0.001
        assert abs(row["y"] - 0.0) < 0.001
        assert row["label"] is None
