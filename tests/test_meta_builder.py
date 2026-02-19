"""Tests for MetaDatabaseBuilder — builds _meta/ database from user registry."""

import tempfile
import shutil
from pathlib import Path

import pytest

from typed_tables.schema import Schema
from typed_tables.ttg.meta_builder import MetaDatabaseBuilder


@pytest.fixture
def tmpdir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


def _build_meta(schema_text: str, tmpdir: Path):
    """Parse schema and build meta database, return (schema, meta_reg, meta_storage)."""
    db_path = tmpdir / "testdb"
    schema = Schema.parse(schema_text, db_path)
    builder = MetaDatabaseBuilder(schema.registry, db_path)
    meta_reg, meta_storage = builder.build()
    return schema, meta_reg, meta_storage


def _resolve_name(record, meta_reg, meta_storage, type_name):
    """Resolve the name field from a record."""
    name_val = record.get("name")
    if isinstance(name_val, str):
        return name_val
    if isinstance(name_val, tuple) and len(name_val) == 2:
        start, length = name_val
        type_def = meta_reg.get(type_name)
        base = type_def.resolve_base_type()
        for f in base.fields:
            if f.name == "name":
                arr = meta_storage.get_array_table_for_type(f.type_def)
                return "".join(arr.get(start, length))
    return None


class TestBasicTypes:
    """Test that basic type definitions create correct meta records."""

    def test_composite_creates_composite_def(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "type Person { name: string, age: uint8 }", tmpdir
        )
        table = meta_storage.get_table("CompositeDef")
        assert table.count >= 1
        names = [
            _resolve_name(table.get(i), meta_reg, meta_storage, "CompositeDef")
            for i in range(table.count)
        ]
        assert "Person" in names
        schema.close()

    def test_interface_creates_interface_def(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "interface Entity { name: string }", tmpdir
        )
        table = meta_storage.get_table("InterfaceDef")
        assert table.count >= 1
        names = [
            _resolve_name(table.get(i), meta_reg, meta_storage, "InterfaceDef")
            for i in range(table.count)
        ]
        assert "Entity" in names
        schema.close()

    def test_enum_creates_enum_def(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "enum Color { red, green, blue }", tmpdir
        )
        table = meta_storage.get_table("EnumDef")
        assert table.count >= 1
        names = [
            _resolve_name(table.get(i), meta_reg, meta_storage, "EnumDef")
            for i in range(table.count)
        ]
        assert "Color" in names
        schema.close()

    def test_alias_creates_alias_def(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "alias uuid = uint128", tmpdir
        )
        table = meta_storage.get_table("AliasDef")
        assert table.count >= 1
        names = [
            _resolve_name(table.get(i), meta_reg, meta_storage, "AliasDef")
            for i in range(table.count)
        ]
        assert "uuid" in names
        schema.close()


class TestFieldDefs:
    """Test that field definitions are correctly represented."""

    def test_composite_fields_count(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "type Person { name: string, age: uint8, active: boolean }", tmpdir
        )
        table = meta_storage.get_table("FieldDef")
        assert table.count == 3
        schema.close()

    def test_enum_typed_field(self, tmpdir):
        """Fields referencing enum types are correctly created."""
        schema, meta_reg, meta_storage = _build_meta(
            "enum Color { red, green, blue }\ntype Pixel { x: uint16, y: uint16, color: Color }",
            tmpdir,
        )
        table = meta_storage.get_table("FieldDef")
        assert table.count == 3  # x, y, color
        schema.close()

    def test_interface_fields(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "interface Entity { name: string }\ntype Person from Entity { age: uint8 }",
            tmpdir,
        )
        # Entity has 1 field, Person has 2 (name + age)
        table = meta_storage.get_table("FieldDef")
        assert table.count == 3  # Entity.name, Person.name, Person.age
        schema.close()


class TestWrappingTypes:
    """Test wrapping type definitions (alias, array, set, dict)."""

    def test_array_type(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "type Sensor { readings: int32[] }", tmpdir
        )
        table = meta_storage.get_table("ArrayDef")
        assert table.count >= 1
        schema.close()

    def test_set_type(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "type Tags { items: {string} }", tmpdir
        )
        table = meta_storage.get_table("SetDef")
        assert table.count >= 1
        schema.close()

    def test_dict_type(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "type Scores { data: {string: float64} }", tmpdir
        )
        table = meta_storage.get_table("DictDef")
        assert table.count >= 1
        # Entry composite should exist
        comp_table = meta_storage.get_table("CompositeDef")
        names = [
            _resolve_name(comp_table.get(i), meta_reg, meta_storage, "CompositeDef")
            for i in range(comp_table.count)
        ]
        assert any("Dict_" in n for n in names)
        schema.close()

    def test_self_referential_array(self, tmpdir):
        """Node[] wrapping type for self-referential composite."""
        schema, meta_reg, meta_storage = _build_meta(
            "type Node { value: uint8, children: Node[] }", tmpdir
        )
        table = meta_storage.get_table("ArrayDef")
        names = [
            _resolve_name(table.get(i), meta_reg, meta_storage, "ArrayDef")
            for i in range(table.count)
        ]
        assert "Node[]" in names
        schema.close()


class TestVariants:
    """Test enum variant definitions."""

    def test_variant_count(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "enum Color { red, green, blue }", tmpdir
        )
        table = meta_storage.get_table("VariantDef")
        assert table.count == 3
        schema.close()

    def test_swift_enum_variant_fields(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "enum Shape { none, circle(r: float32), rect(w: float32, h: float32) }",
            tmpdir,
        )
        table = meta_storage.get_table("VariantDef")
        assert table.count == 3  # none, circle, rect
        # circle has 1 field, rect has 2 fields
        field_table = meta_storage.get_table("FieldDef")
        assert field_table.count >= 3  # r, w, h
        schema.close()


class TestStaleness:
    """Test staleness detection and caching."""

    def test_rebuild_on_first_build(self, tmpdir):
        schema, _, _ = _build_meta("type Person { name: string }", tmpdir)
        meta_path = tmpdir / "testdb" / "_meta"
        assert meta_path.exists()
        assert (meta_path / "_source_hash").exists()
        schema.close()

    def test_no_rebuild_when_unchanged(self, tmpdir):
        db_path = tmpdir / "testdb"
        schema = Schema.parse("type Person { name: string }", db_path)

        # First build
        builder1 = MetaDatabaseBuilder(schema.registry, db_path)
        builder1.build()

        # Second build should be cached
        builder2 = MetaDatabaseBuilder(schema.registry, db_path)
        assert not builder2.is_stale()
        schema.close()

    def test_rebuild_when_schema_changes(self, tmpdir):
        db_path = tmpdir / "testdb"
        schema = Schema.parse("type Person { name: string }", db_path)

        # First build
        builder = MetaDatabaseBuilder(schema.registry, db_path)
        builder.build()

        # Change the schema (add a type)
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.query_executor import QueryExecutor

        parser = QueryParser()
        parser.build(debug=False, write_tables=False)
        executor = QueryExecutor(schema.storage, schema.registry)
        stmts = parser.parse_program("type Pet { name: string }")
        for stmt in stmts:
            executor.execute(stmt)

        # Should now be stale
        builder2 = MetaDatabaseBuilder(schema.registry, db_path)
        assert builder2.is_stale()
        schema.close()


class TestInheritance:
    """Test inheritance relationships in meta records."""

    def test_composite_parent(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "type Person { name: string }\ntype Employee from Person { dept: string }",
            tmpdir,
        )
        comp_table = meta_storage.get_table("CompositeDef")
        # Employee should have a parent reference
        for i in range(comp_table.count):
            r = comp_table.get(i)
            name = _resolve_name(r, meta_reg, meta_storage, "CompositeDef")
            if name == "Employee":
                assert r.get("parent") is not None
                break
        else:
            pytest.fail("Employee not found in CompositeDef table")
        schema.close()

    def test_composite_implements_interface(self, tmpdir):
        schema, meta_reg, meta_storage = _build_meta(
            "interface Entity { name: string }\ntype Person from Entity { age: uint8 }",
            tmpdir,
        )
        comp_table = meta_storage.get_table("CompositeDef")
        for i in range(comp_table.count):
            r = comp_table.get(i)
            name = _resolve_name(r, meta_reg, meta_storage, "CompositeDef")
            if name == "Person":
                ifaces = r.get("interfaces")
                assert ifaces is not None
                if isinstance(ifaces, tuple):
                    _, length = ifaces
                    assert length >= 1
                break
        else:
            pytest.fail("Person not found in CompositeDef table")
        schema.close()


class TestNoDuplicates:
    """Test that no duplicate records are created."""

    def test_no_duplicate_dict_entry(self, tmpdir):
        """Dict entry composites should not be duplicated."""
        schema, meta_reg, meta_storage = _build_meta(
            "type Scores { data: {string: float64} }", tmpdir
        )
        comp_table = meta_storage.get_table("CompositeDef")
        names = [
            _resolve_name(comp_table.get(i), meta_reg, meta_storage, "CompositeDef")
            for i in range(comp_table.count)
        ]
        dict_entries = [n for n in names if n and "Dict_" in n]
        # Each dict entry type should appear exactly once
        assert len(dict_entries) == len(set(dict_entries))
        schema.close()
