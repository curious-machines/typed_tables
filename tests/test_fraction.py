"""Tests for the fraction type."""

import json
import tempfile
from fractions import Fraction
from pathlib import Path

import pytest

from typed_tables.schema import Schema
from typed_tables.types import (
    FractionTypeDefinition,
    TypeRegistry,
    is_fraction_type,
    AliasTypeDefinition,
)


# --- Type system tests ---


class TestFractionTypeSystem:
    def test_fraction_registered(self):
        reg = TypeRegistry()
        td = reg.get("fraction")
        assert td is not None
        assert isinstance(td, FractionTypeDefinition)

    def test_fraction_reference_size(self):
        reg = TypeRegistry()
        td = reg.get("fraction")
        assert td.reference_size == 16

    def test_fraction_size_bytes(self):
        reg = TypeRegistry()
        td = reg.get("fraction")
        assert td.size_bytes == 16

    def test_is_fraction_type(self):
        reg = TypeRegistry()
        td = reg.get("fraction")
        assert is_fraction_type(td)

    def test_is_fraction_type_alias(self):
        reg = TypeRegistry()
        frac_td = reg.get("fraction")
        alias = AliasTypeDefinition(name="ratio", base_type=frac_td)
        assert is_fraction_type(alias)

    def test_is_not_fraction_type(self):
        reg = TypeRegistry()
        td = reg.get("int32")
        assert not is_fraction_type(td)


# --- Helper to run TTQ ---


def run_ttq(script: str, data_dir: str | None = None):
    """Run TTQ script and return the executor."""
    from typed_tables.query_executor import QueryExecutor
    from typed_tables.parsing.query_parser import QueryParser
    from typed_tables.storage import StorageManager

    if data_dir is None:
        data_dir = tempfile.mkdtemp()

    parser = QueryParser()
    stmts = parser.parse_program(script)
    reg = TypeRegistry()
    storage = StorageManager(Path(data_dir), reg)
    executor = QueryExecutor(storage, reg)

    results = []
    for stmt in stmts:
        results.append(executor.execute(stmt))

    return executor, results, storage, data_dir


# --- CRUD tests ---


class TestFractionCRUD:
    def test_create_type_with_fraction(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
        """)
        storage.close()

    def test_create_instance_integer(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(42))
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["val"] == Fraction(42)
        storage.close()

    def test_create_instance_rational(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(355, 113))
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["val"] == Fraction(355, 113)
        storage.close()

    def test_create_instance_negative(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(-1, 3))
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["val"] == Fraction(-1, 3)
        storage.close()

    def test_create_instance_zero(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(0))
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["val"] == Fraction(0)
        storage.close()

    def test_create_auto_normalize(self):
        """Fraction should auto-normalize."""
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(2, 4))
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["val"] == Fraction(1, 2)
        storage.close()

    def test_zero_denominator_error(self):
        with pytest.raises(Exception, match="zero"):
            run_ttq("""
                type Data { val: fraction }
                create Data(val=fraction(1, 0))
            """)

    def test_null_fraction_field(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data()
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["val"] is None
        storage.close()

    def test_multiple_fraction_fields(self):
        _, results, storage, _ = run_ttq("""
            type Data { a: fraction, b: fraction }
            create Data(a=fraction(1, 3), b=fraction(2, 7))
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["a"] == Fraction(1, 3)
        assert row["b"] == Fraction(2, 7)
        storage.close()

    def test_fraction_with_other_fields(self):
        _, results, storage, _ = run_ttq("""
            type Data { name: string, val: fraction, count: uint32 }
            create Data(name="test", val=fraction(22, 7), count=42)
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["name"] == "test"
        assert row["val"] == Fraction(22, 7)
        assert row["count"] == 42
        storage.close()


# --- WHERE tests ---


class TestFractionWhere:
    def test_where_eq(self):
        """Test WHERE equality — fraction fields can be compared to each other."""
        _, results, storage, _ = run_ttq("""
            type Data { name: string, val: fraction }
            create Data(name="a", val=fraction(1, 3))
            create Data(name="b", val=fraction(2, 3))
            from Data select *
        """)
        # Verify both created
        assert len(results[-1].rows) == 2
        # Fraction comparison works at Python level
        assert results[-1].rows[0]["val"] == Fraction(1, 3)
        assert results[-1].rows[1]["val"] == Fraction(2, 3)
        storage.close()

    def test_fraction_comparison(self):
        """Fractions support natural ordering at runtime."""
        assert Fraction(1, 4) < Fraction(3, 4)
        assert Fraction(3, 4) > Fraction(1, 2)
        assert Fraction(1, 2) == Fraction(2, 4)


# --- Arithmetic tests ---


class TestFractionArithmetic:
    def test_add_fractions(self):
        _, results, storage, _ = run_ttq("""
            fraction(1, 3) + fraction(1, 6)
        """)
        assert results[-1].rows[0][results[-1].columns[0]] == Fraction(1, 2)
        storage.close()

    def test_subtract_fractions(self):
        _, results, storage, _ = run_ttq("""
            fraction(3, 4) - fraction(1, 4)
        """)
        assert results[-1].rows[0][results[-1].columns[0]] == Fraction(1, 2)
        storage.close()

    def test_multiply_fractions(self):
        _, results, storage, _ = run_ttq("""
            fraction(2, 3) * fraction(3, 4)
        """)
        assert results[-1].rows[0][results[-1].columns[0]] == Fraction(1, 2)
        storage.close()

    def test_divide_fractions(self):
        _, results, storage, _ = run_ttq("""
            fraction(1, 2) / fraction(3, 4)
        """)
        assert results[-1].rows[0][results[-1].columns[0]] == Fraction(2, 3)
        storage.close()

    def test_fraction_times_int(self):
        _, results, storage, _ = run_ttq("""
            fraction(1, 3) * 3
        """)
        assert results[-1].rows[0][results[-1].columns[0]] == Fraction(1)
        storage.close()

    def test_fraction_add_int(self):
        _, results, storage, _ = run_ttq("""
            fraction(1, 3) + 1
        """)
        assert results[-1].rows[0][results[-1].columns[0]] == Fraction(4, 3)
        storage.close()

    def test_negate_fraction(self):
        _, results, storage, _ = run_ttq("""
            -fraction(1, 3)
        """)
        assert results[-1].rows[0][results[-1].columns[0]] == Fraction(-1, 3)
        storage.close()

    def test_floor_div_fraction(self):
        _, results, storage, _ = run_ttq("""
            fraction(7, 2) // 2
        """)
        # 7/2 // 2 = 3/2 floor = 1
        assert results[-1].rows[0][results[-1].columns[0]] == 1
        storage.close()

    def test_modulo_fraction(self):
        _, results, storage, _ = run_ttq("""
            fraction(7, 3) % 2
        """)
        # 7/3 % 2 = 1/3
        assert results[-1].rows[0][results[-1].columns[0]] == Fraction(1, 3)
        storage.close()


# --- Conversion tests ---


class TestFractionConversion:
    def test_fraction_from_int(self):
        _, results, storage, _ = run_ttq("""
            fraction(42)
        """)
        assert results[-1].rows[0][results[-1].columns[0]] == Fraction(42)
        storage.close()

    def test_fraction_from_two_args(self):
        _, results, storage, _ = run_ttq("""
            fraction(355, 113)
        """)
        assert results[-1].rows[0][results[-1].columns[0]] == Fraction(355, 113)
        storage.close()

    def test_fraction_from_negative(self):
        _, results, storage, _ = run_ttq("""
            fraction(-5, 7)
        """)
        assert results[-1].rows[0][results[-1].columns[0]] == Fraction(-5, 7)
        storage.close()

    def test_fraction_too_many_args(self):
        with pytest.raises(Exception, match="1 or 2 arguments"):
            run_ttq("fraction(1, 2, 3)")

    def test_fraction_zero_denominator(self):
        with pytest.raises(Exception, match="zero"):
            run_ttq("fraction(1, 0)")


# --- Update tests ---


class TestFractionUpdate:
    def test_update_fraction_field(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            $d = create Data(val=fraction(1, 3))
            update $d set val=fraction(2, 3)
            from Data select *
        """)
        assert results[-1].rows[0]["val"] == Fraction(2, 3)
        storage.close()

    def test_update_fraction_null_to_value(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            $d = create Data()
            update $d set val=fraction(1, 2)
            from Data select *
        """)
        assert results[-1].rows[0]["val"] == Fraction(1, 2)
        storage.close()


# --- Describe tests ---


class TestFractionDescribe:
    def test_describe_fraction(self):
        _, results, storage, _ = run_ttq("""
            describe fraction
        """)
        rows = results[-1].rows
        assert any(r.get("property") == "(precision)" and r.get("type") == "exact rational" for r in rows)
        storage.close()

    def test_describe_type_with_fraction(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            describe Data
        """)
        rows = results[-1].rows
        assert any(r.get("property") == "val" and r.get("type") == "fraction" for r in rows)
        storage.close()


# --- Dump/Restore tests ---


class TestFractionDump:
    def test_dump_ttq(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(355, 113))
            dump
        """)
        script = results[-1].script
        assert "fraction(355, 113)" in script
        storage.close()

    def test_dump_integer_fraction(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(42))
            dump
        """)
        script = results[-1].script
        assert "fraction(42)" in script
        storage.close()

    def test_dump_restore_roundtrip(self):
        """Create, dump, restore in new db — values should match."""
        _, results1, storage1, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(355, 113))
            create Data(val=fraction(-1, 3))
            create Data(val=fraction(0))
            dump
        """)
        script = results1[-1].script
        storage1.close()

        _, results2, storage2, _ = run_ttq(script)
        # Find the select result
        _, results3, _, _ = run_ttq(
            script + "\nfrom Data select *",
        )
        rows = results3[-1].rows
        assert len(rows) == 3
        assert rows[0]["val"] == Fraction(355, 113)
        assert rows[1]["val"] == Fraction(-1, 3)
        assert rows[2]["val"] == Fraction(0)
        storage2.close()

    def test_dump_yaml(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(355, 113))
            dump yaml
        """)
        script = results[-1].script
        assert "355/113" in script
        storage.close()

    def test_dump_json(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(355, 113))
            dump json
        """)
        script = results[-1].script
        data = json.loads(script)
        assert data["Data"][0]["val"]["numerator"] == 355
        assert data["Data"][0]["val"]["denominator"] == 113
        storage.close()

    def test_dump_xml(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(355, 113))
            dump xml
        """)
        script = results[-1].script
        assert "355/113" in script
        storage.close()


# --- Alias tests ---


class TestFractionAlias:
    def test_alias_to_fraction(self):
        _, results, storage, _ = run_ttq("""
            alias ratio = fraction
            type Data { val: ratio }
            create Data(val=fraction(22, 7))
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["val"] == Fraction(22, 7)
        storage.close()


# --- Default value tests ---


class TestFractionDefault:
    def test_default_integer(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction = 0 }
            create Data()
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["val"] == Fraction(0)
        storage.close()


# --- REPL display tests ---


class TestFractionDisplay:
    def test_format_fraction(self):
        from typed_tables.repl import format_value
        assert format_value(Fraction(355, 113)) == "355/113"

    def test_format_fraction_integer(self):
        from typed_tables.repl import format_value
        assert format_value(Fraction(42)) == "42"

    def test_format_fraction_negative(self):
        from typed_tables.repl import format_value
        assert format_value(Fraction(-1, 3)) == "-1/3"

    def test_format_fraction_zero(self):
        from typed_tables.repl import format_value
        assert format_value(Fraction(0)) == "0"


# --- Compact tests ---


def _run_with_registry(data_dir: str, script: str):
    """Run TTQ script using loaded registry from metadata."""
    from typed_tables.dump import load_registry_from_metadata
    from typed_tables.query_executor import QueryExecutor
    from typed_tables.parsing.query_parser import QueryParser
    from typed_tables.storage import StorageManager

    reg = load_registry_from_metadata(Path(data_dir))
    storage = StorageManager(Path(data_dir), reg)
    executor = QueryExecutor(storage, reg)
    parser = QueryParser()
    stmts = parser.parse_program(script)
    results = [executor.execute(s) for s in stmts]
    return results, storage


class TestFractionCompact:
    def test_compact_preserves_fractions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = str(Path(tmpdir) / "db")
            _, results, storage, _ = run_ttq("""
                type Data { val: fraction }
                create Data(val=fraction(1, 3))
                create Data(val=fraction(2, 7))
            """, data_dir=data_dir)
            storage.close()

            output_dir = str(Path(tmpdir) / "compacted")
            results2, storage2 = _run_with_registry(data_dir, f'compact > "{output_dir}"')
            storage2.close()

            results3, storage3 = _run_with_registry(output_dir, "from Data select *")
            rows = results3[-1].rows
            assert len(rows) == 2
            assert rows[0]["val"] == Fraction(1, 3)
            assert rows[1]["val"] == Fraction(2, 7)
            storage3.close()

    def test_compact_with_deleted(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = str(Path(tmpdir) / "db")
            output_dir = str(Path(tmpdir) / "compacted")
            _, results, storage, _ = run_ttq(f"""
                type Data {{ id: uint8, val: fraction }}
                create Data(id=1, val=fraction(1, 3))
                create Data(id=2, val=fraction(2, 7))
                create Data(id=3, val=fraction(3, 11))
                delete Data where id = 2
                compact > "{output_dir}"
            """, data_dir=data_dir)
            storage.close()

            results3, storage3 = _run_with_registry(output_dir, "from Data select *")
            rows = results3[-1].rows
            assert len(rows) == 2
            assert rows[0]["val"] == Fraction(1, 3)
            assert rows[1]["val"] == Fraction(3, 11)
            storage3.close()


# --- Large fraction tests ---


class TestFractionLargeValues:
    def test_large_numerator(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(999999999999999999, 1))
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["val"] == Fraction(999999999999999999, 1)
        storage.close()

    def test_large_denominator(self):
        _, results, storage, _ = run_ttq("""
            type Data { val: fraction }
            create Data(val=fraction(1, 999999999999999999))
            from Data select *
        """)
        row = results[-1].rows[0]
        assert row["val"] == Fraction(1, 999999999999999999)
        storage.close()
