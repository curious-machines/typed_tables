"""Tests for the compact command."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.dump import load_registry_from_metadata
from typed_tables.parsing.query_parser import CompactQuery, QueryParser
from typed_tables.query_executor import CompactResult, CreateResult, QueryExecutor, QueryResult
from typed_tables.storage import StorageManager
from typed_tables.types import EnumTypeDefinition, EnumValue, TypeRegistry


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


def _run(executor, text):
    """Parse and execute a TTQ program, returning the last result."""
    parser = QueryParser()
    stmts = parser.parse_program(text)
    result = None
    for stmt in stmts:
        result = executor.execute(stmt)
    return result


class TestCompactParsing:
    def test_parse_compact(self):
        """Parser produces CompactQuery with output_path."""
        parser = QueryParser()
        query = parser.parse('compact to "output_dir"')
        assert isinstance(query, CompactQuery)
        assert query.output_path == "output_dir"


class TestCompactExecution:
    def test_compact_empty_database(self, executor, db_dir):
        """Compact with no user types produces valid empty DB."""
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)
        assert result.records_before == 0
        assert result.records_after == 0
        assert (out / "_metadata.json").exists()

    def test_compact_no_deletions(self, executor, db_dir):
        """All records preserved unchanged when nothing deleted."""
        _run(executor, """
            create type Person { name: string, age: uint8 }
            create Person(name="Alice", age=30)
            create Person(name="Bob", age=25)
        """)
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)
        assert result.records_before == 2
        assert result.records_after == 2

        # Verify data in compacted database
        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        sel = parser.parse("from Person select *")
        res = exec2.execute(sel)
        assert len(res.rows) == 2
        names = {r["name"] for r in res.rows}
        assert names == {"Alice", "Bob"}
        storage2.close()

    def test_compact_removes_tombstones(self, executor, db_dir):
        """Deleted records absent, live records present with correct data."""
        _run(executor, """
            create type Person { name: string, age: uint8 }
            create Person(name="Alice", age=30)
            create Person(name="Bob", age=25)
            create Person(name="Charlie", age=35)
            delete Person where name="Bob"
        """)
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)
        assert result.records_before == 3
        assert result.records_after == 2

        # Verify compacted data
        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Person select *"))
        assert len(res.rows) == 2
        names = {r["name"] for r in res.rows}
        assert names == {"Alice", "Charlie"}
        storage2.close()

    def test_compact_remaps_composite_refs(self, executor, db_dir):
        """Composite ref indices updated after earlier record deleted."""
        _run(executor, """
            create type Person { name: string, age: uint8 }
            create type Team { lead: Person, name: string }
            create Person(name="Alice", age=30)
            create Person(name="Bob", age=25)
            create Person(name="Charlie", age=35)
            create Team(lead=Person(2), name="Alpha")
            delete Person where name="Bob"
        """)
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        # Charlie was index 2, should be remapped to 1
        res = exec2.execute(parser.parse("from Team select *"))
        assert len(res.rows) == 1
        lead = res.rows[0]["lead"]
        # select * shows composite refs as "<Type[index]>"
        assert lead == "<Person[1]>"  # Charlie remapped from 2 to 1

        # Verify Person table directly
        res2 = exec2.execute(parser.parse("from Person select *"))
        names = {r["name"] for r in res2.rows}
        assert names == {"Alice", "Charlie"}
        storage2.close()

    def test_compact_remaps_interface_refs(self, executor, db_dir):
        """Interface (type_id, index) remapped correctly."""
        _run(executor, """
            create interface Animal { name: string }
            create type Dog from Animal { breed: string }
            create type Shelter { resident: Animal }
            create Dog(name="Rex", breed="Lab")
            create Dog(name="Spot", breed="Dalmatian")
            create Dog(name="Buddy", breed="Golden")
            create Shelter(resident=Dog(2))
            delete Dog where name="Spot"
        """)
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Shelter select *"))
        assert len(res.rows) == 1
        resident = res.rows[0]["resident"]
        # select * shows interface refs as "<Type[index]>"
        assert resident == "<Dog[1]>"  # Buddy remapped from 2 to 1

        # Verify Dog table directly
        res2 = exec2.execute(parser.parse("from Dog select *"))
        names = {r["name"] for r in res2.rows}
        assert names == {"Rex", "Buddy"}
        storage2.close()

    def test_compact_compacts_arrays(self, executor, db_dir):
        """Orphaned array elements removed, data preserved for live records."""
        _run(executor, """
            create type Sensor { name: string, readings: uint8[] }
            create Sensor(name="A", readings=[1, 2, 3])
            create Sensor(name="B", readings=[4, 5, 6, 7])
            create Sensor(name="C", readings=[8, 9])
            delete Sensor where name="B"
        """)
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Sensor select *"))
        assert len(res.rows) == 2
        rows_by_name = {r["name"]: r for r in res.rows}
        assert rows_by_name["A"]["readings"] == [1, 2, 3]
        assert rows_by_name["C"]["readings"] == [8, 9]

        # Verify element table is smaller: only 5 elements (not 9)
        arr_type = reg2.get("uint8[]")
        arr_table = storage2.get_array_table_for_type(arr_type)
        assert arr_table.count == 5
        storage2.close()

    def test_compact_compacts_variants(self, executor, db_dir):
        """Orphaned variant records removed, enum values preserved."""
        _run(executor, """
            create enum Shape { none, circle(r: float32) }
            create type Canvas { name: string, bg: Shape }
            create Canvas(name="A", bg=.circle(r=10.0))
            create Canvas(name="B", bg=.circle(r=20.0))
            create Canvas(name="C", bg=.none)
            delete Canvas where name="A"
        """)
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Canvas select *"))
        assert len(res.rows) == 2
        rows_by_name = {r["name"]: r for r in res.rows}
        bg_b = rows_by_name["B"]["bg"]
        assert isinstance(bg_b, EnumValue)
        assert bg_b.variant_name == "circle"
        assert abs(bg_b.fields["r"] - 20.0) < 0.001
        bg_c = rows_by_name["C"]["bg"]
        assert isinstance(bg_c, EnumValue)
        assert bg_c.variant_name == "none"
        storage2.close()

    def test_compact_preserves_c_style_enums(self, executor, db_dir):
        """C-style enum values unchanged."""
        _run(executor, """
            create enum Color { red, green, blue }
            create type Pixel { x: uint16, color: Color }
            create Pixel(x=0, color=.red)
            create Pixel(x=1, color=.blue)
        """)
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Pixel select *"))
        assert len(res.rows) == 2
        colors = {r["x"]: r["color"] for r in res.rows}
        assert colors[0].variant_name == "red"
        assert colors[1].variant_name == "blue"
        storage2.close()

    def test_compact_dangling_ref_becomes_null(self, executor, db_dir):
        """Ref to deleted record becomes null."""
        _run(executor, """
            create type Node { value: uint8, next: Node }
            create Node(value=1, next=null)
            create Node(value=2, next=Node(0))
            delete Node where value=1
        """)
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Node select *"))
        assert len(res.rows) == 1
        assert res.rows[0]["value"] == 2
        assert res.rows[0]["next"] is None  # Dangling ref became null
        storage2.close()

    def test_compact_error_if_exists(self, executor, db_dir):
        """Error when output path already exists."""
        existing = db_dir / "existing_dir"
        existing.mkdir()
        result = _run(executor, f'compact to "{existing}"')
        assert isinstance(result, CompactResult)
        assert "already exists" in result.message

    def test_compact_stats(self, executor, db_dir):
        """CompactResult reports correct before/after counts."""
        _run(executor, """
            create type Item { value: uint8 }
            create Item(value=1)
            create Item(value=2)
            create Item(value=3)
            create Item(value=4)
            create Item(value=5)
            delete Item where value=2
            delete Item where value=4
        """)
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)
        assert result.records_before == 5
        assert result.records_after == 3
        assert result.output_path == str(out)

    def test_compact_queryable(self, executor, db_dir):
        """Compacted DB can be loaded and queried end-to-end."""
        _run(executor, """
            create type Person { name: string, age: uint8 }
            create type Team { lead: Person, name: string }
            create Person(name="Alice", age=30)
            create Person(name="Bob", age=25)
            create Person(name="Charlie", age=35)
            create Team(lead=Person(2), name="Alpha")
            create Team(lead=Person(0), name="Beta")
            delete Person where name="Bob"
        """)
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)

        # Load compacted database and verify full queryability
        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()

        # Persons: Alice (0), Charlie (1) — no Bob
        res = exec2.execute(parser.parse("from Person select *"))
        assert len(res.rows) == 2
        names = sorted(r["name"] for r in res.rows)
        assert names == ["Alice", "Charlie"]

        # Teams: Alpha lead=Charlie (index 2->1), Beta lead=Alice (index 0->0)
        res = exec2.execute(parser.parse("from Team select * sort by name"))
        assert len(res.rows) == 2
        assert res.rows[0]["name"] == "Alpha"
        assert res.rows[0]["lead"] == "<Person[1]>"  # Charlie remapped from 2 to 1
        assert res.rows[1]["name"] == "Beta"
        assert res.rows[1]["lead"] == "<Person[0]>"  # Alice stays at 0

        # Can also query with filters
        res = exec2.execute(parser.parse('from Person select * where age >= 30'))
        assert len(res.rows) == 2  # Alice (30) and Charlie (35)

        storage2.close()

    def test_compact_empty_arrays(self, executor, db_dir):
        """Empty arrays are preserved correctly."""
        _run(executor, """
            create type List { items: uint8[] }
            create List(items=[])
            create List(items=[1, 2])
        """)
        out = db_dir / "compacted"
        result = _run(executor, f'compact to "{out}"')
        assert isinstance(result, CompactResult)

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from List select *"))
        assert len(res.rows) == 2
        items_lists = sorted(res.rows, key=lambda r: len(r["items"]))
        assert items_lists[0]["items"] == []
        assert items_lists[1]["items"] == [1, 2]
        storage2.close()
