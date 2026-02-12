"""Tests for the status command (disk usage and table breakdown)."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.parsing.query_parser import QueryParser
from typed_tables.query_executor import QueryExecutor, QueryResult
from typed_tables.repl import _analyze_table_file, _format_size, print_status
from typed_tables.storage import StorageManager
from typed_tables.types import TypeRegistry


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


def _run(executor, *stmts):
    """Execute one or more TTQ statements and return the last result."""
    parser = QueryParser()
    result = None
    for stmt in stmts:
        queries = parser.parse_program(stmt)
        for q in queries:
            result = executor.execute(q)
    return result


# --- _format_size helper ---

class TestFormatSize:
    def test_bytes(self):
        assert _format_size(0) == "0 B"
        assert _format_size(1) == "1 B"
        assert _format_size(512) == "512 B"
        assert _format_size(1023) == "1023 B"

    def test_kilobytes(self):
        assert _format_size(1024) == "1 KB"
        assert _format_size(1536) == "1.5 KB"
        assert _format_size(4096) == "4 KB"

    def test_megabytes(self):
        assert _format_size(1024 * 1024) == "1 MB"
        assert _format_size(1024 * 1024 * 5) == "5 MB"

    def test_gigabytes(self):
        assert _format_size(1024 * 1024 * 1024) == "1 GB"
        assert _format_size(int(1024 * 1024 * 1024 * 2.5)) == "2.5 GB"


# --- print_status integration tests ---

class TestStatusNoDatabase:
    def test_no_database(self, capsys):
        """status with no database prints 'No database selected.'"""
        print_status(None, None)
        out = capsys.readouterr().out
        assert "No database selected." in out

    def test_no_executor(self, capsys, db_dir):
        """status with a data_dir but no executor prints just the path."""
        print_status(db_dir, None)
        out = capsys.readouterr().out
        assert str(db_dir) in out


class TestStatusEmptyDatabase:
    def test_empty_database(self, capsys, db_dir, executor):
        """status with an empty database shows 'No tables.'"""
        print_status(db_dir, executor)
        out = capsys.readouterr().out
        assert str(db_dir) in out
        assert "No tables" in out


class TestStatusWithData:
    def test_basic_composite(self, capsys, db_dir, executor):
        """status shows composite table with correct record count."""
        _run(executor,
             'type Person { name: string, age: uint8 }',
             'create Person(name="Alice", age=30)',
             'create Person(name="Bob", age=25)')

        print_status(db_dir, executor)
        out = capsys.readouterr().out

        assert "Database:" in out
        assert "Total size:" in out
        assert "Person" in out
        # Should show 2 records, 2 live, 0 deleted
        lines = out.strip().split("\n")
        person_line = [l for l in lines if "Person" in l and "Composite" in l]
        assert len(person_line) == 1
        # Check record counts
        assert "2" in person_line[0]  # records and live

    def test_array_table_shown(self, capsys, db_dir, executor):
        """status shows array element tables."""
        _run(executor,
             'type Sensor { name: string, readings: int8[] }',
             'create Sensor(name="temp", readings=[25, 26, 27])')

        print_status(db_dir, executor)
        out = capsys.readouterr().out

        # Should show Sensor (composite), string (array), int8[] (array)
        assert "Sensor" in out
        assert "string" in out

    def test_total_row_present(self, capsys, db_dir, executor):
        """status includes a TOTAL row."""
        _run(executor,
             'type Point { x: uint8, y: uint8 }',
             'create Point(x=1, y=2)')

        print_status(db_dir, executor)
        out = capsys.readouterr().out

        assert "TOTAL" in out


class TestStatusWithDeletions:
    def test_deleted_records_counted(self, capsys, db_dir, executor):
        """status shows correct deleted count after deleting records."""
        _run(executor,
             'type Item { value: uint8 }',
             'create Item(value=1)',
             'create Item(value=2)',
             'create Item(value=3)',
             'delete Item where value=2')

        print_status(db_dir, executor)
        out = capsys.readouterr().out

        # Find the Item line
        lines = out.strip().split("\n")
        item_line = [l for l in lines if "Item" in l and "Composite" in l]
        assert len(item_line) == 1
        # Should have 3 records, 2 live, 1 deleted
        parts = item_line[0].split("|")
        # columns: table, kind, records, live, deleted, ...
        records_col = parts[2].strip()
        live_col = parts[3].strip()
        deleted_col = parts[4].strip()
        assert records_col == "3"
        assert live_col == "2"
        assert deleted_col == "1"

    def test_dead_space_nonzero(self, capsys, db_dir, executor):
        """After deletion, dead space should be > 0."""
        _run(executor,
             'type Item { value: uint8 }',
             'create Item(value=1)',
             'create Item(value=2)',
             'delete Item where value=1')

        print_status(db_dir, executor)
        out = capsys.readouterr().out

        # dead_size column should not be "0 B" for Item
        lines = out.strip().split("\n")
        item_line = [l for l in lines if "Item" in l and "Composite" in l]
        assert len(item_line) == 1
        parts = item_line[0].split("|")
        dead_size_col = parts[7].strip()  # dead_size column
        assert dead_size_col != "0 B"


class TestSavingsCalculation:
    def test_savings_zero_when_table_fits_in_initial_size(self, db_dir, executor):
        """If live data fits in initial 4096-byte file, savings = 0 even with deletions."""
        # A uint8 record is 2 bytes (1 byte null bitmap + 1 byte value).
        # 3 records = 6 bytes data + 8 byte header = 14 bytes, well under 4096.
        # Deleting one doesn't change the compacted file size (still 4096).
        _run(executor,
             'type Item { value: uint8 }',
             'create Item(value=1)',
             'create Item(value=2)',
             'create Item(value=3)',
             'delete Item where value=2')

        bin_path = db_dir / "Item.bin"
        result = _analyze_table_file(bin_path, db_dir, executor)
        assert result is not None
        # File is 4096, compacted would also be 4096, so savings = 0
        assert result["_savings_raw"] == 0
        # But dead space is nonzero (the tombstone occupies record bytes)
        assert result["_dead_size_raw"] > 0

    def test_savings_nonzero_when_file_has_grown(self, db_dir, executor):
        """If file has grown beyond initial size but live data fits in smaller size, savings > 0."""
        # Create enough records to force file growth past 4096, then delete most
        # uint8 record = 2 bytes. Capacity in 4096: (4096-8)//2 = 2044 records
        # Need > 2044 records to force growth to 8192
        _run(executor, 'type Item { value: uint8 }')
        for i in range(2045):
            _run(executor, f'create Item(value={i % 256})')
        # Now delete all but one
        _run(executor, 'delete Item where value != 0')

        bin_path = db_dir / "Item.bin"
        result = _analyze_table_file(bin_path, db_dir, executor)
        assert result is not None
        # File should be 8192 (grew once). Live data fits in 4096.
        assert result["_file_size_raw"] == 8192
        assert result["_savings_raw"] == 8192 - 4096  # 4096 bytes saved


class TestStatusEnumVariants:
    def test_variant_tables_shown(self, capsys, db_dir, executor):
        """status shows enum variant tables in subdirectories."""
        _run(executor,
             'enum Shape { none, circle(r: float32), rect(w: float32, h: float32) }',
             'type Canvas { name: string, shape: Shape }',
             'create Canvas(name="c1", shape=.circle(r=5.0))',
             'create Canvas(name="c2", shape=.rect(w=10.0, h=20.0))')

        print_status(db_dir, executor)
        out = capsys.readouterr().out

        # Should show Shape/circle and Shape/rect variant tables
        assert "Shape/circle" in out
        assert "Shape/rect" in out
        # Both should have kind "Variant"
        lines = out.strip().split("\n")
        variant_lines = [l for l in lines if "Variant" in l]
        assert len(variant_lines) == 2


class TestAnalyzeTableFile:
    def test_returns_none_for_unknown(self, db_dir, executor):
        """_analyze_table_file returns None for unknown .bin files in subdirs."""
        unknown_dir = db_dir / "unknown_subdir" / "deep"
        unknown_dir.mkdir(parents=True)
        unknown_file = unknown_dir / "mystery.bin"
        unknown_file.write_bytes(b"\x00" * 100)
        result = _analyze_table_file(unknown_file, db_dir, executor)
        assert result is None

    def test_unknown_root_file(self, db_dir, executor):
        """_analyze_table_file returns Unknown kind for unrecognized root .bin."""
        unknown_file = db_dir / "orphan.bin"
        unknown_file.write_bytes(b"\x00" * 100)
        result = _analyze_table_file(unknown_file, db_dir, executor)
        assert result is not None
        assert result["kind"] == "Unknown"
        assert result["records"] == "?"

    def test_composite_metrics(self, db_dir, executor):
        """_analyze_table_file returns correct metrics for a composite table."""
        _run(executor,
             'type Item { value: uint8 }',
             'create Item(value=1)',
             'create Item(value=2)')

        bin_path = db_dir / "Item.bin"
        result = _analyze_table_file(bin_path, db_dir, executor)
        assert result is not None
        assert result["kind"] == "Composite"
        assert result["_records_raw"] == 2
        assert result["_live_raw"] == 2
        assert result["_deleted_raw"] == 0
        assert result["_file_size_raw"] > 0
        assert result["_dead_size_raw"] == 0
