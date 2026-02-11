"""Tests for the archive and restore commands."""

from __future__ import annotations

import gzip
import shutil
import struct
import tempfile
from pathlib import Path

import pytest

from typed_tables.dump import load_registry_from_metadata
from typed_tables.parsing.query_parser import ArchiveQuery, QueryParser, RestoreQuery
from typed_tables.query_executor import (
    ArchiveResult,
    QueryExecutor,
    RestoreResult,
    execute_restore,
)
from typed_tables.storage import StorageManager
from typed_tables.types import EnumValue, TypeRegistry


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


class TestArchiveParsing:
    def test_parse_archive(self):
        """Parser produces ArchiveQuery with output_file."""
        parser = QueryParser()
        query = parser.parse('archive to "backup.ttar"')
        assert isinstance(query, ArchiveQuery)
        assert query.output_file == "backup.ttar"

    def test_parse_restore(self):
        """Parser produces RestoreQuery with archive_file and output_path."""
        parser = QueryParser()
        query = parser.parse('restore "backup.ttar" to "restored_db"')
        assert isinstance(query, RestoreQuery)
        assert query.archive_file == "backup.ttar"
        assert query.output_path == "restored_db"

    def test_parse_restore_no_target(self):
        """Parser produces RestoreQuery with output_path=None when TO is omitted."""
        parser = QueryParser()
        query = parser.parse('restore "backup.ttar"')
        assert isinstance(query, RestoreQuery)
        assert query.archive_file == "backup.ttar"
        assert query.output_path is None


class TestArchiveExecution:
    def test_archive_empty_database(self, executor, db_dir):
        """Archive + restore empty DB produces valid empty DB."""
        ttar = db_dir / "empty.ttar"
        result = _run(executor, f'archive to "{ttar}"')
        assert isinstance(result, ArchiveResult)
        assert ttar.exists()
        assert result.total_bytes > 0

        out = db_dir / "restored"
        res = execute_restore(RestoreQuery(archive_file=str(ttar), output_path=str(out)))
        assert isinstance(res, RestoreResult)
        assert (out / "_metadata.json").exists()

    def test_archive_simple_types(self, executor, db_dir):
        """Archive DB with primitive/string fields, restore, verify."""
        _run(executor, """
            type Person { name: string, age: uint8 }
            create Person(name="Alice", age=30)
            create Person(name="Bob", age=25)
        """)
        ttar = db_dir / "backup.ttar"
        result = _run(executor, f'archive to "{ttar}"')
        assert isinstance(result, ArchiveResult)
        assert result.file_count > 0

        out = db_dir / "restored"
        execute_restore(RestoreQuery(archive_file=str(ttar), output_path=str(out)))

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Person select *"))
        assert len(res.rows) == 2
        names = {r["name"] for r in res.rows}
        assert names == {"Alice", "Bob"}
        storage2.close()

    def test_archive_composite_refs(self, executor, db_dir):
        """Composite refs survive round-trip."""
        _run(executor, """
            type Person { name: string }
            type Team { lead: Person, name: string }
            create Person(name="Alice")
            create Person(name="Bob")
            create Team(lead=Person(1), name="Alpha")
        """)
        ttar = db_dir / "backup.ttar"
        _run(executor, f'archive to "{ttar}"')

        out = db_dir / "restored"
        execute_restore(RestoreQuery(archive_file=str(ttar), output_path=str(out)))

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Team select *"))
        assert len(res.rows) == 1
        assert res.rows[0]["lead"] == "<Person[1]>"
        storage2.close()

    def test_archive_arrays(self, executor, db_dir):
        """Array fields survive round-trip."""
        _run(executor, """
            type Sensor { name: string, readings: uint8[] }
            create Sensor(name="temp", readings=[10, 20, 30])
            create Sensor(name="humidity", readings=[50, 60])
        """)
        ttar = db_dir / "backup.ttar"
        _run(executor, f'archive to "{ttar}"')

        out = db_dir / "restored"
        execute_restore(RestoreQuery(archive_file=str(ttar), output_path=str(out)))

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Sensor select *"))
        assert len(res.rows) == 2
        rows_by_name = {r["name"]: r for r in res.rows}
        assert rows_by_name["temp"]["readings"] == [10, 20, 30]
        assert rows_by_name["humidity"]["readings"] == [50, 60]
        storage2.close()

    def test_archive_enums_c_style(self, executor, db_dir):
        """C-style enum values survive round-trip."""
        _run(executor, """
            enum Color { red, green, blue }
            type Pixel { x: uint16, color: Color }
            create Pixel(x=0, color=.red)
            create Pixel(x=1, color=.blue)
        """)
        ttar = db_dir / "backup.ttar"
        _run(executor, f'archive to "{ttar}"')

        out = db_dir / "restored"
        execute_restore(RestoreQuery(archive_file=str(ttar), output_path=str(out)))

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

    def test_archive_enums_swift_style(self, executor, db_dir):
        """Swift-style enums (variant tables) survive round-trip."""
        _run(executor, """
            enum Shape { none, circle(r: float32) }
            type Canvas { name: string, bg: Shape }
            create Canvas(name="A", bg=.circle(r=10.0))
            create Canvas(name="B", bg=.none)
        """)
        ttar = db_dir / "backup.ttar"
        _run(executor, f'archive to "{ttar}"')

        out = db_dir / "restored"
        execute_restore(RestoreQuery(archive_file=str(ttar), output_path=str(out)))

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Canvas select *"))
        assert len(res.rows) == 2
        rows_by_name = {r["name"]: r for r in res.rows}
        bg_a = rows_by_name["A"]["bg"]
        assert isinstance(bg_a, EnumValue)
        assert bg_a.variant_name == "circle"
        assert abs(bg_a.fields["r"] - 10.0) < 0.001
        bg_b = rows_by_name["B"]["bg"]
        assert isinstance(bg_b, EnumValue)
        assert bg_b.variant_name == "none"
        storage2.close()

    def test_archive_compacts_first(self, executor, db_dir):
        """Archive after deleting records produces clean archive."""
        _run(executor, """
            type Person { name: string, age: uint8 }
            create Person(name="Alice", age=30)
            create Person(name="Bob", age=25)
            create Person(name="Charlie", age=35)
            delete Person where name="Bob"
        """)
        ttar = db_dir / "backup.ttar"
        _run(executor, f'archive to "{ttar}"')

        out = db_dir / "restored"
        execute_restore(RestoreQuery(archive_file=str(ttar), output_path=str(out)))

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Person select *"))
        assert len(res.rows) == 2
        names = {r["name"] for r in res.rows}
        assert names == {"Alice", "Charlie"}
        storage2.close()

    def test_archive_interfaces(self, executor, db_dir):
        """Interface refs survive round-trip."""
        _run(executor, """
            interface Animal { name: string }
            type Dog from Animal { breed: string }
            type Shelter { resident: Animal }
            create Dog(name="Rex", breed="Lab")
            create Shelter(resident=Dog(0))
        """)
        ttar = db_dir / "backup.ttar"
        _run(executor, f'archive to "{ttar}"')

        out = db_dir / "restored"
        execute_restore(RestoreQuery(archive_file=str(ttar), output_path=str(out)))

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Shelter select *"))
        assert len(res.rows) == 1
        assert res.rows[0]["resident"] == "<Dog[0]>"
        storage2.close()

    def test_archive_null_fields(self, executor, db_dir):
        """Null fields preserved through round-trip."""
        _run(executor, """
            type Node { value: uint8, next: Node }
            create Node(value=1, next=null)
            create Node(value=2)
        """)
        ttar = db_dir / "backup.ttar"
        _run(executor, f'archive to "{ttar}"')

        out = db_dir / "restored"
        execute_restore(RestoreQuery(archive_file=str(ttar), output_path=str(out)))

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Node select *"))
        assert len(res.rows) == 2
        for row in res.rows:
            assert row["next"] is None
        storage2.close()

    def test_archive_default_values(self, executor, db_dir):
        """Type defaults survive metadata round-trip."""
        _run(executor, """
            type Config { name: string, level: uint8 = 5 }
            create Config(name="test")
        """)
        ttar = db_dir / "backup.ttar"
        _run(executor, f'archive to "{ttar}"')

        out = db_dir / "restored"
        execute_restore(RestoreQuery(archive_file=str(ttar), output_path=str(out)))

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Config select *"))
        assert len(res.rows) == 1
        assert res.rows[0]["level"] == 5
        storage2.close()

    def test_archive_appends_ttar_extension(self, executor, db_dir):
        """Extension auto-appended when missing."""
        _run(executor, """
            type Item { value: uint8 }
            create Item(value=1)
        """)
        ttar_name = str(db_dir / "backup")
        result = _run(executor, f'archive to "{ttar_name}"')
        assert isinstance(result, ArchiveResult)
        assert result.output_file.endswith(".ttar")
        assert Path(result.output_file).exists()

    def test_archive_preserves_ttar_extension(self, executor, db_dir):
        """Extension not doubled when already present."""
        _run(executor, """
            type Item { value: uint8 }
            create Item(value=1)
        """)
        ttar_name = str(db_dir / "backup.ttar")
        result = _run(executor, f'archive to "{ttar_name}"')
        assert isinstance(result, ArchiveResult)
        assert result.output_file == ttar_name

    def test_archive_error_file_exists(self, executor, db_dir):
        """Error when output file already exists."""
        existing = db_dir / "existing.ttar"
        existing.write_bytes(b"dummy")
        result = _run(executor, f'archive to "{existing}"')
        assert isinstance(result, ArchiveResult)
        assert "already exists" in result.message

    def test_restore_error_archive_missing(self, db_dir):
        """Error when archive file doesn't exist."""
        result = execute_restore(
            RestoreQuery(archive_file="/nonexistent.ttar", output_path=str(db_dir / "out"))
        )
        assert isinstance(result, RestoreResult)
        assert "not found" in result.message

    def test_restore_error_output_exists(self, executor, db_dir):
        """Error when output path already exists."""
        _run(executor, """
            type Item { value: uint8 }
            create Item(value=1)
        """)
        ttar = db_dir / "backup.ttar"
        _run(executor, f'archive to "{ttar}"')

        existing_out = db_dir / "existing_out"
        existing_out.mkdir()
        result = execute_restore(
            RestoreQuery(archive_file=str(ttar), output_path=str(existing_out))
        )
        assert isinstance(result, RestoreResult)
        assert "already exists" in result.message

    def test_restore_error_bad_magic(self, db_dir):
        """Error on invalid file."""
        bad_file = db_dir / "bad.ttar"
        bad_file.write_bytes(b"NOT_TTAR_FILE_HEADER")
        result = execute_restore(
            RestoreQuery(archive_file=str(bad_file), output_path=str(db_dir / "out"))
        )
        assert isinstance(result, RestoreResult)
        assert "bad magic" in result.message

    def test_restore_queryable(self, executor, db_dir):
        """Full round-trip: create -> archive -> restore -> query."""
        _run(executor, """
            type Person { name: string, age: uint8 }
            type Team { lead: Person, name: string }
            create Person(name="Alice", age=30)
            create Person(name="Bob", age=25)
            create Team(lead=Person(0), name="Alpha")
            create Team(lead=Person(1), name="Beta")
        """)
        ttar = db_dir / "backup.ttar"
        _run(executor, f'archive to "{ttar}"')

        out = db_dir / "restored"
        execute_restore(RestoreQuery(archive_file=str(ttar), output_path=str(out)))

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()

        # Query persons
        res = exec2.execute(parser.parse("from Person select * sort by name"))
        assert len(res.rows) == 2
        assert res.rows[0]["name"] == "Alice"
        assert res.rows[0]["age"] == 30
        assert res.rows[1]["name"] == "Bob"
        assert res.rows[1]["age"] == 25

        # Query teams with composite refs
        res = exec2.execute(parser.parse("from Team select * sort by name"))
        assert len(res.rows) == 2
        assert res.rows[0]["name"] == "Alpha"
        assert res.rows[0]["lead"] == "<Person[0]>"
        assert res.rows[1]["name"] == "Beta"
        assert res.rows[1]["lead"] == "<Person[1]>"

        # Filter
        res = exec2.execute(parser.parse('from Person select * where age >= 30'))
        assert len(res.rows) == 1
        assert res.rows[0]["name"] == "Alice"

        storage2.close()

    def test_restore_without_database(self, executor, db_dir):
        """Restore works without a loaded database (via module-level function)."""
        _run(executor, """
            type Item { value: uint8 }
            create Item(value=42)
        """)
        ttar = db_dir / "backup.ttar"
        _run(executor, f'archive to "{ttar}"')

        # Call execute_restore directly — no executor needed
        out = db_dir / "standalone_restore"
        result = execute_restore(
            RestoreQuery(archive_file=str(ttar), output_path=str(out))
        )
        assert isinstance(result, RestoreResult)
        assert result.file_count > 0
        assert (out / "_metadata.json").exists()

        # Verify it's queryable
        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Item select *"))
        assert len(res.rows) == 1
        assert res.rows[0]["value"] == 42
        storage2.close()

    def test_restore_derives_path_from_ttar(self, executor, db_dir):
        """restore 'backup.ttar' creates 'backup' directory."""
        _run(executor, """
            type Item { value: uint8 }
            create Item(value=1)
        """)
        ttar = db_dir / "mydb.ttar"
        _run(executor, f'archive to "{ttar}"')

        result = execute_restore(RestoreQuery(archive_file=str(ttar)))
        assert isinstance(result, RestoreResult)
        expected = db_dir / "mydb"
        assert expected.exists()
        assert (expected / "_metadata.json").exists()

        reg2 = load_registry_from_metadata(expected)
        storage2 = StorageManager(expected, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Item select *"))
        assert len(res.rows) == 1
        storage2.close()

    def test_restore_derives_path_from_ttar_gz(self, executor, db_dir):
        """restore 'backup.ttar.gz' creates 'backup' directory."""
        _run(executor, """
            type Item { value: uint8 }
            create Item(value=1)
        """)
        ttar_gz = db_dir / "mydb.ttar.gz"
        _run(executor, f'archive to "{ttar_gz}"')

        result = execute_restore(RestoreQuery(archive_file=str(ttar_gz)))
        assert isinstance(result, RestoreResult)
        expected = db_dir / "mydb"
        assert expected.exists()
        assert (expected / "_metadata.json").exists()

    def test_restore_derived_path_error_if_exists(self, executor, db_dir):
        """Error when derived output path already exists."""
        _run(executor, """
            type Item { value: uint8 }
            create Item(value=1)
        """)
        ttar = db_dir / "mydb.ttar"
        _run(executor, f'archive to "{ttar}"')

        # Create the directory that would be the derived path
        (db_dir / "mydb").mkdir()

        result = execute_restore(RestoreQuery(archive_file=str(ttar)))
        assert isinstance(result, RestoreResult)
        assert "already exists" in result.message

    def test_archive_binary_format(self, executor, db_dir):
        """Verify the binary format of the archive header."""
        _run(executor, """
            type Item { value: uint8 }
            create Item(value=1)
        """)
        ttar = db_dir / "format_check.ttar"
        _run(executor, f'archive to "{ttar}"')

        with open(ttar, "rb") as f:
            # Magic
            assert f.read(4) == b"TTAR"
            # Version
            version = struct.unpack("<H", f.read(2))[0]
            assert version == 1
            # Metadata length
            meta_len = struct.unpack("<I", f.read(4))[0]
            assert meta_len > 0
            # Skip metadata
            f.read(meta_len)
            # File count
            file_count = struct.unpack("<I", f.read(4))[0]
            assert file_count > 0


class TestGzipSupport:
    def test_archive_gzip_roundtrip(self, executor, db_dir):
        """Archive to .gz and restore produces valid database."""
        _run(executor, """
            type Person { name: string, age: uint8 }
            create Person(name="Alice", age=30)
            create Person(name="Bob", age=25)
        """)
        ttar_gz = db_dir / "backup.ttar.gz"
        result = _run(executor, f'archive to "{ttar_gz}"')
        assert isinstance(result, ArchiveResult)
        assert ttar_gz.exists()

        # Verify it's actually gzipped
        with open(ttar_gz, "rb") as f:
            magic = f.read(2)
            assert magic == b"\x1f\x8b"  # gzip magic bytes

        out = db_dir / "restored"
        execute_restore(RestoreQuery(archive_file=str(ttar_gz), output_path=str(out)))

        reg2 = load_registry_from_metadata(out)
        storage2 = StorageManager(out, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Person select *"))
        assert len(res.rows) == 2
        names = {r["name"] for r in res.rows}
        assert names == {"Alice", "Bob"}
        storage2.close()

    def test_archive_gzip_smaller(self, executor, db_dir):
        """Gzipped archive is smaller than uncompressed."""
        _run(executor, """
            type Person { name: string, age: uint8 }
            create Person(name="Alice", age=30)
            create Person(name="Bob", age=25)
            create Person(name="Charlie", age=35)
            create Person(name="Diana", age=28)
        """)
        plain = db_dir / "backup.ttar"
        _run(executor, f'archive to "{plain}"')
        compressed = db_dir / "backup.ttar.gz"
        _run(executor, f'archive to "{compressed}"')

        assert compressed.stat().st_size < plain.stat().st_size

    def test_dump_gzip_roundtrip(self, executor, db_dir):
        """Dump to .gz creates gzipped file that can be decompressed."""
        _run(executor, """
            type Item { value: uint8 }
            create Item(value=42)
        """)
        from typed_tables.repl import print_result
        result = _run(executor, 'dump to "unused"')
        # Manually set output_file and write via print_result
        gz_path = db_dir / "dump.ttq.gz"
        result.output_file = str(gz_path)
        print_result(result)

        assert gz_path.exists()
        # Verify it's gzipped
        with open(gz_path, "rb") as f:
            assert f.read(2) == b"\x1f\x8b"
        # Verify content decompresses to valid TTQ
        with gzip.open(gz_path, "rt") as f:
            content = f.read()
        assert "create Item" in content or "type Item" in content

    def test_execute_gzip_file(self, executor, db_dir):
        """Dump to .ttq.gz then execute it into a fresh database."""
        from typed_tables.repl import print_result, run_file

        _run(executor, """
            type Person { name: string, age: uint8 }
            create Person(name="Alice", age=30)
            create Person(name="Bob", age=25)
        """)
        # Dump to gzipped TTQ
        gz_path = db_dir / "dump.ttq.gz"
        result = _run(executor, 'dump to "unused"')
        result.output_file = str(gz_path)
        print_result(result)
        assert gz_path.exists()

        # Execute the gzipped script into a new database
        new_db = db_dir / "from_gz"
        new_db.mkdir()
        exit_code, _ = run_file(gz_path, new_db, verbose=False)
        assert exit_code == 0

        # Verify data arrived
        reg2 = load_registry_from_metadata(new_db)
        storage2 = StorageManager(new_db, reg2)
        exec2 = QueryExecutor(storage2, reg2)
        parser = QueryParser()
        res = exec2.execute(parser.parse("from Person select *"))
        assert len(res.rows) == 2
        names = {r["name"] for r in res.rows}
        assert names == {"Alice", "Bob"}
        storage2.close()
