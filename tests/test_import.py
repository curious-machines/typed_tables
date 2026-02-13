"""Tests for the import statement, system types, and path alias."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.parsing.query_parser import ImportQuery, QueryParser, ShowTypesQuery
from typed_tables.query_executor import CreateResult, ImportResult, QueryExecutor, QueryResult
from typed_tables.storage import StorageManager
from typed_tables.types import AliasTypeDefinition, StringTypeDefinition, TypeRegistry


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


# --- Parsing ---


class TestImportParsing:
    def test_parse_import(self):
        """import 'setup.ttq' -> ImportQuery(file_path='setup.ttq')."""
        parser = QueryParser()
        query = parser.parse('import "setup.ttq"')
        assert isinstance(query, ImportQuery)
        assert query.file_path == "setup.ttq"

    def test_parse_import_in_program(self):
        """Multi-statement with import parses correctly."""
        parser = QueryParser()
        stmts = parser.parse_program(
            'type Foo { x: uint8 }\n'
            'import "other.ttq"\n'
        )
        assert len(stmts) == 2
        assert isinstance(stmts[1], ImportQuery)
        assert stmts[1].file_path == "other.ttq"

    def test_parse_show_system_types(self):
        """show system types -> ShowTypesQuery(filter='system')."""
        parser = QueryParser()
        query = parser.parse("show system types")
        assert isinstance(query, ShowTypesQuery)
        assert query.filter == "system"


# --- Executor ---


class TestImportExecution:
    def test_import_creates_import_record_type(self, executor, db_dir):
        """After first import, _ImportRecord type exists in registry."""
        script = db_dir / "setup.ttq"
        script.write_text('type Point { x: float32, y: float32 }')

        _run(executor, f'import "{script}"')
        assert executor.registry.get("_ImportRecord") is not None

    def test_import_executes_script(self, executor, db_dir):
        """Import runs the script contents (types created, instances inserted)."""
        script = db_dir / "setup.ttq"
        script.write_text(
            'type Point { x: float32, y: float32 }\n'
            'create Point(x=1.0, y=2.0)'
        )

        result = _run(executor, f'import "{script}"')
        assert isinstance(result, ImportResult)
        assert result.skipped is False
        assert "2 statements" in result.message

        # Verify type was created
        assert executor.registry.get("Point") is not None

    def test_import_skips_second_run(self, executor, db_dir):
        """Second import of same file returns skipped=True."""
        script = db_dir / "setup.ttq"
        script.write_text('type Point { x: float32, y: float32 }')

        result1 = _run(executor, f'import "{script}"')
        assert isinstance(result1, ImportResult)
        assert result1.skipped is False

        result2 = _run(executor, f'import "{script}"')
        assert isinstance(result2, ImportResult)
        assert result2.skipped is True
        assert "Already imported" in result2.message

    def test_import_different_files(self, executor, db_dir):
        """Importing two different files both execute."""
        script1 = db_dir / "types1.ttq"
        script1.write_text('type A { x: uint8 }')

        script2 = db_dir / "types2.ttq"
        script2.write_text('type B { y: uint16 }')

        result1 = _run(executor, f'import "{script1}"')
        assert isinstance(result1, ImportResult)
        assert result1.skipped is False

        result2 = _run(executor, f'import "{script2}"')
        assert isinstance(result2, ImportResult)
        assert result2.skipped is False

        assert executor.registry.get("A") is not None
        assert executor.registry.get("B") is not None

    def test_import_rejects_use_drop_restore(self, executor, db_dir):
        """Lifecycle commands rejected inside imported scripts (inherits from execute)."""
        script = db_dir / "bad.ttq"
        script.write_text('use some_other_db')

        with pytest.raises(RuntimeError, match="not allowed inside executed scripts"):
            _run(executor, f'import "{script}"')

    def test_import_auto_extension(self, executor, db_dir):
        """Import auto-appends .ttq extension if file not found."""
        script = db_dir / "setup.ttq"
        script.write_text('type Foo { x: uint8 }')

        # Import without extension
        result = _run(executor, f'import "{db_dir / "setup"}"')
        assert isinstance(result, ImportResult)
        assert result.skipped is False


# --- System Types ---


class TestSystemTypes:
    def test_create_type_rejects_underscore_prefix(self, executor):
        """type _Foo { x: uint8 } -> error message."""
        result = _run(executor, 'type _Foo { x: uint8 }')
        assert isinstance(result, CreateResult)
        assert "reserved for system use" in result.message

    def test_create_enum_rejects_underscore_prefix(self, executor):
        """enum _Color { ... } -> error."""
        result = _run(executor, 'enum _Color { red, green }')
        assert isinstance(result, CreateResult)
        assert "reserved for system use" in result.message

    def test_create_interface_rejects_underscore_prefix(self, executor):
        """interface _I { ... } -> error."""
        result = _run(executor, 'interface _I { x: uint8 }')
        assert isinstance(result, CreateResult)
        assert "reserved for system use" in result.message

    def test_create_alias_rejects_underscore_prefix(self, executor):
        """alias _x = uint8 -> error."""
        result = _run(executor, 'alias _x = uint8')
        assert isinstance(result, CreateResult)
        assert "reserved for system use" in result.message

    def test_forward_type_rejects_underscore_prefix(self, executor):
        """forward _Foo -> error."""
        result = _run(executor, 'forward _Foo')
        assert isinstance(result, CreateResult)
        assert "reserved for system use" in result.message

    def test_show_types_hides_system_types(self, executor, db_dir):
        """_ImportRecord not in show types results."""
        script = db_dir / "setup.ttq"
        script.write_text('type Visible { x: uint8 }')

        _run(executor, f'import "{script}"')
        _run(executor, 'create Visible(x=1)')

        result = _run(executor, 'show types')
        type_names = [row["type"] for row in result.rows]
        assert "_ImportRecord" not in type_names
        assert "Visible" in type_names

    def test_show_system_types(self, executor, db_dir):
        """show system types returns only _-prefixed types."""
        script = db_dir / "setup.ttq"
        script.write_text('type Visible { x: uint8 }')

        _run(executor, f'import "{script}"')
        _run(executor, 'create Visible(x=1)')

        result = _run(executor, 'show system types')
        type_names = [row["type"] for row in result.rows]
        assert "_ImportRecord" in type_names
        # User types should not appear
        assert "Visible" not in type_names

    def test_dump_excludes_system_types(self, executor, db_dir):
        """dump output does not contain _ImportRecord."""
        script = db_dir / "setup.ttq"
        script.write_text('type Visible { x: uint8 }')

        _run(executor, f'import "{script}"')
        _run(executor, 'create Visible(x=1)')

        result = _run(executor, 'dump')
        assert "_ImportRecord" not in result.script
        assert "Visible" in result.script

    def test_delete_system_type_blocked(self, executor, db_dir):
        """delete _ImportRecord → error (without force)."""
        script = db_dir / "setup.ttq"
        script.write_text('type Foo { x: uint8 }')
        _run(executor, f'import "{script}"')

        from typed_tables.query_executor import DeleteResult
        result = _run(executor, 'delete _ImportRecord')
        assert isinstance(result, DeleteResult)
        assert "system type" in result.message
        assert result.deleted_count == 0

    def test_delete_force_system_type_allowed(self, executor, db_dir):
        """delete! _ImportRecord → allowed (force)."""
        script = db_dir / "setup.ttq"
        script.write_text('type Foo { x: uint8 }')
        _run(executor, f'import "{script}"')

        from typed_tables.query_executor import DeleteResult
        result = _run(executor, 'delete! _ImportRecord')
        assert isinstance(result, DeleteResult)
        assert result.deleted_count > 0

    def test_parse_delete_force(self):
        """delete! Type → DeleteQuery(force=True)."""
        from typed_tables.parsing.query_parser import DeleteQuery
        parser = QueryParser()
        query = parser.parse('delete! Foo')
        assert isinstance(query, DeleteQuery)
        assert query.table == "Foo"
        assert query.force is True
        assert query.where is None

    def test_parse_delete_force_where(self):
        """delete! Type where ... → DeleteQuery(force=True, where=...)."""
        from typed_tables.parsing.query_parser import DeleteQuery
        parser = QueryParser()
        query = parser.parse('delete! Foo where x = 1')
        assert isinstance(query, DeleteQuery)
        assert query.table == "Foo"
        assert query.force is True
        assert query.where is not None

    def test_graph_excludes_unreferenced_path(self, executor):
        """path alias should not appear in graph when unused."""
        result = _run(executor, 'graph')
        sources = {row["source"] for row in result.rows}
        assert "path" not in sources

    def test_graph_includes_path_when_used(self, executor):
        """path alias should appear in graph when a user type uses it."""
        _run(executor, 'type Config { file: path }')
        result = _run(executor, 'graph')
        sources = {row["source"] for row in result.rows}
        assert "path" in sources

    def test_graph_excludes_system_types(self, executor, db_dir):
        """_ImportRecord should not appear in graph."""
        script = db_dir / "setup.ttq"
        script.write_text('type Foo { x: uint8 }')
        _run(executor, f'import "{script}"')

        result = _run(executor, 'graph')
        sources = {row["source"] for row in result.rows}
        targets = {row["target"] for row in result.rows}
        assert "_ImportRecord" not in sources
        assert "_ImportRecord" not in targets


# --- Built-in path alias ---


class TestPathAlias:
    def test_path_alias_exists(self):
        """TypeRegistry().get('path') returns an AliasTypeDefinition."""
        registry = TypeRegistry()
        path_type = registry.get("path")
        assert path_type is not None
        assert isinstance(path_type, AliasTypeDefinition)

    def test_path_alias_resolves_to_string(self):
        """path resolves to StringTypeDefinition."""
        registry = TypeRegistry()
        path_type = registry.get("path")
        base = path_type.resolve_base_type()
        assert isinstance(base, StringTypeDefinition)


# --- Integration ---


class TestImportPathNormalization:
    def test_dotslash_and_bare_are_same(self, executor, db_dir):
        """./setup.ttq and setup.ttq are treated as the same import."""
        script = db_dir / "setup.ttq"
        script.write_text('type Foo { x: uint8 }')

        result1 = _run(executor, f'import "{script}"')
        assert result1.skipped is False

        # Import again with ./ prefix — should be skipped
        dotslash = str(script.parent) + "/./setup.ttq"
        result2 = _run(executor, f'import "{dotslash}"')
        assert result2.skipped is True

    def test_relative_path_stored_as_relative(self, executor, db_dir):
        """Relative import path stays relative in the import key."""
        # Create the target script and a parent that imports it relatively
        script = db_dir / "setup.ttq"
        script.write_text('type Foo { x: uint8 }')

        parent = db_dir / "main.ttq"
        parent.write_text('import "setup.ttq"')

        # Execute the parent script — this causes import with relative path "setup.ttq"
        _run(executor, f'execute "{parent}"')

        # Verify the stored path is relative by checking the _ImportRecord table
        result = _run(executor, 'from _ImportRecord select *')
        scripts = [row["script"] for row in result.rows]
        assert "setup.ttq" in scripts

    def test_absolute_path_stored_as_absolute(self, executor, db_dir):
        """Absolute import path stays absolute in the import key."""
        script = db_dir / "setup.ttq"
        script.write_text('type Foo { x: uint8 }')

        result = _run(executor, f'import "{script}"')
        assert result.skipped is False
        assert result.file_path == str(script)

    def test_auto_extension_included_in_key(self, executor, db_dir):
        """Auto-appended .ttq extension is included in the normalized key."""
        script = db_dir / "setup.ttq"
        script.write_text('type Foo { x: uint8 }')

        result = _run(executor, f'import "{db_dir / "setup"}"')
        assert result.skipped is False
        # The key should include the .ttq extension
        assert result.file_path.endswith("setup.ttq")


# --- Dump Archive ---


class TestDumpArchive:
    def test_parse_dump_archive(self):
        """dump archive -> DumpQuery(include_system=True)."""
        from typed_tables.parsing.query_parser import DumpQuery
        parser = QueryParser()
        query = parser.parse('dump archive')
        assert isinstance(query, DumpQuery)
        assert query.include_system is True

    def test_parse_dump_archive_yaml(self):
        """dump archive yaml -> DumpQuery(include_system=True, format='yaml')."""
        from typed_tables.parsing.query_parser import DumpQuery
        parser = QueryParser()
        query = parser.parse('dump archive yaml')
        assert isinstance(query, DumpQuery)
        assert query.include_system is True
        assert query.format == "yaml"

    def test_parse_dump_archive_pretty(self):
        """dump archive pretty -> DumpQuery(include_system=True, pretty=True)."""
        from typed_tables.parsing.query_parser import DumpQuery
        parser = QueryParser()
        query = parser.parse('dump archive pretty')
        assert isinstance(query, DumpQuery)
        assert query.include_system is True
        assert query.pretty is True

    def test_dump_archive_includes_system_types(self, executor, db_dir):
        """dump archive includes _ImportRecord in output."""
        script = db_dir / "setup.ttq"
        script.write_text('type Visible { x: uint8 }')

        _run(executor, f'import "{script}"')
        _run(executor, 'create Visible(x=1)')

        result = _run(executor, 'dump archive')
        assert "_ImportRecord" in result.script
        assert "Visible" in result.script

    def test_dump_without_archive_excludes_system_types(self, executor, db_dir):
        """Regular dump still excludes _ImportRecord."""
        script = db_dir / "setup.ttq"
        script.write_text('type Visible { x: uint8 }')

        _run(executor, f'import "{script}"')
        _run(executor, 'create Visible(x=1)')

        result = _run(executor, 'dump')
        assert "_ImportRecord" not in result.script
        assert "Visible" in result.script


# --- Integration ---


class TestImportIntegration:
    def test_import_persists_across_sessions(self, db_dir):
        """Create executor, import file, close. New executor on same db -> import skipped."""
        # First session
        registry1 = TypeRegistry()
        storage1 = StorageManager(db_dir, registry1)
        executor1 = QueryExecutor(storage1, registry1)

        script = db_dir / "setup.ttq"
        script.write_text('type Point { x: float32, y: float32 }')

        result1 = _run(executor1, f'import "{script}"')
        assert isinstance(result1, ImportResult)
        assert result1.skipped is False

        # Save metadata and close
        storage1.save_metadata()
        storage1.close()

        # Second session — reload from disk
        from typed_tables.dump import load_registry_from_metadata

        registry2 = load_registry_from_metadata(db_dir)
        storage2 = StorageManager(db_dir, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        result2 = _run(executor2, f'import "{script}"')
        assert isinstance(result2, ImportResult)
        assert result2.skipped is True
        assert "Already imported" in result2.message

        storage2.close()
