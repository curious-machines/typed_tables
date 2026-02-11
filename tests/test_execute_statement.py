"""Tests for the execute statement as a TTQ query."""

from __future__ import annotations

import gzip
import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.parsing.query_parser import ExecuteQuery, QueryParser
from typed_tables.query_executor import ExecuteResult, QueryExecutor
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


def _run(executor, text):
    """Parse and execute a TTQ program, returning the last result."""
    parser = QueryParser()
    stmts = parser.parse_program(text)
    result = None
    for stmt in stmts:
        result = executor.execute(stmt)
    return result


class TestExecuteParsing:
    def test_parse_execute(self):
        """Parser produces ExecuteQuery with file_path."""
        parser = QueryParser()
        query = parser.parse('execute "script.ttq"')
        assert isinstance(query, ExecuteQuery)
        assert query.file_path == "script.ttq"

    def test_parse_execute_gz(self):
        """Parser produces ExecuteQuery for .gz files."""
        parser = QueryParser()
        query = parser.parse('execute "script.ttq.gz"')
        assert isinstance(query, ExecuteQuery)
        assert query.file_path == "script.ttq.gz"

    def test_parse_execute_in_program(self):
        """Execute can appear in a multi-statement program."""
        parser = QueryParser()
        stmts = parser.parse_program(
            'type Foo { x: uint8 }\n'
            'execute "other.ttq"\n'
        )
        assert len(stmts) == 2
        assert isinstance(stmts[1], ExecuteQuery)
        assert stmts[1].file_path == "other.ttq"


class TestExecuteExecution:
    def test_basic_execute(self, executor, db_dir):
        """Execute a simple script that creates a type."""
        script = db_dir / "types.ttq"
        script.write_text('type Point { x: float32, y: float32 }')

        result = _run(executor, f'execute "{script}"')
        assert isinstance(result, ExecuteResult)
        assert result.statements_executed == 1

        # Verify the type was created
        result2 = _run(executor, 'from Point select *')
        assert result2.columns == ["_index", "x", "y"]

    def test_execute_multiple_statements(self, executor, db_dir):
        """Execute a script with multiple statements."""
        script = db_dir / "setup.ttq"
        script.write_text(
            'type Person { name: string, age: uint8 }\n'
            'create Person(name="Alice", age=30)\n'
            'create Person(name="Bob", age=25)\n'
        )

        result = _run(executor, f'execute "{script}"')
        assert isinstance(result, ExecuteResult)
        assert result.statements_executed == 3

        result2 = _run(executor, 'from Person select *')
        assert len(result2.rows) == 2

    def test_execute_auto_extension(self, executor, db_dir):
        """Execute auto-appends .ttq extension when file not found."""
        script = db_dir / "defs.ttq"
        script.write_text('type Color { r: uint8, g: uint8, b: uint8 }')

        # Reference without extension
        result = _run(executor, f'execute "{db_dir / "defs"}"')
        assert isinstance(result, ExecuteResult)
        assert result.statements_executed == 1

    def test_execute_auto_extension_gz(self, executor, db_dir):
        """Execute auto-appends .ttq.gz extension when file not found."""
        script = db_dir / "defs.ttq.gz"
        with gzip.open(script, "wt", encoding="utf-8") as f:
            f.write('type Pixel { x: uint16, y: uint16 }')

        result = _run(executor, f'execute "{db_dir / "defs"}"')
        assert isinstance(result, ExecuteResult)
        assert result.statements_executed == 1

    def test_execute_gzip_file(self, executor, db_dir):
        """Execute a gzip-compressed script."""
        script = db_dir / "types.ttq.gz"
        with gzip.open(script, "wt", encoding="utf-8") as f:
            f.write('type Vec2 { x: float64, y: float64 }')

        result = _run(executor, f'execute "{script}"')
        assert isinstance(result, ExecuteResult)
        assert result.statements_executed == 1

        result2 = _run(executor, 'from Vec2 select *')
        assert "x" in result2.columns

    def test_execute_relative_path(self, executor, db_dir):
        """Execute resolves relative paths from the calling script."""
        subdir = db_dir / "lib"
        subdir.mkdir()

        # Inner script in subdir
        inner = subdir / "types.ttq"
        inner.write_text('type Widget { name: string }')

        # Outer script references inner with relative path
        outer = db_dir / "main.ttq"
        outer.write_text('execute "lib/types.ttq"')

        # Set script stack to db_dir so relative paths work
        executor._script_stack.append(db_dir)
        result = _run(executor, f'execute "{outer}"')
        assert isinstance(result, ExecuteResult)

        # Verify the type from the inner script was created
        result2 = _run(executor, 'from Widget select *')
        assert "name" in result2.columns

    def test_execute_nested_relative_path(self, executor, db_dir):
        """Nested execute resolves relative to the calling script's directory."""
        lib_dir = db_dir / "lib"
        lib_dir.mkdir()
        sub_dir = lib_dir / "sub"
        sub_dir.mkdir()

        # Leaf script in sub/
        leaf = sub_dir / "leaf.ttq"
        leaf.write_text('type Leaf { value: uint8 }')

        # Mid script in lib/ references sub/leaf.ttq
        mid = lib_dir / "mid.ttq"
        mid.write_text('execute "sub/leaf.ttq"')

        # Top script references lib/mid.ttq
        top = db_dir / "top.ttq"
        top.write_text('execute "lib/mid.ttq"')

        executor._script_stack.append(db_dir)
        result = _run(executor, f'execute "{top}"')
        assert isinstance(result, ExecuteResult)

        # Verify the type from the leaf script was created
        result2 = _run(executor, 'from Leaf select *')
        assert "value" in result2.columns

    def test_execute_cycle_detection(self, executor, db_dir):
        """Re-executing an already-loaded script raises an error."""
        script = db_dir / "self.ttq"
        script.write_text(f'execute "{script}"')

        with pytest.raises(RuntimeError, match="already loaded.*circular"):
            _run(executor, f'execute "{script}"')

    def test_execute_indirect_cycle_detection(self, executor, db_dir):
        """Indirect circular execute (A -> B -> A) raises an error."""
        script_a = db_dir / "a.ttq"
        script_b = db_dir / "b.ttq"

        script_a.write_text(f'execute "{script_b}"')
        script_b.write_text(f'execute "{script_a}"')

        with pytest.raises(RuntimeError, match="already loaded.*circular"):
            _run(executor, f'execute "{script_a}"')

    def test_execute_same_script_twice_is_error(self, executor, db_dir):
        """Executing the same script twice in sequence is an error."""
        script = db_dir / "types.ttq"
        script.write_text('type Foo { x: uint8 }')

        _run(executor, f'execute "{script}"')

        with pytest.raises(RuntimeError, match="already loaded"):
            _run(executor, f'execute "{script}"')

    def test_execute_rejects_use(self, executor, db_dir):
        """UseQuery is not allowed inside executed scripts."""
        script = db_dir / "bad.ttq"
        script.write_text('use other_db')

        with pytest.raises(RuntimeError, match="UseQuery.*not allowed"):
            _run(executor, f'execute "{script}"')

    def test_execute_rejects_drop(self, executor, db_dir):
        """DropDatabaseQuery is not allowed inside executed scripts."""
        script = db_dir / "bad.ttq"
        script.write_text('drop! some_db')

        with pytest.raises(RuntimeError, match="DropDatabaseQuery.*not allowed"):
            _run(executor, f'execute "{script}"')

    def test_execute_rejects_restore(self, executor, db_dir):
        """RestoreQuery is not allowed inside executed scripts."""
        script = db_dir / "bad.ttq"
        script.write_text('restore "backup.ttar" to "out"')

        with pytest.raises(RuntimeError, match="RestoreQuery.*not allowed"):
            _run(executor, f'execute "{script}"')

    def test_execute_file_not_found(self, executor, db_dir):
        """Execute raises error when file not found."""
        with pytest.raises(FileNotFoundError, match="Script file not found"):
            _run(executor, f'execute "{db_dir / "nonexistent.ttq"}"')

    def test_execute_result_message(self, executor, db_dir):
        """Execute result has a descriptive message."""
        script = db_dir / "hello.ttq"
        script.write_text(
            'type Greeting { msg: string }\n'
            'create Greeting(msg="hello")\n'
        )

        result = _run(executor, f'execute "{script}"')
        assert "2 statements" in result.message
        assert str(script) in result.file_path

    def test_execute_empty_script(self, executor, db_dir):
        """Execute an empty script succeeds with 0 statements."""
        script = db_dir / "empty.ttq"
        script.write_text("")

        result = _run(executor, f'execute "{script}"')
        assert isinstance(result, ExecuteResult)
        assert result.statements_executed == 0

    def test_execute_with_existing_types(self, executor, db_dir):
        """Execute a script that uses types already defined."""
        # Create a type first
        _run(executor, 'type Address { city: string }')

        # Script uses the existing type
        script = db_dir / "person.ttq"
        script.write_text(
            'type Person { name: string, address: Address }\n'
        )

        result = _run(executor, f'execute "{script}"')
        assert result.statements_executed == 1

        # Verify Person type references Address
        result2 = _run(executor, 'describe Person')
        fields = {r["property"]: r["type"] for r in result2.rows}
        assert fields["address"] == "Address"


class TestExecuteInRunFile:
    """Test that execute works when invoked through run_file."""

    def test_run_file_with_execute(self, db_dir):
        """run_file executes scripts containing execute statements."""
        from typed_tables.repl import run_file

        # Create inner script
        inner = db_dir / "inner.ttq"
        inner.write_text('type Inner { val: uint8 }')

        # Create outer script
        outer = db_dir / "outer.ttq"
        outer.write_text(
            f'use "{db_dir / "test_db"}"\n'
            f'execute "{inner}"\n'
            f'create Inner(val=42)\n'
        )

        exit_code, final_dir = run_file(outer, None)
        assert exit_code == 0
        assert final_dir is not None

    def test_run_file_relative_execute(self, db_dir):
        """run_file resolves execute paths relative to the script."""
        from typed_tables.repl import run_file

        lib = db_dir / "lib"
        lib.mkdir()

        # Inner script
        inner = lib / "types.ttq"
        inner.write_text('type LibType { x: uint8 }')

        # Main script uses relative path
        main = db_dir / "main.ttq"
        main.write_text(
            f'use "{db_dir / "test_db"}"\n'
            'execute "lib/types.ttq"\n'
        )

        exit_code, _ = run_file(main, None)
        assert exit_code == 0
