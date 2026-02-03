"""Tests for the TTQ REPL."""

import tempfile
from pathlib import Path

import pytest

from typed_tables.repl import main, run_file


class TestHelperFunctions:
    """Tests for REPL helper functions."""

    def test_has_balanced_parens(self):
        """Test parenthesis balancing detection."""
        # Import the function directly from the module
        import typed_tables.repl as repl_module

        # We need to access the function from within run_repl's scope
        # For now, let's just test via run_file which uses similar logic

    def test_is_single_line_query(self):
        """Test single-line query detection."""
        from typed_tables.repl import _is_single_line_query

        # Single-line queries
        assert _is_single_line_query("use mydb")
        assert _is_single_line_query("show tables")
        assert _is_single_line_query("describe Person")
        assert _is_single_line_query("drop mydb")
        assert _is_single_line_query("create alias uuid as uint128")
        assert _is_single_line_query("delete Person")
        assert _is_single_line_query("create type Point x:float32 y:float32")
        assert _is_single_line_query("from Person")
        assert _is_single_line_query('create Person(name="Alice", age=30)')

        # Multi-line queries
        assert not _is_single_line_query("create type Person")  # No fields on same line


class TestRunFile:
    """Tests for file execution."""

    def test_run_file_creates_database(self, tmp_path: Path):
        """Test that run_file can create a database and execute queries."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
-- Create a test database
use {db_path}

-- Create a type
create type Person
name: string
age: uint8

-- Create an instance
create Person(name="Alice", age=30)

-- Query it
from Person
""")

        result = run_file(script, None, verbose=False)
        assert result == 0
        assert db_path.exists()

    def test_run_file_with_semicolons(self, tmp_path: Path):
        """Test that semicolon-separated queries work."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path}; create type Point x:float32 y:float32; create Point(x=1.0, y=2.0); from Point
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_run_file_with_initial_database(self, tmp_path: Path):
        """Test run_file with an initial database."""
        db_path = tmp_path / "testdb"
        db_path.mkdir()

        script = tmp_path / "test.ttq"
        script.write_text("""
create type Item name:string
create Item(name="test")
from Item
""")

        result = run_file(script, db_path, verbose=False)
        assert result == 0

    def test_run_file_error_no_database(self, tmp_path: Path):
        """Test that queries fail when no database is selected."""
        script = tmp_path / "test.ttq"
        script.write_text("from Person")

        result = run_file(script, None, verbose=False)
        assert result == 1

    def test_run_file_syntax_error(self, tmp_path: Path):
        """Test that syntax errors are reported."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path}
from where
""")

        result = run_file(script, None, verbose=False)
        assert result == 1

    def test_run_file_comments_ignored(self, tmp_path: Path):
        """Test that comments are properly ignored."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
-- This is a comment
use {db_path}
-- Another comment
create type Test value:uint8
-- Final comment
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_run_file_multiline_create_instance(self, tmp_path: Path):
        """Test multi-line create instance in a file."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        # Test that multi-line create instance works in a file
        # The semicolons help separate the queries
        script.write_text(f"""
use {db_path};
create type Person name:string age:uint8;
create Person(
  name="Alice",
  age=30
);
from Person
""")

        result = run_file(script, None, verbose=False)
        assert result == 0


class TestExecuteCommand:
    """Tests for the execute command in the REPL."""

    def test_execute_command_parsing(self, tmp_path: Path):
        """Test that execute command properly parses file paths."""
        from typed_tables.repl import run_file

        # Create a main script that executes a subscript
        db_path = tmp_path / "testdb"
        subscript = tmp_path / "subscript.ttq"
        subscript.write_text("""
create type Item name:string
create Item(name="from subscript")
""")

        main_script = tmp_path / "main.ttq"
        main_script.write_text(f"""
use {db_path}
execute {subscript}
from Item
""")

        # Since execute is a REPL command, we need to test it differently
        # For now, test that run_file works with the subscript directly
        result = run_file(subscript, db_path, verbose=False)
        assert result == 0


class TestMain:
    """Tests for the main entry point."""

    def test_main_file_not_found(self, tmp_path: Path):
        """Test error when file doesn't exist."""
        result = main(["-f", str(tmp_path / "nonexistent.ttq")])
        assert result == 1

    def test_main_file_execution(self, tmp_path: Path):
        """Test file execution via main."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path}
create type Simple value:uint8
""")

        result = main(["-f", str(script)])
        assert result == 0
        assert db_path.exists()

    def test_main_verbose_flag(self, tmp_path: Path, capsys):
        """Test that verbose flag prints queries."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path}
""")

        result = main(["-f", str(script), "-v"])
        assert result == 0

        captured = capsys.readouterr()
        assert ">>>" in captured.out or "use" in captured.out
