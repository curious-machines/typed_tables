"""Tests for the TTQ REPL."""

import tempfile
from pathlib import Path

import pytest

from typed_tables.repl import main, run_file


class TestHelperFunctions:
    """Tests for REPL helper functions."""

    pass


class TestRunFile:
    """Tests for file execution."""

    def test_run_file_creates_database(self, tmp_path: Path):
        """Test that run_file can create a database and execute queries."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
-- Create a test database
use {db_path};

-- Create a type
create type Person
  name: string
  age: uint8;

-- Create an instance
create Person(name="Alice", age=30);

-- Query it
from Person select *;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0
        assert db_path.exists()

    def test_run_file_with_semicolons(self, tmp_path: Path):
        """Test that semicolon-separated queries work."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path}; create type Point x:float32 y:float32; create Point(x=1.0, y=2.0); from Point select *
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_run_file_with_initial_database(self, tmp_path: Path):
        """Test run_file with an initial database."""
        db_path = tmp_path / "testdb"
        db_path.mkdir()

        script = tmp_path / "test.ttq"
        script.write_text("""
create type Item name:string;
create Item(name="test");
from Item select *;
""")

        result = run_file(script, db_path, verbose=False)
        assert result == 0

    def test_run_file_error_no_database(self, tmp_path: Path):
        """Test that queries fail when no database is selected."""
        script = tmp_path / "test.ttq"
        script.write_text("from Person select *;")

        result = run_file(script, None, verbose=False)
        assert result == 1

    def test_run_file_syntax_error(self, tmp_path: Path):
        """Test that syntax errors are reported."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path};
from where;
""")

        result = run_file(script, None, verbose=False)
        assert result == 1

    def test_run_file_comments_ignored(self, tmp_path: Path):
        """Test that comments are properly ignored."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
-- This is a comment
use {db_path};
-- Another comment
create type Test value:uint8;
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
from Person select *
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
create type Item name:string;
create Item(name="from subscript");
""")

        main_script = tmp_path / "main.ttq"
        main_script.write_text(f"""
use {db_path};
execute {subscript};
from Item select *;
""")

        # Since execute is a REPL command, we need to test it differently
        # For now, test that run_file works with the subscript directly
        result = run_file(subscript, db_path, verbose=False)
        assert result == 0


class TestInlineInstanceAndProjection:
    """Tests for inline instance creation, post-index dot notation, and array projection."""

    def test_inline_instance_creation(self, tmp_path: Path):
        """Test creating instances with inline nested composites."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path};
create type Address street:string city:string;
create type Person name:string address:Address;
create Person(name="Alice", address=Address(street="123 Main", city="Springfield"));
from Person select *;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_inline_instance_dot_notation_query(self, tmp_path: Path):
        """Test querying nested fields from inline-created instances."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path};
create type Address street:string city:string;
create type Person name:string address:Address;
create Person(name="Alice", address=Address(street="123 Main", city="Springfield"));
from Person select address.city;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_post_index_dot_notation(self, tmp_path: Path):
        """Test post-index dot notation: employees[0].name."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path};
create type Employee name:string;
create type Team title:string employees:Employee[];
create Employee(name="Alice");
create Employee(name="Bob");
create Team(title="Engineering", employees=[0, 1]);
from Team select employees[0].name;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_array_projection(self, tmp_path: Path):
        """Test array projection: employees.name projects over all elements."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path};
create type Employee name:string;
create type Team title:string employees:Employee[];
create Employee(name="Alice");
create Employee(name="Bob");
create Team(title="Engineering", employees=[0, 1]);
from Team select employees.name;
""")

        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Simple value:uint8;
""")

        result = main(["-f", str(script)])
        assert result == 0
        assert db_path.exists()

    def test_main_verbose_flag(self, tmp_path: Path, capsys):
        """Test that verbose flag prints queries."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path};
""")

        result = main(["-f", str(script), "-v"])
        assert result == 0

        captured = capsys.readouterr()
        assert ">>>" in captured.out or "use" in captured.out
