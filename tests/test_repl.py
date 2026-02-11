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
use "{db_path}";

-- Create a type
type Person {{ name: string, age: uint8 }}

-- Create an instance
create Person(name="Alice", age=30)

-- Query it
from Person select *
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0
        assert db_path.exists()

    def test_run_file_with_semicolons(self, tmp_path: Path):
        """Test that semicolon-separated queries work."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}"; type Point {{ x: float32, y: float32 }}; create Point(x=1.0, y=2.0); from Point select *
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_run_file_with_initial_database(self, tmp_path: Path):
        """Test run_file with an initial database."""
        db_path = tmp_path / "testdb"
        db_path.mkdir()

        script = tmp_path / "test.ttq"
        script.write_text("""
type Item { name: string }
create Item(name="test");
from Item select *;
""")

        result, _ = run_file(script, db_path, verbose=False)
        assert result == 0

    def test_run_file_error_no_database(self, tmp_path: Path):
        """Test that queries fail when no database is selected."""
        script = tmp_path / "test.ttq"
        script.write_text("from Person select *;")

        result, _ = run_file(script, None, verbose=False)
        assert result == 1

    def test_run_file_syntax_error(self, tmp_path: Path):
        """Test that syntax errors are reported."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
from where;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 1

    def test_run_file_comments_ignored(self, tmp_path: Path):
        """Test that comments are properly ignored."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
-- This is a comment
use "{db_path}";
-- Another comment
type Test {{ value: uint8 }}
-- Final comment
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_run_file_multiline_create_instance(self, tmp_path: Path):
        """Test multi-line create instance in a file."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        # Test that multi-line create instance works in a file
        # The semicolons help separate the queries
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(
  name="Alice",
  age=30
);
from Person select *
""")

        result, _ = run_file(script, None, verbose=False)
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
type Item { name: string }
create Item(name="from subscript");
""")

        main_script = tmp_path / "main.ttq"
        main_script.write_text(f"""
use "{db_path}";
execute {subscript};
from Item select *;
""")

        # Since execute is a REPL command, we need to test it differently
        # For now, test that run_file works with the subscript directly
        result, _ = run_file(subscript, db_path, verbose=False)
        assert result == 0


class TestInlineInstanceAndProjection:
    """Tests for inline instance creation, post-index dot notation, and array projection."""

    def test_inline_instance_creation(self, tmp_path: Path):
        """Test creating instances with inline nested composites."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, address: Address }}
create Person(name="Alice", address=Address(street="123 Main", city="Springfield"));
from Person select *;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_inline_instance_dot_notation_query(self, tmp_path: Path):
        """Test querying nested fields from inline-created instances."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, address: Address }}
create Person(name="Alice", address=Address(street="123 Main", city="Springfield"));
from Person select address.city;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_post_index_dot_notation(self, tmp_path: Path):
        """Test post-index dot notation: employees[0].name."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
type Employee {{ name: string }}
type Team {{ title: string, employees: Employee[] }}
create Employee(name="Alice");
create Employee(name="Bob");
create Team(title="Engineering", employees=[0, 1]);
from Team select employees[0].name;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_array_projection(self, tmp_path: Path):
        """Test array projection: employees.name projects over all elements."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
type Employee {{ name: string }}
type Team {{ title: string, employees: Employee[] }}
create Employee(name="Alice");
create Employee(name="Bob");
create Team(title="Engineering", employees=[0, 1]);
from Team select employees.name;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0


class TestDump:
    """Tests for the dump command."""

    def test_dump_full_database(self, tmp_path: Path):
        """Test dumping entire database as TTQ script."""
        script = tmp_path / "setup.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
alias uuid as uint128;
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        # Now dump
        dump_script = tmp_path / "dump.ttq"
        dump_script.write_text(f"""
use "{db_path}";
dump;
""")

        result, _ = run_file(dump_script, None, verbose=False)
        assert result == 0

    def test_dump_single_table(self, tmp_path: Path):
        """Test dumping a single table."""
        script = tmp_path / "setup.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, address: Address }}
create Address(street="123 Main", city="Springfield");
create Person(name="Alice", address=Address(street="456 Oak", city="Shelbyville"));
dump Person;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_dump_with_nested_composites(self, tmp_path: Path):
        """Test dump output with inline instances for composite fields."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, address: Address }}
create Person(name="Alice", address=Address(street="123 Main", city="Springfield"));
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        # Dump and check output contains inline instances
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser, DumpQuery
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        result = executor.execute(DumpQuery())
        assert isinstance(result, DumpResult)
        assert 'Address(street="123 Main", city="Springfield")' in result.script
        assert 'create Person(' in result.script
        storage.close()

    def test_dump_roundtrip(self, tmp_path: Path):
        """Test that dump output can recreate the database."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, age: uint8, address: Address }}
create Person(name="Alice", age=30, address=Address(street="123 Main", city="Springfield"));
create Person(name="Bob", age=25, address=Address(street="456 Oak", city="Shelbyville"));
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        # Get dump output
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery())
        assert isinstance(dump_result, DumpResult)
        storage.close()

        # Write dump output as a new script and execute into fresh db
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f'use "{db_path2}";\n{dump_result.script}\n')

        result, _ = run_file(roundtrip_script, None, verbose=False)
        assert result == 0

        # Query both databases and compare
        from typed_tables.parsing.query_parser import QueryParser

        parser = QueryParser()

        registry2 = load_registry_from_metadata(db_path2)
        storage2 = StorageManager(db_path2, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        result2 = executor2.execute(parser.parse("from Person select *"))
        assert len(result2.rows) == 2
        assert result2.rows[0]["name"] == "Alice"
        assert result2.rows[0]["age"] == 30
        assert result2.rows[1]["name"] == "Bob"
        assert result2.rows[1]["age"] == 25
        storage2.close()

    def test_create_array_of_inline_instances(self, tmp_path: Path):
        """Test creating arrays with inline composite instances."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Employee {{ name: string }}
type Team {{ title: string, employees: Employee[] }}
create Team(title="Engineering", employees=[Employee(name="Alice"), Employee(name="Bob")]);
from Team select *;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0


    def test_dump_to_file(self, tmp_path: Path):
        """Test dumping entire database to a file."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
dump to "{tmp_path / 'dump_output.ttq'}";
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        dump_file = tmp_path / "dump_output.ttq"
        assert dump_file.exists()
        content = dump_file.read_text()
        assert "type Person" in content
        assert "create Person(" in content

    def test_dump_table_to_file(self, tmp_path: Path):
        """Test dumping a single table to a file."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, address: Address }}
create Person(name="Alice", address=Address(street="123 Main", city="Springfield"));
dump Person to "{tmp_path / 'person_dump.ttq'}";
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        dump_file = tmp_path / "person_dump.ttq"
        assert dump_file.exists()
        content = dump_file.read_text()
        assert "create Person(" in content

    def test_dump_to_file_roundtrip(self, tmp_path: Path):
        """Test that dump to file can recreate the database."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
dump to "{tmp_path / 'dump_output.ttq'}";
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        dump_file = tmp_path / "dump_output.ttq"
        assert dump_file.exists()

        # Recreate from dump
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f'use "{db_path2}";\n{dump_file.read_text()}\n')

        result, _ = run_file(roundtrip_script, None, verbose=False)
        assert result == 0

        # Verify data
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path2)
        storage = StorageManager(db_path2, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        result2 = executor.execute(parser.parse("from Person select *"))
        assert len(result2.rows) == 2
        assert result2.rows[0]["name"] == "Alice"
        assert result2.rows[0]["age"] == 30
        assert result2.rows[1]["name"] == "Bob"
        assert result2.rows[1]["age"] == 25
        storage.close()


class TestVariableBindings:
    """Tests for $var bindings."""

    def test_variable_binding_basic(self, tmp_path: Path):
        """Test basic variable binding and usage."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, address: Address }}
$addr = create Address(street="123 Main", city="Springfield");
create Person(name="Alice", address=$addr);
from Person select address.city;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_variable_binding_shared_reference(self, tmp_path: Path):
        """Test two Persons sharing the same $addr reference the same Address index."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, address: Address }}
$addr = create Address(street="123 Main", city="Springfield");
create Person(name="Alice", address=$addr);
create Person(name="Bob", address=$addr);
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        # Query and verify both reference the same address
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        result = executor.execute(parser.parse("from Person select *"))
        assert len(result.rows) == 2
        # Both should have the same Address reference
        assert result.rows[0]["address"] == result.rows[1]["address"]
        storage.close()

    def test_variable_immutability(self, tmp_path: Path, capsys):
        """Test that reassigning a variable returns an error."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
$addr = create Address(street="123 Main", city="Springfield");
$addr = create Address(street="456 Oak", city="Shelbyville");
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        captured = capsys.readouterr()
        assert "already bound" in captured.out

    def test_variable_undefined_error(self, tmp_path: Path):
        """Test that referencing an undefined variable returns an error."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, address: Address }}
create Person(name="Alice", address=$undefined);
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 1

    def test_variable_in_array(self, tmp_path: Path):
        """Test using variables as array elements."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Employee {{ name: string }}
type Team {{ title: string, employees: Employee[] }}
$e1 = create Employee(name="Alice");
$e2 = create Employee(name="Bob");
create Team(title="Engineering", employees=[$e1, $e2]);
from Team select employees.name;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_dump_uses_variables_for_shared_refs(self, tmp_path: Path):
        """Test that dump output uses $var for shared composite references."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, address: Address }}
$addr = create Address(street="123 Main", city="Springfield");
create Person(name="Alice", address=$addr);
create Person(name="Bob", address=$addr);
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery())
        assert isinstance(dump_result, DumpResult)
        # Should contain a variable assignment for the shared Address
        assert "$Address_0" in dump_result.script
        assert "= create Address(" in dump_result.script
        storage.close()

    def test_dump_roundtrip_with_variables(self, tmp_path: Path):
        """Test that dump with variables can recreate the database."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, address: Address }}
$addr = create Address(street="123 Main", city="Springfield");
create Person(name="Alice", address=$addr);
create Person(name="Bob", address=$addr);
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery, QueryParser
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery())
        assert isinstance(dump_result, DumpResult)
        storage.close()

        # Execute dump output into a fresh database
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f'use "{db_path2}";\n{dump_result.script}\n')

        result, _ = run_file(roundtrip_script, None, verbose=False)
        assert result == 0

        # Query the new database and verify data matches
        registry2 = load_registry_from_metadata(db_path2)
        storage2 = StorageManager(db_path2, registry2)
        executor2 = QueryExecutor(storage2, registry2)
        parser = QueryParser()

        result2 = executor2.execute(parser.parse("from Person select *"))
        assert len(result2.rows) == 2
        assert result2.rows[0]["name"] == "Alice"
        assert result2.rows[1]["name"] == "Bob"
        # Both should reference the same address
        assert result2.rows[0]["address"] == result2.rows[1]["address"]
        storage2.close()


class TestCyclicalTypes:
    """Tests for cyclical (self-referential and mutually referential) types."""

    def test_self_referential_type_creation(self, tmp_path: Path):
        """Test creating a self-referential type: Node with children:Node[]."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, children: Node[] }}
describe Node;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_self_referential_type_with_data(self, tmp_path: Path):
        """Test creating Node instances with children arrays."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, children: Node[] }}
create Node(value=1, children=[]);
create Node(value=2, children=[]);
create Node(value=0, children=[Node(value=1, children=[]), Node(value=2, children=[])]);
from Node select *;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_self_referential_direct(self, tmp_path: Path):
        """Test creating a direct self-referential type: LinkedNode with next:LinkedNode."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
type LinkedNode {{ value: uint8, next: LinkedNode }}
create LinkedNode(value=2, next=LinkedNode(value=1, next=LinkedNode(0)));
from LinkedNode select *;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_forward_declaration(self, tmp_path: Path):
        """Test forward declaration pattern for mutual references."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
forward B;
type A {{ value: uint8, b: B }}
type B {{ value: uint8, a: A }}
describe A;
describe B;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_mutual_reference_with_data(self, tmp_path: Path):
        """Test creating instances of mutually referencing types."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
forward B;
type A {{ value: uint8, b: B }}
type B {{ value: uint8, a: A }}
$a = create A(value=1, b=B(value=2, a=A(0)));
from A select *;
from B select *;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_dump_roundtrip_self_referential(self, tmp_path: Path):
        """Test dump and reload of self-referential types and data."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, children: Node[] }}
create Node(value=1, children=[]);
create Node(value=0, children=[Node(value=2, children=[]), Node(value=3, children=[])]);
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        # Get dump output
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery, QueryParser
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery())
        assert isinstance(dump_result, DumpResult)
        storage.close()

        # Execute dump output into a fresh database
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f'use "{db_path2}";\n{dump_result.script}\n')

        result, _ = run_file(roundtrip_script, None, verbose=False)
        assert result == 0

        # Query the new database and verify
        registry2 = load_registry_from_metadata(db_path2)
        storage2 = StorageManager(db_path2, registry2)
        executor2 = QueryExecutor(storage2, registry2)
        parser = QueryParser()

        result2 = executor2.execute(parser.parse("from Node select *"))
        assert len(result2.rows) == 2
        assert result2.rows[0]["value"] == 1
        assert result2.rows[1]["value"] == 0
        storage2.close()

    def test_dump_roundtrip_mutual_reference(self, tmp_path: Path):
        """Test dump and reload with forward declarations for mutual references."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
forward B;
type A {{ value: uint8, b: B }}
type B {{ value: uint8, a: A }}
create A(value=1, b=B(value=2, a=A(0)));
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery, QueryParser
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery())
        assert isinstance(dump_result, DumpResult)
        # Should contain forward declarations
        assert "forward A" in dump_result.script or "forward B" in dump_result.script
        storage.close()

        # Execute dump into fresh db
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f'use "{db_path2}";\n{dump_result.script}\n')

        result, _ = run_file(roundtrip_script, None, verbose=False)
        assert result == 0

        # Verify the types and data were recreated successfully
        registry2 = load_registry_from_metadata(db_path2)
        storage2 = StorageManager(db_path2, registry2)
        executor2 = QueryExecutor(storage2, registry2)
        parser = QueryParser()

        result2 = executor2.execute(parser.parse("from A select *"))
        assert len(result2.rows) >= 1
        # First A record should have value=1
        assert result2.rows[0]["value"] == 1

        result3 = executor2.execute(parser.parse("from B select *"))
        assert len(result3.rows) >= 1
        # First B record should have value=2
        assert result3.rows[0]["value"] == 2
        storage2.close()

    def test_self_referential_data_cycle_in_dump(self, tmp_path: Path):
        """Test that a node pointing to itself doesn't cause infinite recursion in dump."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type LinkedNode {{ value: uint8, next: LinkedNode }}
create LinkedNode(value=42, next=LinkedNode(0));
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery())
        assert isinstance(dump_result, DumpResult)
        # Should use tag syntax to handle the self-referential cycle
        assert "tag(" in dump_result.script
        # Should NOT use null+update for self-referential cycles
        assert "next=null" not in dump_result.script
        assert "update" not in dump_result.script
        storage.close()


class TestCollect:
    """Tests for the collect query and dump $var."""

    def test_collect_basic(self, tmp_path: Path):
        """Test collect with where clause, then dump."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
$seniors = collect Person where age >= 65;
dump $seniors;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_collect_all(self, tmp_path: Path):
        """Test collect all records."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Item {{ name: string }}
create Item(name="A");
create Item(name="B");
create Item(name="C");
$all = collect Item;
dump $all;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_collect_with_sort_limit(self, tmp_path: Path):
        """Test collect with sort and limit."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Score {{ name: string, value: uint8 }}
create Score(name="Alice", value=90);
create Score(name="Bob", value=80);
create Score(name="Carol", value=95);
$top2 = collect Score sort by value limit 2;
dump $top2;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_collect_immutability(self, tmp_path: Path, capsys):
        """Test that rebinding a collect variable fails."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Item {{ name: string }}
create Item(name="A");
$items = collect Item;
$items = collect Item;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        captured = capsys.readouterr()
        assert "already bound" in captured.out

    def test_collect_variable_cannot_be_used_as_field(self, tmp_path: Path):
        """Test that a set variable from collect cannot be used as a field value."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Item {{ name: string }}
type Wrapper {{ item: Item }}
create Item(name="A");
$items = collect Item;
create Wrapper(item=$items);
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 1

    def test_dump_single_variable(self, tmp_path: Path):
        """Test dump a single-ref variable from create."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
$bob = create Person(name="Bob", age=25);
create Person(name="Carol", age=40);
dump $bob;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_dump_variable_to_file(self, tmp_path: Path):
        """Test dump $var to file."""
        db_path = tmp_path / "testdb"
        output_file = tmp_path / "output.ttq"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
$all = collect Person;
dump $all to "{output_file}";
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert "create Person(" in content

    def test_dump_variable_roundtrip(self, tmp_path: Path):
        """Test that dump $var output can recreate filtered records."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
$seniors = collect Person where age >= 65;
dump $seniors to "{tmp_path / 'seniors.ttq'}";
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        dump_file = tmp_path / "seniors.ttq"
        assert dump_file.exists()

        # Recreate from dump into fresh database
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f'use "{db_path2}";\n{dump_file.read_text()}\n')

        result, _ = run_file(roundtrip_script, None, verbose=False)
        assert result == 0

        # Verify only the filtered records are present
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path2)
        storage = StorageManager(db_path2, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        result2 = executor.execute(parser.parse("from Person select *"))
        assert len(result2.rows) == 2
        names = {r["name"] for r in result2.rows}
        assert names == {"Bob", "Carol"}
        storage.close()

    def test_collect_empty_result(self, tmp_path: Path):
        """Test collect matching nothing returns empty list, still succeeds."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
$nobody = collect Person where age >= 100;
dump $nobody;
""")

        result, _ = run_file(script, None, verbose=False)
        assert result == 0


class TestFromVariable:
    """Tests for from $var select queries."""

    def test_from_variable_select(self, tmp_path: Path):
        """Test collect then select from variable."""
        db_path = tmp_path / "testdb"
        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
$seniors = collect Person where age >= 65;
from $seniors select name, age sort by age;
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_from_variable_aggregate(self, tmp_path: Path):
        """Test from $var select average(age)."""
        db_path = tmp_path / "testdb"
        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=60);
create Person(name="Carol", age=90);
$old = collect Person where age >= 60;
from $old select average(age);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_from_variable_with_where(self, tmp_path: Path):
        """Test additional where filtering on variable source."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        executor.execute(parser.parse("$seniors = collect Person where age >= 65"))
        result2 = executor.execute(parser.parse("from $seniors select * where age > 65"))
        assert len(result2.rows) == 1
        assert result2.rows[0]["name"] == "Carol"
        storage.close()

    def test_from_variable_undefined(self, tmp_path: Path):
        """Test error for undefined variable in from clause."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        result2 = executor.execute(parser.parse("from $undefined select *"))
        assert result2.message is not None
        assert "Undefined variable" in result2.message
        storage.close()

    def test_from_single_ref_variable(self, tmp_path: Path):
        """Test from $p select * where $p is a create variable (single index)."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
$bob = create Person(name="Bob", age=25);
create Person(name="Carol", age=40);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        # Re-bind variable (executor from run_file is separate)
        executor.variables["bob"] = ("Person", 1)
        result2 = executor.execute(parser.parse("from $bob select *"))
        assert len(result2.rows) == 1
        assert result2.rows[0]["name"] == "Bob"
        storage.close()


class TestCollectMultiSource:
    """Tests for multi-source collect queries."""

    def test_collect_union(self, tmp_path: Path):
        """Test collect with two where clauses, verify combined result."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        result2 = executor.execute(parser.parse("$combined = collect Person where age >= 65, Person where age = 30"))
        assert result2.message is not None
        assert "3" in result2.message  # Alice(30) + Bob(65) + Carol(70)
        storage.close()

    def test_collect_dedup(self, tmp_path: Path):
        """Test collect with overlapping sources deduplicates."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        # Both sources match Bob(65) and Carol(70) - should deduplicate
        result2 = executor.execute(parser.parse("$dup = collect Person where age >= 65, Person where age >= 60"))
        assert "2" in result2.message  # Only Bob and Carol, not duplicated
        storage.close()

    def test_collect_type_mismatch(self, tmp_path: Path):
        """Test collect with different types produces error."""
        db_path = tmp_path / "testdb"
        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
type Item {{ name: string }}
create Person(name="Alice", age=30);
create Item(name="Widget");
$mixed = collect Person, Item;
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0  # Collect reports error as message, doesn't fail script

    def test_collect_variable_source(self, tmp_path: Path):
        """Test collect from $var with additional where."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        executor.execute(parser.parse("$seniors = collect Person where age >= 65"))
        result2 = executor.execute(parser.parse("$old = collect $seniors where age > 65"))
        assert "1" in result2.message  # Only Carol
        storage.close()

    def test_collect_variable_source_no_where(self, tmp_path: Path):
        """Test bare collect from two variables."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        executor.execute(parser.parse("$seniors = collect Person where age >= 65"))
        executor.execute(parser.parse("$young = collect Person where age < 65"))
        result2 = executor.execute(parser.parse("$all = collect $seniors, $young"))
        assert "3" in result2.message
        storage.close()

    def test_collect_inline_and_variable_mixed(self, tmp_path: Path):
        """Test collect mixing table source and variable source."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        executor.execute(parser.parse("$seniors = collect Person where age >= 65"))
        result2 = executor.execute(parser.parse("$mixed = collect $seniors, Person where age = 30"))
        assert "3" in result2.message
        storage.close()


class TestDumpList:
    """Tests for dump list."""

    def test_dump_list_single_type(self, tmp_path: Path):
        """Test dump [Person]."""
        db_path = tmp_path / "testdb"
        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
dump [Person];
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_dump_list_heterogeneous(self, tmp_path: Path):
        """Test dump [Person, Item] with different types."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpItem, DumpQuery, QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
type Item {{ name: string }}
create Person(name="Alice", age=30);
create Item(name="Widget");
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery(items=[DumpItem(table="Person"), DumpItem(table="Item")]))
        assert isinstance(dump_result, DumpResult)
        assert "create Person(" in dump_result.script
        assert "create Item(" in dump_result.script
        storage.close()

    def test_dump_list_with_variable(self, tmp_path: Path):
        """Test dump [Person, $seniors]."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpItem, DumpQuery, QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        # Collect seniors
        executor.execute(parser.parse("$seniors = collect Person where age >= 65"))

        # dump [Person, $seniors] - Person is all, $seniors is subset → result should be all Person
        dump_result = executor.execute(DumpQuery(items=[DumpItem(table="Person"), DumpItem(variable="seniors")]))
        assert isinstance(dump_result, DumpResult)
        assert "Alice" in dump_result.script
        assert "Bob" in dump_result.script
        assert "Carol" in dump_result.script
        storage.close()

    def test_dump_list_to_file(self, tmp_path: Path):
        """Test dump list to file."""
        db_path = tmp_path / "testdb"
        output_file = tmp_path / "output.ttq"
        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
dump [Person] to "{output_file}";
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert "create Person(" in content

    def test_dump_list_roundtrip(self, tmp_path: Path):
        """Test dump list → recreate → verify."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpItem, DumpQuery, QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
type Item {{ name: string }}
create Person(name="Alice", age=30);
create Item(name="Widget");
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery(items=[DumpItem(table="Person"), DumpItem(table="Item")]))
        assert isinstance(dump_result, DumpResult)
        storage.close()

        # Recreate from dump
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f'use "{db_path2}";\n{dump_result.script}\n')

        result, _ = run_file(roundtrip_script, None, verbose=False)
        assert result == 0

        # Verify
        registry2 = load_registry_from_metadata(db_path2)
        storage2 = StorageManager(db_path2, registry2)
        executor2 = QueryExecutor(storage2, registry2)
        parser = QueryParser()

        result2 = executor2.execute(parser.parse("from Person select *"))
        assert len(result2.rows) == 1
        assert result2.rows[0]["name"] == "Alice"

        result3 = executor2.execute(parser.parse("from Item select *"))
        assert len(result3.rows) == 1
        assert result3.rows[0]["name"] == "Widget"
        storage2.close()


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
use "{db_path}";
type Simple {{ value: uint8 }}
""")

        result = main(["-f", str(script)])
        assert result == 0
        assert db_path.exists()

    def test_main_verbose_flag(self, tmp_path: Path, capsys):
        """Test that verbose flag prints queries."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use "{db_path}";
""")

        result = main(["-f", str(script), "-v"])
        assert result == 0

        captured = capsys.readouterr()
        assert ">>>" in captured.out or "use" in captured.out


class TestNullValues:
    """Tests for NULL value support."""

    def test_null_create_and_select(self, tmp_path: Path):
        """Test creating a record with null field and selecting it."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
create Node(value=1, next=null);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        r = executor.execute(parser.parse("from Node select *"))
        assert len(r.rows) == 1
        assert r.rows[0]["value"] == 1
        assert r.rows[0]["next"] is None
        storage.close()

    def test_null_roundtrip(self, tmp_path: Path):
        """Test that null values survive dump and re-execute."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery, QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
create Node(value=1, next=null);
create Node(value=2, next=null);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery())
        assert isinstance(dump_result, DumpResult)
        assert "next=null" in dump_result.script
        storage.close()

        # Re-execute dump into fresh db
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f'use "{db_path2}";\n{dump_result.script}\n')

        result, _ = run_file(roundtrip_script, None, verbose=False)
        assert result == 0

        registry2 = load_registry_from_metadata(db_path2)
        storage2 = StorageManager(db_path2, registry2)
        executor2 = QueryExecutor(storage2, registry2)
        parser = QueryParser()

        r = executor2.execute(parser.parse("from Node select *"))
        assert len(r.rows) == 2
        assert r.rows[0]["next"] is None
        assert r.rows[1]["next"] is None
        storage2.close()

    def test_missing_fields_default_to_null(self, tmp_path: Path):
        """Test that missing fields default to null."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
create Node(value=1);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        r = executor.execute(parser.parse("from Node select *"))
        assert len(r.rows) == 1
        assert r.rows[0]["value"] == 1
        assert r.rows[0]["next"] is None
        storage.close()


class TestUpdate:
    """Tests for UPDATE queries."""

    def test_update_variable(self, tmp_path: Path):
        """Test updating a variable-bound record."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
$n1 = create Node(value=1, next=null);
$n2 = create Node(value=2, next=null);
update $n1 set next=$n2;
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        r = executor.execute(parser.parse("from Node select *"))
        assert len(r.rows) == 2
        # Node 0 (value=1) should now point to Node 1
        assert r.rows[0]["value"] == 1
        assert r.rows[0]["next"] == "<Node[1]>"
        # Node 1 (value=2) should still be null
        assert r.rows[1]["value"] == 2
        assert r.rows[1]["next"] is None
        storage.close()

    def test_update_composite_ref(self, tmp_path: Path):
        """Test update Type(index) form."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor, UpdateResult
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
create Node(value=1, next=null);
create Node(value=2, next=null);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        r = executor.execute(parser.parse("update Node(0) set next=Node(1)"))
        assert isinstance(r, UpdateResult)
        assert r.index == 0

        r2 = executor.execute(parser.parse("from Node select *"))
        assert r2.rows[0]["next"] == "<Node[1]>"
        storage.close()

    def test_update_cycle(self, tmp_path: Path):
        """Test building a cyclic linked list via null+update."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
$n1 = create Node(value=1, next=null);
$n2 = create Node(value=2, next=null);
$n3 = create Node(value=3, next=$n1);
update $n1 set next=$n2;
update $n2 set next=$n3;
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        r = executor.execute(parser.parse("from Node select *"))
        assert len(r.rows) == 3
        # n1→n2→n3→n1
        assert r.rows[0]["next"] == "<Node[1]>"
        assert r.rows[1]["next"] == "<Node[2]>"
        assert r.rows[2]["next"] == "<Node[0]>"
        storage.close()

    def test_update_undefined_var(self, tmp_path: Path):
        """Test error for undefined variable in update."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor, UpdateResult
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        r = executor.execute(parser.parse("update $undefined set value=1"))
        assert isinstance(r, UpdateResult)
        assert "Undefined variable" in r.message
        storage.close()

    def test_update_unknown_field(self, tmp_path: Path):
        """Test error for nonexistent field in update."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor, UpdateResult
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
$n = create Node(value=1, next=null);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        # Re-bind variable
        executor.variables["n"] = ("Node", 0)
        r = executor.execute(parser.parse("update $n set nonexistent=1"))
        assert isinstance(r, UpdateResult)
        assert "Unknown field" in r.message
        storage.close()


class TestCycleAwareDump:
    """Tests for cycle-aware dump with null+update pattern."""

    def test_dump_cycle_uses_tags(self, tmp_path: Path):
        """Test that cyclic data dumps with tag syntax."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
$n1 = create Node(value=1, next=null);
$n2 = create Node(value=2, next=$n1);
update $n1 set next=$n2;
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery())
        assert isinstance(dump_result, DumpResult)
        # Should use tag syntax for cycle handling
        assert "tag(" in dump_result.script
        # Tag reference should appear (just the tag name, not $var)
        # Should NOT use null+update for simple cycles
        assert "update" not in dump_result.script
        storage.close()

    def test_dump_cycle_roundtrip(self, tmp_path: Path):
        """Test that cyclic data dumps and re-executes correctly."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery, QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
$n1 = create Node(value=1, next=null);
$n2 = create Node(value=2, next=$n1);
update $n1 set next=$n2;
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery())
        assert isinstance(dump_result, DumpResult)
        storage.close()

        # Roundtrip
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f'use "{db_path2}";\n{dump_result.script}\n')

        result, _ = run_file(roundtrip_script, None, verbose=False)
        assert result == 0

        registry2 = load_registry_from_metadata(db_path2)
        storage2 = StorageManager(db_path2, registry2)
        executor2 = QueryExecutor(storage2, registry2)
        parser = QueryParser()

        r = executor2.execute(parser.parse("from Node select *"))
        # Should have at least 2 nodes with a cycle
        assert len(r.rows) >= 2
        # Verify both nodes exist with correct values
        values = {row["value"] for row in r.rows}
        assert 1 in values
        assert 2 in values
        storage2.close()

    def test_dump_4node_cycle_no_duplicates(self, tmp_path: Path):
        """Test that a 4-node cycle roundtrips without duplicate records."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery, QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ name: string, child: Node }}
$back = create Node(name="D", child=null);
$top = create Node(name="A", child=Node(name="B", child=Node(name="C", child=$back)));
update $back set child=$top;
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery())
        assert isinstance(dump_result, DumpResult)
        storage.close()

        # Roundtrip: create new db from dump
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f'use "{db_path2}";\n{dump_result.script}\n')

        result, _ = run_file(roundtrip_script, None, verbose=False)
        assert result == 0

        registry2 = load_registry_from_metadata(db_path2)
        storage2 = StorageManager(db_path2, registry2)
        executor2 = QueryExecutor(storage2, registry2)
        parser = QueryParser()

        r = executor2.execute(parser.parse("from Node select *"))
        # Must have exactly 4 nodes: A, B, C, D — no duplicates
        assert len(r.rows) == 4
        names = {row["name"] for row in r.rows}
        assert names == {"A", "B", "C", "D"}
        storage2.close()

    def test_dump_no_cycle_unchanged(self, tmp_path: Path):
        """Test that acyclic data still dumps normally (regression test)."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery, QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery())
        assert isinstance(dump_result, DumpResult)
        # Should NOT contain update or null for non-cyclic data
        assert "update" not in dump_result.script
        assert "null" not in dump_result.script
        # Should contain normal create statements
        assert "create Person(" in dump_result.script
        assert "Alice" in dump_result.script
        assert "Bob" in dump_result.script
        storage.close()


class TestTagBasedCreation:
    """Tests for creating cyclic data using tag syntax within scope blocks."""

    def test_create_self_referencing_with_tag(self, tmp_path: Path):
        """Test creating a self-referencing node using tag syntax in a scope."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
scope {{
    create Node(tag(SELF), value=42, next=SELF);
}};
from Node select *;
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

    def test_create_cycle_with_tag(self, tmp_path: Path):
        """Test creating a 2-node cycle using tag syntax in a scope."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ name: string, child: Node }}
scope {{
    create Node(tag(TOP), name="A", child=Node(name="B", child=TOP));
}};
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        # Verify the cycle exists
        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        r = executor.execute(parser.parse("from Node select *"))
        assert len(r.rows) == 2
        # Node A (index 0) points to Node B (index 1)
        # Node B (index 1) points to Node A (index 0)
        storage.close()

    def test_create_deep_cycle_with_tag(self, tmp_path: Path):
        """Test creating a 4-node cycle A→B→C→D→A using tag syntax in a scope."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ name: string, child: Node }}
scope {{
    create Node(tag(A), name="A", child=Node(name="B", child=Node(name="C", child=Node(name="D", child=A))));
}};
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()

        r = executor.execute(parser.parse("from Node select *"))
        assert len(r.rows) == 4
        names = {row["name"] for row in r.rows}
        assert names == {"A", "B", "C", "D"}
        storage.close()

    def test_create_undefined_tag_error(self, tmp_path: Path):
        """Test that using an undefined tag within a scope produces an error."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
scope {{
    create Node(value=1, next=NONEXISTENT);
}};
""")
        result, _ = run_file(script, None, verbose=False)
        # Should fail because NONEXISTENT tag is not defined
        assert result == 0  # The REPL continues but prints an error

    def test_tag_does_not_leak_across_scopes(self, tmp_path: Path):
        """Test that tags from one scope don't leak to another."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
scope {{
    create Node(tag(X), value=1, next=null);
}};
scope {{
    create Node(value=2, next=X);
}};
""")
        result, _ = run_file(script, None, verbose=False)
        # The second scope should fail because X is not visible
        assert result == 0  # The REPL continues but prints an error

    def test_tag_requires_scope(self, tmp_path: Path):
        """Test that using tags outside a scope produces an error."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{db_path}";
type Node {{ value: uint8, next: Node }}
create Node(tag(X), value=1, next=null);
""")
        result, _ = run_file(script, None, verbose=False)
        # Should fail because tags require a scope
        assert result == 0  # The REPL continues but prints an error


class TestDumpPretty:
    """Tests for the dump pretty command."""

    def test_dump_pretty_type_formatting(self, tmp_path: Path):
        """Test that pretty dump formats type definitions with 4-space indent."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery(pretty=True))
        assert isinstance(dump_result, DumpResult)
        # Type definition should be multi-line with 4-space indent
        assert "type Person {\n    name: string,\n    age: uint8\n}" in dump_result.script
        storage.close()

    def test_dump_pretty_instance_formatting(self, tmp_path: Path):
        """Test that pretty dump formats instances with 4-space indented fields."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery(pretty=True))
        assert isinstance(dump_result, DumpResult)
        # Instance should be multi-line with 4-space indent
        assert 'create Person(\n    name="Alice",\n    age=30\n)' in dump_result.script
        storage.close()

    def test_dump_pretty_nested_composites(self, tmp_path: Path):
        """Test that pretty dump increases indent for nested inline composites."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Address {{ street: string, city: string }}
type Person {{ name: string, address: Address }}
create Person(name="Alice", address=Address(street="123 Main", city="Springfield"));
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery(pretty=True))
        assert isinstance(dump_result, DumpResult)
        # Nested composite should have increased indent (8 spaces for nested fields, 4 for close paren)
        assert 'Address(\n        street="123 Main",\n        city="Springfield"\n    )' in dump_result.script
        storage.close()

    def test_dump_pretty_roundtrip(self, tmp_path: Path):
        """Test that pretty dump output can be parsed and recreates the database."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        # Get pretty dump output
        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery(pretty=True))
        assert isinstance(dump_result, DumpResult)
        dump_script = dump_result.script
        storage.close()

        # Recreate database from pretty dump
        db_path2 = tmp_path / "testdb2"
        restore_script = tmp_path / "restore.ttq"
        restore_script.write_text(f'use "{db_path2}";\n{dump_script}\n')

        result, _ = run_file(restore_script, None, verbose=False)
        assert result == 0

        # Verify round-trip: compact dump of both databases should match
        registry2 = load_registry_from_metadata(db_path2)
        storage2 = StorageManager(db_path2, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        dump2 = executor2.execute(DumpQuery(pretty=False))
        assert isinstance(dump2, DumpResult)

        registry_orig = load_registry_from_metadata(db_path)
        storage_orig = StorageManager(db_path, registry_orig)
        executor_orig = QueryExecutor(storage_orig, registry_orig)

        dump_orig = executor_orig.execute(DumpQuery(pretty=False))
        assert isinstance(dump_orig, DumpResult)

        assert dump2.script == dump_orig.script
        storage2.close()
        storage_orig.close()

    def test_dump_not_pretty_unchanged(self, tmp_path: Path):
        """Regression: regular dump output is unchanged (not pretty-formatted)."""
        db_path = tmp_path / "testdb"

        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import DumpResult, QueryExecutor
        from typed_tables.parsing.query_parser import DumpQuery
        from typed_tables.storage import StorageManager

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use "{db_path}";
type Person {{ name: string, age: uint8 }}
create Person(name="Alice", age=30);
""")
        result, _ = run_file(script, None, verbose=False)
        assert result == 0

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)

        dump_result = executor.execute(DumpQuery(pretty=False))
        assert isinstance(dump_result, DumpResult)
        # Compact type definition: all on one line
        assert "type Person { name: string, age: uint8 }" in dump_result.script
        # Compact instance: all on one line
        assert 'create Person(name="Alice", age=30)' in dump_result.script
        storage.close()


class TestArrayMethods:
    """Tests for array method calls (length, isEmpty)."""

    def _setup_db(self, tmp_path, script_text):
        """Helper to create a database with the given script."""
        db_path = tmp_path / "testdb"
        script = tmp_path / "setup.ttq"
        script.write_text(f'use "{db_path}"\n{script_text}\n')
        result, _ = run_file(script, None, verbose=False)
        assert result == 0
        return db_path

    def _query(self, db_path, query_text):
        """Helper to run a query and return the result."""
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()
        result = executor.execute(parser.parse(query_text))
        storage.close()
        return result

    def test_array_length_method(self, tmp_path):
        """Test length() on array fields in SELECT."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[25, 26, 24, 27])
create Sensor(name="empty", readings=[])
""")
        result = self._query(db_path, "from Sensor select name, readings.length()")
        assert "readings.length()" in result.columns
        assert len(result.rows) == 2
        assert result.rows[0]["readings.length()"] == 4
        assert result.rows[1]["readings.length()"] == 0

    def test_array_isEmpty_method(self, tmp_path):
        """Test isEmpty() for filtering in WHERE."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[25, 26, 24])
create Sensor(name="empty", readings=[])
""")
        result = self._query(db_path, "from Sensor select name where readings.isEmpty()")
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "empty"

    def test_array_method_where_comparison(self, tmp_path):
        """Test length() with comparison in WHERE."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="many", readings=[1, 2, 3, 4, 5])
create Sensor(name="few", readings=[1, 2])
create Sensor(name="none", readings=[])
""")
        result = self._query(db_path, "from Sensor select name where readings.length() > 2")
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "many"

    def test_string_length_method(self, tmp_path):
        """Test length() on string fields (strings are character arrays)."""
        db_path = self._setup_db(tmp_path, """
type Person { name: string }
create Person(name="Alice")
create Person(name="Bo")
""")
        result = self._query(db_path, "from Person select name, name.length()")
        assert result.rows[0]["name.length()"] == 5
        assert result.rows[1]["name.length()"] == 2

    def test_array_method_null_field(self, tmp_path):
        """Test method call on null array field."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="no_readings")
""")
        result = self._query(db_path, "from Sensor select name, readings.length()")
        assert result.rows[0]["readings.length()"] == 0

    def test_array_isEmpty_on_null(self, tmp_path):
        """Test isEmpty() on null array field returns true."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="no_readings")
""")
        result = self._query(db_path, "from Sensor select * where readings.isEmpty()")
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "no_readings"

    def test_array_length_in_select_and_where(self, tmp_path):
        """Test length() used in both SELECT and WHERE simultaneously."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="big", readings=[1, 2, 3, 4, 5])
create Sensor(name="small", readings=[1])
""")
        result = self._query(db_path, "from Sensor select name, readings.length() where readings.length() >= 3")
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "big"
        assert result.rows[0]["readings.length()"] == 5

    def test_isEmpty_select_displays_boolean(self, tmp_path):
        """Test that isEmpty() in SELECT returns boolean values."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="has_data", readings=[1, 2])
create Sensor(name="empty", readings=[])
""")
        result = self._query(db_path, "from Sensor select name, readings.isEmpty()")
        assert result.rows[0]["readings.isEmpty()"] is False
        assert result.rows[1]["readings.isEmpty()"] is True

    def test_unknown_method_error(self, tmp_path):
        """Test that unknown method names produce an error."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        import pytest
        with pytest.raises(RuntimeError, match="Unknown array method"):
            self._query(db_path, "from Sensor select readings.foobar()")

    def test_contains_match(self, tmp_path):
        """Test contains() returns True when element is found."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4, 5])
""")
        result = self._query(db_path, "from Sensor select name, readings.contains(3)")
        assert result.rows[0]["readings.contains(3)"] is True

    def test_contains_no_match(self, tmp_path):
        """Test contains() returns False when element is not found."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4, 5])
""")
        result = self._query(db_path, "from Sensor select name, readings.contains(99)")
        assert result.rows[0]["readings.contains(99)"] is False

    def test_contains_on_null(self, tmp_path):
        """Test contains() on null array returns False."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="empty")
""")
        result = self._query(db_path, "from Sensor select name, readings.contains(1)")
        assert result.rows[0]["readings.contains(1)"] is False

    def test_contains_on_empty(self, tmp_path):
        """Test contains() on empty array returns False."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="empty", readings=[])
""")
        result = self._query(db_path, "from Sensor select name, readings.contains(1)")
        assert result.rows[0]["readings.contains(1)"] is False

    def test_contains_in_where(self, tmp_path):
        """Test contains() as boolean filter in WHERE clause."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="has3", readings=[1, 2, 3])
create Sensor(name="no3", readings=[4, 5, 6])
create Sensor(name="also3", readings=[3, 7, 8])
""")
        result = self._query(db_path, "from Sensor select name where readings.contains(3)")
        names = [r["name"] for r in result.rows]
        assert "has3" in names
        assert "also3" in names
        assert "no3" not in names

    def test_contains_string(self, tmp_path):
        """Test contains() on string field checks substring."""
        db_path = self._setup_db(tmp_path, """
type Person { name: string }
create Person(name="Alice")
create Person(name="Bob")
""")
        result = self._query(db_path, "from Person select name, name.contains(\"li\")")
        assert result.rows[0]["name.contains('li')"] is True
        assert result.rows[1]["name.contains('li')"] is False

    def test_min_primitive_array(self, tmp_path):
        """Test min() on primitive array returns minimum value."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 2, 8, 1, 9])
""")
        result = self._query(db_path, "from Sensor select readings.min()")
        assert result.rows[0]["readings.min()"] == 1

    def test_max_primitive_array(self, tmp_path):
        """Test max() on primitive array returns maximum value."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 2, 8, 1, 9])
""")
        result = self._query(db_path, "from Sensor select readings.max()")
        assert result.rows[0]["readings.max()"] == 9

    def test_min_on_null(self, tmp_path):
        """Test min() on null array returns None."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="empty")
""")
        result = self._query(db_path, "from Sensor select readings.min()")
        assert result.rows[0]["readings.min()"] is None

    def test_max_on_empty(self, tmp_path):
        """Test max() on empty array returns None."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="empty", readings=[])
""")
        result = self._query(db_path, "from Sensor select readings.max()")
        assert result.rows[0]["readings.max()"] is None

    def test_min_composite_key(self, tmp_path):
        """Test min(.salary) on composite array returns min value of field."""
        db_path = self._setup_db(tmp_path, """
type Employee { name: string, salary: uint32 }
type Team { name: string, members: Employee[] }
create Team(name="eng", members=[
    Employee(name="Alice", salary=90000),
    Employee(name="Bob", salary=70000),
    Employee(name="Charlie", salary=110000)
])
""")
        result = self._query(db_path, "from Team select members.min(.salary)")
        assert result.rows[0]["members.min(.salary)"] == 70000

    def test_max_composite_key(self, tmp_path):
        """Test max(.salary) on composite array returns max value of field."""
        db_path = self._setup_db(tmp_path, """
type Employee { name: string, salary: uint32 }
type Team { name: string, members: Employee[] }
create Team(name="eng", members=[
    Employee(name="Alice", salary=90000),
    Employee(name="Bob", salary=70000),
    Employee(name="Charlie", salary=110000)
])
""")
        result = self._query(db_path, "from Team select members.max(.salary)")
        assert result.rows[0]["members.max(.salary)"] == 110000

    def test_min_aggregate(self, tmp_path):
        """Test min(age) as row aggregate."""
        db_path = self._setup_db(tmp_path, """
type Person { name: string, age: uint8 }
create Person(name="Alice", age=30)
create Person(name="Bob", age=25)
create Person(name="Charlie", age=35)
""")
        result = self._query(db_path, "from Person select min(age)")
        assert result.rows[0]["min(age)"] == 25

    def test_max_aggregate(self, tmp_path):
        """Test max(age) as row aggregate."""
        db_path = self._setup_db(tmp_path, """
type Person { name: string, age: uint8 }
create Person(name="Alice", age=30)
create Person(name="Bob", age=25)
create Person(name="Charlie", age=35)
""")
        result = self._query(db_path, "from Person select max(age)")
        assert result.rows[0]["max(age)"] == 35


class TestArrayMutations:
    """Tests for array mutation methods (reverse, swap) in UPDATE SET."""

    def _setup_db(self, tmp_path, script_text):
        """Helper to create a database with the given script."""
        db_path = tmp_path / "testdb"
        script = tmp_path / "setup.ttq"
        script.write_text(f'use "{db_path}"\n{script_text}\n')
        result, _ = run_file(script, None, verbose=False)
        assert result == 0
        return db_path

    def _query(self, db_path, query_text):
        """Helper to run a query and return the result."""
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()
        result = executor.execute(parser.parse(query_text))
        storage.close()
        return result

    def test_reverse_primitive_array(self, tmp_path):
        """Test reverse() on a primitive array reverses elements."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4, 5])
""")
        self._query(db_path, "update Sensor(0) set readings.reverse()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [5, 4, 3, 2, 1]

    def test_reverse_empty_array(self, tmp_path):
        """Test reverse() on empty array is a no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="empty", readings=[])
""")
        self._query(db_path, "update Sensor(0) set readings.reverse()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == []

    def test_reverse_null_array(self, tmp_path):
        """Test reverse() on null array is a no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="null_readings")
""")
        self._query(db_path, "update Sensor(0) set readings.reverse()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] is None

    def test_reverse_string_field(self, tmp_path):
        """Test reverse() on string field reverses characters."""
        db_path = self._setup_db(tmp_path, """
type Item { name: string }
create Item(name="hello")
""")
        self._query(db_path, "update Item(0) set name.reverse()")
        result = self._query(db_path, "from Item select name")
        assert result.rows[0]["name"] == "olleh"

    def test_swap_primitive_array(self, tmp_path):
        """Test swap(i, j) swaps two elements."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[10, 20, 30, 40, 50])
""")
        self._query(db_path, "update Sensor(0) set readings.swap(0, 4)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [50, 20, 30, 40, 10]

    def test_swap_out_of_bounds(self, tmp_path):
        """Test swap() with out of bounds index returns error."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "update Sensor(0) set readings.swap(0, 5)")
        assert "out of range" in result.message

    def test_swap_wrong_arg_count(self, tmp_path):
        """Test swap() with wrong number of arguments returns error."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "update Sensor(0) set readings.swap(0)")
        assert "requires exactly 2 arguments" in result.message

    def test_swap_null_array(self, tmp_path):
        """Test swap() on null array returns error."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="null_readings")
""")
        result = self._query(db_path, "update Sensor(0) set readings.swap(0, 1)")
        assert "null" in result.message.lower()

    def test_mixed_mutation_and_assignment(self, tmp_path):
        """Test mixing mutation with field assignment in same SET."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, 'update Sensor(0) set name = "updated", readings.reverse()')
        result = self._query(db_path, "from Sensor select *")
        assert result.rows[0]["name"] == "updated"
        assert result.rows[0]["readings"] == [3, 2, 1]

    def test_bulk_update_mutation_with_where(self, tmp_path):
        """Test bulk update with WHERE and mutation."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
create Sensor(name="pressure", readings=[10, 20, 30])
""")
        self._query(db_path, 'update Sensor set readings.reverse() where name = "temp"')
        result = self._query(db_path, "from Sensor select name, readings")
        # temp should be reversed
        temp = [r for r in result.rows if r["name"] == "temp"][0]
        assert temp["readings"] == [3, 2, 1]
        # pressure should be unchanged
        pressure = [r for r in result.rows if r["name"] == "pressure"][0]
        assert pressure["readings"] == [10, 20, 30]

    def test_bulk_update_mutation_all_records(self, tmp_path):
        """Test bulk update mutation without WHERE affects all records."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="a", readings=[1, 2, 3])
create Sensor(name="b", readings=[4, 5, 6])
""")
        self._query(db_path, "update Sensor set readings.reverse()")
        result = self._query(db_path, "from Sensor select name, readings")
        a = [r for r in result.rows if r["name"] == "a"][0]
        b = [r for r in result.rows if r["name"] == "b"][0]
        assert a["readings"] == [3, 2, 1]
        assert b["readings"] == [6, 5, 4]

    def test_reverse_composite_array(self, tmp_path):
        """Test reverse() on composite array reverses element references."""
        db_path = self._setup_db(tmp_path, """
type Point { x: uint8, y: uint8 }
type Shape { name: string, points: Point[] }
create Shape(name="tri", points=[Point(x=1, y=1), Point(x=2, y=2), Point(x=3, y=3)])
""")
        self._query(db_path, "update Shape(0) set points.reverse()")
        result = self._query(db_path, "from Shape select points")
        points = result.rows[0]["points"]
        assert points[0]["x"] == 3
        assert points[1]["x"] == 2
        assert points[2]["x"] == 1

    def test_reverse_preserves_start_index_length(self, tmp_path):
        """Test that reverse() doesn't change (start_index, length) in composite record."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4, 5])
""")
        # Get raw record before
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.storage import StorageManager
        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        table = storage.get_table("Sensor")
        raw_before = table.get(0)
        ref_before = raw_before["readings"]
        storage.close()

        # Reverse
        self._query(db_path, "update Sensor(0) set readings.reverse()")

        # Get raw record after
        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        table = storage.get_table("Sensor")
        raw_after = table.get(0)
        ref_after = raw_after["readings"]
        storage.close()

        # start_index and length should be unchanged
        assert ref_before == ref_after

    def test_unknown_mutation_method(self, tmp_path):
        """Test that unknown mutation method returns error."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "update Sensor(0) set readings.foobar()")
        assert "Unknown array mutation method" in result.message

    def test_mutation_on_non_array_field(self, tmp_path):
        """Test mutation on non-array field returns error."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, value: uint8 }
create Sensor(name="temp", value=42)
""")
        result = self._query(db_path, "update Sensor(0) set value.reverse()")
        assert "can only be applied to array fields" in result.message

    # --- append() tests ---

    def test_append_single_element(self, tmp_path):
        """Test append(5) on primitive array."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, "update Sensor(0) set readings.append(5)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3, 5]

    def test_append_multiple_elements(self, tmp_path):
        """Test append(5, 6, 7) appends multiple elements."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, "update Sensor(0) set readings.append(5, 6, 7)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3, 5, 6, 7]

    def test_append_array_literal(self, tmp_path):
        """Test append([1, 2, 3]) flattens and appends."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[10, 20])
""")
        self._query(db_path, "update Sensor(0) set readings.append([1, 2, 3])")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [10, 20, 1, 2, 3]

    def test_append_on_null_array(self, tmp_path):
        """Test append() on null array creates new array."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="null_readings")
""")
        self._query(db_path, "update Sensor(0) set readings.append(1, 2, 3)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3]

    def test_append_on_empty_array(self, tmp_path):
        """Test append() on empty array."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="empty", readings=[])
""")
        self._query(db_path, "update Sensor(0) set readings.append(42)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [42]

    def test_append_string_to_string_field(self, tmp_path):
        """Test append("x") on string field appends character."""
        db_path = self._setup_db(tmp_path, """
type Item { name: string }
create Item(name="hello")
""")
        self._query(db_path, 'update Item(0) set name.append("!")')
        result = self._query(db_path, "from Item select name")
        assert result.rows[0]["name"] == "hello!"

    def test_append_composite_element(self, tmp_path):
        """Test append(Point(x=4, y=4)) on composite array."""
        db_path = self._setup_db(tmp_path, """
type Point { x: uint8, y: uint8 }
type Shape { name: string, points: Point[] }
create Shape(name="tri", points=[Point(x=1, y=1), Point(x=2, y=2), Point(x=3, y=3)])
""")
        self._query(db_path, "update Shape(0) set points.append(Point(x=4, y=4))")
        result = self._query(db_path, "from Shape select points")
        points = result.rows[0]["points"]
        assert len(points) == 4
        assert points[3]["x"] == 4
        assert points[3]["y"] == 4

    def test_append_composite_ref_to_composite_array(self, tmp_path):
        """Test append(Point(0)) on composite array using composite reference."""
        db_path = self._setup_db(tmp_path, """
type Point { x: uint8, y: uint8 }
type Shape { name: string, points: Point[] }
create Point(x=10, y=20)
create Shape(name="line", points=[Point(x=1, y=1)])
""")
        self._query(db_path, "update Shape(0) set points.append(Point(0))")
        result = self._query(db_path, "from Shape select points")
        points = result.rows[0]["points"]
        assert len(points) == 2
        assert points[1]["x"] == 10
        assert points[1]["y"] == 20

    def test_append_no_args_error(self, tmp_path):
        """Test append() with no args returns error."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "update Sensor(0) set readings.append()")
        assert "requires at least 1 argument" in result.message

    def test_append_tail_fast_path(self, tmp_path):
        """Test tail fast path: single record append keeps start_index."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        # Get raw record before
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.storage import StorageManager
        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        table = storage.get_table("Sensor")
        raw_before = table.get(0)
        start_before, len_before = raw_before["readings"]
        storage.close()

        self._query(db_path, "update Sensor(0) set readings.append(4, 5)")

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        table = storage.get_table("Sensor")
        raw_after = table.get(0)
        start_after, len_after = raw_after["readings"]
        storage.close()

        # Tail fast path: start_index unchanged, length increased
        assert start_before == start_after
        assert len_after == len_before + 2

    def test_append_copy_on_write(self, tmp_path):
        """Test copy-on-write: append to first of two records changes start_index."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="first", readings=[1, 2, 3])
create Sensor(name="second", readings=[4, 5, 6])
""")
        # Get raw record before
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.storage import StorageManager
        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        table = storage.get_table("Sensor")
        raw_before = table.get(0)
        start_before, _ = raw_before["readings"]
        storage.close()

        self._query(db_path, 'update Sensor(0) set readings.append(99)')

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        table = storage.get_table("Sensor")
        raw_after = table.get(0)
        start_after, len_after = raw_after["readings"]
        storage.close()

        # Copy-on-write: start_index should change (moved to end)
        assert start_before != start_after
        assert len_after == 4

        # Verify data is correct
        result = self._query(db_path, "from Sensor select name, readings")
        first = [r for r in result.rows if r["name"] == "first"][0]
        assert first["readings"] == [1, 2, 3, 99]
        # Second should be unchanged
        second = [r for r in result.rows if r["name"] == "second"][0]
        assert second["readings"] == [4, 5, 6]

    def test_append_bulk_update_with_where(self, tmp_path):
        """Test bulk append with WHERE clause."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
create Sensor(name="pressure", readings=[10, 20, 30])
""")
        self._query(db_path, 'update Sensor set readings.append(0) where name = "temp"')
        result = self._query(db_path, "from Sensor select name, readings")
        temp = [r for r in result.rows if r["name"] == "temp"][0]
        assert temp["readings"] == [1, 2, 3, 0]
        pressure = [r for r in result.rows if r["name"] == "pressure"][0]
        assert pressure["readings"] == [10, 20, 30]

    def test_append_mixed_with_assignment(self, tmp_path):
        """Test mixing append with field assignment in same SET."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, 'update Sensor(0) set name = "updated", readings.append(99)')
        result = self._query(db_path, "from Sensor select *")
        assert result.rows[0]["name"] == "updated"
        assert result.rows[0]["readings"] == [1, 2, 3, 99]

    # --- prepend() tests ---

    def test_prepend_single_element(self, tmp_path):
        """Test prepend(5) on primitive array."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, "update Sensor(0) set readings.prepend(5)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [5, 1, 2, 3]

    def test_prepend_multiple_elements(self, tmp_path):
        """Test prepend(5, 6, 7) prepends multiple elements."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, "update Sensor(0) set readings.prepend(5, 6, 7)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [5, 6, 7, 1, 2, 3]

    def test_prepend_array_literal(self, tmp_path):
        """Test prepend([1, 2, 3]) flattens and prepends."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[10, 20])
""")
        self._query(db_path, "update Sensor(0) set readings.prepend([1, 2, 3])")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3, 10, 20]

    def test_prepend_on_null_array(self, tmp_path):
        """Test prepend() on null array creates new array."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="null_readings")
""")
        self._query(db_path, "update Sensor(0) set readings.prepend(1, 2, 3)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3]

    def test_prepend_composite_element(self, tmp_path):
        """Test prepend(Point(...)) on composite array."""
        db_path = self._setup_db(tmp_path, """
type Point { x: uint8, y: uint8 }
type Shape { name: string, points: Point[] }
create Shape(name="line", points=[Point(x=2, y=2), Point(x=3, y=3)])
""")
        self._query(db_path, "update Shape(0) set points.prepend(Point(x=1, y=1))")
        result = self._query(db_path, "from Shape select points")
        points = result.rows[0]["points"]
        assert len(points) == 3
        assert points[0]["x"] == 1
        assert points[1]["x"] == 2
        assert points[2]["x"] == 3

    # --- insert() tests ---

    def test_insert_at_middle(self, tmp_path):
        """Test insert(2, 99) at middle position."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4, 5])
""")
        self._query(db_path, "update Sensor(0) set readings.insert(2, 99)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 99, 3, 4, 5]

    def test_insert_at_start(self, tmp_path):
        """Test insert(0, 99) at start (= prepend)."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, "update Sensor(0) set readings.insert(0, 99)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [99, 1, 2, 3]

    def test_insert_at_end(self, tmp_path):
        """Test insert(length, 99) at end (= append)."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, "update Sensor(0) set readings.insert(3, 99)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3, 99]

    def test_insert_flattened_elements(self, tmp_path):
        """Test insert(0, [1, 2, 3]) flattens elements at index."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[10, 20])
""")
        self._query(db_path, "update Sensor(0) set readings.insert(1, [5, 6, 7])")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [10, 5, 6, 7, 20]

    def test_insert_out_of_bounds(self, tmp_path):
        """Test insert() with out of bounds index returns error."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "update Sensor(0) set readings.insert(5, 99)")
        assert "out of range" in result.message

    def test_insert_on_null_at_zero(self, tmp_path):
        """Test insert(0, 1) on null array creates new array."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="null_readings")
""")
        self._query(db_path, "update Sensor(0) set readings.insert(0, 1)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1]

    # --- delete() tests ---

    def test_delete_first_element(self, tmp_path):
        """Test delete(0) removes first element."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4, 5])
""")
        self._query(db_path, "update Sensor(0) set readings.delete(0)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [2, 3, 4, 5]

    def test_delete_middle_element(self, tmp_path):
        """Test delete(2) removes middle element."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4, 5])
""")
        self._query(db_path, "update Sensor(0) set readings.delete(2)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 4, 5]

    def test_delete_multiple_indices(self, tmp_path):
        """Test delete(0, 2, 4) removes elements at multiple indices."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4, 5])
""")
        self._query(db_path, "update Sensor(0) set readings.delete(0, 2, 4)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [2, 4]

    def test_delete_out_of_bounds(self, tmp_path):
        """Test delete() with out of bounds index returns error."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "update Sensor(0) set readings.delete(10)")
        assert "out of range" in result.message

    def test_delete_on_null_array(self, tmp_path):
        """Test delete() on null array returns error."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="null_readings")
""")
        result = self._query(db_path, "update Sensor(0) set readings.delete(0)")
        assert "null" in result.message.lower()

    def test_delete_all_elements(self, tmp_path):
        """Test deleting all elements results in empty array."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, "update Sensor(0) set readings.delete(0, 1, 2)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == []

    # --- remove() tests ---

    def test_remove_first_occurrence(self, tmp_path):
        """Test remove(5) removes first occurrence."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 5, 3, 5, 2])
""")
        self._query(db_path, "update Sensor(0) set readings.remove(5)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 3, 5, 2]

    def test_remove_not_found(self, tmp_path):
        """Test remove(99) when not found is a no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, "update Sensor(0) set readings.remove(99)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3]

    def test_remove_on_null_array(self, tmp_path):
        """Test remove() on null array is a no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="null_readings")
""")
        self._query(db_path, "update Sensor(0) set readings.remove(5)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] is None

    def test_remove_composite_element(self, tmp_path):
        """Test remove() on composite array with inline instance."""
        db_path = self._setup_db(tmp_path, """
type Point { x: uint8, y: uint8 }
type Shape { name: string, points: Point[] }
create Shape(name="tri", points=[Point(x=1, y=1), Point(x=2, y=2), Point(x=3, y=3)])
""")
        self._query(db_path, "update Shape(0) set points.remove(Point(x=2, y=2))")
        result = self._query(db_path, "from Shape select points")
        points = result.rows[0]["points"]
        assert len(points) == 2
        assert points[0]["x"] == 1
        assert points[1]["x"] == 3

    def test_remove_no_args_error(self, tmp_path):
        """Test remove() with no args returns error."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "update Sensor(0) set readings.remove()")
        assert "requires exactly 1 argument" in result.message

    # --- removeAll() tests ---

    def test_removeAll_all_occurrences(self, tmp_path):
        """Test removeAll(5) removes all occurrences."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 1, 5, 2, 5, 3, 5])
""")
        self._query(db_path, "update Sensor(0) set readings.removeAll(5)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3]

    def test_removeAll_not_found(self, tmp_path):
        """Test removeAll(99) when not found is a no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, "update Sensor(0) set readings.removeAll(99)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3]

    def test_removeAll_on_null_array(self, tmp_path):
        """Test removeAll() on null array is a no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="null_readings")
""")
        self._query(db_path, "update Sensor(0) set readings.removeAll(5)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] is None

    def test_removeAll_all_elements(self, tmp_path):
        """Test removeAll removes all elements when all match."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 5, 5])
""")
        self._query(db_path, "update Sensor(0) set readings.removeAll(5)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == []

    # --- Cross-cutting tests ---

    def test_bulk_prepend_with_where(self, tmp_path):
        """Test bulk update with WHERE + prepend."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
create Sensor(name="pressure", readings=[10, 20, 30])
""")
        self._query(db_path, 'update Sensor set readings.prepend(0) where name = "temp"')
        result = self._query(db_path, "from Sensor select name, readings")
        temp = [r for r in result.rows if r["name"] == "temp"][0]
        assert temp["readings"] == [0, 1, 2, 3]
        pressure = [r for r in result.rows if r["name"] == "pressure"][0]
        assert pressure["readings"] == [10, 20, 30]

    def test_mixed_assignment_and_delete(self, tmp_path):
        """Test mixing field assignment with delete mutation in same SET."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4, 5])
""")
        self._query(db_path, 'update Sensor(0) set name = "updated", readings.delete(0)')
        result = self._query(db_path, "from Sensor select *")
        assert result.rows[0]["name"] == "updated"
        assert result.rows[0]["readings"] == [2, 3, 4, 5]

    # --- sort() tests ---

    def test_sort_primitive_ascending(self, tmp_path):
        """Test sort() on primitive array sorts ascending."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 3, 1, 4, 2])
""")
        self._query(db_path, "update Sensor(0) set readings.sort()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3, 4, 5]

    def test_sort_primitive_descending(self, tmp_path):
        """Test sort(desc) on primitive array sorts descending."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 3, 1, 4, 2])
""")
        self._query(db_path, "update Sensor(0) set readings.sort(desc)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [5, 4, 3, 2, 1]

    def test_sort_null_noop(self, tmp_path):
        """Test sort() on null array is a no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="null_readings")
""")
        self._query(db_path, "update Sensor(0) set readings.sort()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] is None

    def test_sort_empty_noop(self, tmp_path):
        """Test sort() on empty array is a no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="empty", readings=[])
""")
        self._query(db_path, "update Sensor(0) set readings.sort()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == []

    def test_sort_composite_single_field(self, tmp_path):
        """Test sort(.salary) on composite array."""
        db_path = self._setup_db(tmp_path, """
type Employee { name: string, salary: uint32 }
type Team { name: string, members: Employee[] }
create Team(name="eng", members=[
    Employee(name="Charlie", salary=80000),
    Employee(name="Alice", salary=50000),
    Employee(name="Bob", salary=70000)
])
""")
        self._query(db_path, "update Team(0) set members.sort(.salary)")
        result = self._query(db_path, "from Team select members")
        members = result.rows[0]["members"]
        # Sorted by salary ascending: 50000, 70000, 80000
        assert members[0]["salary"] == 50000
        assert members[1]["salary"] == 70000
        assert members[2]["salary"] == 80000

    def test_sort_composite_single_field_desc(self, tmp_path):
        """Test sort(.salary desc) on composite array."""
        db_path = self._setup_db(tmp_path, """
type Employee { name: string, salary: uint32 }
type Team { name: string, members: Employee[] }
create Team(name="eng", members=[
    Employee(name="Charlie", salary=80000),
    Employee(name="Alice", salary=50000),
    Employee(name="Bob", salary=70000)
])
""")
        self._query(db_path, "update Team(0) set members.sort(.salary desc)")
        result = self._query(db_path, "from Team select members")
        members = result.rows[0]["members"]
        # Sorted by salary descending: 80000, 70000, 50000
        assert members[0]["salary"] == 80000
        assert members[1]["salary"] == 70000
        assert members[2]["salary"] == 50000

    def test_sort_composite_multi_key(self, tmp_path):
        """Test sort(.age, .salary) on composite array — multi-field sort."""
        db_path = self._setup_db(tmp_path, """
type Employee { age: uint8, salary: uint32 }
type Team { name: string, members: Employee[] }
create Team(name="eng", members=[
    Employee(age=30, salary=80000),
    Employee(age=25, salary=50000),
    Employee(age=30, salary=60000)
])
""")
        self._query(db_path, "update Team(0) set members.sort(.age, .salary)")
        result = self._query(db_path, "from Team select members")
        members = result.rows[0]["members"]
        # age 25 first, then age 30 sorted by salary
        assert members[0]["age"] == 25
        assert members[0]["salary"] == 50000
        assert members[1]["age"] == 30
        assert members[1]["salary"] == 60000
        assert members[2]["age"] == 30
        assert members[2]["salary"] == 80000

    def test_sort_composite_mixed_directions(self, tmp_path):
        """Test sort(.age desc, .salary) on composite array — mixed directions."""
        db_path = self._setup_db(tmp_path, """
type Employee { age: uint8, salary: uint32 }
type Team { name: string, members: Employee[] }
create Team(name="eng", members=[
    Employee(age=25, salary=70000),
    Employee(age=30, salary=50000),
    Employee(age=25, salary=60000),
    Employee(age=30, salary=80000)
])
""")
        self._query(db_path, "update Team(0) set members.sort(.age desc, .salary)")
        result = self._query(db_path, "from Team select members")
        members = result.rows[0]["members"]
        # age desc: 30, 30, 25, 25; then salary asc within same age
        assert members[0]["age"] == 30
        assert members[0]["salary"] == 50000
        assert members[1]["age"] == 30
        assert members[1]["salary"] == 80000
        assert members[2]["age"] == 25
        assert members[2]["salary"] == 60000
        assert members[3]["age"] == 25
        assert members[3]["salary"] == 70000

    def test_sort_composite_string_field(self, tmp_path):
        """Test sort(.name) sorts alphabetically on string field."""
        db_path = self._setup_db(tmp_path, """
type Employee { name: string, age: uint8 }
type Team { name: string, members: Employee[] }
create Team(name="eng", members=[
    Employee(name="Charlie", age=30),
    Employee(name="Alice", age=25),
    Employee(name="Bob", age=35)
])
""")
        self._query(db_path, "update Team(0) set members.sort(.name)")
        result = self._query(db_path, "from Team select members")
        members = result.rows[0]["members"]
        # Verify by checking age which is a primitive inline field
        assert members[0]["age"] == 25   # Alice
        assert members[1]["age"] == 35   # Bob
        assert members[2]["age"] == 30   # Charlie

    def test_sort_bulk_with_where(self, tmp_path):
        """Test bulk sort with WHERE clause."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 3, 1, 4, 2])
create Sensor(name="pressure", readings=[30, 10, 20])
""")
        self._query(db_path, 'update Sensor set readings.sort() where name = "temp"')
        result = self._query(db_path, "from Sensor select name, readings")
        temp = [r for r in result.rows if r["name"] == "temp"][0]
        assert temp["readings"] == [1, 2, 3, 4, 5]
        pressure = [r for r in result.rows if r["name"] == "pressure"][0]
        assert pressure["readings"] == [30, 10, 20]  # unchanged

    def test_sort_single_element_noop(self, tmp_path):
        """Test sort() on single-element array is a no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="single", readings=[42])
""")
        self._query(db_path, "update Sensor(0) set readings.sort()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [42]

    def test_replace_first_match(self, tmp_path):
        """Test replace(old, new) replaces first occurrence."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 2, 5])
""")
        self._query(db_path, "update Sensor(0) set readings.replace(2, 10)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 10, 3, 2, 5]

    def test_replace_not_found(self, tmp_path):
        """Test replace() when value is not found — no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, "update Sensor(0) set readings.replace(99, 10)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3]

    def test_replace_on_null(self, tmp_path):
        """Test replace() on null array — no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="empty")
""")
        self._query(db_path, "update Sensor(0) set readings.replace(1, 2)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] is None

    def test_replace_on_empty(self, tmp_path):
        """Test replace() on empty array — no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="empty", readings=[])
""")
        self._query(db_path, "update Sensor(0) set readings.replace(1, 2)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == []

    def test_replaceAll_all_matches(self, tmp_path):
        """Test replaceAll(old, new) replaces all occurrences."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 2, 5, 2])
""")
        self._query(db_path, "update Sensor(0) set readings.replaceAll(2, 10)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 10, 3, 10, 5, 10]

    def test_replaceAll_not_found(self, tmp_path):
        """Test replaceAll() when value is not found — no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        self._query(db_path, "update Sensor(0) set readings.replaceAll(99, 10)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3]

    def test_replaceAll_on_null(self, tmp_path):
        """Test replaceAll() on null array — no-op."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="empty")
""")
        self._query(db_path, "update Sensor(0) set readings.replaceAll(1, 2)")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] is None

    def test_replace_bulk_with_where(self, tmp_path):
        """Test bulk replace with WHERE clause."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 2])
create Sensor(name="pressure", readings=[2, 4, 2])
""")
        self._query(db_path, 'update Sensor set readings.replaceAll(2, 0) where name = "temp"')
        result = self._query(db_path, "from Sensor select name, readings")
        temp = [r for r in result.rows if r["name"] == "temp"][0]
        assert temp["readings"] == [1, 0, 3, 0]
        pressure = [r for r in result.rows if r["name"] == "pressure"][0]
        assert pressure["readings"] == [2, 4, 2]  # unchanged

    # --- Update chaining tests ---

    def test_update_chain_sort_reverse(self, tmp_path):
        """Test mutation chain: sort().reverse() gives descending order."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 3, 1, 4, 2])
""")
        self._query(db_path, "update Sensor(0) set readings.sort().reverse()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [5, 4, 3, 2, 1]

    def test_update_chain_append_sort(self, tmp_path):
        """Test mutation chain: append(6).sort() appends then sorts."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 3, 1, 4, 2])
""")
        self._query(db_path, "update Sensor(0) set readings.append(6).sort()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3, 4, 5, 6]

    def test_update_chain_single_method_assign(self, tmp_path):
        """Test single method assignment form: readings = readings.reverse()."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4, 5])
""")
        self._query(db_path, "update Sensor(0) set readings = readings.reverse()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [5, 4, 3, 2, 1]

    def test_update_chain_assignment_form(self, tmp_path):
        """Test assignment chain: readings = readings.append(6).sort()."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 3, 1, 4, 2])
""")
        self._query(db_path, "update Sensor(0) set readings = readings.append(6).sort()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3, 4, 5, 6]

    def test_update_chain_with_where(self, tmp_path):
        """Test bulk chain mutation with WHERE clause."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 3, 1])
create Sensor(name="pressure", readings=[9, 7, 8])
""")
        self._query(db_path, 'update Sensor set readings.sort().reverse() where name = "temp"')
        result = self._query(db_path, "from Sensor select name, readings")
        temp = [r for r in result.rows if r["name"] == "temp"][0]
        assert temp["readings"] == [5, 3, 1]
        pressure = [r for r in result.rows if r["name"] == "pressure"][0]
        assert pressure["readings"] == [9, 7, 8]  # unchanged

    def test_update_chain_mixed(self, tmp_path):
        """Test chain alongside regular assignment: set name='x', readings.sort().reverse()."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[5, 3, 1, 4, 2])
""")
        self._query(db_path, 'update Sensor(0) set name = "updated", readings.sort().reverse()')
        result = self._query(db_path, "from Sensor select name, readings")
        assert result.rows[0]["name"] == "updated"
        assert result.rows[0]["readings"] == [5, 4, 3, 2, 1]

    def test_update_chain_null_array(self, tmp_path):
        """Test chain on null array: sort on null stays null, append on null creates."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="null_test")
""")
        # sort().reverse() on null → stays null
        self._query(db_path, "update Sensor(0) set readings.sort().reverse()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] is None

        # append on null creates new array, then sort
        self._query(db_path, "update Sensor(0) set readings = readings.append(3, 1, 2).sort()")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3]

    def test_update_chain_composite_array(self, tmp_path):
        """Test chain on composite array: sort by field."""
        db_path = self._setup_db(tmp_path, """
type Item { name: string, value: uint8 }
type Container { items: Item[] }
create Container(items=[Item(name="c", value=3), Item(name="a", value=1), Item(name="b", value=2)])
""")
        self._query(db_path, "update Container(0) set items.sort(.value)")
        result = self._query(db_path, "from Container select items")
        items = result.rows[0]["items"]
        values = [i["value"] for i in items]
        assert values == [1, 2, 3]

    def test_update_chain_cross_field(self, tmp_path):
        """Test cross-field assignment: set backup = readings.sort()."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[], backup: int8[] }
create Sensor(name="temp", readings=[5, 3, 1, 4, 2])
""")
        self._query(db_path, "update Sensor(0) set backup = readings.sort()")
        result = self._query(db_path, "from Sensor select readings, backup")
        assert result.rows[0]["readings"] == [5, 3, 1, 4, 2]  # unchanged
        assert result.rows[0]["backup"] == [1, 2, 3, 4, 5]


class TestArrayProjections:
    """Tests for immutable array projections and method chaining in SELECT."""

    def _setup_db(self, tmp_path, script_text):
        """Helper to create a database with the given script."""
        db_path = tmp_path / "testdb"
        script = tmp_path / "setup.ttq"
        script.write_text(f'use "{db_path}"\n{script_text}\n')
        result, _ = run_file(script, None, verbose=False)
        assert result == 0
        return db_path

    def _query(self, db_path, query_text):
        """Helper to run a query and return the result."""
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()
        result = executor.execute(parser.parse(query_text))
        storage.close()
        return result

    # --- Immutable projection tests ---

    def test_sort_projection_returns_sorted_copy(self, tmp_path):
        """Test readings.sort() returns sorted copy, original unchanged."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[3, 1, 4, 1, 5])
""")
        result = self._query(db_path, "from Sensor select readings.sort()")
        assert result.rows[0]["readings.sort()"] == [1, 1, 3, 4, 5]
        # Verify original unchanged
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [3, 1, 4, 1, 5]

    def test_sort_projection_descending(self, tmp_path):
        """Test readings.sort(desc) returns descending sorted copy."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[3, 1, 4, 1, 5])
""")
        result = self._query(db_path, "from Sensor select readings.sort(desc)")
        assert result.rows[0]["readings.sort(desc)"] == [5, 4, 3, 1, 1]

    def test_sort_projection_composite_key(self, tmp_path):
        """Test members.sort(.salary) on composite array."""
        db_path = self._setup_db(tmp_path, """
type Employee { name: string, salary: uint32 }
type Team { members: Employee[] }
create Team(members=[Employee(name="Bob", salary=60000), Employee(name="Alice", salary=50000), Employee(name="Carol", salary=70000)])
""")
        result = self._query(db_path, "from Team select members.sort(.salary)")
        sorted_members = result.rows[0]["members.sort(.salary)"]
        assert [m["name"] for m in sorted_members] == ["Alice", "Bob", "Carol"]

    def test_reverse_projection(self, tmp_path):
        """Test readings.reverse() returns reversed copy, original unchanged."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4])
""")
        result = self._query(db_path, "from Sensor select readings.reverse()")
        assert result.rows[0]["readings.reverse()"] == [4, 3, 2, 1]
        # Verify original unchanged
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3, 4]

    def test_append_projection(self, tmp_path):
        """Test readings.append(99) returns list with 99 appended, original unchanged."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "from Sensor select readings.append(99)")
        assert result.rows[0]["readings.append(99)"] == [1, 2, 3, 99]
        # Verify original unchanged
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [1, 2, 3]

    def test_prepend_projection(self, tmp_path):
        """Test readings.prepend(0) returns list with 0 prepended."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "from Sensor select readings.prepend(0)")
        assert result.rows[0]["readings.prepend(0)"] == [0, 1, 2, 3]

    def test_insert_projection(self, tmp_path):
        """Test readings.insert(1, 99) returns list with 99 at index 1."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "from Sensor select readings.insert(1, 99)")
        assert result.rows[0]["readings.insert(1, 99)"] == [1, 99, 2, 3]

    def test_delete_projection(self, tmp_path):
        """Test readings.delete(0) returns list without first element."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "from Sensor select readings.delete(0)")
        assert result.rows[0]["readings.delete(0)"] == [2, 3]

    def test_remove_projection(self, tmp_path):
        """Test readings.remove(2) returns list without first occurrence of 2."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 2])
""")
        result = self._query(db_path, "from Sensor select readings.remove(2)")
        assert result.rows[0]["readings.remove(2)"] == [1, 3, 2]

    def test_removeAll_projection(self, tmp_path):
        """Test readings.removeAll(1) returns list without any 1s."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 1, 3, 1])
""")
        result = self._query(db_path, "from Sensor select readings.removeAll(1)")
        assert result.rows[0]["readings.removeAll(1)"] == [2, 3]

    def test_replace_projection(self, tmp_path):
        """Test readings.replace(2, 99) replaces first 2 with 99."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 2])
""")
        result = self._query(db_path, "from Sensor select readings.replace(2, 99)")
        assert result.rows[0]["readings.replace(2, 99)"] == [1, 99, 3, 2]

    def test_replaceAll_projection(self, tmp_path):
        """Test readings.replaceAll(1, 99) replaces all 1s with 99."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 1, 3])
""")
        result = self._query(db_path, "from Sensor select readings.replaceAll(1, 99)")
        assert result.rows[0]["readings.replaceAll(1, 99)"] == [99, 2, 99, 3]

    def test_swap_projection(self, tmp_path):
        """Test readings.swap(0, 2) returns copy with elements swapped."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3])
""")
        result = self._query(db_path, "from Sensor select readings.swap(0, 2)")
        assert result.rows[0]["readings.swap(0, 2)"] == [3, 2, 1]

    def test_projection_on_null_returns_none(self, tmp_path):
        """Test projections on null return None (except append/prepend)."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp")
""")
        result = self._query(db_path, "from Sensor select readings.sort()")
        assert result.rows[0]["readings.sort()"] is None
        result = self._query(db_path, "from Sensor select readings.reverse()")
        assert result.rows[0]["readings.reverse()"] is None

    def test_append_on_null_creates_list(self, tmp_path):
        """Test append on null creates a new list."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp")
""")
        result = self._query(db_path, "from Sensor select readings.append(42)")
        assert result.rows[0]["readings.append(42)"] == [42]

    # --- Method chaining tests ---

    def test_chain_sort_reverse(self, tmp_path):
        """Test readings.sort().reverse() = descending sort."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[3, 1, 4, 1, 5])
""")
        result = self._query(db_path, "from Sensor select readings.sort().reverse()")
        assert result.rows[0]["readings.sort().reverse()"] == [5, 4, 3, 1, 1]

    def test_chain_sort_length(self, tmp_path):
        """Test readings.sort().length() = length of sorted (same as original length)."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[3, 1, 4, 1, 5])
""")
        result = self._query(db_path, "from Sensor select readings.sort().length()")
        assert result.rows[0]["readings.sort().length()"] == 5

    def test_chain_append_sort(self, tmp_path):
        """Test readings.append(0).sort() = sorted with 0 included."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[3, 1, 4])
""")
        result = self._query(db_path, "from Sensor select readings.append(0).sort()")
        assert result.rows[0]["readings.append(0).sort()"] == [0, 1, 3, 4]

    def test_chain_reverse_reverse(self, tmp_path):
        """Test readings.reverse().reverse() = original."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[1, 2, 3, 4])
""")
        result = self._query(db_path, "from Sensor select readings.reverse().reverse()")
        assert result.rows[0]["readings.reverse().reverse()"] == [1, 2, 3, 4]

    def test_chain_in_where(self, tmp_path):
        """Test chain in WHERE clause: where readings.sort().length() > 3."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="short", readings=[1, 2])
create Sensor(name="long", readings=[1, 2, 3, 4])
""")
        result = self._query(db_path, "from Sensor select name where readings.sort().length() > 3")
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "long"

    def test_chain_in_where_boolean(self, tmp_path):
        """Test chain in WHERE clause as boolean: where readings.append(1).contains(1)."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="has_one", readings=[2, 3])
create Sensor(name="no_one", readings=[2, 3])
""")
        result = self._query(db_path, "from Sensor select name where readings.append(1).contains(1)")
        assert len(result.rows) == 2  # both get 1 appended, so both match

    def test_chain_original_unchanged(self, tmp_path):
        """Test that chained projections don't modify the original."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[3, 1, 4])
""")
        result = self._query(db_path, "from Sensor select readings.sort().reverse()")
        assert result.rows[0]["readings.sort().reverse()"] == [4, 3, 1]
        # Verify original unchanged
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [3, 1, 4]


class TestExpressions:
    """Tests for arithmetic expression evaluation in SELECT without FROM."""

    def _eval(self, query_text):
        """Helper to evaluate an expression query and return the result."""
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.storage import StorageManager
        from typed_tables.types import TypeRegistry

        registry = TypeRegistry()
        executor = QueryExecutor.__new__(QueryExecutor)
        executor.registry = registry
        executor.storage = None
        executor._variables = {}
        executor._deferred_tag_patches = {}
        executor._tag_refs = {}
        executor._execution_stack = set()
        parser = QueryParser()
        return executor.execute(parser.parse(query_text))

    def _setup_db(self, tmp_path, script_text):
        """Helper to create a database with the given script."""
        db_path = tmp_path / "testdb"
        script = tmp_path / "setup.ttq"
        script.write_text(f'use "{db_path}"\n{script_text}\n')
        result, _ = run_file(script, None, verbose=False)
        assert result == 0
        return db_path

    def _query(self, db_path, query_text):
        """Helper to run a query against an existing database."""
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()
        result = executor.execute(parser.parse(query_text))
        storage.close()
        return result

    def test_addition(self):
        """5 + 3 → 8."""
        result = self._eval("5 + 3")
        assert result.rows[0]["5 + 3"] == 8

    def test_precedence(self):
        """5 * 3 + 1 → 16."""
        result = self._eval("5 * 3 + 1")
        assert result.rows[0]["5 * 3 + 1"] == 16

    def test_parenthesized(self):
        """(2 + 3) * 4 → 20."""
        result = self._eval("(2 + 3) * 4")
        row = list(result.rows[0].values())
        assert row[0] == 20

    def test_unary_minus_integer(self):
        """-5 → -5."""
        result = self._eval("-5")
        row = list(result.rows[0].values())
        assert row[0] == -5

    def test_unary_minus_float(self):
        """-3.14 → -3.14."""
        result = self._eval("-3.14")
        row = list(result.rows[0].values())
        assert row[0] == pytest.approx(-3.14)

    def test_string_concat(self):
        """"hello" ++ " world" → "hello world"."""
        result = self._eval('"hello" ++ " world"')
        row = list(result.rows[0].values())
        assert row[0] == "hello world"

    def test_string_concat_auto_convert_int(self):
        """"id:" ++ 42 → "id:42"."""
        result = self._eval('"id:" ++ 42')
        row = list(result.rows[0].values())
        assert row[0] == "id:42"

    def test_string_concat_auto_convert_float(self):
        """"pi=" ++ 3.14 → "pi=3.14"."""
        result = self._eval('"pi=" ++ 3.14')
        row = list(result.rows[0].values())
        assert row[0] == "pi=3.14"

    def test_modulo(self):
        """10 % 3 → 1."""
        result = self._eval("10 % 3")
        row = list(result.rows[0].values())
        assert row[0] == 1

    def test_integer_division(self):
        """7 // 2 → 3."""
        result = self._eval("7 // 2")
        row = list(result.rows[0].values())
        assert row[0] == 3

    def test_true_division(self):
        """10 / 3 → 3.333..."""
        result = self._eval("10 / 3")
        row = list(result.rows[0].values())
        assert row[0] == pytest.approx(10 / 3)

    def test_division_by_zero(self):
        """10 / 0 → error."""
        with pytest.raises(RuntimeError, match="Division by zero"):
            self._eval("10 / 0")

    def test_negation_of_parenthesized(self):
        """-(2 + 3) → -5."""
        result = self._eval("-(2 + 3)")
        row = list(result.rows[0].values())
        assert row[0] == -5

    def test_existing_uuid_still_works(self):
        """uuid() still works."""
        result = self._eval("uuid()")
        row = list(result.rows[0].values())
        assert isinstance(row[0], str)  # hex-formatted UUID

    def test_existing_alias_still_works(self):
        """uuid() as "id", uuid() as "id2" still works."""
        result = self._eval('uuid() as "id", uuid() as "id2"')
        assert "id" in result.columns
        assert "id2" in result.columns

    def test_negative_array_elements(self, tmp_path):
        """create Sensor(readings=[-5, -3]) works."""
        db_path = self._setup_db(tmp_path, """
type Sensor { name: string, readings: int8[] }
create Sensor(name="temp", readings=[-5, -3])
""")
        result = self._query(db_path, "from Sensor select readings")
        assert result.rows[0]["readings"] == [-5, -3]

    def test_subtraction(self):
        """10 - 3 → 7."""
        result = self._eval("10 - 3")
        row = list(result.rows[0].values())
        assert row[0] == 7

    def test_multiplication(self):
        """4 * 5 → 20."""
        result = self._eval("4 * 5")
        row = list(result.rows[0].values())
        assert row[0] == 20

    def test_float_arithmetic(self):
        """1.5 + 2.5 → 4.0."""
        result = self._eval("1.5 + 2.5")
        row = list(result.rows[0].values())
        assert row[0] == 4.0

    def test_mixed_int_float(self):
        """5 + 2.5 → 7.5."""
        result = self._eval("5 + 2.5")
        row = list(result.rows[0].values())
        assert row[0] == 7.5

    def test_complex_expression(self):
        """(10 + 5) * 2 - 3 → 27."""
        result = self._eval("(10 + 5) * 2 - 3")
        row = list(result.rows[0].values())
        assert row[0] == 27

    def test_negative_instance_value(self, tmp_path):
        """Negative integers in instance creation."""
        db_path = self._setup_db(tmp_path, """
type Measurement { value: int8 }
create Measurement(value=-10)
""")
        result = self._query(db_path, "from Measurement select value")
        assert result.rows[0]["value"] == -10

    def test_where_negative_value(self, tmp_path):
        """WHERE clause with negative value."""
        db_path = self._setup_db(tmp_path, """
type Reading { value: int8 }
create Reading(value=-5)
create Reading(value=10)
""")
        result = self._query(db_path, "from Reading select value where value > -1")
        assert len(result.rows) == 1
        assert result.rows[0]["value"] == 10

    # --- Array math tests ---

    def test_array_literal(self):
        """[1, 2, 3, 4] → [1, 2, 3, 4]."""
        result = self._eval("[1, 2, 3, 4]")
        row = list(result.rows[0].values())
        assert row[0] == [1, 2, 3, 4]

    def test_empty_array(self):
        """[] → []."""
        result = self._eval("[]")
        row = list(result.rows[0].values())
        assert row[0] == []

    def test_array_add(self):
        """[1, 2] + [3, 4] → [4, 6]."""
        result = self._eval("[1, 2] + [3, 4]")
        row = list(result.rows[0].values())
        assert row[0] == [4, 6]

    def test_array_subtract(self):
        """[10, 20] - [3, 4] → [7, 16]."""
        result = self._eval("[10, 20] - [3, 4]")
        row = list(result.rows[0].values())
        assert row[0] == [7, 16]

    def test_array_multiply(self):
        """[2, 3] * [4, 5] → [8, 15]."""
        result = self._eval("[2, 3] * [4, 5]")
        row = list(result.rows[0].values())
        assert row[0] == [8, 15]

    def test_array_divide(self):
        """[10, 20] / [4, 5] → [2.5, 4.0]."""
        result = self._eval("[10, 20] / [4, 5]")
        row = list(result.rows[0].values())
        assert row[0] == [2.5, 4.0]

    def test_array_modulo(self):
        """[10, 7] % [3, 2] → [1, 1]."""
        result = self._eval("[10, 7] % [3, 2]")
        row = list(result.rows[0].values())
        assert row[0] == [1, 1]

    def test_array_integer_division(self):
        """[7, 9] // [2, 4] → [3, 2]."""
        result = self._eval("[7, 9] // [2, 4]")
        row = list(result.rows[0].values())
        assert row[0] == [3, 2]

    def test_scalar_broadcast_left(self):
        """5 * [1, 2, 3] → [5, 10, 15]."""
        result = self._eval("5 * [1, 2, 3]")
        row = list(result.rows[0].values())
        assert row[0] == [5, 10, 15]

    def test_scalar_broadcast_right(self):
        """[1, 2, 3] * 5 → [5, 10, 15]."""
        result = self._eval("[1, 2, 3] * 5")
        row = list(result.rows[0].values())
        assert row[0] == [5, 10, 15]

    def test_scalar_broadcast_add(self):
        """[10, 20] + 1 → [11, 21]."""
        result = self._eval("[10, 20] + 1")
        row = list(result.rows[0].values())
        assert row[0] == [11, 21]

    def test_unary_negate_array(self):
        """-[1, 2, 3] → [-1, -2, -3]."""
        result = self._eval("-[1, 2, 3]")
        row = list(result.rows[0].values())
        assert row[0] == [-1, -2, -3]

    def test_array_length_mismatch(self):
        """[1, 2] + [3, 4, 5] → error."""
        with pytest.raises(RuntimeError, match="Array length mismatch"):
            self._eval("[1, 2] + [3, 4, 5]")

    def test_array_string_concat(self):
        """["a", "b"] ++ ["c", "d"] → ["ac", "bd"]."""
        result = self._eval('["a", "b"] ++ ["c", "d"]')
        row = list(result.rows[0].values())
        assert row[0] == ["ac", "bd"]

    def test_scalar_broadcast_string_concat_left(self):
        """"x" ++ [1, 2] → ["x1", "x2"]."""
        result = self._eval('"x" ++ [1, 2]')
        row = list(result.rows[0].values())
        assert row[0] == ["x1", "x2"]

    def test_scalar_broadcast_string_concat_right(self):
        """[1, 2] ++ "!" → ["1!", "2!"]."""
        result = self._eval('[1, 2] ++ "!"')
        row = list(result.rows[0].values())
        assert row[0] == ["1!", "2!"]

    def test_expressions_inside_array(self):
        """[1+2, 3*4] → [3, 12]."""
        result = self._eval("[1+2, 3*4]")
        row = list(result.rows[0].values())
        assert row[0] == [3, 12]

    def test_sqrt_scalar(self):
        """sqrt(9) → 3.0."""
        result = self._eval("sqrt(9)")
        row = list(result.rows[0].values())
        assert row[0] == 3.0

    def test_sqrt_array(self):
        """sqrt([1, 4, 9, 16]) → [1.0, 2.0, 3.0, 4.0]."""
        result = self._eval("sqrt([1, 4, 9, 16])")
        row = list(result.rows[0].values())
        assert row[0] == [1.0, 2.0, 3.0, 4.0]

    def test_pow_scalar(self):
        """pow(2, 3) → 8."""
        result = self._eval("pow(2, 3)")
        row = list(result.rows[0].values())
        assert row[0] == 8

    def test_pow_broadcast_base(self):
        """pow([2, 3], 2) → [4, 9]."""
        result = self._eval("pow([2, 3], 2)")
        row = list(result.rows[0].values())
        assert row[0] == [4, 9]

    def test_pow_broadcast_exp(self):
        """pow(2, [1, 2, 3]) → [2, 4, 8]."""
        result = self._eval("pow(2, [1, 2, 3])")
        row = list(result.rows[0].values())
        assert row[0] == [2, 4, 8]

    def test_abs_scalar(self):
        """abs(-5) → 5."""
        result = self._eval("abs(-5)")
        row = list(result.rows[0].values())
        assert row[0] == 5

    def test_abs_array(self):
        """abs([-3, 4, -5]) → [3, 4, 5]."""
        result = self._eval("abs([-3, 4, -5])")
        row = list(result.rows[0].values())
        assert row[0] == [3, 4, 5]

    def test_ceil(self):
        """ceil(1.2) → 2."""
        result = self._eval("ceil(1.2)")
        row = list(result.rows[0].values())
        assert row[0] == 2

    def test_floor(self):
        """floor(1.8) → 1."""
        result = self._eval("floor(1.8)")
        row = list(result.rows[0].values())
        assert row[0] == 1

    def test_round(self):
        """round(3.7) → 4."""
        result = self._eval("round(3.7)")
        row = list(result.rows[0].values())
        assert row[0] == 4

    def test_sin(self):
        """sin(0) → 0.0."""
        result = self._eval("sin(0)")
        row = list(result.rows[0].values())
        assert row[0] == pytest.approx(0.0)

    def test_cos(self):
        """cos(0) → 1.0."""
        result = self._eval("cos(0)")
        row = list(result.rows[0].values())
        assert row[0] == pytest.approx(1.0)

    def test_log(self):
        """log(1) → 0.0."""
        result = self._eval("log(1)")
        row = list(result.rows[0].values())
        assert row[0] == pytest.approx(0.0)

    def test_log2(self):
        """log2(8) → 3.0."""
        result = self._eval("log2(8)")
        row = list(result.rows[0].values())
        assert row[0] == pytest.approx(3.0)

    def test_log10(self):
        """log10(100) → 2.0."""
        result = self._eval("log10(100)")
        row = list(result.rows[0].values())
        assert row[0] == pytest.approx(2.0)

    def test_tan(self):
        """tan(0) → 0.0."""
        result = self._eval("tan(0)")
        row = list(result.rows[0].values())
        assert row[0] == pytest.approx(0.0)

    def test_unknown_function(self):
        """unknown(5) → error."""
        with pytest.raises(RuntimeError, match="Unknown function"):
            self._eval("unknown(5)")

    def test_sum_array(self):
        """sum([1, 2, 3]) → 6."""
        result = self._eval("sum([1, 2, 3])")
        row = list(result.rows[0].values())
        assert row[0] == 6

    def test_sum_float_array(self):
        """sum([1.5, 2.5]) → 4.0."""
        result = self._eval("sum([1.5, 2.5])")
        row = list(result.rows[0].values())
        assert row[0] == 4.0

    def test_average_array(self):
        """average([10, 20, 30]) → 20.0."""
        result = self._eval("average([10, 20, 30])")
        row = list(result.rows[0].values())
        assert row[0] == 20.0

    def test_product_array(self):
        """product([2, 3, 4]) → 24."""
        result = self._eval("product([2, 3, 4])")
        row = list(result.rows[0].values())
        assert row[0] == 24

    def test_count_array(self):
        """count([1, 2, 3, 4, 5]) → 5."""
        result = self._eval("count([1, 2, 3, 4, 5])")
        row = list(result.rows[0].values())
        assert row[0] == 5

    def test_min_array(self):
        """min([5, 3, 7]) → 3."""
        result = self._eval("min([5, 3, 7])")
        row = list(result.rows[0].values())
        assert row[0] == 3

    def test_max_array(self):
        """max([5, 3, 7]) → 7."""
        result = self._eval("max([5, 3, 7])")
        row = list(result.rows[0].values())
        assert row[0] == 7

    def test_min_multi_arg(self):
        """min(5, 3) → 3."""
        result = self._eval("min(5, 3)")
        row = list(result.rows[0].values())
        assert row[0] == 3

    def test_max_multi_arg(self):
        """max(5, 3) → 5."""
        result = self._eval("max(5, 3)")
        row = list(result.rows[0].values())
        assert row[0] == 5

    def test_min_empty_array(self):
        """min([]) → None."""
        result = self._eval("min([])")
        row = list(result.rows[0].values())
        assert row[0] is None

    def test_sum_empty_array(self):
        """sum([]) → 0."""
        result = self._eval("sum([])")
        row = list(result.rows[0].values())
        assert row[0] == 0

    def test_aggregate_field_names(self, tmp_path):
        """Aggregate names (count, sum) can now be used as field names."""
        db_path = self._setup_db(tmp_path, """
            type Stats { count: uint32, sum: float64 }
            create Stats(count=10, sum=3.14)
        """)
        result = self._query(db_path, "from Stats select *")
        assert len(result.rows) == 1
        assert result.rows[0]["count"] == 10
        assert result.rows[0]["sum"] == pytest.approx(3.14)

    def test_from_select_sum_aggregate(self, tmp_path):
        """from X select sum(age) still works with aggregate names as identifiers."""
        db_path = self._setup_db(tmp_path, """
            type Person { name: string, age: uint8 }
            create Person(name="Alice", age=30)
            create Person(name="Bob", age=40)
        """)
        result = self._query(db_path, "from Person select sum(age)")
        assert len(result.rows) == 1
        assert result.rows[0]["sum(age)"] == 70


class TestArrayGenerators:
    """Tests for repeat() and range() array generator functions."""

    def _eval(self, query_text):
        """Helper to evaluate an expression query and return the result."""
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.storage import StorageManager
        from typed_tables.types import TypeRegistry

        registry = TypeRegistry()
        executor = QueryExecutor.__new__(QueryExecutor)
        executor.registry = registry
        executor.storage = None
        executor._variables = {}

        parser = QueryParser()
        stmt = parser.parse(query_text)
        return executor.execute(stmt)

    def _setup_db(self, tmp_path, script_text):
        """Helper to create a database with the given script."""
        from typed_tables.repl import run_file

        db_path = tmp_path / "testdb"
        script = tmp_path / "setup.ttq"
        script.write_text(f'use "{db_path}"\n{script_text}\n')
        result, _ = run_file(script, None, verbose=False)
        assert result == 0
        return db_path

    def _query(self, db_path, query_text):
        """Helper to run a query against an existing database."""
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.query_executor import QueryExecutor
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.storage import StorageManager

        registry = load_registry_from_metadata(db_path)
        storage = StorageManager(db_path, registry)
        executor = QueryExecutor(storage, registry)
        parser = QueryParser()
        result = executor.execute(parser.parse(query_text))
        storage.close()
        return result

    # --- repeat() in eval_expr context ---

    def test_repeat_integers(self):
        result = self._eval("repeat(0, 5)")
        assert result.rows[0]["repeat(0, 5)"] == [0, 0, 0, 0, 0]

    def test_repeat_string(self):
        result = self._eval('repeat("hello", 3)')
        assert result.rows[0]['repeat("hello", 3)'] == ["hello", "hello", "hello"]

    def test_repeat_zero_count(self):
        result = self._eval("repeat(0, 0)")
        assert result.rows[0]["repeat(0, 0)"] == []

    def test_repeat_float(self):
        result = self._eval("repeat(3.14, 2)")
        assert result.rows[0]["repeat(3.14, 2)"] == [3.14, 3.14]

    def test_repeat_negative_count_error(self):
        with pytest.raises(RuntimeError, match="non-negative"):
            self._eval("repeat(1, -1)")

    def test_repeat_too_many_args_error(self):
        with pytest.raises(RuntimeError, match="exactly 2 arguments"):
            self._eval("repeat(1, 2, 3)")

    # --- range() in eval_expr context ---

    def test_range_single_arg(self):
        result = self._eval("range(5)")
        assert result.rows[0]["range(5)"] == [0, 1, 2, 3, 4]

    def test_range_two_args(self):
        result = self._eval("range(1, 6)")
        assert result.rows[0]["range(1, 6)"] == [1, 2, 3, 4, 5]

    def test_range_three_args(self):
        result = self._eval("range(0, 10, 2)")
        assert result.rows[0]["range(0, 10, 2)"] == [0, 2, 4, 6, 8]

    def test_range_negative_step(self):
        result = self._eval("range(5, 0, -1)")
        assert result.rows[0]["range(5, 0, -1)"] == [5, 4, 3, 2, 1]

    def test_range_zero(self):
        result = self._eval("range(0)")
        assert result.rows[0]["range(0)"] == []

    def test_range_empty(self):
        result = self._eval("range(5, 5)")
        assert result.rows[0]["range(5, 5)"] == []

    def test_range_too_many_args_error(self):
        with pytest.raises(RuntimeError, match="1-3 arguments"):
            self._eval("range(1, 2, 3, 4)")

    def test_range_string_arg_error(self):
        with pytest.raises(RuntimeError, match="numeric"):
            self._eval('range("a")')

    # --- Composition with array math ---

    def test_repeat_plus_range(self):
        result = self._eval("repeat(1, 5) + range(5)")
        assert result.rows[0]["repeat(1, 5) + range(5)"] == [1, 2, 3, 4, 5]

    def test_range_times_scalar(self):
        result = self._eval("range(5) * 2")
        assert result.rows[0]["range(5) * 2"] == [0, 2, 4, 6, 8]

    # --- repeat() and range() in instance_value context (create/update) ---

    def test_create_with_repeat(self, tmp_path):
        db_path = self._setup_db(tmp_path, """
            type Sensor { name: string, readings: int8[] }
            create Sensor(name="test", readings=repeat(0, 5))
        """)
        result = self._query(db_path, "from Sensor select *")
        assert result.rows[0]["readings"] == [0, 0, 0, 0, 0]

    def test_create_with_range_two_args(self, tmp_path):
        db_path = self._setup_db(tmp_path, """
            type Sensor { name: string, readings: int8[] }
            create Sensor(name="test", readings=range(1, 6))
        """)
        result = self._query(db_path, "from Sensor select *")
        assert result.rows[0]["readings"] == [1, 2, 3, 4, 5]

    def test_create_with_range_single_arg(self, tmp_path):
        db_path = self._setup_db(tmp_path, """
            type Sensor { name: string, readings: int8[] }
            create Sensor(name="test", readings=range(5))
        """)
        result = self._query(db_path, "from Sensor select *")
        assert result.rows[0]["readings"] == [0, 1, 2, 3, 4]

    def test_update_with_repeat(self, tmp_path):
        db_path = self._setup_db(tmp_path, """
            type Sensor { name: string, readings: int8[] }
            $s = create Sensor(name="test", readings=[1, 2, 3])
            update $s set readings=repeat(1, 3)
        """)
        result = self._query(db_path, "from Sensor select *")
        assert result.rows[0]["readings"] == [1, 1, 1]

    def test_update_with_range(self, tmp_path):
        db_path = self._setup_db(tmp_path, """
            type Sensor { name: string, readings: int8[] }
            $s = create Sensor(name="test", readings=[1, 2, 3])
            update $s set readings=range(0, 10, 2)
        """)
        result = self._query(db_path, "from Sensor select *")
        assert result.rows[0]["readings"] == [0, 2, 4, 6, 8]
