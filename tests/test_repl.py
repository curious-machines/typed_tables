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


class TestDump:
    """Tests for the dump command."""

    def test_dump_full_database(self, tmp_path: Path):
        """Test dumping entire database as TTQ script."""
        script = tmp_path / "setup.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path};
create alias uuid as uint128;
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

        # Now dump
        dump_script = tmp_path / "dump.ttq"
        dump_script.write_text(f"""
use {db_path};
dump;
""")

        result = run_file(dump_script, None, verbose=False)
        assert result == 0

    def test_dump_single_table(self, tmp_path: Path):
        """Test dumping a single table."""
        script = tmp_path / "setup.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path};
create type Address street:string city:string;
create type Person name:string address:Address;
create Address(street="123 Main", city="Springfield");
create Person(name="Alice", address=Address(street="456 Oak", city="Shelbyville"));
dump Person;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_dump_with_nested_composites(self, tmp_path: Path):
        """Test dump output with inline instances for composite fields."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use {db_path};
create type Address street:string city:string;
create type Person name:string address:Address;
create Person(name="Alice", address=Address(street="123 Main", city="Springfield"));
""")

        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Address street:string city:string;
create type Person name:string age:uint8 address:Address;
create Person(name="Alice", age=30, address=Address(street="123 Main", city="Springfield"));
create Person(name="Bob", age=25, address=Address(street="456 Oak", city="Shelbyville"));
""")

        result = run_file(script, None, verbose=False)
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
        roundtrip_script.write_text(f"use {db_path2};\n{dump_result.script}\n")

        result = run_file(roundtrip_script, None, verbose=False)
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
use {db_path};
create type Employee name:string;
create type Team title:string employees:Employee[];
create Team(title="Engineering", employees=[Employee(name="Alice"), Employee(name="Bob")]);
from Team select *;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0


    def test_dump_to_file(self, tmp_path: Path):
        """Test dumping entire database to a file."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
dump to "{tmp_path / 'dump_output.ttq'}";
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

        dump_file = tmp_path / "dump_output.ttq"
        assert dump_file.exists()
        content = dump_file.read_text()
        assert "create type Person" in content
        assert "create Person(" in content

    def test_dump_table_to_file(self, tmp_path: Path):
        """Test dumping a single table to a file."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use {db_path};
create type Address street:string city:string;
create type Person name:string address:Address;
create Person(name="Alice", address=Address(street="123 Main", city="Springfield"));
dump Person to "{tmp_path / 'person_dump.ttq'}";
""")

        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
dump to "{tmp_path / 'dump_output.ttq'}";
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

        dump_file = tmp_path / "dump_output.ttq"
        assert dump_file.exists()

        # Recreate from dump
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f"use {db_path2};\n{dump_file.read_text()}\n")

        result = run_file(roundtrip_script, None, verbose=False)
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
use {db_path};
create type Address street:string city:string;
create type Person name:string address:Address;
$addr = create Address(street="123 Main", city="Springfield");
create Person(name="Alice", address=$addr);
from Person select address.city;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_variable_binding_shared_reference(self, tmp_path: Path):
        """Test two Persons sharing the same $addr reference the same Address index."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use {db_path};
create type Address street:string city:string;
create type Person name:string address:Address;
$addr = create Address(street="123 Main", city="Springfield");
create Person(name="Alice", address=$addr);
create Person(name="Bob", address=$addr);
""")

        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Address street:string city:string;
$addr = create Address(street="123 Main", city="Springfield");
$addr = create Address(street="456 Oak", city="Shelbyville");
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

        captured = capsys.readouterr()
        assert "already bound" in captured.out

    def test_variable_undefined_error(self, tmp_path: Path):
        """Test that referencing an undefined variable returns an error."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type Address street:string city:string;
create type Person name:string address:Address;
create Person(name="Alice", address=$undefined);
""")

        result = run_file(script, None, verbose=False)
        assert result == 1

    def test_variable_in_array(self, tmp_path: Path):
        """Test using variables as array elements."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type Employee name:string;
create type Team title:string employees:Employee[];
$e1 = create Employee(name="Alice");
$e2 = create Employee(name="Bob");
create Team(title="Engineering", employees=[$e1, $e2]);
from Team select employees.name;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_dump_uses_variables_for_shared_refs(self, tmp_path: Path):
        """Test that dump output uses $var for shared composite references."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use {db_path};
create type Address street:string city:string;
create type Person name:string address:Address;
$addr = create Address(street="123 Main", city="Springfield");
create Person(name="Alice", address=$addr);
create Person(name="Bob", address=$addr);
""")

        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Address street:string city:string;
create type Person name:string address:Address;
$addr = create Address(street="123 Main", city="Springfield");
create Person(name="Alice", address=$addr);
create Person(name="Bob", address=$addr);
""")

        result = run_file(script, None, verbose=False)
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
        roundtrip_script.write_text(f"use {db_path2};\n{dump_result.script}\n")

        result = run_file(roundtrip_script, None, verbose=False)
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
use {db_path};
create type Node value:uint8 children:Node[];
describe Node;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_self_referential_type_with_data(self, tmp_path: Path):
        """Test creating Node instances with children arrays."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path};
create type Node value:uint8 children:Node[];
create Node(value=1, children=[]);
create Node(value=2, children=[]);
create Node(value=0, children=[Node(value=1, children=[]), Node(value=2, children=[])]);
from Node select *;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_self_referential_direct(self, tmp_path: Path):
        """Test creating a direct self-referential type: LinkedNode with next:LinkedNode."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path};
create type LinkedNode value:uint8 next:LinkedNode;
create LinkedNode(value=2, next=LinkedNode(value=1, next=LinkedNode(0)));
from LinkedNode select *;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_forward_declaration(self, tmp_path: Path):
        """Test forward declaration pattern for mutual references."""
        script = tmp_path / "test.ttq"
        db_path = tmp_path / "testdb"

        script.write_text(f"""
use {db_path};
create type B;
create type A value:uint8 b:B;
create type B value:uint8 a:A;
describe A;
describe B;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_mutual_reference_with_data(self, tmp_path: Path):
        """Test creating instances of mutually referencing types."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type B;
create type A value:uint8 b:B;
create type B value:uint8 a:A;
$a = create A(value=1, b=B(value=2, a=A(0)));
from A select *;
from B select *;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_dump_roundtrip_self_referential(self, tmp_path: Path):
        """Test dump and reload of self-referential types and data."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use {db_path};
create type Node value:uint8 children:Node[];
create Node(value=1, children=[]);
create Node(value=0, children=[Node(value=2, children=[]), Node(value=3, children=[])]);
""")

        result = run_file(script, None, verbose=False)
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
        roundtrip_script.write_text(f"use {db_path2};\n{dump_result.script}\n")

        result = run_file(roundtrip_script, None, verbose=False)
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
use {db_path};
create type B;
create type A value:uint8 b:B;
create type B value:uint8 a:A;
create A(value=1, b=B(value=2, a=A(0)));
""")

        result = run_file(script, None, verbose=False)
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
        assert "create type A;" in dump_result.script or "create type B;" in dump_result.script
        storage.close()

        # Execute dump into fresh db
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f"use {db_path2};\n{dump_result.script}\n")

        result = run_file(roundtrip_script, None, verbose=False)
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
use {db_path};
create type LinkedNode value:uint8 next:LinkedNode;
create LinkedNode(value=42, next=LinkedNode(0));
""")

        result = run_file(script, None, verbose=False)
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
        # Should use CompositeRef syntax to break the cycle
        assert "LinkedNode(0)" in dump_result.script
        storage.close()


class TestCollect:
    """Tests for the collect query and dump $var."""

    def test_collect_basic(self, tmp_path: Path):
        """Test collect with where clause, then dump."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
$seniors = collect Person where age >= 65;
dump $seniors;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_collect_all(self, tmp_path: Path):
        """Test collect all records."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type Item name:string;
create Item(name="A");
create Item(name="B");
create Item(name="C");
$all = collect Item;
dump $all;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_collect_with_sort_limit(self, tmp_path: Path):
        """Test collect with sort and limit."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type Score name:string value:uint8;
create Score(name="Alice", value=90);
create Score(name="Bob", value=80);
create Score(name="Carol", value=95);
$top2 = collect Score sort by value limit 2;
dump $top2;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_collect_immutability(self, tmp_path: Path, capsys):
        """Test that rebinding a collect variable fails."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type Item name:string;
create Item(name="A");
$items = collect Item;
$items = collect Item;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

        captured = capsys.readouterr()
        assert "already bound" in captured.out

    def test_collect_variable_cannot_be_used_as_field(self, tmp_path: Path):
        """Test that a set variable from collect cannot be used as a field value."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type Item name:string;
create type Wrapper item:Item;
create Item(name="A");
$items = collect Item;
create Wrapper(item=$items);
""")

        result = run_file(script, None, verbose=False)
        assert result == 1

    def test_dump_single_variable(self, tmp_path: Path):
        """Test dump a single-ref variable from create."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
$bob = create Person(name="Bob", age=25);
create Person(name="Carol", age=40);
dump $bob;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_dump_variable_to_file(self, tmp_path: Path):
        """Test dump $var to file."""
        db_path = tmp_path / "testdb"
        output_file = tmp_path / "output.ttq"

        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
$all = collect Person;
dump $all to "{output_file}";
""")

        result = run_file(script, None, verbose=False)
        assert result == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert "create Person(" in content

    def test_dump_variable_roundtrip(self, tmp_path: Path):
        """Test that dump $var output can recreate filtered records."""
        db_path = tmp_path / "testdb"

        script = tmp_path / "setup.ttq"
        script.write_text(f"""
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
$seniors = collect Person where age >= 65;
dump $seniors to "{tmp_path / 'seniors.ttq'}";
""")

        result = run_file(script, None, verbose=False)
        assert result == 0

        dump_file = tmp_path / "seniors.ttq"
        assert dump_file.exists()

        # Recreate from dump into fresh database
        db_path2 = tmp_path / "testdb2"
        roundtrip_script = tmp_path / "roundtrip.ttq"
        roundtrip_script.write_text(f"use {db_path2};\n{dump_file.read_text()}\n")

        result = run_file(roundtrip_script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
$nobody = collect Person where age >= 100;
dump $nobody;
""")

        result = run_file(script, None, verbose=False)
        assert result == 0


class TestFromVariable:
    """Tests for from $var select queries."""

    def test_from_variable_select(self, tmp_path: Path):
        """Test collect then select from variable."""
        db_path = tmp_path / "testdb"
        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
$seniors = collect Person where age >= 65;
from $seniors select name, age sort by age;
""")
        result = run_file(script, None, verbose=False)
        assert result == 0

    def test_from_variable_aggregate(self, tmp_path: Path):
        """Test from $var select average(age)."""
        db_path = tmp_path / "testdb"
        script = tmp_path / "test.ttq"
        script.write_text(f"""
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=60);
create Person(name="Carol", age=90);
$old = collect Person where age >= 60;
from $old select average(age);
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
$bob = create Person(name="Bob", age=25);
create Person(name="Carol", age=40);
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create type Item name:string;
create Person(name="Alice", age=30);
create Item(name="Widget");
$mixed = collect Person, Item;
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=25);
dump [Person];
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create type Item name:string;
create Person(name="Alice", age=30);
create Item(name="Widget");
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
create Person(name="Bob", age=65);
create Person(name="Carol", age=70);
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create Person(name="Alice", age=30);
dump [Person] to "{output_file}";
""")
        result = run_file(script, None, verbose=False)
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
use {db_path};
create type Person name:string age:uint8;
create type Item name:string;
create Person(name="Alice", age=30);
create Item(name="Widget");
""")
        result = run_file(script, None, verbose=False)
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
        roundtrip_script.write_text(f"use {db_path2};\n{dump_result.script}\n")

        result = run_file(roundtrip_script, None, verbose=False)
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
