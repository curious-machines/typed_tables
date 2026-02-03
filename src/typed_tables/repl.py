"""Interactive REPL for TTQ (Typed Tables Query) language."""

from __future__ import annotations

import argparse
import readline  # noqa: F401 - enables line editing in input()
import shutil
import sys
from pathlib import Path
from typing import Any

from typed_tables.dump import load_registry_from_metadata
from typed_tables.parsing.query_parser import DropDatabaseQuery, QueryParser, UseQuery
from typed_tables.query_executor import CreateResult, DeleteResult, DropResult, QueryExecutor, QueryResult, UseResult
from typed_tables.storage import StorageManager
from typed_tables.types import TypeRegistry


def format_value(value: Any) -> str:
    """Format a value for display."""
    if value is None:
        return "NULL"
    elif isinstance(value, bool):
        return "true" if value else "false"
    elif isinstance(value, int):
        if value > 0xFFFFFFFF:
            return f"0x{value:x}"
        return str(value)
    elif isinstance(value, float):
        return f"{value:.6g}"
    elif isinstance(value, str):
        if len(value) > 40:
            return repr(value[:37] + "...")
        return repr(value)
    elif isinstance(value, list):
        if all(isinstance(v, str) and len(v) == 1 for v in value):
            s = "".join(value)
            if len(s) > 40:
                return repr(s[:37] + "...")
            return repr(s)
        if len(value) > 5:
            return f"[{len(value)} items]"
        return str(value)
    else:
        return str(value)


def print_result(result: QueryResult, max_width: int = 80) -> None:
    """Print query results in a formatted table."""
    # Special handling for UseResult, CreateResult, DeleteResult, DropResult - show message as success, not error
    if isinstance(result, (UseResult, CreateResult, DeleteResult, DropResult)):
        if result.message:
            print(result.message)
        if not result.rows:
            return
    elif result.message:
        print(f"Error: {result.message}")
        return

    if not result.rows:
        print("(no results)")
        return

    # Calculate column widths
    col_widths = {}
    for col in result.columns:
        col_widths[col] = len(col)

    for row in result.rows:
        for col in result.columns:
            val = format_value(row.get(col))
            col_widths[col] = max(col_widths[col], len(val))

    # Cap column widths
    max_col_width = 40
    for col in col_widths:
        col_widths[col] = min(col_widths[col], max_col_width)

    # Print header
    header = " | ".join(col.ljust(col_widths[col])[:col_widths[col]] for col in result.columns)
    print(header)
    print("-" * len(header))

    # Print rows
    for row in result.rows:
        values = []
        for col in result.columns:
            val = format_value(row.get(col))
            if len(val) > col_widths[col]:
                val = val[: col_widths[col] - 3] + "..."
            values.append(val.ljust(col_widths[col]))
        print(" | ".join(values))

    print(f"\n({len(result.rows)} row{'s' if len(result.rows) != 1 else ''})")


def run_repl(data_dir: Path | None) -> int:
    """Run the interactive REPL."""
    print(f"TTQ REPL - Typed Tables Query Language")
    if data_dir:
        print(f"Data directory: {data_dir}")
    else:
        print("No data directory loaded. Use 'use <path>' to select a database.")
    print(f"Type 'help' for commands, 'exit' to quit.\n")

    registry: TypeRegistry | None = None
    storage: StorageManager | None = None
    executor: QueryExecutor | None = None

    def load_database(path: Path) -> tuple[TypeRegistry, StorageManager, QueryExecutor, bool]:
        """Load a database from the given path. Returns (registry, storage, executor, is_new)."""
        is_new = not path.exists()
        if is_new:
            path.mkdir(parents=True, exist_ok=True)
            reg = TypeRegistry()
        else:
            reg = load_registry_from_metadata(path)
        stor = StorageManager(path, reg)
        exec = QueryExecutor(stor, reg)
        return reg, stor, exec, is_new

    if data_dir:
        try:
            registry, storage, executor, is_new = load_database(data_dir)
            if is_new:
                print(f"Created new database: {data_dir}")
        except Exception as e:
            print(f"Error loading data: {e}", file=sys.stderr)
            return 1

    parser = QueryParser()

    # Command history
    history_file = Path.home() / ".ttq_history"
    try:
        readline.read_history_file(history_file)
    except FileNotFoundError:
        pass

    def is_multiline_query(line: str) -> bool:
        """Check if this query needs multi-line input."""
        lower = line.lower()
        # Single-line queries that don't need continuation
        if lower.startswith("show") or lower.startswith("describe") or lower.startswith("use"):
            return False
        # create type needs multi-line for field definitions
        if lower.startswith("create type"):
            return True
        # Regular queries can span multiple lines
        return True

    def needs_continuation(line: str) -> bool:
        """Check if we need more input for this query."""
        lower = line.lower()
        # create type continues until empty line (field definitions on separate lines)
        if lower.startswith("create type"):
            return True
        # Other queries end with semicolon or empty line for continuation
        return not line.endswith(";")

    try:
        while True:
            try:
                line = input("ttq> ").strip()
            except EOFError:
                print()
                break

            if not line:
                continue

            # Handle special commands
            if line.lower() == "exit" or line.lower() == "quit":
                break
            elif line.lower() == "help":
                print_help()
                continue
            elif line.lower() == "clear":
                print("\033[2J\033[H", end="")
                continue

            # Parse and execute query
            try:
                # Handle multi-line queries
                if is_multiline_query(line) and needs_continuation(line):
                    is_create_type = line.lower().startswith("create type")
                    while True:
                        try:
                            continuation = input("...> ")
                            if is_create_type:
                                # For create type, preserve newlines (fields on separate lines)
                                if not continuation.strip():
                                    break
                                line += "\n" + continuation
                            else:
                                # For other queries, join with space
                                continuation = continuation.strip()
                                if not continuation:
                                    break
                                line += " " + continuation
                                if line.endswith(";"):
                                    break
                        except EOFError:
                            break

                # Remove trailing semicolon
                if line.endswith(";"):
                    line = line[:-1]

                # Check if we need a database for this query
                lower = line.lower().strip()
                needs_db = not (lower.startswith("use") or lower.startswith("drop") or lower.startswith("select "))
                if needs_db and executor is None:
                    print("No database selected. Use 'use <path>' to select a database first.")
                    print()
                    continue

                query = parser.parse(line)

                # Handle USE query specially - switch databases
                if isinstance(query, UseQuery):
                    if not query.path:
                        # Empty path - exit current database
                        if storage:
                            storage.close()
                        storage = None
                        registry = None
                        executor = None
                        data_dir = None
                        print("Exited database. No database selected.")
                    else:
                        new_path = Path(query.path)
                        try:
                            if storage:
                                storage.close()
                            registry, storage, executor, is_new = load_database(new_path)
                            data_dir = new_path
                            if is_new:
                                print(f"Created new database: {new_path}")
                            else:
                                print(f"Switched to database: {new_path}")
                        except Exception as e:
                            print(f"Error loading database: {e}")
                    print()
                    continue

                # Handle DROP query specially - doesn't need executor
                if isinstance(query, DropDatabaseQuery):
                    drop_path = Path(query.path)
                    if not drop_path.exists():
                        print(f"Database does not exist: {drop_path}")
                    elif drop_path == data_dir:
                        # Dropping current database - close it first
                        if storage:
                            storage.close()
                        storage = None
                        registry = None
                        executor = None
                        data_dir = None
                        try:
                            shutil.rmtree(drop_path)
                            print(f"Dropped database: {drop_path}")
                            print("No database selected.")
                        except Exception as e:
                            print(f"Error dropping database: {e}")
                    else:
                        try:
                            shutil.rmtree(drop_path)
                            print(f"Dropped database: {drop_path}")
                        except Exception as e:
                            print(f"Error dropping database: {e}")
                    print()
                    continue

                result = executor.execute(query)  # type: ignore

                # Handle UseResult - switch databases
                if isinstance(result, UseResult):
                    if not result.path:
                        # Empty path means exit current database
                        if storage:
                            storage.close()
                        storage = None
                        registry = None
                        executor = None
                        data_dir = None
                        print("Exited database. No database selected.")
                    else:
                        new_path = Path(result.path)
                        try:
                            if storage:
                                storage.close()
                            registry, storage, executor, is_new = load_database(new_path)
                            data_dir = new_path
                            if is_new:
                                print(f"Created new database: {new_path}")
                            else:
                                print(f"Switched to database: {new_path}")
                        except Exception as e:
                            print(f"Error loading database: {e}")
                # Handle DropResult - delete database
                elif isinstance(result, DropResult):
                    drop_path = Path(result.path)
                    if not drop_path.exists():
                        print(f"Database does not exist: {drop_path}")
                    elif drop_path == data_dir:
                        print("Cannot drop the currently active database. Use 'use' to switch first.")
                    else:
                        try:
                            shutil.rmtree(drop_path)
                            print(f"Dropped database: {drop_path}")
                        except Exception as e:
                            print(f"Error dropping database: {e}")
                else:
                    print_result(result)

            except SyntaxError as e:
                print(f"Syntax error: {e}")
            except Exception as e:
                print(f"Error: {e}")

            print()

    finally:
        # Save history
        try:
            readline.set_history_length(1000)
            readline.write_history_file(history_file)
        except Exception:
            pass

        if storage:
            storage.close()

    return 0


def print_help() -> None:
    """Print help information."""
    print("""
TTQ - Typed Tables Query Language

DATABASE:
  use <path>               Switch to (or create) a database directory
  use                      Exit current database (no database selected)
  drop <path>              Delete a database directory (can drop current db)
  show tables              List all tables
  describe <table>         Show table structure (use quotes for special names)

CREATE:
  create type <Name>       Create a new composite type (fields on following lines)
    field: type              - Each field on its own line
                             - End with empty line
  create type <Name> from <Parent>
                           Create a type inheriting from another type
  create alias <name> as <type>
                           Create a type alias
  create <Type>(...)       Create an instance of a type
    field=value, ...         - Field values separated by commas
    field=uuid()             - Use uuid() to generate a UUID
    field=OtherType(index)   - Reference an existing composite instance

DELETE:
  delete <table> where ... Delete matching records (soft delete)
  delete <table>           Delete all records in table

QUERIES:
  from <table>                        Select all records
  from "<table>"                      Use quotes for special names (e.g., "character[]")
  from <table> select *               Same as above
  from <table> select field1, field2  Select specific fields
  from <table> select field.nested    Select nested composite fields (dot notation)
  from <table> where <condition>      Filter records
  from <table> sort by field1, field2 Sort results
  from <table> offset N limit M       Paginate results
  from <table> group by field         Group results

CONDITIONS:
  field = value            Equality
  field != value           Inequality
  field < value            Less than
  field <= value           Less than or equal
  field > value            Greater than
  field >= value           Greater than or equal
  field starts with "str"  String prefix match
  field matches /regex/    Regular expression match
  cond1 and cond2          Logical AND
  cond1 or cond2           Logical OR
  not condition            Logical NOT

AGGREGATES:
  count()                  Count records
  sum(field)               Sum of field values
  average(field)           Average of field values
  product(field)           Product of field values

EXPRESSIONS (SELECT without FROM):
  select uuid()            Generate a random UUID
  select 1, 2, 3           Evaluate literal values
  select uuid(), uuid()    Multiple expressions
  select uuid() as "id"    Name the result column

TYPES:
  string                   Alias for character[]
  Primitive types: bit, character, uint8, int8, uint16, int16,
                   uint32, int32, uint64, int64, uint128, int128,
                   float32, float64
  Array types: Add [] suffix (e.g., uint8[], character[])

EXAMPLES:
  use ./my_database

  create type Person
  name: string
  age: uint8

  create Person(name="Alice", age=30)

  from Person
  from Person select name, age where age >= 18
  from Person where name starts with "A" sort by name
  from Person select age, count() group by age
  from Person select average(age)

OTHER:
  help                     Show this help
  exit, quit               Exit the REPL
  clear                    Clear the screen

Queries can span multiple lines. End with semicolon or press Enter twice.
""")


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    arg_parser = argparse.ArgumentParser(
        description="Interactive REPL for Typed Tables Query Language"
    )
    arg_parser.add_argument(
        "data_dir",
        type=Path,
        nargs="?",
        default=None,
        help="Path to the data directory containing table files (optional)",
    )
    arg_parser.add_argument(
        "-c", "--command",
        type=str,
        help="Execute a single command and exit",
    )

    args = arg_parser.parse_args(argv)

    if args.command:
        if not args.data_dir:
            print("Error: Data directory required when using -c/--command", file=sys.stderr)
            return 1
        if not args.data_dir.exists():
            print(f"Error: Data directory not found: {args.data_dir}", file=sys.stderr)
            return 1
        # Execute single command
        try:
            registry = load_registry_from_metadata(args.data_dir)
            storage = StorageManager(args.data_dir, registry)
            parser = QueryParser()
            executor = QueryExecutor(storage, registry)

            query = parser.parse(args.command)
            result = executor.execute(query)

            # Handle special results
            if isinstance(result, DropResult):
                drop_path = Path(result.path)
                if not drop_path.exists():
                    print(f"Database does not exist: {drop_path}")
                elif drop_path == args.data_dir:
                    print("Cannot drop the currently active database.")
                else:
                    shutil.rmtree(drop_path)
                    print(f"Dropped database: {drop_path}")
            elif isinstance(result, UseResult):
                print(f"Use 'ttq {result.path}' to switch databases")
            else:
                print_result(result)

            storage.close()
            return 0
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    if args.data_dir and not args.data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}", file=sys.stderr)
        return 1

    return run_repl(args.data_dir)


if __name__ == "__main__":
    sys.exit(main())
