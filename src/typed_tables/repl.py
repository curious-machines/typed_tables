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
from typed_tables.query_executor import CollectResult, CreateResult, DeleteResult, DropResult, DumpResult, QueryExecutor, QueryResult, ScopeResult, UpdateResult, UseResult, VariableAssignmentResult
from typed_tables.storage import StorageManager
from typed_tables.types import TypeRegistry


def _split_statements(content: str) -> list[str]:
    """Split content into statements on semicolons, respecting brace nesting.

    Scope blocks contain semicolons inside braces, so we need to track
    brace depth and only split on semicolons at depth 0.

    Also handles string literals to avoid matching braces/semicolons inside them.
    """
    statements = []
    current = []
    brace_depth = 0
    in_string = False
    escape_next = False
    i = 0

    while i < len(content):
        ch = content[i]

        if escape_next:
            current.append(ch)
            escape_next = False
            i += 1
            continue

        if ch == '\\' and in_string:
            current.append(ch)
            escape_next = True
            i += 1
            continue

        if ch == '"':
            in_string = not in_string
            current.append(ch)
            i += 1
            continue

        if in_string:
            current.append(ch)
            i += 1
            continue

        # Not in string
        if ch == '{':
            brace_depth += 1
            current.append(ch)
        elif ch == '}':
            brace_depth = max(0, brace_depth - 1)
            current.append(ch)
        elif ch == ';' and brace_depth == 0:
            # End of statement at top level
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(ch)
        i += 1

    # Handle any remaining content
    stmt = ''.join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


def format_value(value: Any, max_items: int = 10, max_width: int = 40) -> str:
    """Format a value for display.

    Args:
        value: The value to format
        max_items: Maximum number of array items to show before eliding
        max_width: Maximum character width before truncating
    """
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
        if len(value) > max_width:
            return repr(value[:max_width - 3] + "...")
        return repr(value)
    elif isinstance(value, list):
        # Check if it's a character array (string-like)
        if all(isinstance(v, str) and len(v) == 1 for v in value):
            s = "".join(value)
            if len(s) > max_width:
                return repr(s[:max_width - 3] + "...")
            return repr(s)

        # Format each element
        formatted = []
        for i, v in enumerate(value):
            if i >= max_items:
                remaining = len(value) - max_items
                formatted.append(f"...+{remaining} more")
                break
            formatted.append(format_value(v, max_items, max_width))

        result = "[" + ", ".join(formatted) + "]"

        # Truncate if too long
        if len(result) > max_width:
            # Try to show as much as possible
            truncated = result[:max_width - 4] + "...]"
            return truncated

        return result
    else:
        s = str(value)
        if len(s) > max_width:
            return s[:max_width - 3] + "..."
        return s


def print_result(result: QueryResult, max_width: int = 80) -> None:
    """Print query results in a formatted table."""
    # Special handling for DumpResult - print script or write to file
    if isinstance(result, DumpResult):
        if result.output_file:
            try:
                Path(result.output_file).write_text(result.script)
                print(f"Dumped to {result.output_file}")
            except Exception as e:
                print(f"Error writing to {result.output_file}: {e}")
        elif result.script:
            print(result.script)
        return

    # Special handling for UseResult, CreateResult, DeleteResult, DropResult, VariableAssignmentResult, CollectResult, ScopeResult - show message as success, not error
    if isinstance(result, (UseResult, CreateResult, DeleteResult, DropResult, VariableAssignmentResult, CollectResult, UpdateResult, ScopeResult)):
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

    def has_balanced_parens(line: str) -> bool:
        """Check if parentheses are balanced in the line."""
        count = 0
        in_string = False
        escape = False
        for char in line:
            if escape:
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if char == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if char == "(":
                count += 1
            elif char == ")":
                count -= 1
        return count == 0

    def needs_continuation(line: str) -> bool:
        """Check if we need more input for this query.

        A query is complete when it ends with a semicolon.
        For create instance, parentheses must also be balanced before the semicolon.
        """
        stripped = line.strip()
        if not stripped:
            return False
        # If it ends with a semicolon, check paren balance for create instance, variable assignment, or update
        if stripped.endswith(";"):
            lower = stripped.lower()
            if lower.startswith("create ") and not lower.startswith("create type") and "(" in stripped:
                return not has_balanced_parens(stripped)
            if stripped.startswith("$") and "(" in stripped:
                return not has_balanced_parens(stripped)
            if lower.startswith("update ") and "(" in stripped:
                return not has_balanced_parens(stripped)
            return False
        return True

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
            elif line.lower().startswith("execute "):
                # Execute a script file
                script_path = Path(line[8:].strip().strip('"').strip("'"))
                if not script_path.exists():
                    print(f"Error: File not found: {script_path}")
                    print()
                    continue

                # Run the script file, passing current database state
                print(f"Executing {script_path}...")
                result = run_file(script_path, data_dir, verbose=True)
                if result != 0:
                    print(f"Script execution failed with errors.")
                else:
                    print(f"Script execution completed.")

                # Reload database state after script execution
                # (script may have changed database or created new types)
                if data_dir and data_dir.exists():
                    try:
                        if storage:
                            storage.close()
                        registry, storage, executor, _ = load_database(data_dir)
                    except Exception as e:
                        print(f"Warning: Could not reload database: {e}")
                        storage = None
                        registry = None
                        executor = None

                print()
                continue

            # Parse and execute query
            try:
                # Handle multi-line queries (continue until semicolon)
                if needs_continuation(line):
                    while True:
                        try:
                            continuation = input("...> ")
                            stripped = continuation.strip()
                            if not stripped:
                                # Empty line cancels continuation
                                break
                            line += " " + stripped
                            if not needs_continuation(line):
                                break
                        except EOFError:
                            break

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
    field=[1, 2, 3]          - Array literal
                             - Fields can span multiple lines (close paren to finish)
  create <Type>(tag(NAME), ...)
                           Declare a tag for cyclic references (see CYCLIC DATA)

DELETE:
  delete <table> where ... Delete matching records (soft delete)
  delete <table>           Delete all records in table

UPDATE:
  update $var set field=value, ...
                           Update fields on a variable-bound record
  update <Type>(index) set field=value, ...
                           Update fields on a specific record by index

NULL VALUES:
  create <Type>(field=null) Set a field to null (no entry created)
  Missing fields default to null

QUERIES:
  from <table> select *               Select all records
  from "<table>" select *             Use quotes for special names (e.g., "character[]")
  from <table> select field1, field2  Select specific fields
  from <table> select field.nested    Select nested composite fields (dot notation)
  from <table> select * where <cond>  Filter records
  from <table> select * sort by f1    Sort results
  from <table> select * offset N limit M  Paginate results
  from <table> select * group by field    Group results
  from $var select *                  Select from a variable (set or single ref)
  from $var select * where <cond>     Filter variable records further

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

  from Person select *
  from Person select name, age where age >= 18
  from Person select * where name starts with "A" sort by name
  from Person select age, count() group by age
  from Person select average(age)

VARIABLES:
  $var = create <Type>(...) Bind a created instance to an immutable variable
  create <Type>(field=$var) Use a variable as a field value
  create <Type>(arr=[$v1, $v2])
                           Use variables as array elements

COLLECT:
  $var = collect <Type>    Collect all record indices into a variable
  $var = collect <Type> where <cond>
                           Collect filtered record indices
  $var = collect <Type> sort by f limit N
                           Collect with sort/limit
  $var = collect <Type> where <cond> group by f sort by f offset N limit M
                           Full collect syntax (all clauses optional)
  $var = collect <Type> where ..., <Type> where ...
                           Multi-source collect (same type, union, dedup)
  $var = collect $other where <cond>
                           Collect from an existing variable
  $var = collect $a, $b    Combine multiple variables

DUMP:
  dump                     Dump entire database as executable TTQ script
  dump <table>             Dump a single table as TTQ script
  dump to "file"           Dump entire database to a file
  dump <table> to "file"   Dump a single table to a file
  dump $var                Dump records referenced by a variable
  dump $var to "file"      Dump variable records to a file
  dump [Person, $var, ...]
                           Dump a list of tables and/or variables
  dump [...] to "file"     Dump list to a file
                           Shared references are emitted as $var bindings
  dump pretty              Pretty-print with multi-line indented formatting
  dump pretty <table>      Pretty-print a single table
                           (pretty can be added to any dump variant above)
  dump yaml                Dump as YAML (uses anchors/aliases for references)
  dump yaml pretty         Pretty-print YAML output
                           (yaml can be combined with other dump options)

CYCLIC DATA:
  Tags allow creating cyclic data structures. Tags must be used within a
  scope block. A tag declares a name for the record being created, which
  can be referenced by nested records to form cycles.

  Scope block syntax:
    scope { <statements> };

  Self-referencing (node points to itself):
    scope { create Node(tag(SELF), value=42, next=SELF); };

  Two-node cycle (A→B→A):
    scope { create Node(tag(A), name="A", child=Node(name="B", child=A)); };

  Tags and variables declared inside a scope are destroyed when the scope
  exits. Tags cannot be redefined within a scope.

  The dump command is cycle-aware and automatically emits scope blocks with
  tag syntax when serializing cyclic data, ensuring roundtrip fidelity.

OTHER:
  help                     Show this help
  exit, quit               Exit the REPL
  clear                    Clear the screen
  execute <file>           Execute queries from a file

Queries can span multiple lines. End with semicolon or press Enter on empty line.
""")


def run_file(file_path: Path, data_dir: Path | None, verbose: bool = False) -> int:
    """Execute queries from a file.

    Args:
        file_path: Path to the file containing queries
        data_dir: Optional initial data directory
        verbose: If True, print each query before executing

    Returns:
        0 on success, 1 on error
    """
    from typed_tables.types import TypeRegistry

    # Read file content
    try:
        content = file_path.read_text()
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        return 1

    # Strip comments (lines starting with --)
    lines = []
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    content = "\n".join(lines)

    # Split on semicolons to get individual queries (brace-aware)
    queries = _split_statements(content)

    if not queries:
        print("No queries found in file", file=sys.stderr)
        return 1

    # Initialize state
    registry: TypeRegistry | None = None
    storage: StorageManager | None = None
    executor: QueryExecutor | None = None
    parser = QueryParser()

    def load_database(path: Path) -> tuple[TypeRegistry, StorageManager, QueryExecutor, bool]:
        """Load a database from the given path."""
        metadata_file = path / "_metadata.json"
        is_new = not path.exists() or not metadata_file.exists()
        if is_new:
            path.mkdir(parents=True, exist_ok=True)
            reg = TypeRegistry()
        else:
            reg = load_registry_from_metadata(path)
        stor = StorageManager(path, reg)
        exec = QueryExecutor(stor, reg)
        return reg, stor, exec, is_new

    # Load initial database if provided
    if data_dir:
        try:
            registry, storage, executor, is_new = load_database(data_dir)
            if verbose and is_new:
                print(f"Created new database: {data_dir}")
        except Exception as e:
            print(f"Error loading database: {e}", file=sys.stderr)
            return 1

    # Execute each query
    for query_text in queries:
        if verbose:
            # Print query with prefix
            for i, line in enumerate(query_text.split("\n")):
                prefix = ">>> " if i == 0 else "... "
                print(f"{prefix}{line}")

        try:
            # Check if we need a database for this query
            lower = query_text.lower().strip()
            needs_db = not (lower.startswith("use") or lower.startswith("drop") or lower.startswith("select "))

            if needs_db and executor is None:
                print("Error: No database selected. Use 'use <path>' first.", file=sys.stderr)
                if storage:
                    storage.close()
                return 1

            query = parser.parse(query_text)

            # Handle USE query specially
            if isinstance(query, UseQuery):
                if not query.path:
                    if storage:
                        storage.close()
                    storage = None
                    registry = None
                    executor = None
                    data_dir = None
                    if verbose:
                        print("Exited database.")
                else:
                    new_path = Path(query.path)
                    try:
                        if storage:
                            storage.close()
                        registry, storage, executor, is_new = load_database(new_path)
                        data_dir = new_path
                        if verbose:
                            if is_new:
                                print(f"Created new database: {new_path}")
                            else:
                                print(f"Switched to database: {new_path}")
                    except Exception as e:
                        print(f"Error loading database: {e}", file=sys.stderr)
                        return 1
                continue

            # Handle DROP query specially
            if isinstance(query, DropDatabaseQuery):
                drop_path = Path(query.path)
                if not drop_path.exists():
                    print(f"Database does not exist: {drop_path}")
                elif drop_path == data_dir:
                    if storage:
                        storage.close()
                    storage = None
                    registry = None
                    executor = None
                    data_dir = None
                    try:
                        shutil.rmtree(drop_path)
                        if verbose:
                            print(f"Dropped database: {drop_path}")
                    except Exception as e:
                        print(f"Error dropping database: {e}", file=sys.stderr)
                        return 1
                else:
                    try:
                        shutil.rmtree(drop_path)
                        if verbose:
                            print(f"Dropped database: {drop_path}")
                    except Exception as e:
                        print(f"Error dropping database: {e}", file=sys.stderr)
                        return 1
                continue

            # Execute query
            result = executor.execute(query)  # type: ignore
            print_result(result)

        except SyntaxError as e:
            print(f"Syntax error: {e}", file=sys.stderr)
            if storage:
                storage.close()
            return 1
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            if storage:
                storage.close()
            return 1

    # Cleanup
    if storage:
        storage.close()

    return 0


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
    arg_parser.add_argument(
        "-f", "--file",
        type=Path,
        help="Execute queries from a file and exit",
    )
    arg_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print each query before executing (for -f/--file)",
    )

    args = arg_parser.parse_args(argv)

    # Handle file execution
    if args.file:
        if not args.file.exists():
            print(f"Error: File not found: {args.file}", file=sys.stderr)
            return 1
        return run_file(args.file, args.data_dir, args.verbose)

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
