"""Interactive REPL for TTQ (Typed Tables Query) language."""

from __future__ import annotations

import argparse
import gzip
import readline  # noqa: F401 - enables line editing in input()
import shutil
import sys
from pathlib import Path
from typing import Any

from typed_tables.dump import load_registry_from_metadata
from typed_tables.parsing.query_parser import DropDatabaseQuery, ExecuteQuery, ImportQuery, QueryParser, RestoreQuery, UseQuery
from typed_tables.query_executor import ArchiveResult, CollectResult, CompactResult, CreateResult, DeleteResult, DropResult, DumpResult, ExecuteResult, ImportResult, QueryExecutor, QueryResult, RestoreResult, ScopeResult, UpdateResult, UseResult, VariableAssignmentResult, execute_restore
from typed_tables.storage import StorageManager
from typed_tables.types import EnumValue, TypeRegistry


def _balance_counts(text: str) -> tuple[int, int]:
    """Return (paren_balance, brace_balance) for text, ignoring strings."""
    paren = 0
    brace = 0
    in_string = False
    escape_next = False
    for ch in text:
        if escape_next:
            escape_next = False
            continue
        if ch == '\\' and in_string:
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == '(':
            paren += 1
        elif ch == ')':
            paren -= 1
        elif ch == '{':
            brace += 1
        elif ch == '}':
            brace -= 1
    return paren, brace


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
    elif isinstance(value, EnumValue):
        if value.fields:
            field_strs = [f"{k}={format_value(v, max_items, max_width)}" for k, v in value.fields.items()]
            return f"{value.variant_name}({', '.join(field_strs)})"
        return value.variant_name
    elif isinstance(value, list):
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
                if result.output_file.endswith(".gz"):
                    with gzip.open(result.output_file, "wt", encoding="utf-8") as f:
                        f.write(result.script)
                else:
                    Path(result.output_file).write_text(result.script)
                print(f"Dumped to {result.output_file}")
            except Exception as e:
                print(f"Error writing to {result.output_file}: {e}")
        elif result.script:
            print(result.script)
        return

    # Special handling for UseResult, CreateResult, DeleteResult, DropResult, VariableAssignmentResult, CollectResult, ScopeResult - show message as success, not error
    if isinstance(result, (UseResult, CreateResult, DeleteResult, DropResult, VariableAssignmentResult, CollectResult, UpdateResult, ScopeResult, CompactResult, ArchiveResult, RestoreResult, ExecuteResult, ImportResult)):
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
    temp_databases: set[Path] = set()

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

    def needs_continuation(line: str) -> bool:
        """Check if we need more input for this query.

        A query is complete when:
        - It ends with `;` (explicit terminator, for backward compat)
        - It ends with `)` and parens are balanced (instance creation)
        - It ends with `}` and braces are balanced (type def or scope)
        - Parens and braces are balanced and it looks like a complete statement

        For multi-line input, an empty line also terminates.
        """
        stripped = line.strip()
        if not stripped:
            return False

        paren, brace = _balance_counts(stripped)

        # Explicit semicolon terminator — always complete if balanced
        if stripped.endswith(";"):
            return paren != 0 or brace != 0

        # Unbalanced parens or braces — definitely need more
        if paren != 0 or brace != 0:
            return True

        # Balanced and ends with ) — complete (instance creation, update, etc.)
        if stripped.endswith(")"):
            return False

        # Balanced and ends with } — complete (type def, scope block)
        if stripped.endswith("}"):
            return False

        # Simple statements that are complete without ; or ) or }
        lower = stripped.lower()
        simple_prefixes = (
            "show ", "describe ", "use ", "use", "drop", "drop!", "drop ",
            "drop! ", "dump", "delete ", "delete!", "delete! ", "from ",
            "select ", "forward ",
            "compact ", "archive ", "restore ", "execute ", "import ",
        )
        for prefix in simple_prefixes:
            if lower.startswith(prefix) or lower == prefix.strip():
                return False

        # Collect query: $var = collect ...
        if stripped.startswith("$") and "collect" in lower:
            return False

        # Default: need more input
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
            elif line.lower().rstrip(";").strip() == "status":
                if data_dir:
                    print(f"Database: {data_dir}")
                else:
                    print("No database selected.")
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
                needs_db = not (lower.startswith("use") or lower.startswith("drop") or lower.startswith("select ") or lower.startswith("restore") or lower.startswith("execute "))
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
                            if query.temporary:
                                temp_databases.add(new_path.resolve())
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
                    # Resolve target path
                    if query.path is None:
                        if data_dir is None:
                            print("No database selected. Nothing to drop.")
                            print()
                            continue
                        drop_path = data_dir
                    else:
                        drop_path = Path(query.path)

                    if not drop_path.exists():
                        print(f"Database does not exist: {drop_path}")
                    else:
                        # Confirm unless forced
                        if not query.force:
                            try:
                                answer = input(f"Drop database '{drop_path}'? [y/N] ").strip().lower()
                            except EOFError:
                                answer = ""
                            if answer not in ("y", "yes"):
                                print("Cancelled.")
                                print()
                                continue

                        if drop_path == data_dir:
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

                # Handle RESTORE query - doesn't need executor
                if isinstance(query, RestoreQuery):
                    result = execute_restore(query)
                    print_result(result)
                    print()
                    continue

                # Handle EXECUTE query specially in the REPL — route through
                # run_file() so that scripts containing use/drop work correctly.
                if isinstance(query, ExecuteQuery):
                    script_path = Path(query.file_path)
                    if not script_path.exists() and not script_path.suffix:
                        for ext in (".ttq", ".ttq.gz"):
                            candidate = Path(str(script_path) + ext)
                            if candidate.exists():
                                script_path = candidate
                                break
                    if not script_path.exists():
                        print(f"Error: File not found: {script_path}")
                        print()
                        continue

                    print(f"Executing {script_path}...")
                    exit_code, new_data_dir = run_file(script_path, data_dir, verbose=True)
                    if exit_code != 0:
                        print("Script execution failed with errors.")
                    else:
                        print("Script execution completed.")

                    # Adopt the script's final database state
                    if storage:
                        storage.close()
                        storage = None
                        registry = None
                        executor = None
                    data_dir = new_data_dir
                    if data_dir and data_dir.exists():
                        try:
                            registry, storage, executor, _ = load_database(data_dir)
                        except Exception as e:
                            print(f"Warning: Could not reload database: {e}")
                            storage = None
                            registry = None
                            executor = None

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
                            if result.temporary:
                                temp_databases.add(new_path.resolve())
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

        # Clean up temporary databases
        for temp_path in temp_databases:
            if temp_path.exists():
                try:
                    shutil.rmtree(temp_path)
                    print(f"Cleaning up temporary database: {temp_path}")
                except Exception as e:
                    print(f"Error cleaning up temporary database {temp_path}: {e}")

    return 0


def print_help() -> None:
    """Print help information."""
    print("""
TTQ - Typed Tables Query Language

DATABASE:
  status                   Show the currently active database
  use <path>               Switch to (or create) a database directory
  use <path> as temp       Switch to a temporary database (deleted on exit)
  use                      Exit current database (no database selected)
  drop                     Drop the current database (with confirmation)
  drop!                    Drop the current database (no confirmation)
  drop <path>              Drop a database directory (with confirmation)
  drop! <path>             Drop a database directory (no confirmation)
  show types               List all types
  describe <type>          Show type structure (use quotes for special names)

CREATE:
  create type <Name> { field: type, ... }
                           Create a new composite type
  create type <Name> from <Parent> { field: type, ... }
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
  delete <type> where ...  Delete matching records (soft delete)
  delete <type>            Delete all records of a type

UPDATE:
  update $var set field=value, ...
                           Update fields on a variable-bound record
  update <Type>(index) set field=value, ...
                           Update fields on a specific record by index

NULL VALUES:
  create <Type>(field=null) Set a field to null (no entry created)
  Missing fields default to null

QUERIES:
  from <type> select *               Select all records
  from "<type>" select *             Use quotes for special names (e.g., "character[]")
  from <type> select field1, field2  Select specific fields
  from <type> select field.nested    Select nested composite fields (dot notation)
  from <type> select * where <cond>  Filter records
  from <type> select * sort by f1    Sort results
  from <type> select * offset N limit M  Paginate results
  from <type> select * group by field    Group results
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
  string                   Built-in string type (stored as character[], displayed as string)
  Primitive types: bit, character, uint8, int8, uint16, int16,
                   uint32, int32, uint64, int64, uint128, int128,
                   float32, float64
  Array types: Add [] suffix (e.g., uint8[], character[])

EXAMPLES:
  use ./my_database

  create type Person { name: string, age: uint8 }

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
  dump <type>              Dump a single type as TTQ script
  dump to "file"           Dump entire database to a file
  dump <type> to "file"    Dump a single type to a file
  dump $var                Dump records referenced by a variable
  dump $var to "file"      Dump variable records to a file
  dump [Person, $var, ...]
                           Dump a list of types and/or variables
  dump [...] to "file"     Dump list to a file
                           Shared references are emitted as $var bindings
  dump pretty              Pretty-print with multi-line indented formatting
  dump pretty <type>       Pretty-print a single type
                           (pretty can be added to any dump variant above)
  dump yaml                Dump as YAML (uses anchors/aliases for references)
  dump yaml pretty         Pretty-print YAML output
                           (yaml can be combined with other dump options)
  dump json                Dump as JSON (uses $id/$ref for references)
  dump json pretty         Pretty-print JSON output
                           (json can be combined with other dump options)
  dump xml                 Dump as XML (uses id/ref="#id" for references)
  dump xml pretty          Pretty-print XML output
                           (xml can be combined with other dump options)
  dump to "file.ttq.gz"   Gzip-compress the output (.gz suffix on any format)
  dump archive             Dump including system types (full database state)
  dump archive yaml        Dump archive as YAML (combinable with other options)

DELETE:
  delete <type>            Delete all records of a type
  delete <type> where ...  Delete matching records
  delete! <type>           Force-delete (bypasses system type protection)
  delete! <type> where ... Force-delete matching records

ARCHIVE & RESTORE:
  archive to "file.ttar"   Compact and bundle database into a single file
                           (.ttar extension added automatically if missing)
  archive to "file.ttar.gz"
                           Gzip-compressed archive
  restore "file.ttar" to "path"
                           Extract archive into a new database directory
                           (does not require a loaded database)
  restore "file.ttar"     Restore to directory derived from filename
                           ("backup.ttar" → "backup", "backup.ttar.gz" → "backup")
  restore "file.ttar.gz" to "path"
                           Restore from a gzip-compressed archive

CYCLIC DATA:
  Tags allow creating cyclic data structures. Tags must be used within a
  scope block. A tag declares a name for the record being created, which
  can be referenced by nested records to form cycles.

  Scope block syntax:
    scope { <statements> }

  Self-referencing (node points to itself):
    scope { create Node(tag(SELF), value=42, next=SELF) }

  Two-node cycle (A→B→A):
    scope { create Node(tag(A), name="A", child=Node(name="B", child=A)) }

  Tags and variables declared inside a scope are destroyed when the scope
  exits. Tags cannot be redefined within a scope.

  The dump command is cycle-aware and automatically emits scope blocks with
  tag syntax when serializing cyclic data, ensuring roundtrip fidelity.

EXECUTE SCRIPT:
  execute "file.ttq"       Execute queries from a file
  execute "file.ttq.gz"    Execute from a gzip-compressed file
                           In the REPL: scripts may use/drop/restore
                           In nested scripts: use/drop/restore not allowed
                           Paths resolve relative to the calling script
                           Re-executing an already-loaded script is an error

IMPORT SCRIPT (execute once):
  import "file.ttq"        Execute a script once per database
  import "file.ttq"        Subsequent imports are silently skipped
  import "file.ttq.gz"     Gzip-compressed files supported
                           Import tracking is stored in the database

OTHER:
  help                     Show this help
  exit, quit               Exit the REPL
  clear                    Clear the screen

Queries can span multiple lines. Semicolons are optional.
End with closing ) or }, or press Enter on empty line.
""")


def run_file(file_path: Path, data_dir: Path | None, verbose: bool = False) -> tuple[int, Path | None]:
    """Execute queries from a file.

    Args:
        file_path: Path to the file containing queries
        data_dir: Optional initial data directory
        verbose: If True, print each query before executing

    Returns:
        (exit_code, final_data_dir) — 0 on success, 1 on error
    """
    from typed_tables.types import TypeRegistry

    # Read file content
    try:
        if file_path.suffix == ".gz":
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                content = f.read()
        else:
            content = file_path.read_text()
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        return 1, data_dir

    # Initialize state
    registry: TypeRegistry | None = None
    storage: StorageManager | None = None
    executor: QueryExecutor | None = None
    parser = QueryParser()

    # Track the script directory for relative path resolution in execute statements
    script_dir = file_path.resolve().parent

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
        # Set script context so execute statements resolve relative paths
        exec._script_stack.append(script_dir)
        exec._loaded_scripts.add(str(file_path.resolve()))
        return reg, stor, exec, is_new

    # Load initial database if provided
    if data_dir:
        try:
            registry, storage, executor, is_new = load_database(data_dir)
            if verbose and is_new:
                print(f"Created new database: {data_dir}")
        except Exception as e:
            print(f"Error loading database: {e}", file=sys.stderr)
            return 1, data_dir

    # Parse all queries at once using multi-statement parser
    try:
        queries = parser.parse_program(content)
    except SyntaxError as e:
        print(f"Syntax error: {e}", file=sys.stderr)
        if storage:
            storage.close()
        return 1, data_dir

    if not queries:
        # Empty file is okay — no error
        if storage:
            storage.close()
        return 0, data_dir

    # Execute each query
    for query in queries:
        if verbose:
            # Print a summary of the query type
            query_type = type(query).__name__
            print(f">>> [{query_type}]")

        try:
            # Check if we need a database for this query
            needs_db = not isinstance(query, (UseQuery, DropDatabaseQuery, RestoreQuery))
            # EvalQuery (SELECT without FROM) doesn't need a database
            from typed_tables.parsing.query_parser import EvalQuery
            if isinstance(query, EvalQuery):
                needs_db = False

            if needs_db and executor is None:
                print("Error: No database selected. Use 'use <path>' first.", file=sys.stderr)
                if storage:
                    storage.close()
                return 1, data_dir

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
                        return 1, data_dir
                continue

            # Handle RESTORE query specially - doesn't need executor
            if isinstance(query, RestoreQuery):
                result = execute_restore(query)
                print_result(result)
                continue

            # Handle DROP query specially
            if isinstance(query, DropDatabaseQuery):
                if query.path is None:
                    if data_dir is None:
                        print("No database selected. Nothing to drop.", file=sys.stderr)
                        continue
                    drop_path = data_dir
                else:
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
                        return 1, data_dir
                else:
                    try:
                        shutil.rmtree(drop_path)
                        if verbose:
                            print(f"Dropped database: {drop_path}")
                    except Exception as e:
                        print(f"Error dropping database: {e}", file=sys.stderr)
                        return 1, data_dir
                continue

            # Execute query
            result = executor.execute(query)  # type: ignore
            print_result(result)

        except SyntaxError as e:
            print(f"Syntax error: {e}", file=sys.stderr)
            if storage:
                storage.close()
            return 1, data_dir
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            if storage:
                storage.close()
            return 1, data_dir

    # Cleanup
    if storage:
        storage.close()

    return 0, data_dir


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
        exit_code, _ = run_file(args.file, args.data_dir, args.verbose)
        return exit_code

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
                drop_path = Path(result.path) if result.path else args.data_dir
                if not drop_path.exists():
                    print(f"Database does not exist: {drop_path}")
                else:
                    storage.close()
                    shutil.rmtree(drop_path)
                    print(f"Dropped database: {drop_path}")
                    return 0
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
