"""Interactive REPL for TTQ (Typed Tables Query) language."""

from __future__ import annotations

import argparse
import gzip
import os
import readline  # noqa: F401 - enables line editing in input()
import shutil
import sys
from pathlib import Path
from typing import Any

from typed_tables.dump import load_registry_from_metadata
from typed_tables.parsing.query_parser import ArchiveQuery, DropDatabaseQuery, EvalQuery, ExecuteQuery, ImportQuery, QueryParser, RestoreQuery, UseQuery
from typed_tables.query_executor import ArchiveResult, CollectResult, CompactResult, CreateResult, DeleteResult, DropResult, DumpResult, ExecuteResult, ImportResult, QueryExecutor, QueryResult, RestoreResult, ScopeResult, UpdateResult, UseResult, VariableAssignmentResult, execute_restore
from typed_tables.storage import StorageManager
from fractions import Fraction

from typed_tables.types import (
    ArrayTypeDefinition,
    BigInt,
    BigUInt,
    CompositeTypeDefinition,
    EnumTypeDefinition,
    EnumValue,
    InterfaceTypeDefinition,
    SetValue,
    TypeRegistry,
)


def _balance_counts(text: str) -> tuple[int, int, int]:
    """Return (paren_balance, brace_balance, bracket_balance) for text, ignoring strings."""
    paren = 0
    brace = 0
    bracket = 0
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
        elif ch == '[':
            bracket += 1
        elif ch == ']':
            bracket -= 1
    return paren, brace, bracket


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
    elif isinstance(value, (BigInt, BigUInt)):
        return str(int(value))
    elif isinstance(value, Fraction):
        if value.denominator == 1:
            return str(value.numerator)
        return str(value)
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
    elif isinstance(value, dict):
        # Dictionary value
        formatted = []
        for i, (k, v) in enumerate(value.items()):
            if i >= max_items:
                remaining = len(value) - max_items
                formatted.append(f"...+{remaining} more")
                break
            formatted.append(f"{format_value(k, max_items, max_width)}: {format_value(v, max_items, max_width)}")
        result = "{" + ", ".join(formatted) + "}"
        if len(result) > max_width:
            return result[:max_width - 4] + "...}"
        return result
    elif isinstance(value, SetValue):
        # Set value
        formatted = []
        for i, v in enumerate(value):
            if i >= max_items:
                remaining = len(value) - max_items
                formatted.append(f"...+{remaining} more")
                break
            formatted.append(format_value(v, max_items, max_width))
        result = "{" + ", ".join(formatted) + "}"
        if len(result) > max_width:
            return result[:max_width - 4] + "...}"
        return result
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


def _format_size(n: int) -> str:
    """Format a byte count as a human-readable string."""
    if n < 1024:
        return f"{n} B"
    elif n < 1024 * 1024:
        v = n / 1024
        return f"{v:.1f} KB" if v != int(v) else f"{int(v)} KB"
    elif n < 1024 * 1024 * 1024:
        v = n / (1024 * 1024)
        return f"{v:.1f} MB" if v != int(v) else f"{int(v)} MB"
    else:
        v = n / (1024 * 1024 * 1024)
        return f"{v:.1f} GB" if v != int(v) else f"{int(v)} GB"


def _analyze_table_file(
    bin_path: Path, data_dir: Path, executor: QueryExecutor
) -> dict[str, Any] | None:
    """Analyze a single .bin file and return its metrics.

    Returns a dict with table status info, or None if the file can't be analyzed.
    """
    HEADER_SIZE = 8
    rel = bin_path.relative_to(data_dir)
    parts = rel.parts  # e.g., ("Person.bin",) or ("Shape", "circle.bin")
    file_size = os.path.getsize(bin_path)

    if len(parts) == 1:
        # Root-level .bin file: composite table or array element table
        table_name = rel.stem
        type_def = executor.registry.get(table_name)
        if type_def is None:
            return {
                "table": table_name,
                "kind": "Unknown",
                "records": "?",
                "live": "?",
                "deleted": "?",
                "file_size": _format_size(file_size),
                "live_size": "?",
                "dead_size": "?",
                "savings": "?",
                "_file_size_raw": file_size,
                "_live_size_raw": 0,
                "_dead_size_raw": 0,
                "_savings_raw": 0,
                "_records_raw": 0,
                "_live_raw": 0,
                "_deleted_raw": 0,
            }

        base = type_def.resolve_base_type()

        if isinstance(base, ArrayTypeDefinition):
            # Array element table — no deletion concept
            kind = "Array"
            try:
                array_table = executor.storage.get_array_table(table_name)
                table = array_table.element_table
            except Exception:
                return None
        elif isinstance(base, CompositeTypeDefinition):
            kind = "Composite"
            try:
                table = executor.storage.get_table(table_name)
            except Exception:
                return None
        elif isinstance(base, EnumTypeDefinition) and base.has_associated_values:
            # Shouldn't have root-level enum .bin, but handle gracefully
            return None
        elif isinstance(base, InterfaceTypeDefinition):
            return None
        else:
            return None

    elif len(parts) == 2:
        # Subdirectory: enum variant table (e.g., Shape/circle.bin)
        enum_name = parts[0]
        variant_name = Path(parts[1]).stem
        kind = "Variant"

        enum_def = executor.registry.get(enum_name)
        if enum_def is None:
            return None
        base = enum_def.resolve_base_type()
        if not isinstance(base, EnumTypeDefinition):
            return None

        try:
            table = executor.storage.get_variant_table(base, variant_name)
        except Exception:
            return None

        table_name = f"{enum_name}/{variant_name}"
    else:
        return None

    record_size = table._record_size
    total_records = table.count

    # Count live records
    if kind == "Array":
        # Array element tables don't support deletion
        live_records = total_records
        deleted_records = 0
    else:
        deleted_records = 0
        for i in range(total_records):
            if table.is_deleted(i):
                deleted_records += 1
        live_records = total_records - deleted_records

    live_size = HEADER_SIZE + live_records * record_size
    dead_size = deleted_records * record_size

    # Savings: how much smaller the file would be after compaction.
    # Compacted files start at INITIAL_SIZE (4096) and double, so the
    # compacted file size is the smallest power-of-2 >= INITIAL_SIZE
    # that fits header + live_records * record_size.
    INITIAL_SIZE = 4096
    needed = HEADER_SIZE + live_records * record_size
    compacted_size = INITIAL_SIZE
    while compacted_size < needed:
        compacted_size *= 2
    savings = max(0, file_size - compacted_size)

    return {
        "table": table_name,
        "kind": kind,
        "records": str(total_records),
        "live": str(live_records),
        "deleted": str(deleted_records),
        "file_size": _format_size(file_size),
        "live_size": _format_size(live_size),
        "dead_size": _format_size(dead_size),
        "savings": _format_size(savings),
        # Raw values for totals computation
        "_file_size_raw": file_size,
        "_live_size_raw": live_size,
        "_dead_size_raw": dead_size,
        "_savings_raw": savings,
        "_records_raw": total_records,
        "_live_raw": live_records,
        "_deleted_raw": deleted_records,
    }


def print_status(data_dir: Path | None, executor: QueryExecutor | None) -> None:
    """Print database status with disk usage and per-table breakdown."""
    if not data_dir:
        print("No database selected.")
        return

    print(f"Database: {data_dir}")

    if executor is None:
        return

    # Discover all .bin files
    bin_files = sorted(data_dir.rglob("*.bin"))
    if not bin_files:
        print("No tables.")
        return

    # Analyze each file
    rows: list[dict[str, Any]] = []
    for bin_path in bin_files:
        info = _analyze_table_file(bin_path, data_dir, executor)
        if info is not None:
            rows.append(info)

    if not rows:
        print("No tables.")
        return

    # Compute totals
    total_file_size = sum(r["_file_size_raw"] for r in rows)
    total_live_size = sum(r["_live_size_raw"] for r in rows)
    total_dead_size = sum(r["_dead_size_raw"] for r in rows)
    total_savings = sum(r["_savings_raw"] for r in rows)
    total_records = sum(r["_records_raw"] for r in rows)
    total_live = sum(r["_live_raw"] for r in rows)
    total_deleted = sum(r["_deleted_raw"] for r in rows)

    print(f"Total size: {_format_size(total_file_size)} ({len(rows)} table{'s' if len(rows) != 1 else ''})")
    print()

    # Build display rows (strip raw keys)
    columns = ["table", "kind", "records", "live", "deleted", "file_size", "live_size", "dead_size", "savings"]
    display_rows: list[dict[str, str]] = []
    for r in rows:
        display_rows.append({
            "table": r["table"],
            "kind": r["kind"],
            "records": str(r["_records_raw"]) if r["records"] != "?" else "?",
            "live": str(r["_live_raw"]) if r["live"] != "?" else "?",
            "deleted": str(r["_deleted_raw"]) if r["deleted"] != "?" else "?",
            "file_size": r["file_size"],
            "live_size": r["live_size"],
            "dead_size": r["dead_size"],
            "savings": r["savings"],
        })

    # Add totals row
    totals_row = {
        "table": "TOTAL",
        "kind": "",
        "records": str(total_records),
        "live": str(total_live),
        "deleted": str(total_deleted),
        "file_size": _format_size(total_file_size),
        "live_size": _format_size(total_live_size),
        "dead_size": _format_size(total_dead_size),
        "savings": _format_size(total_savings),
    }
    all_rows = display_rows + [totals_row]

    # Calculate column widths
    col_widths = {col: len(col) for col in columns}
    for row in all_rows:
        for col in columns:
            col_widths[col] = max(col_widths[col], len(row.get(col, "")))

    # Print header
    header = " | ".join(col.ljust(col_widths[col]) for col in columns)
    print(header)
    print("-" * len(header))

    # Print data rows
    for row in display_rows:
        vals = [row.get(col, "").ljust(col_widths[col]) for col in columns]
        print(" | ".join(vals))

    # Print separator and totals
    print("-" * len(header))
    vals = [totals_row.get(col, "").ljust(col_widths[col]) for col in columns]
    print(" | ".join(vals))


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

        paren, brace, bracket = _balance_counts(stripped)

        # Explicit semicolon terminator — always complete if balanced
        if stripped.endswith(";"):
            return paren != 0 or brace != 0 or bracket != 0

        # Unbalanced parens, braces, or brackets — definitely need more
        if paren != 0 or brace != 0 or bracket != 0:
            return True

        # Balanced and ends with ) — complete (instance creation, update, etc.)
        if stripped.endswith(")"):
            return False

        # Balanced and ends with } — complete (type def, scope block)
        if stripped.endswith("}"):
            return False

        # Balanced and ends with ] — complete (array literal)
        if stripped.endswith("]"):
            return False

        # Simple statements that are complete without ; or ) or } or ]
        lower = stripped.lower()
        simple_prefixes = (
            "show ", "describe ", "use ", "use", "drop", "drop!", "drop ",
            "drop! ", "dump", "delete ", "delete!", "delete! ", "from ",
            "forward ", "alias ",
            "compact ", "archive ", "restore ", "execute ", "import ",
        )
        for prefix in simple_prefixes:
            if lower.startswith(prefix) or lower == prefix.strip():
                return False

        # Collect query: $var = collect ...
        if stripped.startswith("$") and "collect" in lower:
            return False

        # Eval expressions: starts with number, string literal, or minus sign
        # These are complete single-line expressions like "5 + 3", "\"hello\"", "-42"
        if stripped[0].isdigit() or stripped[0] == '"' or stripped[0] == '-' or stripped[0] == '+':
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
            elif line.lower() == "help" or line.lower().startswith("help "):
                parts = line.strip().split(None, 1)
                topic = parts[1].strip().rstrip(";") if len(parts) > 1 else None
                print_help(topic)
                continue
            elif line.lower() == "clear":
                print("\033[2J\033[H", end="")
                continue
            elif line.lower().rstrip(";").strip() == "status":
                print_status(data_dir, executor)
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

                query = parser.parse(line)

                # Check if we need a database for this query
                needs_db = not isinstance(query, (UseQuery, DropDatabaseQuery, EvalQuery, RestoreQuery, ExecuteQuery))
                if needs_db and executor is None:
                    print("No database selected. Use 'use <path>' to select a database first.")
                    print()
                    continue

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
                                if query.temporary:
                                    temp_databases.add(new_path.resolve())
                                    print(f"Created new temporary database: {new_path}")
                                else:
                                    print(f"Created new database: {new_path}")
                            else:
                                print(f"Switched to database: {new_path}")
                                if query.temporary:
                                    print("Note: 'as temporary' ignored — existing databases are not deleted on exit. Use 'drop' to delete.")
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
                    # Auto-use the restored database
                    if result.output_path:
                        new_path = Path(result.output_path)
                        try:
                            if storage:
                                storage.close()
                            registry, storage, executor, _ = load_database(new_path)
                            data_dir = new_path
                            print(f"Switched to database: {new_path}")
                        except Exception as e:
                            print(f"Error loading restored database: {e}")
                    print()
                    continue

                # Handle EXECUTE query specially in the REPL — route through
                # run_file() so that scripts containing use/drop work correctly.
                if isinstance(query, ExecuteQuery):
                    script_path = Path(query.file_path)
                    if not script_path.is_file() and not script_path.suffix:
                        for ext in (".ttq", ".ttq.gz"):
                            candidate = Path(str(script_path) + ext)
                            if candidate.is_file():
                                script_path = candidate
                                break
                    if not script_path.is_file():
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

                # Handle ArchiveResult with existing file — prompt for overwrite
                if isinstance(result, ArchiveResult) and result.exists:
                    try:
                        answer = input(f"Overwrite {result.output_file}? [y/N] ").strip().lower()
                    except EOFError:
                        answer = ""
                    if answer in ("y", "yes"):
                        query.overwrite = True  # type: ignore[union-attr]
                        result = executor.execute(query)  # type: ignore
                    else:
                        print("Archive cancelled.")
                        print()
                        continue

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
                                if result.temporary:
                                    temp_databases.add(new_path.resolve())
                                    print(f"Created new temporary database: {new_path}")
                                else:
                                    print(f"Created new database: {new_path}")
                            else:
                                print(f"Switched to database: {new_path}")
                                if result.temporary:
                                    print("Note: 'as temporary' ignored — existing databases are not deleted on exit. Use 'drop' to delete.")
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


_HELP_TOPICS: dict[str, str] = {
    "database": """\
DATABASE:
  status                   Show database disk usage and table breakdown
  use <path>               Switch to (or create) a database directory
  use <path> as temporary  Switch to a temporary database (deleted on exit)
  use                      Exit current database (no database selected)
  drop                     Drop the current database (with confirmation)
  drop!                    Drop the current database (no confirmation)
  drop <path>              Drop a database directory (with confirmation)
  drop! <path>             Drop a database directory (no confirmation)""",

    "show": """\
SHOW & DESCRIBE:
  show types               List all user-defined types
  show composites          List composite types only
  show interfaces          List interface types only
  show enums               List enumeration types only
  show aliases             List alias types only
  show primitives          List built-in primitive types
  show system types        List internal system types (_-prefixed)
  describe <type>          Show type structure and fields
  describe <type>.<variant>
                           Show fields of an enum variant

  All show commands support: sort by <field>

  See also: graph (unified schema exploration command)""",

    "definitions": """\
DEFINITIONS:
  type <Name> { field: type, ... }
                           Define a new composite type
  type <Name> from <Parent> { field: type, ... }
                           Define a type inheriting from another type or interface
  alias <name> = <type>    Define a type alias
  enum <Name> { a, b, c }
                           Define a C-style enumeration
  enum <Name> { none, circle(r: float32), ... }
                           Define a Swift-style enum with associated values
  enum <Name> : uint8 { ... }
                           Define an enum with a backing type (enables arithmetic)
  interface <Name> { ... }
                           Define an interface (inherited by types via "from")
  interface <Name> from <Parent> { ... }
                           Inherit fields from another interface
  interface <Name> from <A>, <B> { ... }
                           Multiple interface parents (diamond merge allowed)
  forward <Name>           Forward-declare a type (for mutual references)

  Default values:
    type T { age: uint8 = 0, name: string = "unknown" }
    Fields without defaults default to NULL.
    Supported: primitives, strings, arrays, enums (dot notation).
    Not supported: function calls, inline instances, composite refs.

  Interface defaults are inherited by implementing types:
    interface Positioned { x: float32 = 0.0, y: float32 = 0.0 }
    type Point from Positioned { label: string }

  Overflow policy modifiers (integer fields only):
    type T { x: saturating uint8, y: wrapping int16 }""",

    "create": """\
CREATE:
  create <Type>(...)              Create an instance of a type
    field=value, ...              - Field values separated by commas
    field=uuid()                  - Use uuid() to generate a UUID
    field=OtherType(index)        - Reference an existing composite instance
    field=OtherType(...)          - Inline instance creation
    field=[1, 2, 3]               - Array literal
    field={"a", "b"}              - Set literal (unique elements)
    field={"key": val, ...}       - Dict literal (unique keys)
    field={,}                     - Empty set
    field={:}                     - Empty dict
    field=.variant(...)           - Enum value (shorthand dot notation)
    field=EnumType.variant        - Enum value (fully qualified)
                                  - Fields can span multiple lines (close paren to finish)

  create <Type>(tag(NAME), ...)
                                  Declare a tag for cyclic references (see: help cyclic)

  $var = create <Type>(...)       Bind a created instance to a variable

  Self-referential types:
    type Node { value: uint8, children: Node[] }
    create Node(value=0, children=[Node(value=1, children=[])])

  NULL values:
    Use field=null to set a field to null explicitly.
    Fields omitted during creation default to null.
    NULL values display as "NULL" in select results.""",

    "delete": """\
DELETE:
  delete <type>            Delete all records of a type (soft delete / tombstone)
  delete <type> where ...  Delete matching records
  delete! <type>           Force-delete (bypasses system type protection)
  delete! <type> where ... Force-delete matching records

  Deleted records become tombstones. Use "compact" to reclaim space.""",

    "update": """\
UPDATE:
  update $var set field=value, ...
                           Update fields on a variable-bound record
  update <Type>(index) set field=value, ...
                           Update fields on a specific record by index
  update <Type> set field=value where <cond>
                           Bulk update all matching records
  update <Type> set field=value
                           Bulk update all records of a type

  Enum values in SET and WHERE:
    update Pixel set color=.blue where color=.green
    update Pixel set color=Color.blue where color=Color.red

  Array/set/dict/string mutations in SET:
    update $s set readings.sort()
    update $s set readings = readings.append(5).sort()
    update $s set tags.add("new")
    update $s set tags.union({"a", "b"})
    update $s set scores.remove("midterm")
    update $s set name.uppercase()
    update $s set name = name.trim().uppercase()

  See also: help strings, help arrays, help sets, help dictionaries""",

    "queries": """\
QUERIES:
  from <Type> select ...              Select records from a type
  from $var select ...                Select from a collected variable

  Select clause:
    select *                          All fields
    select field1, field2             Specific fields
    select field.nested               Nested fields (dot notation)

  Modifiers (append to any query):
    ... where <cond>                  Filter (see: help conditions)
    ... sort by field [desc]          Sort results
    ... offset N limit M              Paginate results
    ... group by field                Group results

  Array/dict indexing in select:
    readings[0]                       First element
    readings[-1]                      Last element
    readings[0:5]                     Slice (start:end)
    readings[-3:]                     Last 3 elements
    scores["midterm"]                 Dict value by key

  Array projection (composite arrays):
    employees.name                    Map field across all elements
    employees[0].name                 Indexed then dot access

  Enum queries:
    from Shape select *               Overview (shows _variant column)
    from Shape.circle select *        Variant-specific (WHERE allowed)
    from Shape.circle select cx where r > 10

  Type-based query:
    from uint8 select *               Scan all composites with uint8 fields

  Use quotes for special type names: from "character[]" select *""",

    "conditions": """\
CONDITIONS (used in WHERE clauses):
  field = value            Equality
  field != value           Inequality
  field < value            Less than
  field <= value           Less than or equal
  field > value            Greater than
  field >= value           Greater than or equal
  field starts with "str"  String prefix match
  field matches /regex/    Regular expression match
  field is null            Check for null
  field is not null        Check for non-null
  cond1 and cond2          Logical AND
  cond1 or cond2           Logical OR
  not condition            Logical NOT

  Conditions work in: select ... where, delete ... where,
  update ... where, collect ... where""",

    "aggregates": """\
AGGREGATES:
  In FROM ... SELECT queries:
    count()                  Count records
    sum(field)               Sum of field values
    average(field)           Average of field values
    product(field)           Product of field values
    min(field)               Minimum field value
    max(field)               Maximum field value
    from Person select age, count() group by age

  In bare eval expressions (operating on arrays):
    sum([1, 2, 3])           6
    average([10, 20])        15.0
    min(5, 3)                3 (multi-argument form)
    max([5, 3, 7])           7

  Note: aggregate names are not reserved — they can also be used as field names.""",

    "expressions": """\
EXPRESSIONS (bare eval — no FROM needed):
  uuid()                   Generate a random UUID
  1, 2, 3                  Evaluate literal values
  uuid(), uuid()           Multiple expressions (comma-separated)
  uuid() named "id"        Name the result column
  5 + 3                    Arithmetic: +, -, *, /, %, //
  "hello" ++ " world"      String concatenation (++ operator)
  [1, 2, 3]                Array literals
  [1, 9, 5].sort()         Method calls on expressions
  [1,2,3].contains(2)      Array method calls
  sum([1, 2, 3])           Aggregate functions on arrays
  min(5, 3)                Multi-argument min/max
  sqrt(16), abs(-5)        Math functions
  boolean(1), string(42)   Type cast functions
  fraction(355, 113)       Exact rational number
  bigint(42), biguint(42)  Arbitrary-precision integers

  Array math (element-wise):
    [1,2] + [3,4]          [4, 6]
    5 * [1,2,3]            [5, 10, 15]

  Math functions (all vectorize over arrays):
    sqrt, pow, abs, ceil, floor, round,
    log, log2, log10, sin, cos, tan""",

    "math": """\
TYPED MATH:
  Type-annotated literals:
    5i8, 5i16, 5i32, 5i64          Signed integers
    5u8, 5u16, 5u32, 5u64, 5u128   Unsigned integers
    5.0f32, 5.0f64                 Floats
    0xFFu8, 0b1010i8               Hex/binary with suffix

  Type checking (both operands must match):
    5i8 + 3i8                      OK: same type
    5i8 + 3                        OK: bare literal adopts typed operand's type
    5i8 + 3i16                     Error: type mismatch

  Type conversion functions:
    int16(42)                      Convert scalar to int16
    int16([1,2,3])                 Element-wise: array conversion
    float64(age)                   Field value conversion
    int8(200)                      Error: 200 overflows int8
    bigint(42)                     Arbitrary-precision signed integer
    biguint(42)                    Arbitrary-precision unsigned integer
    fraction(355, 113)             Exact rational 355/113
    fraction(3)                    Exact rational 3/1
    boolean(1)                     true; boolean(0) → false
    string(42)                     "42" (convert any value to string)

  Division:
    7i8 / 2i8                      Floor division for typed integers
    Both / and // are floor division for integers.
    Use float64(x) / float64(y) for true division.

  Overflow policy on fields:
    x: saturating uint8            Clamp to 0..255
    y: wrapping int8               Modular arithmetic
    (default)                      Error on overflow

  Enum arithmetic (with backing type: enum Color : uint8 { ... }):
    Color.red + 1                  uint8 value 1 (result is integer, not enum)
    Color(0)                       Enum conversion by discriminant
    Color("red")                   Enum conversion by variant name

  Bit type (not numeric — use boolean functions):
    and(a, b), or(a, b)            Logical AND/OR
    not(a), xor(a, b)              Logical NOT, XOR
    uint8(flag)                    Cast bit to integer (0 or 1)
    bit(1)                         Cast integer to bit (only 0 or 1 accepted)""",

    "types": """\
TYPES:
  Built-in primitive types:
    bit                       1-bit value (use boolean functions, not arithmetic)
    character                 Single character
    uint8, int8               8-bit unsigned/signed integer
    uint16, int16             16-bit unsigned/signed integer
    uint32, int32             32-bit unsigned/signed integer
    uint64, int64             64-bit unsigned/signed integer
    uint128, int128           128-bit unsigned/signed integer
    float16                   16-bit floating point (half precision)
    float32, float64          32/64-bit floating point

  Special types:
    string                    Built-in (stored as character[], displayed as "Alice")
    boolean                   Built-in (stored as bit, displayed as true/false)

  Extended numeric types:
    bigint                    Arbitrary-precision signed integer
    biguint                   Arbitrary-precision unsigned integer
    fraction                  Exact rational number (e.g., fraction(355, 113) → 355/113)

  Collection types:
    int32[]  or  [int32]      Array (ordered, duplicates allowed)
    {int32}                   Set (ordered, unique elements)
    {string: int32}           Dictionary (ordered key-value pairs, unique keys)

  Aliases:
    alias uuid = uint128      Create a named type alias

  See also: help strings, help arrays, help sets, help dictionaries""",

    "arrays": """\
ARRAYS:
  Definition:
    type Sensor { readings: int32[] }
    type Sensor { readings: [int32] }     Prefix syntax (equivalent)

  Literals:
    [1, 2, 3]                             Array of integers
    []                                    Empty array

  Read-only methods (SELECT / WHERE / eval):
    .length()                  Number of elements
    .isEmpty()                 True if length is zero
    .contains(val)             True if val is in the array
    .min()                     Minimum numeric value
    .max()                     Maximum numeric value
    .min(.field)               Min by field (composite arrays)
    .max(.field)               Max by field (composite arrays)

  Projection methods (SELECT — return a copy, no storage writes):
    .sort()                    Sort elements (ascending)
    .sort(.field)              Sort by field (composite arrays)
    .sort(.field desc)         Sort descending
    .reverse()                 Reverse element order
    .append(val, ...)          Add to end (flatten array args)
    .prepend(val, ...)         Add to beginning
    .insert(idx, val, ...)     Insert at index
    .delete(idx)               Remove element at index
    .remove(val)               Remove first occurrence of val
    .removeAll(val)            Remove all occurrences of val
    .replace(old, new)         Replace first occurrence
    .replaceAll(old, new)      Replace all occurrences
    .swap(i, j)                Swap elements at indices i and j

  Mutation methods (UPDATE SET — modify in place):
    update $s set readings.reverse()
    update $s set readings.sort()
    update $s set readings.append(42)
    update $s set readings.prepend(0)
    update $s set readings.insert(2, 99)
    update $s set readings.delete(0)
    update $s set readings.remove(42)
    update $s set readings.removeAll(0)
    update $s set readings.replace(1, 2)
    update $s set readings.replaceAll(0, -1)
    update $s set readings.swap(0, 1)

  Chaining (SELECT or UPDATE):
    from Sensor select readings.sort().reverse()
    update $s set readings = readings.append(5).sort()
    update $s set backup = readings.sort()

  Indexing:
    readings[0]                First element
    readings[-1]               Last element
    readings[0:5]              Slice (start:end)
    readings[-3:]              Last 3 elements
    readings[:-1]              All but last

  Array math (element-wise):
    [1,2] + [3,4]             [4, 6]
    5 * [1,2,3]               [5, 10, 15]""",

    "sets": """\
SETS:
  Definition:
    type Student { tags: {string} }
    type Data { nums: {int32} }

  Literals:
    {"math", "science"}        Set of strings
    {1, 2, 3}                  Set of integers
    {,}                        Empty set (explicit)
    {}                         Empty set (inferred from field type)

  Duplicate elements are rejected on creation:
    create X(tags={1, 2, 1})   Error: Duplicate element

  Read-only methods (SELECT / WHERE / eval):
    .length()                  Number of elements
    .isEmpty()                 True if length is zero
    .contains(val)             True if val is in the set

  Set algebra methods (SELECT — return a new set):
    .add(val)                  Add element (no-op if already present)
    .union({...})              Elements in either set
    .intersect({...})          Elements in both sets
    .difference({...})         Elements in this but not other
    .symmetric_difference({...})
                               Elements in either but not both

  Array-compatible methods (also work on sets, preserving SetValue):
    .sort()                    Sort elements
    .reverse()                 Reverse order
    .remove(val)               Remove first occurrence
    .removeAll(val)            Remove all occurrences

  Mutation methods (UPDATE SET):
    update $x set tags.add("new")
    update $x set tags.union({"a", "b"})
    update $x set tags.intersect({"a"})
    update $x set tags.difference({"old"})
    update $x set tags.symmetric_difference({"a", "b"})

  Chaining (SELECT or UPDATE):
    from X select tags.add(5).sort()
    update $x set tags = tags.add(5).sort()

  Sets display as {elem1, elem2} in SELECT and dump output.""",

    "strings": """\
STRINGS:
  Strings are stored as character[] but displayed as joined text ("Alice").

  Operators (WHERE clauses):
    field starts with "prefix"   String prefix match
    field matches /regex/        Regular expression match

  Shared read-only methods (also work on arrays/sets/dicts):
    .length()                  Number of characters
    .isEmpty()                 True if length is zero
    .contains("substr")        True if substring is found

  String-only read methods (SELECT / WHERE / eval):
    .uppercase()               Convert to uppercase
    .lowercase()               Convert to lowercase
    .capitalize()              Capitalize first character
    .trim()                    Strip whitespace from both ends
    .trimStart()               Strip leading whitespace
    .trimEnd()                 Strip trailing whitespace
    .startsWith("prefix")      True if string starts with prefix
    .endsWith("suffix")        True if string ends with suffix
    .indexOf("substr")         Index of first occurrence (-1 if not found)
    .lastIndexOf("substr")     Index of last occurrence (-1 if not found)
    .padStart(len)             Pad start with spaces to reach length
    .padStart(len, "0")        Pad start with specified character
    .padEnd(len)               Pad end with spaces to reach length
    .padEnd(len, ".")          Pad end with specified character
    .repeat(n)                 Repeat the string n times
    .split(",")                Split into a string array by delimiter
    .match("pattern")          Regex search: returns [full, group1, ...] or null

  Array-compatible methods (also work on strings):
    .sort()                    Sort characters alphabetically
    .reverse()                 Reverse character order
    .append("!")               Append to end
    .prepend(">>")             Prepend to start
    .insert(5, "!")            Insert string at index
    .delete(0)                 Remove character at index
    .remove("world")           Remove first occurrence of substring
    .removeAll("ab")           Remove all occurrences of substring
    .replace("old", "new")     Replace first occurrence of substring
    .replaceAll("old", "new")  Replace all occurrences of substring
    .swap(0, 2)                Swap characters at two indices

  Mutation methods (UPDATE SET — modify in storage):
    update $x set name.uppercase()
    update $x set name.lowercase()
    update $x set name.capitalize()
    update $x set name.trim()
    update $x set name.trimStart()
    update $x set name.trimEnd()
    update $x set name.padStart(10, "0")
    update $x set name.padEnd(10)
    update $x set name.repeat(2)

  Chaining (SELECT or UPDATE):
    from Item select name.trim().uppercase()
    from Item select name.lowercase().replace("hello", "hi")
    update $x set name = name.trim().uppercase()

  WHERE clause examples:
    from Item select * where name.startsWith("Hello")
    from Item select * where name.indexOf("World") >= 0
    from Item select * where name.length() > 5

  Eval expressions:
    "hello world".uppercase()         "HELLO WORLD"
    "  hello  ".trim()                "hello"
    "a,b,c".split(",")               ["a", "b", "c"]
    "abc".repeat(3)                   "abcabcabc"

  Note: substring() is not needed — use array slicing:
    name[0:5], name[-3:], name[1:-1]""",

    "dictionaries": """\
DICTIONARIES:
  Definition:
    type Student { scores: {string: float64} }
    type Lookup { data: {int32: string} }

  Literals:
    {"math": 92.5, "eng": 88.0}  Dict with string keys
    {1: "one", 2: "two"}         Dict with integer keys
    {:}                           Empty dict (explicit)
    {}                            Empty dict (inferred from field type)

  Duplicate keys are rejected on creation:
    create X(scores={"a": 1.0, "a": 2.0})  Error: Duplicate key

  Bracket access (SELECT):
    scores["midterm"]            Value for key, or NULL if missing

  Read-only methods (SELECT / WHERE / eval):
    .length()                  Number of key-value pairs
    .isEmpty()                 True if length is zero
    .contains(key)             True if key exists (same as hasKey)
    .hasKey(key)               True if key exists

  Projection methods (SELECT — return a copy):
    .keys()                    Set of all keys (returns SetValue)
    .values()                  List of all values
    .entries()                 List of {key: k, value: v} dicts
    .remove(key)               New dict without the specified key

  Mutation methods (UPDATE SET):
    update $x set scores.remove("midterm")

  Chaining (SELECT):
    scores.keys().length()       Number of keys
    scores.remove("a").length()  Length after removing a key

  Dicts display as {key: val, ...} in SELECT and dump output.
  Internal storage uses entry composites (Dict_<key>_<val> types),
  which are hidden from show types and dump.""",

    "variables": """\
VARIABLES:
  $var = create <Type>(...) Bind a created instance to an immutable variable
  create <Type>(field=$var) Use a variable as a field value
  create <Type>(arr=[$v1, $v2])
                           Use variables as array elements
  update $var set f=value  Update fields on a variable-bound record
  from $var select *       Select from a variable
  dump $var                Dump records referenced by a variable

  Variables are immutable bindings — once assigned, they cannot be reassigned.
  Variables persist for the duration of the REPL session or script execution.""",

    "collect": """\
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

  Collected variables can be used with FROM, UPDATE, DUMP, and other commands.""",

    "dump": """\
DUMP:
  dump                        Dump entire database as executable TTQ script
  dump <type>                 Dump a single type
  dump $var                   Dump records referenced by a variable
  dump [Person, $var, ...]    Dump a list of types and/or variables
  dump > "file"               Dump to a file (any variant supports ">")
  dump > "file.ttq.gz"        Gzip-compressed output (.gz suffix on any format)

  Format modifiers (combinable with any variant above):
    dump pretty               Multi-line indented formatting
    dump yaml                 YAML format (anchors/aliases for references)
    dump yaml pretty          Pretty-print YAML
    dump json                 JSON format ($id/$ref for references)
    dump json pretty          Pretty-print JSON
    dump xml                  XML format (id/ref="#id" for references)
    dump xml pretty           Pretty-print XML

  System types:
    dump archive              Include system types (full database state)
    dump archive yaml         Combinable with format modifiers

  File extension behavior:
    dump > "file"               Auto-appends .ttq (or .yaml/.json/.xml)
    dump > "file.ttq.gz"        Gzip-compressed output (.gz on any format)

  Shared references are automatically emitted as $var bindings.
  The dump command is cycle-aware and emits scope/tag syntax for cycles.""",

    "graph": """\
GRAPH (schema exploration):
  Explore the type reference graph. Output as a table, DOT file, or TTQ script.
  Columns: kind, source, field, target. Arrow direction: referrer → referent.

  Basic:
    graph                            All type edges as table
    graph <type>                     Edges involving a specific type
    graph [<t1>, <t2>]               Edges involving multiple types
    graph all Interfaces             All interfaces expanded (focus by kind)
    graph all Composites             All composites expanded
    graph sort by source             Sort table output

  File output (extension determines format):
    graph > "file.dot"               Graphviz DOT format
    graph > "file.ttq"               TTQ script format
    graph > "file"                   No extension → assumes .ttq
    graph <type> > "file.dot"        Focus type to file
    In DOT: dashed = extends, dotted = implements, labeled = fields

  View modes (control which edges appear):
    graph <type> structure           Only extends/implements edges (no field→type)
    graph <type> declared            Only fields the type itself defines
    graph <type> stored              All fields on the record (inherited + own)
    graph <type> stored origin       Annotate each field's defining type

  Depth control (number of edges to traverse from focus):
    graph <type> depth 0             Focus node only (no edges)
    graph <type> depth 1             Direct edges only (fields, extends, etc.)
    graph <type> depth 2             Direct edges + 1 level of expansion
    graph <type> structure depth 2   Structure view, 2 levels deep
    graph <type> stored depth 1      Field edges only (aliases not expanded)
    graph <type> stored depth 2      Field edges + 1 level of alias resolution

  Filters (include or exclude by type, field, or kind):
    graph showing type string                  Paths leading to string
    graph showing field [name, age]            Paths to name/age field edges
    graph showing kind Interface               Paths leading to any interface
    graph showing kind Primitive               Paths leading to any primitive
    graph excluding type [uint8, uint16]       Hide specific types
    graph <type> showing type float32 excluding field speed

  Focus by kind (valid: Composite, Interface, Enum, Alias, Array, Set,
                 Dictionary, Primitive — singular or plural):
    graph all Interfaces             All interfaces expanded
    graph all Aliases                Alias→target forest
    graph all Enums                  All enums expanded
    graph all Primitives             All used primitives

  Path-to queries (find inheritance paths):
    graph <type> to <target>                   Path + target expansion
    graph <type> to [<t1>, <t2>]               Paths to multiple targets
    graph <type> to <target> depth 0           Linear path only (no expansion)
    graph <type> to <target> depth 1           Expand one level from target
    graph <type> to <target> > "p.dot"         Output path as DOT

  Metadata dict (DOT output — inline properties and style files):
    graph{"title": "My Schema"} > "f.dot"
    graph{"style": "custom.style"} > "f.dot"
    graph{"title": "Schema", "direction": "TB"} > "f.dot"
    graph{"style": "base.style", "direction": "TB"} > "f.dot"

  Style files use TTQ dictionary syntax:
    { "direction": "LR",            Graph direction (LR, TB, etc.)
      "composite.color": "#4A90D9", Node color for composites
      "interface.color": "#7B68EE", Node color for interfaces
      "focus.color": "#FFD700" }    Highlight color for focus type

  TTQ output includes: enum NodeRole { focus, context, endpoint, leaf },
  TypeNode with name/kind/role fields, and Edge with source/target/field_name.""",

    "archive": """\
ARCHIVE, RESTORE & COMPACT:
  archive                  Archive to <database_name>.ttar (prompts if exists)
  archive > "file.ttar"    Compact and bundle database into a specific file
                           (.ttar extension added automatically if missing)
  archive > "file.ttar.gz"
                           Gzip-compressed archive

  restore "file.ttar" to "path"
                           Extract archive into a new database directory
                           (does not require a loaded database)
  restore "file.ttar"      Restore to directory derived from filename
                           ("backup.ttar" -> "backup", "backup.ttar.gz" -> "backup")
  restore "file.ttar.gz" to "path"
                           Restore from a gzip-compressed archive

  compact > "path"         Create a compacted copy of the database
                           Removes tombstones and unreferenced data
                           Remaps all references to new indices

  restore auto-detects extensions: "backup" tries backup.ttar, backup.ttar.gz""",

    "cyclic": """\
CYCLIC DATA:
  Tags allow creating cyclic data structures. Tags must be used within a
  scope block. A tag declares a name for the record being created, which
  can be referenced by nested records to form cycles.

  Scope block syntax:
    scope { <statements> }

  Self-referencing (node points to itself):
    scope { create Node(tag(SELF), value=42, next=SELF) }

  Two-node cycle (A->B->A):
    scope { create Node(tag(A), name="A", child=Node(name="B", child=A)) }

  Four-node cycle (A->B->C->D->A):
    scope {
      create Node(tag(A), name="A",
        child=Node(name="B",
          child=Node(name="C",
            child=Node(name="D", child=A))))
    }

  Tags and variables declared inside a scope are destroyed when the scope
  exits. Tags cannot be redefined within a scope.

  Alternative: create with null + update:
    $n1 = create Node(value=1, next=null)
    $n2 = create Node(value=2, next=$n1)
    update $n1 set next=$n2

  The dump command is cycle-aware and automatically emits scope blocks with
  tag syntax when serializing cyclic data, ensuring roundtrip fidelity.""",

    "scripts": """\
EXECUTE & IMPORT:
  execute "file.ttq"       Execute queries from a file
  execute "file.ttq.gz"    Execute from a gzip-compressed file
                           In the REPL: scripts may use/drop/restore
                           In nested scripts: use/drop/restore not allowed
                           Paths resolve relative to the calling script
                           Re-executing an already-loaded script is an error

  import "file.ttq"        Execute a script once per database
  import "file.ttq"        Subsequent imports are silently skipped
  import "file.ttq.gz"     Gzip-compressed files supported
                           Import tracking is stored in the database
                           Dropping and recreating the database resets history

  Auto-extension: "setup" tries setup.ttq, setup.ttq.gz if not found""",
}

_HELP_ALIASES: dict[str, str] = {
    "select": "queries",
    "from": "queries",
    "where": "conditions",
    "enum": "definitions",
    "type": "definitions",
    "alias": "definitions",
    "interface": "definitions",
    "forward": "definitions",
    "scope": "cyclic",
    "tag": "cyclic",
    "execute": "scripts",
    "import": "scripts",
    "restore": "archive",
    "compact": "archive",
    "describe": "show",
    "null": "create",
    "use": "database",
    "drop": "database",
    "status": "database",
    "sort": "queries",
    "group": "queries",
    "limit": "queries",
    "offset": "queries",
    "showing": "graph",
    "excluding": "graph",
    "depth": "graph",
    "structure": "graph",
    "declared": "graph",
    "stored": "graph",
    "path": "graph",
    "yaml": "dump",
    "json": "dump",
    "xml": "dump",
    "pretty": "dump",
    "boolean": "types",
    "string": "strings",
    "bit": "types",
    "array": "arrays",
    "set": "sets",
    "dict": "dictionaries",
    "dictionary": "dictionaries",
    "dicts": "dictionaries",
    "overflow": "math",
    "saturating": "math",
    "wrapping": "math",
}


def print_help(topic: str | None = None) -> None:
    """Print help information, optionally for a specific topic."""
    if topic is None:
        print("""
TTQ - Typed Tables Query Language

  database      use, drop, status
  show          show types/enums/interfaces/..., describe
  definitions   type, alias, enum, interface, forward, defaults
  create        create instances, inline values, arrays, null
  delete        delete records, force delete
  update        update fields by variable, index, or bulk
  queries       from...select, sort, offset/limit, group by
  conditions    =, !=, <, >, starts with, matches, and/or/not
  aggregates    count, sum, average, product, min, max
  expressions   uuid(), literals, named, arithmetic, methods
  math          typed literals, type checking, overflow policies
  types         built-in primitives, string, boolean, collections
  strings       string methods: uppercase, trim, split, match, ...
  arrays        array methods: sort, append, remove, contains, ...
  sets          set methods: add, union, intersect, difference, ...
  dictionaries  dict methods: hasKey, keys, values, remove, ...
  variables     $var bindings, usage in create/update/select
  collect       collect records into variables
  dump          dump database (TTQ, YAML, JSON, XML)
  graph         schema exploration: view modes, filters, path-to
  archive       archive, restore, compact
  cyclic        scope blocks, tags for cyclic references
  scripts       execute, import

Type "help <topic>" for details. Example: help dump

Other commands: help, exit/quit, clear
Queries can span multiple lines. Semicolons are optional.
""")
        return

    key = topic.lower().strip()
    # Resolve aliases
    key = _HELP_ALIASES.get(key, key)

    if key in _HELP_TOPICS:
        print()
        print(_HELP_TOPICS[key])
        print()
    else:
        print(f'\nUnknown help topic "{topic}". Type "help" for available topics.\n')


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
                # Auto-use the restored database
                if result.output_path:
                    new_path = Path(result.output_path)
                    try:
                        if storage:
                            storage.close()
                        registry, storage, executor, _ = load_database(new_path)
                        data_dir = new_path
                        print(f"Switched to database: {new_path}")
                    except Exception as e:
                        print(f"Error loading restored database: {e}", file=sys.stderr)
                        return 1, data_dir
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
        file_path = args.file
        if not file_path.is_file() and not file_path.suffix:
            for ext in (".ttq", ".ttq.gz"):
                candidate = Path(str(file_path) + ext)
                if candidate.is_file():
                    file_path = candidate
                    break
        if not file_path.is_file():
            print(f"Error: File not found: {file_path}", file=sys.stderr)
            return 1
        exit_code, _ = run_file(file_path, args.data_dir, args.verbose)
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
