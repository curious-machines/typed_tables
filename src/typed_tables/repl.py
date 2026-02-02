"""Interactive REPL for TTQ (Typed Tables Query) language."""

from __future__ import annotations

import argparse
import readline  # noqa: F401 - enables line editing in input()
import sys
from pathlib import Path
from typing import Any

from typed_tables.dump import load_registry_from_metadata
from typed_tables.parsing.query_parser import QueryParser
from typed_tables.query_executor import QueryExecutor, QueryResult
from typed_tables.storage import StorageManager


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
    if result.message:
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


def run_repl(data_dir: Path) -> int:
    """Run the interactive REPL."""
    print(f"TTQ REPL - Typed Tables Query Language")
    print(f"Data directory: {data_dir}")
    print(f"Type 'help' for commands, 'exit' to quit.\n")

    try:
        registry = load_registry_from_metadata(data_dir)
        storage = StorageManager(data_dir, registry)
    except Exception as e:
        print(f"Error loading data: {e}", file=sys.stderr)
        return 1

    parser = QueryParser()
    executor = QueryExecutor(storage, registry)

    # Command history
    history_file = Path.home() / ".ttq_history"
    try:
        readline.read_history_file(history_file)
    except FileNotFoundError:
        pass

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
                # Handle multi-line queries (ended by semicolon or empty line)
                while not line.endswith(";") and not line.lower().startswith("show") and not line.lower().startswith("describe"):
                    try:
                        continuation = input("...> ").strip()
                        if not continuation:
                            break
                        line += " " + continuation
                    except EOFError:
                        break

                # Remove trailing semicolon
                if line.endswith(";"):
                    line = line[:-1]

                query = parser.parse(line)
                result = executor.execute(query)
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

        storage.close()

    return 0


def print_help() -> None:
    """Print help information."""
    print("""
TTQ - Typed Tables Query Language

COMMANDS:
  show tables              List all tables
  describe <table>         Show table structure

QUERIES:
  from <table>                        Select all records
  from <table> select *               Same as above
  from <table> select field1, field2  Select specific fields
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

EXAMPLES:
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
        help="Path to the data directory containing table files",
    )
    arg_parser.add_argument(
        "-c", "--command",
        type=str,
        help="Execute a single command and exit",
    )

    args = arg_parser.parse_args(argv)

    if not args.data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}", file=sys.stderr)
        return 1

    if args.command:
        # Execute single command
        try:
            registry = load_registry_from_metadata(args.data_dir)
            storage = StorageManager(args.data_dir, registry)
            parser = QueryParser()
            executor = QueryExecutor(storage, registry)

            query = parser.parse(args.command)
            result = executor.execute(query)
            print_result(result)

            storage.close()
            return 0
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    return run_repl(args.data_dir)


if __name__ == "__main__":
    sys.exit(main())
