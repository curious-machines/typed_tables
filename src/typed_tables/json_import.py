"""Import JSON files into Typed Tables using the JsonValue schema.

Reads a JSON file and emits a TTQ script that creates JsonDocument
instances using the JsonValue enum from json_schema.ttq.

Usage:
    ttq-json-import input.json                    # prints to stdout
    ttq-json-import input.json -o output.ttq      # writes to file
    ttq-json-import input.json -n "my_data"       # custom document name
    ttq-json-import a.json b.json -o output.ttq   # multiple files
"""

import argparse
import json
import sys
from pathlib import Path


def _escape_string(s: str) -> str:
    """Escape a string for TTQ double-quoted literals."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")


def _json_to_ttq(value, indent: int = 0) -> str:
    """Recursively convert a Python/JSON value to TTQ JsonValue syntax."""
    prefix = "    " * indent

    if value is None:
        return ".null_val"

    if isinstance(value, bool):
        return f".bool_val(value={'true' if value else 'false'})"

    if isinstance(value, (int, float)):
        # Ensure float representation for the float64 field
        v = float(value)
        # Format without unnecessary trailing zeros, but always have decimal
        if v == int(v) and not (v != v):  # not NaN
            formatted = f"{int(v)}.0"
        else:
            formatted = repr(v)
        return f".number(value={formatted})"

    if isinstance(value, str):
        return f'.str_val(value="{_escape_string(value)}")'

    if isinstance(value, list):
        if not value:
            return ".array(elements=[])"
        inner_indent = indent + 1
        inner_prefix = "    " * inner_indent
        elements = [f"\n{inner_prefix}{_json_to_ttq(item, inner_indent)}" for item in value]
        return f".array(elements=[{',' .join(elements)}\n{prefix}])"

    if isinstance(value, dict):
        if not value:
            return ".object(entries={:})"
        inner_indent = indent + 1
        inner_prefix = "    " * inner_indent
        entries = []
        for k, v in value.items():
            key_str = f'"{_escape_string(k)}"'
            val_str = _json_to_ttq(v, inner_indent)
            entries.append(f"\n{inner_prefix}{key_str}: {val_str}")
        return f".object(entries={{{','.join(entries)}\n{prefix}}})"

    raise TypeError(f"Unsupported JSON value type: {type(value)}")


def json_to_ttq_script(json_data, doc_name: str, schema_path: str | None = None) -> str:
    """Convert parsed JSON data to a complete TTQ script.

    Args:
        json_data: Parsed JSON (from json.load)
        doc_name: Name for the JsonDocument instance
        schema_path: Path to json_schema.ttq for the import statement.
                     If None, uses "json_schema.ttq".
    """
    lines = []
    lines.append(f'-- Imported from JSON: {doc_name}')
    lines.append(f'import "{schema_path or "json_schema.ttq"}"')
    lines.append("")

    root_ttq = _json_to_ttq(json_data, 1)
    lines.append(f'create JsonDocument(name="{_escape_string(doc_name)}", root={root_ttq})')
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Import JSON files into Typed Tables using the JsonValue schema"
    )
    parser.add_argument("files", nargs="+", help="JSON file(s) to import")
    parser.add_argument("-o", "--output", help="Output .ttq file (default: stdout)")
    parser.add_argument("-n", "--name", help="Document name (default: filename without extension)")
    parser.add_argument(
        "--schema-path",
        default=None,
        help='Path to json_schema.ttq for the import statement (default: "json_schema.ttq")',
    )

    args = parser.parse_args()

    parts = []
    for filepath in args.files:
        path = Path(filepath)
        if not path.exists():
            print(f"Error: {filepath} not found", file=sys.stderr)
            sys.exit(1)

        with open(path) as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Error: Invalid JSON in {filepath}: {e}", file=sys.stderr)
                sys.exit(1)

        doc_name = args.name if (args.name and len(args.files) == 1) else path.stem
        parts.append(json_to_ttq_script(data, doc_name, schema_path=args.schema_path))

    output = "\n".join(parts)

    if args.output:
        out_path = Path(args.output)
        with open(out_path, "w") as f:
            f.write(output)
        print(f"Wrote {out_path}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
