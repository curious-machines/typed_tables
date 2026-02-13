"""TTQ Language Server — diagnostics, completion, hover via pygls."""

from __future__ import annotations

import re

from lsprotocol import types
from pygls.lsp.server import LanguageServer

from typed_tables.parsing.query_parser import QueryParser

# ---------------------------------------------------------------------------
# Static data
# ---------------------------------------------------------------------------

BUILTIN_TYPES: dict[str, str] = {
    "bit": "1-bit value (0 or 1)",
    "character": "Unicode character (4 bytes)",
    "uint8": "Unsigned 8-bit integer (1 byte, 0–255)",
    "int8": "Signed 8-bit integer (1 byte, -128–127)",
    "uint16": "Unsigned 16-bit integer (2 bytes)",
    "int16": "Signed 16-bit integer (2 bytes)",
    "uint32": "Unsigned 32-bit integer (4 bytes)",
    "int32": "Signed 32-bit integer (4 bytes)",
    "uint64": "Unsigned 64-bit integer (8 bytes)",
    "int64": "Signed 64-bit integer (8 bytes)",
    "uint128": "Unsigned 128-bit integer (16 bytes)",
    "int128": "Signed 128-bit integer (16 bytes)",
    "float16": "16-bit IEEE 754 float (2 bytes, half precision)",
    "float32": "32-bit IEEE 754 float (4 bytes)",
    "float64": "64-bit IEEE 754 float (8 bytes)",
    "string": "Variable-length UTF-8 string (stored as character[])",
    "boolean": "Boolean value (stored as bit, displayed as true/false)",
    "path": "File path (alias for string)",
    "bigint": "Arbitrary-precision signed integer",
    "biguint": "Arbitrary-precision unsigned integer",
    "fraction": "Exact rational number (e.g., fraction(355, 113))",
}

KEYWORDS: dict[str, str] = {
    "from": "Source clause — specifies the type or variable to query",
    "select": "Projection clause — chooses which fields to return",
    "where": "Filter clause — restricts rows by a condition",
    "offset": "Skip the first N results",
    "limit": "Return at most N results",
    "group": "Group results by one or more fields",
    "sort": "Order results by one or more fields",
    "by": "Used with 'group by' and 'sort by'",
    "and": "Logical AND in conditions",
    "or": "Logical OR in conditions",
    "not": "Logical negation in conditions",
    "starts": "String prefix test (used with 'with')",
    "with": "Used with 'starts with'",
    "matches": "Regex match operator",
    "show": "Display metadata (types, references, etc.)",
    "types": "List all defined types",
    "describe": "Show fields and layout of a type",
    "use": "Select or create a database",
    "create": "Create an instance of a type",
    "type": "Define a composite type",
    "delete": "Remove records matching a condition",
    "as": "Rename a column in select output",
    "alias": "Define a type alias",
    "drop": "Delete an entire database",
    "dump": "Serialize database contents",
    "to": "Destination clause (dump to, compact to, etc.)",
    "collect": "Gather record indices into a variable",
    "null": "Absence-of-value literal",
    "update": "Modify fields on existing records",
    "set": "Used with 'update … set'",
    "pretty": "Pretty-print modifier for dump",
    "tag": "Name a record for cyclic references inside a scope",
    "scope": "Block that enables tag-based cyclic references",
    "forward": "Forward-declare a type for mutual references",
    "enum": "Define an enumeration type",
    "interface": "Define an interface type",
    "interfaces": "List interface types (show interfaces)",
    "composites": "List composite types (show composites)",
    "enums": "List enumeration types (show enums)",
    "primitives": "List primitive types (show primitives)",
    "aliases": "List alias types (show aliases)",
    "graph": "Schema exploration: table, DOT, or TTQ output",
    "yaml": "YAML output format for dump",
    "json": "JSON output format for dump",
    "xml": "XML output format for dump",
    "compact": "Compact the database (remove tombstones)",
    "archive": "Bundle the database into a .ttar archive",
    "restore": "Extract a .ttar archive into a database",
    "execute": "Run queries from a script file",
    "import": "Import a script (execute once per database)",
    "system": "Modifier for system types (show system types)",
    "temporary": "Mark a database as temporary (use … as temporary)",
    "named": "Name a result column (expr named \"label\")",
    "saturating": "Overflow policy: clamp to min/max on overflow",
    "wrapping": "Overflow policy: modular arithmetic on overflow",
}

FUNCTIONS: dict[str, str] = {
    "uuid": "Generate a random UUID value",
    "count": "Count the number of rows",
    "average": "Compute the arithmetic mean",
    "sum": "Compute the sum",
    "product": "Compute the multiplicative product",
    "min": "Compute the minimum value",
    "max": "Compute the maximum value",
}

# Regex to extract position from QueryParser error messages
_POSITION_RE = re.compile(r"(?:at position|\(position) (\d+)")

# Regex to find user-defined type names in source
_USER_TYPE_RE = re.compile(
    r"(?:(?:type|enum|interface)\s+|"
    r"alias\s+)"
    r"(\w+)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Helpers (module-level so they are easy to unit-test)
# ---------------------------------------------------------------------------


def lexpos_to_position(source: str, lexpos: int) -> types.Position:
    """Convert a byte offset into an LSP ``Position(line, character)``."""
    line = source.count("\n", 0, lexpos)
    last_nl = source.rfind("\n", 0, lexpos)
    character = lexpos if last_nl == -1 else lexpos - last_nl - 1
    return types.Position(line=line, character=character)


def _extract_position_from_error(message: str) -> int | None:
    """Return the integer position embedded in a SyntaxError message, or None."""
    m = _POSITION_RE.search(message)
    return int(m.group(1)) if m else None


def _find_user_types(source: str) -> list[str]:
    """Return user-defined type names found in *source*."""
    return [m.group(1) for m in _USER_TYPE_RE.finditer(source)]


def _word_at_position(line_text: str, character: int) -> str:
    """Return the contiguous identifier-like word surrounding *character*."""
    if character < 0 or character >= len(line_text):
        return ""
    ch = line_text[character]
    if not (ch.isalnum() or ch == "_"):
        return ""
    # Scan left
    left = character
    while left > 0 and (line_text[left - 1].isalnum() or line_text[left - 1] == "_"):
        left -= 1
    # Scan right
    right = character
    while right < len(line_text) and (line_text[right].isalnum() or line_text[right] == "_"):
        right += 1
    return line_text[left:right]


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

server = LanguageServer("ttq-language-server", "0.2.0")
_parser = QueryParser()


@server.feature(types.TEXT_DOCUMENT_DID_OPEN)
def did_open(params: types.DidOpenTextDocumentParams) -> None:
    _validate_document(params.text_document.uri)


@server.feature(types.TEXT_DOCUMENT_DID_CHANGE)
def did_change(params: types.DidChangeTextDocumentParams) -> None:
    _validate_document(params.text_document.uri)


def _validate_document(uri: str) -> None:
    doc = server.workspace.get_text_document(uri)
    source = doc.source
    diagnostics: list[types.Diagnostic] = []
    try:
        _parser.parse_program(source)
    except SyntaxError as exc:
        msg = str(exc)
        pos_int = _extract_position_from_error(msg)
        if pos_int is not None:
            start = lexpos_to_position(source, pos_int)
        else:
            # Fallback: end of document
            lines = source.split("\n")
            start = types.Position(line=max(len(lines) - 1, 0), character=0)
        end = types.Position(line=start.line, character=start.character + 1)
        diagnostics.append(
            types.Diagnostic(
                range=types.Range(start=start, end=end),
                severity=types.DiagnosticSeverity.Error,
                source="ttq",
                message=msg,
            )
        )
    server.text_document_publish_diagnostics(
        types.PublishDiagnosticsParams(uri=uri, diagnostics=diagnostics)
    )


@server.feature(
    types.TEXT_DOCUMENT_COMPLETION,
    types.CompletionOptions(trigger_characters=[":", " "]),
)
def completions(params: types.CompletionParams) -> types.CompletionList:
    doc = server.workspace.get_text_document(params.text_document.uri)
    line_text = doc.lines[params.position.line] if params.position.line < len(doc.lines) else ""
    prefix = line_text[: params.position.character].rstrip()

    items: list[types.CompletionItem] = []

    if prefix.endswith(":"):
        # Field type context — offer built-in types + user-defined types
        for name, desc in BUILTIN_TYPES.items():
            items.append(
                types.CompletionItem(
                    label=name,
                    kind=types.CompletionItemKind.TypeParameter,
                    detail=desc,
                )
            )
        for name in _find_user_types(doc.source):
            items.append(
                types.CompletionItem(
                    label=name,
                    kind=types.CompletionItemKind.Class,
                    detail="User-defined type",
                )
            )

    return types.CompletionList(is_incomplete=False, items=items)


@server.feature(types.TEXT_DOCUMENT_HOVER)
def hover(params: types.HoverParams) -> types.Hover | None:
    doc = server.workspace.get_text_document(params.text_document.uri)
    if params.position.line >= len(doc.lines):
        return None
    line_text = doc.lines[params.position.line]
    word = _word_at_position(line_text, params.position.character)
    if not word:
        return None

    content: str | None = None
    lower = word.lower()
    if lower in BUILTIN_TYPES:
        content = f"**{lower}** — {BUILTIN_TYPES[lower]}"
    elif lower in KEYWORDS:
        content = f"**{lower}** — {KEYWORDS[lower]}"

    if content is None:
        return None
    return types.Hover(
        contents=types.MarkupContent(
            kind=types.MarkupKind.Markdown,
            value=content,
        )
    )


def main() -> None:
    server.start_io()


if __name__ == "__main__":
    main()
