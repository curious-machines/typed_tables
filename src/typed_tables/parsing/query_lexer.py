"""Lexer for the TTQ (Typed Tables Query) language."""

import re

import ply.lex as lex


class QueryLexer:
    """Lexer for tokenizing TTQ queries."""

    # Reserved keywords
    reserved = {
        "from": "FROM",
        "select": "SELECT",
        "where": "WHERE",
        "offset": "OFFSET",
        "limit": "LIMIT",
        "group": "GROUP",
        "sort": "SORT",
        "by": "BY",
        "and": "AND",
        "or": "OR",
        "not": "NOT",
        "starts": "STARTS",
        "with": "WITH",
        "matches": "MATCHES",
        "show": "SHOW",
        "types": "TYPES",
        "describe": "DESCRIBE",
        "use": "USE",
        "create": "CREATE",
        "type": "TYPE",
        "delete": "DELETE",
        "as": "AS",
        "alias": "ALIAS",
        "drop": "DROP",
        "dump": "DUMP",
        "to": "TO",
        "collect": "COLLECT",
        "null": "NULL",
        "update": "UPDATE",
        "set": "SET",
        "pretty": "PRETTY",
        "tag": "TAG",
        "scope": "SCOPE",
        "forward": "FORWARD",
        "enum": "ENUM",
        "interface": "INTERFACE",
        "interfaces": "INTERFACES",
        "composites": "COMPOSITES",
        "enums": "ENUMS",
        "primitives": "PRIMITIVES",
        "aliases": "ALIASES",
        "graph": "GRAPH",
        "yaml": "YAML",
        "json": "JSON",
        "xml": "XML",
        "compact": "COMPACT",
        "archive": "ARCHIVE",
        "restore": "RESTORE",
        "execute": "EXECUTE",
        "import": "IMPORT",
        "system": "SYSTEM",
        "temporary": "TEMPORARY",
        "named": "NAMED",
        "asc": "ASC",
        "desc": "DESC",
        "saturating": "SATURATING",
        "wrapping": "WRAPPING",
        "true": "TRUE",
        "false": "FALSE",
        "structure": "STRUCTURE",
        "declared": "DECLARED",
        "stored": "STORED",
        "depth": "DEPTH",
        "showing": "SHOWING",
        "excluding": "EXCLUDING",
    }

    # Token list
    tokens = [
        "VARIABLE",
        "IDENTIFIER",
        "TYPED_INTEGER",
        "TYPED_FLOAT",
        "INTEGER",
        "FLOAT",
        "STRING",
        "REGEX",
        "STAR",
        "COMMA",
        "COLON",
        "DOT",
        "LPAREN",
        "RPAREN",
        "LBRACKET",
        "RBRACKET",
        "LBRACE",
        "RBRACE",
        "EQ",
        "NEQ",
        "LT",
        "LTE",
        "GT",
        "GTE",
        "SEMICOLON",
        "BANG",
        "PLUS",
        "MINUS",
        "SLASH",
        "PERCENT",
        "DOUBLESLASH",
        "CONCAT",
    ] + list(reserved.values())

    # Lexer states: regex state for /pattern/ after MATCHES keyword
    states = (("regex", "exclusive"),)

    # Simple tokens (INITIAL state)
    t_STAR = r"\*"
    t_COMMA = r","
    t_COLON = r":"
    t_DOT = r"\."
    t_LPAREN = r"\("
    t_RPAREN = r"\)"
    t_LBRACKET = r"\["
    t_RBRACKET = r"\]"
    t_LBRACE = r"\{"
    t_RBRACE = r"\}"
    t_EQ = r"="
    t_NEQ = r"!="
    t_LTE = r"<="
    t_LT = r"<"
    t_GTE = r">="
    t_GT = r">"

    # Arithmetic tokens — PLY sorts string-defined tokens longest-first
    t_CONCAT = r"\+\+"
    t_PLUS = r"\+"
    t_MINUS = r"-"
    t_DOUBLESLASH = r"//"
    t_SLASH = r"/"
    t_PERCENT = r"%"

    # Ignored characters (including newlines - semicolons are the statement terminator)
    t_ignore = " \t"

    t_SEMICOLON = r";"
    t_BANG = r"!"

    def __init__(self) -> None:
        self.lexer: lex.LexToken = None  # type: ignore

    # Map of type suffixes to canonical primitive type names
    _TYPE_SUFFIXES = {
        "i8": "int8", "u8": "uint8",
        "i16": "int16", "u16": "uint16",
        "i32": "int32", "u32": "uint32",
        "i64": "int64", "u64": "uint64",
        "i128": "int128", "u128": "uint128",
        "f16": "float16", "f32": "float32", "f64": "float64",
    }

    def t_VARIABLE(self, t: lex.LexToken) -> lex.LexToken:
        r"\$[a-zA-Z_][a-zA-Z0-9_]*"
        t.value = t.value[1:]  # Strip the $ prefix, store just the name
        return t

    def t_TYPED_FLOAT(self, t: lex.LexToken) -> lex.LexToken:
        r"\d+\.\d+(?:f16|f32|f64)"
        # Split into value and suffix
        for suffix in ("f16", "f32", "f64"):
            if t.value.endswith(suffix):
                t.value = (float(t.value[:-len(suffix)]), self._TYPE_SUFFIXES[suffix])
                return t

    def t_TYPED_INTEGER(self, t: lex.LexToken) -> lex.LexToken:
        r"(?:0x[0-9a-fA-F]+|0b[01]+|\d+)(?:i8|u8|i16|u16|i32|u32|i64|u64|i128|u128|f16|f32|f64)\b"
        raw = t.value
        # Find which suffix matches
        for suffix in sorted(self._TYPE_SUFFIXES, key=len, reverse=True):
            if raw.endswith(suffix):
                num_str = raw[:-len(suffix)]
                type_name = self._TYPE_SUFFIXES[suffix]
                if type_name.startswith("float"):
                    t.type = "TYPED_FLOAT"
                    t.value = (float(int(num_str, 0)), type_name)
                else:
                    t.value = (int(num_str, 0), type_name)
                return t

    def t_FLOAT(self, t: lex.LexToken) -> lex.LexToken:
        r"\d+\.\d+"
        t.value = float(t.value)
        return t

    def t_INTEGER(self, t: lex.LexToken) -> lex.LexToken:
        r"\d+"
        t.value = int(t.value)
        return t

    def t_STRING(self, t: lex.LexToken) -> lex.LexToken:
        r'"([^"\\]|\\.)*"'
        # Remove quotes and handle escapes
        t.value = t.value[1:-1].encode().decode("unicode_escape")
        return t

    def t_BACKTICK_IDENTIFIER(self, t: lex.LexToken) -> lex.LexToken:
        r"`[^`]+`"
        # Strip backticks — always produces IDENTIFIER, bypassing keyword lookup
        t.value = t.value[1:-1]
        t.type = "IDENTIFIER"
        return t

    def t_IDENTIFIER(self, t: lex.LexToken) -> lex.LexToken:
        r"[a-zA-Z_][a-zA-Z0-9_]*"
        # Check if it's a reserved word (case-insensitive)
        t.type = self.reserved.get(t.value.lower(), "IDENTIFIER")
        if t.type == "MATCHES":
            t.lexer.begin("regex")
        return t

    def t_NEWLINE(self, t: lex.LexToken) -> None:
        r"\n+"
        t.lexer.lineno += len(t.value)

    def t_COMMENT(self, t: lex.LexToken) -> None:
        r"--[^\n]*"
        pass  # Ignore comments

    def t_error(self, t: lex.LexToken) -> None:
        raise SyntaxError(f"Illegal character '{t.value[0]}' at position {t.lexpos}")

    # --- Exclusive regex state tokens ---

    t_regex_ignore = " \t"

    def t_regex_REGEX(self, t: lex.LexToken) -> lex.LexToken:
        r"/([^/\\]|\\.)*/"
        t.value = t.value[1:-1]
        t.lexer.begin("INITIAL")
        return t

    def t_regex_NEWLINE(self, t: lex.LexToken) -> None:
        r"\n+"
        t.lexer.lineno += len(t.value)

    def t_regex_COMMENT(self, t: lex.LexToken) -> None:
        r"--[^\n]*"
        pass

    def t_regex_error(self, t: lex.LexToken) -> None:
        t.lexer.begin("INITIAL")
        raise SyntaxError(f"Expected regex pattern after 'matches', got '{t.value[0]}' at position {t.lexpos}")

    # --- Lexer methods ---

    def build(self, **kwargs) -> None:  # type: ignore
        """Build the lexer."""
        self.lexer = lex.lex(module=self, **kwargs)

    def input(self, data: str) -> None:
        """Set the input string to tokenize."""
        self.lexer.input(data)

    def token(self) -> lex.LexToken | None:
        """Return the next token."""
        return self.lexer.token()

    def tokenize(self, data: str) -> list[lex.LexToken]:
        """Tokenize the input and return all tokens."""
        self.input(data)
        tokens = []
        while True:
            tok = self.token()
            if tok is None:
                break
            tokens.append(tok)
        return tokens


# Module-level set of reserved keywords (lowercase) for use by other modules
RESERVED_KEYWORDS: frozenset[str] = frozenset(QueryLexer.reserved.keys())


def escape_if_keyword(name: str) -> str:
    """Wrap a name in backticks if it clashes with a reserved keyword."""
    if name.lower() in RESERVED_KEYWORDS:
        return f"`{name}`"
    return name
