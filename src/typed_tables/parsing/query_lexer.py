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
        "count": "COUNT",
        "average": "AVERAGE",
        "sum": "SUM",
        "product": "PRODUCT",
        "use": "USE",
        "create": "CREATE",
        "type": "TYPE",
        "uuid": "UUID",
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
        "references": "REFERENCES",
        "graph": "GRAPH",
        "yaml": "YAML",
        "json": "JSON",
        "xml": "XML",
        "compact": "COMPACT",
        "archive": "ARCHIVE",
        "restore": "RESTORE",
        "execute": "EXECUTE",
    }

    # Token list
    tokens = [
        "VARIABLE",
        "IDENTIFIER",
        "INTEGER",
        "FLOAT",
        "STRING",
        "REGEX",
        "PATH",
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
    ] + list(reserved.values())

    # Simple tokens
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

    # Ignored characters (including newlines - semicolons are the statement terminator)
    t_ignore = " \t"

    t_SEMICOLON = r";"
    t_BANG = r"!"

    def __init__(self) -> None:
        self.lexer: lex.LexToken = None  # type: ignore

    def t_VARIABLE(self, t: lex.LexToken) -> lex.LexToken:
        r"\$[a-zA-Z_][a-zA-Z0-9_]*"
        t.value = t.value[1:]  # Strip the $ prefix, store just the name
        return t

    def t_FLOAT(self, t: lex.LexToken) -> lex.LexToken:
        r"-?\d+\.\d+"
        t.value = float(t.value)
        return t

    def t_INTEGER(self, t: lex.LexToken) -> lex.LexToken:
        r"-?\d+"
        t.value = int(t.value)
        return t

    def t_STRING(self, t: lex.LexToken) -> lex.LexToken:
        r'"([^"\\]|\\.)*"'
        # Remove quotes and handle escapes
        t.value = t.value[1:-1].encode().decode("unicode_escape")
        return t

    def t_PATH(self, t: lex.LexToken) -> lex.LexToken:
        r"(?:\.\.?)?/[a-zA-Z0-9_./\-]+"
        return t

    def t_REGEX(self, t: lex.LexToken) -> lex.LexToken:
        r"/([^/\\]|\\.)*/"
        # Remove slashes
        t.value = t.value[1:-1]
        return t

    def t_IDENTIFIER(self, t: lex.LexToken) -> lex.LexToken:
        r"[a-zA-Z_][a-zA-Z0-9_]*"
        # Check if it's a reserved word (case-insensitive)
        t.type = self.reserved.get(t.value.lower(), "IDENTIFIER")
        return t

    def t_NEWLINE(self, t: lex.LexToken) -> None:
        r"\n+"
        t.lexer.lineno += len(t.value)

    def t_COMMENT(self, t: lex.LexToken) -> None:
        r"--[^\n]*"
        pass  # Ignore comments

    def t_error(self, t: lex.LexToken) -> None:
        raise SyntaxError(f"Illegal character '{t.value[0]}' at position {t.lexpos}")

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
