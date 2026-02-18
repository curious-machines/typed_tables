"""Lexer for TTGE (Typed Tables Graph Expression) statements."""

import ply.lex as lex


class TTGELexer:
    """Lexer for tokenizing TTGE expressions and statements."""

    # No reserved keywords — all identifiers are contextual (D22).
    # The parser handles contextual keywords based on position.

    tokens = [
        "IDENTIFIER",
        "STRING",
        "INTEGER",
        "DOT",
        "PLUS",
        "MINUS",
        "SLASH",
        "PIPE",
        "AMPERSAND",
        "LBRACE",
        "RBRACE",
        "LPAREN",
        "RPAREN",
        "EQUALS",
        "BANG",
        "COMMA",
        "GT",
        "COLON",
    ]

    t_DOT = r"\."
    t_PLUS = r"\+"
    t_MINUS = r"-"
    t_SLASH = r"/"
    t_PIPE = r"\|"
    t_AMPERSAND = r"&"
    t_LPAREN = r"\("
    t_RPAREN = r"\)"
    t_EQUALS = r"="
    t_BANG = r"!"
    t_COMMA = r","
    t_GT = r">"
    t_COLON = r":"
    t_ignore = " \t"

    def __init__(self) -> None:
        self.lexer: lex.LexToken = None  # type: ignore

    def t_LBRACE(self, t: lex.LexToken) -> lex.LexToken:
        r"\{"
        return t

    def t_RBRACE(self, t: lex.LexToken) -> lex.LexToken:
        r"\}"
        return t

    def t_STRING(self, t: lex.LexToken) -> lex.LexToken:
        r'"([^"\\]|\\.)*"'
        t.value = t.value[1:-1]
        return t

    def t_INTEGER(self, t: lex.LexToken) -> lex.LexToken:
        r"\d+"
        t.value = int(t.value)
        return t

    def t_IDENTIFIER(self, t: lex.LexToken) -> lex.LexToken:
        r"[a-zA-Z_][a-zA-Z0-9_]*"
        return t

    def t_NEWLINE(self, t: lex.LexToken) -> None:
        r"\n+"
        t.lexer.lineno += len(t.value)

    def t_COMMENT(self, t: lex.LexToken) -> None:
        r"--[^\n]*"
        pass

    def t_error(self, t: lex.LexToken) -> None:
        raise SyntaxError(f"TTGE: Illegal character '{t.value[0]}' at position {t.lexpos}")

    # --- Lexer methods ---

    def build(self, **kwargs) -> None:  # type: ignore
        self.lexer = lex.lex(module=self, **kwargs)

    def input(self, data: str) -> None:
        self.lexer.input(data)

    def token(self) -> lex.LexToken | None:
        return self.lexer.token()

    def tokenize(self, data: str) -> list[lex.LexToken]:
        self.input(data)
        tokens = []
        while True:
            tok = self.token()
            if tok is None:
                break
            tokens.append(tok)
        return tokens
