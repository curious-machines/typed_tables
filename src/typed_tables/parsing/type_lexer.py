"""Lexer for the type definition DSL."""

import ply.lex as lex


class TypeLexer:
    """Lexer for tokenizing type definition DSL."""

    # Reserved keywords
    reserved = {
        "define": "DEFINE",
        "as": "AS",
    }

    # Token list
    tokens = [
        "IDENTIFIER",
        "LBRACE",
        "RBRACE",
        "LBRACKET",
        "RBRACKET",
        "COLON",
        "COMMA",
    ] + list(reserved.values())

    # Simple tokens
    t_LBRACE = r"\{"
    t_RBRACE = r"\}"
    t_LBRACKET = r"\["
    t_RBRACKET = r"\]"
    t_COLON = r":"
    t_COMMA = r","

    # Ignored characters (spaces, tabs, and newlines)
    t_ignore = " \t"

    # Comments
    t_ignore_COMMENT = r"\#[^\n]*"

    def __init__(self) -> None:
        self.lexer: lex.LexToken = None  # type: ignore

    def t_IDENTIFIER(self, t: lex.LexToken) -> lex.LexToken:
        r"[a-zA-Z_][a-zA-Z0-9_]*"
        # Check if it's a reserved word
        t.type = self.reserved.get(t.value, "IDENTIFIER")
        return t

    def t_NEWLINE(self, t: lex.LexToken) -> None:
        r"\n+"
        t.lexer.lineno += len(t.value)
        # Don't return token — treat as whitespace

    def t_error(self, t: lex.LexToken) -> None:
        raise SyntaxError(f"Illegal character '{t.value[0]}' at line {t.lineno}")

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
