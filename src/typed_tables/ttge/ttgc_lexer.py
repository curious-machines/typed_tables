"""Lexer for TTGC (Typed Tables Graph Config) files."""

import ply.lex as lex


class TTGCLexer:
    """Lexer for tokenizing .ttgc config files."""

    reserved = {
        "selector": "SELECTOR",
        "group": "GROUP",
        "axis": "AXIS",
        "reverse": "REVERSE",
        "axis_group": "AXIS_GROUP",
        "identity": "IDENTITY",
        "shortcut": "SHORTCUT",
    }

    tokens = [
        "IDENTIFIER",
        "STRING",
        "COLON",
        "COMMA",
        "DOT",
        "LBRACE",
        "RBRACE",
        "LBRACKET",
        "RBRACKET",
        "SHORTCUT_VALUE",
    ] + list(reserved.values())

    # Lexer states: shortcut state for raw expression capture
    states = (
        ("shortcut", "exclusive"),
        ("shortcutval", "exclusive"),
    )

    # --- INITIAL state tokens ---

    t_COLON = r":"
    t_COMMA = r","
    t_DOT = r"\."
    t_LBRACKET = r"\["
    t_RBRACKET = r"\]"
    t_ignore = " \t"

    def __init__(self) -> None:
        self.lexer: lex.LexToken = None  # type: ignore

    def t_STRING(self, t: lex.LexToken) -> lex.LexToken:
        r'"([^"\\]|\\.)*"'
        t.value = t.value[1:-1]
        return t

    def t_IDENTIFIER(self, t: lex.LexToken) -> lex.LexToken:
        r"[a-zA-Z_][a-zA-Z0-9_]*"
        t.type = self.reserved.get(t.value, "IDENTIFIER")
        if t.type == "SHORTCUT":
            t.lexer.begin("shortcut")
        return t

    def t_LBRACE(self, t: lex.LexToken) -> lex.LexToken:
        r"\{"
        return t

    def t_RBRACE(self, t: lex.LexToken) -> lex.LexToken:
        r"\}"
        return t

    def t_NEWLINE(self, t: lex.LexToken) -> None:
        r"\n+"
        t.lexer.lineno += len(t.value)

    def t_COMMENT(self, t: lex.LexToken) -> None:
        r"--[^\n]*"
        pass

    def t_error(self, t: lex.LexToken) -> None:
        raise SyntaxError(f"TTGC: Illegal character '{t.value[0]}' at line {t.lexer.lineno}")

    # --- shortcut state: inside the shortcut block ---
    # In this state we parse: STRING COLON <raw_expression_to_eol>
    # But we also need to handle { and } for the block boundaries.

    t_shortcut_ignore = " \t"

    def t_shortcut_STRING(self, t: lex.LexToken) -> lex.LexToken:
        r'"([^"\\]|\\.)*"'
        t.value = t.value[1:-1]
        # After the string key, expect colon then raw value
        return t

    def t_shortcut_COLON(self, t: lex.LexToken) -> lex.LexToken:
        r":"
        # After colon in shortcut block, switch to raw value capture
        t.lexer.begin("shortcutval")
        return t

    def t_shortcut_LBRACE(self, t: lex.LexToken) -> lex.LexToken:
        r"\{"
        return t

    def t_shortcut_RBRACE(self, t: lex.LexToken) -> lex.LexToken:
        r"\}"
        t.lexer.begin("INITIAL")
        return t

    def t_shortcut_NEWLINE(self, t: lex.LexToken) -> None:
        r"\n+"
        t.lexer.lineno += len(t.value)

    def t_shortcut_COMMENT(self, t: lex.LexToken) -> None:
        r"--[^\n]*"
        pass

    def t_shortcut_error(self, t: lex.LexToken) -> None:
        raise SyntaxError(f"TTGC shortcut: Illegal character '{t.value[0]}' at line {t.lexer.lineno}")

    # --- shortcutval state: capture everything to end of line as raw TTGE expression ---

    def t_shortcutval_SHORTCUT_VALUE(self, t: lex.LexToken) -> lex.LexToken:
        r"[^\n]+"
        # Strip trailing whitespace and comments
        value = t.value.strip()
        if "--" in value:
            # Remove trailing comment (but not inside strings)
            # Simple approach: find -- that's not inside quotes
            in_str = False
            for i, ch in enumerate(value):
                if ch == '"':
                    in_str = not in_str
                elif ch == '-' and not in_str and i + 1 < len(value) and value[i + 1] == '-':
                    value = value[:i].strip()
                    break
        t.value = value
        t.lexer.begin("shortcut")
        return t

    def t_shortcutval_NEWLINE(self, t: lex.LexToken) -> None:
        r"\n+"
        t.lexer.lineno += len(t.value)
        t.lexer.begin("shortcut")

    def t_shortcutval_error(self, t: lex.LexToken) -> None:
        raise SyntaxError(f"TTGC shortcut value: Illegal character '{t.value[0]}' at line {t.lexer.lineno}")

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
