"""Parser for TTGE (Typed Tables Graph Expression) statements and expressions."""

from __future__ import annotations

import os
import re

from typed_tables.ttge.ttge_lexer import TTGELexer
from typed_tables.ttge.types import (
    AxisPathPred,
    AxisRef,
    BoolPred,
    ChainExpr,
    ChainOp,
    CompoundAxisOperand,
    ConfigStmt,
    DotExpr,
    ExecuteStmt,
    ExprStmt,
    Expr,
    GroupedNameTerm,
    InfPred,
    IntersectExpr,
    IntPred,
    JoinPred,
    MetaConfigStmt,
    MetaStyleStmt,
    NamePred,
    NameTerm,
    ParenExpr,
    SelectorExpr,
    SetExpr,
    SingleAxisOperand,
    StringPred,
    StyleStmt,
    UnionExpr,
)

import ply.yacc as yacc

_PARSER_DIR = os.path.dirname(os.path.abspath(__file__))


class TTGEParser:
    """Parser for TTGE statements and expressions.

    Statement-level dispatch (config/style/execute/metadata) is handled by
    pre-processing the input before sending it to the expression parser.
    This avoids the LALR ambiguity between `IDENTIFIER expression` (metadata
    prefix) and `expression` (which starts with IDENTIFIER via selectors).
    """

    tokens = TTGELexer.tokens

    # Operator precedence: loosest to tightest
    precedence = (
        ("left", "PIPE"),       # | union
        ("left", "AMPERSAND"),  # & intersection
        ("left", "PLUS", "SLASH", "MINUS"),  # chain operators
    )

    def __init__(self) -> None:
        self._lexer = TTGELexer()
        self._parser: yacc.LRParser | None = None

    def build(self, **kwargs) -> None:  # type: ignore
        self._lexer.build(debug=False, errorlog=yacc.NullLogger())
        kwargs.setdefault("debug", False)
        kwargs.setdefault("write_tables", True)
        kwargs.setdefault("outputdir", _PARSER_DIR)
        self._parser = yacc.yacc(module=self, **kwargs)

    def parse(self, text: str) -> object:
        """Parse a single TTGE statement.

        Handles statement-level dispatch before invoking the expression parser.
        """
        if self._parser is None:
            self.build()
        text = text.strip()
        if not text:
            return ExprStmt(expression=None)

        # Pre-process: detect statement type by leading keyword
        return self._parse_statement(text)

    def _parse_statement(self, text: str) -> object:
        """Dispatch based on leading keyword."""
        # Check for statement-level keywords
        m = re.match(r"([a-zA-Z_]\w*)\s+(.*)", text, re.DOTALL)
        if m:
            first_word = m.group(1)
            rest = m.group(2).strip()

            if first_word == "config":
                # config "file.ttgc"
                return ConfigStmt(file_path=self._parse_string(rest))

            if first_word == "execute":
                # execute "file.ttge"
                return ExecuteStmt(file_path=self._parse_string(rest))

            if first_word == "style":
                return self._parse_style_stmt(rest, meta=False)

            if first_word == "metadata":
                return self._parse_metadata_stmt(rest)

        # Expression statement: expression [sort by ...] [> "file"]
        return self._parse_expr_stmt(text, metadata=False)

    def _parse_metadata_stmt(self, rest: str) -> object:
        """Parse after 'metadata' keyword."""
        m = re.match(r"([a-zA-Z_]\w*)\s+(.*)", rest, re.DOTALL)
        if m:
            second_word = m.group(1)
            inner_rest = m.group(2).strip()
            if second_word == "config":
                return MetaConfigStmt(file_path=self._parse_string(inner_rest))
            if second_word == "style":
                return self._parse_style_stmt(inner_rest, meta=True)
        # metadata <expression> [sort by ...] [> "file"]
        return self._parse_expr_stmt(rest, metadata=True)

    def _parse_style_stmt(self, rest: str, meta: bool) -> object:
        """Parse style arguments: "file" [{...}] or {...}."""
        cls = MetaStyleStmt if meta else StyleStmt
        rest = rest.strip()
        if rest.startswith('"'):
            # style "file" [{...}]
            file_path = self._extract_string(rest)
            after = rest[len(file_path) + 2:].strip()  # skip quotes
            inline = None
            if after.startswith("{"):
                inline = self._parse_dict_literal(after)
            return cls(file_path=file_path, inline=inline)
        elif rest.startswith("{"):
            # style {...}
            inline = self._parse_dict_literal(rest)
            return cls(inline=inline)
        else:
            raise SyntaxError(f"TTGE: Expected string or dict after 'style', got: {rest[:20]}")

    def _parse_expr_stmt(self, text: str, metadata: bool) -> ExprStmt:
        """Parse an expression statement with optional sort/output suffixes."""
        # Extract trailing > "file"
        output_file = None
        m = re.search(r'>\s*"([^"]*)"$', text)
        if m:
            output_file = m.group(1)
            text = text[:m.start()].strip()

        # Extract trailing sort by ...
        sort_by = []
        m = re.search(r'\bsort\s+by\s+((?:source|label|target)(?:\s*,\s*(?:source|label|target))*)$', text)
        if m:
            sort_by = [k.strip() for k in m.group(1).split(",")]
            text = text[:m.start()].strip()

        # Parse the remaining text as an expression
        if not text:
            expr = None
        else:
            expr = self._parser.parse(text, lexer=self._lexer.lexer)
        return ExprStmt(metadata=metadata, expression=expr, sort_by=sort_by, output_file=output_file)

    def _parse_string(self, text: str) -> str:
        """Extract a quoted string from the start of text."""
        text = text.strip()
        if not text.startswith('"'):
            raise SyntaxError(f"TTGE: Expected string, got: {text[:20]}")
        return self._extract_string(text)

    def _extract_string(self, text: str) -> str:
        """Extract content of a quoted string (without quotes)."""
        # Simple: find matching quote
        i = 1
        while i < len(text):
            if text[i] == '\\':
                i += 2
                continue
            if text[i] == '"':
                return text[1:i]
            i += 1
        raise SyntaxError("TTGE: Unterminated string")

    def _parse_dict_literal(self, text: str) -> list[tuple[str, str]]:
        """Parse a {\"key\": \"value\", ...} dict literal."""
        text = text.strip()
        if not text.startswith("{"):
            raise SyntaxError(f"TTGE: Expected '{{', got: {text[:20]}")
        # Find matching }
        depth = 0
        for i, ch in enumerate(text):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    inner = text[1:i].strip()
                    break
        else:
            raise SyntaxError("TTGE: Unmatched '{'")

        if not inner:
            return []

        entries = []
        while inner:
            inner = inner.strip()
            if inner.startswith(","):
                inner = inner[1:].strip()
            if not inner:
                break
            key = self._extract_string(inner)
            inner = inner[len(key) + 2:].strip()  # skip "key"
            if not inner.startswith(":"):
                raise SyntaxError(f"TTGE: Expected ':', got: {inner[:20]}")
            inner = inner[1:].strip()
            value = self._extract_string(inner)
            inner = inner[len(value) + 2:].strip()  # skip "value"
            entries.append((key, value))
        return entries

    def parse_program(self, text: str) -> list:
        """Parse multiple TTGE statements (for .ttge scripts).

        Statements are separated by newlines or semicolons.
        Comments (-- to end of line) are stripped.
        """
        if self._parser is None:
            self.build()
        results = []
        # First strip comments
        lines = []
        for line in text.split("\n"):
            comment_pos = line.find("--")
            if comment_pos >= 0:
                line = line[:comment_pos]
            lines.append(line)
        # Split on semicolons, then by newlines
        cleaned = "\n".join(lines)
        # Split by semicolons first, then each piece by newlines
        pieces: list[str] = []
        for semi_piece in cleaned.split(";"):
            for line in semi_piece.split("\n"):
                stripped = line.strip()
                if stripped:
                    pieces.append(stripped)
        for piece in pieces:
            result = self.parse(piece)
            if result is not None:
                results.append(result)
        return results

    # ---- Grammar rules (expression parser only) ----
    # The statement-level dispatch is handled in _parse_statement above.
    # The PLY parser only handles expression parsing.

    def p_top_expression(self, p: yacc.YaccProduction) -> None:
        """top : expression"""
        p[0] = p[1]

    # ---- Expression grammar ----

    def p_expression_union(self, p: yacc.YaccProduction) -> None:
        """expression : expression PIPE expression"""
        p[0] = UnionExpr(left=p[1], right=p[3])

    def p_expression_intersect(self, p: yacc.YaccProduction) -> None:
        """expression : expression AMPERSAND expression"""
        p[0] = IntersectExpr(left=p[1], right=p[3])

    # Chain expressions
    def p_expression_chain_plus(self, p: yacc.YaccProduction) -> None:
        """expression : expression PLUS axis_operand"""
        base = p[1]
        op = ChainOp(op="+", operand=p[3])
        if isinstance(base, ChainExpr):
            base.ops.append(op)
            p[0] = base
        else:
            p[0] = ChainExpr(base=base, ops=[op])

    def p_expression_chain_slash(self, p: yacc.YaccProduction) -> None:
        """expression : expression SLASH axis_operand"""
        base = p[1]
        op = ChainOp(op="/", operand=p[3])
        if isinstance(base, ChainExpr):
            base.ops.append(op)
            p[0] = base
        else:
            p[0] = ChainExpr(base=base, ops=[op])

    def p_expression_chain_minus_axis(self, p: yacc.YaccProduction) -> None:
        """expression : expression MINUS axis_operand"""
        base = p[1]
        op = ChainOp(op="-", operand=p[3])
        if isinstance(base, ChainExpr):
            base.ops.append(op)
            p[0] = base
        else:
            p[0] = ChainExpr(base=base, ops=[op])

    def p_expression_chain_minus_atom(self, p: yacc.YaccProduction) -> None:
        """expression : expression MINUS atom"""
        base = p[1]
        op = ChainOp(op="-", operand=p[3])
        if isinstance(base, ChainExpr):
            base.ops.append(op)
            p[0] = base
        else:
            p[0] = ChainExpr(base=base, ops=[op])

    # Dot expression: atom.axis.axis...
    def p_expression_dot(self, p: yacc.YaccProduction) -> None:
        """expression : atom DOT axis_chain"""
        p[0] = DotExpr(base=p[1], axes=p[3])

    def p_expression_atom(self, p: yacc.YaccProduction) -> None:
        """expression : atom"""
        p[0] = p[1]

    # ---- Atom ----

    def p_atom_selector(self, p: yacc.YaccProduction) -> None:
        """atom : IDENTIFIER"""
        p[0] = SelectorExpr(name=p[1])

    def p_atom_selector_pred(self, p: yacc.YaccProduction) -> None:
        """atom : IDENTIFIER pred_dict"""
        p[0] = SelectorExpr(name=p[1], predicates=p[2])

    def p_atom_paren(self, p: yacc.YaccProduction) -> None:
        """atom : LPAREN expression RPAREN"""
        p[0] = ParenExpr(expr=p[2])

    def p_atom_set(self, p: yacc.YaccProduction) -> None:
        """atom : LBRACE expr_list RBRACE"""
        p[0] = SetExpr(members=p[2])

    # ---- Axis operand: .axis or .axis.axis or {.axis, .axis} ----

    def p_axis_operand_single(self, p: yacc.YaccProduction) -> None:
        """axis_operand : DOT axis_chain"""
        p[0] = SingleAxisOperand(axes=p[2])

    def p_axis_operand_compound(self, p: yacc.YaccProduction) -> None:
        """axis_operand : LBRACE compound_axis_list RBRACE"""
        p[0] = CompoundAxisOperand(axes=p[2])

    def p_compound_axis_list_single(self, p: yacc.YaccProduction) -> None:
        """compound_axis_list : DOT axis_ref"""
        p[0] = [p[2]]

    def p_compound_axis_list_multi(self, p: yacc.YaccProduction) -> None:
        """compound_axis_list : compound_axis_list COMMA DOT axis_ref"""
        p[0] = p[1] + [p[4]]

    # ---- Axis chain: axis.axis.axis ----

    def p_axis_chain_single(self, p: yacc.YaccProduction) -> None:
        """axis_chain : axis_ref"""
        p[0] = [p[1]]

    def p_axis_chain_multi(self, p: yacc.YaccProduction) -> None:
        """axis_chain : axis_chain DOT axis_ref"""
        p[0] = p[1] + [p[3]]

    # ---- Axis ref: identifier with optional predicates ----

    def p_axis_ref(self, p: yacc.YaccProduction) -> None:
        """axis_ref : IDENTIFIER"""
        p[0] = AxisRef(name=p[1])

    def p_axis_ref_pred(self, p: yacc.YaccProduction) -> None:
        """axis_ref : IDENTIFIER pred_dict"""
        p[0] = AxisRef(name=p[1], predicates=p[2])

    # ---- Expression list (for set literals) ----

    def p_expr_list_single(self, p: yacc.YaccProduction) -> None:
        """expr_list : expression"""
        p[0] = [p[1]]

    def p_expr_list_multi(self, p: yacc.YaccProduction) -> None:
        """expr_list : expr_list COMMA expression"""
        p[0] = p[1] + [p[3]]

    # ---- Predicate dict: {key=value, key=value} ----

    def p_pred_dict(self, p: yacc.YaccProduction) -> None:
        """pred_dict : LBRACE pred_list RBRACE"""
        p[0] = p[2]

    def p_pred_list_single(self, p: yacc.YaccProduction) -> None:
        """pred_list : predicate"""
        p[0] = p[1]

    def p_pred_list_multi(self, p: yacc.YaccProduction) -> None:
        """pred_list : pred_list COMMA predicate"""
        p[0] = {**p[1], **p[3]}

    def p_predicate(self, p: yacc.YaccProduction) -> None:
        """predicate : IDENTIFIER EQUALS pred_value"""
        p[0] = {p[1]: p[3]}

    # ---- Predicate values ----

    def p_pred_value_axis_path(self, p: yacc.YaccProduction) -> None:
        """pred_value : axis_path"""
        p[0] = p[1]

    def p_pred_value_join(self, p: yacc.YaccProduction) -> None:
        """pred_value : join_expr"""
        p[0] = p[1]

    def p_pred_value_integer(self, p: yacc.YaccProduction) -> None:
        """pred_value : INTEGER"""
        p[0] = IntPred(value=p[1])

    def p_pred_value_string(self, p: yacc.YaccProduction) -> None:
        """pred_value : STRING"""
        p[0] = StringPred(value=p[1])

    def p_pred_value_name(self, p: yacc.YaccProduction) -> None:
        """pred_value : name_expr"""
        # Unwrap single-term NamePred when the term is a special type
        val = p[1]
        if isinstance(val, NamePred) and len(val.terms) == 1:
            term = val.terms[0]
            if isinstance(term, (InfPred, BoolPred)):
                p[0] = term
                return
        p[0] = val

    # ---- Name expression: term | term | ... ----

    def p_name_expr_single(self, p: yacc.YaccProduction) -> None:
        """name_expr : name_term"""
        p[0] = NamePred(terms=[p[1]])

    def p_name_expr_multi(self, p: yacc.YaccProduction) -> None:
        """name_expr : name_expr PIPE name_term"""
        p[1].terms.append(p[3])
        p[0] = p[1]

    def p_name_term_plain(self, p: yacc.YaccProduction) -> None:
        """name_term : IDENTIFIER"""
        name = p[1]
        if name in ("inf", "infinity"):
            p[0] = InfPred()
        elif name == "true":
            p[0] = BoolPred(value=True)
        elif name == "false":
            p[0] = BoolPred(value=False)
        else:
            p[0] = NameTerm(negated=False, name=name)

    def p_name_term_negated(self, p: yacc.YaccProduction) -> None:
        """name_term : BANG IDENTIFIER"""
        p[0] = NameTerm(negated=True, name=p[2])

    def p_name_term_grouped(self, p: yacc.YaccProduction) -> None:
        """name_term : LPAREN name_expr RPAREN"""
        p[0] = GroupedNameTerm(negated=False, expr=p[2])

    def p_name_term_negated_grouped(self, p: yacc.YaccProduction) -> None:
        """name_term : BANG LPAREN name_expr RPAREN"""
        p[0] = GroupedNameTerm(negated=True, expr=p[3])

    # ---- Axis path: .ident.ident... ----

    def p_axis_path(self, p: yacc.YaccProduction) -> None:
        """axis_path : DOT IDENTIFIER"""
        p[0] = AxisPathPred(steps=[p[2]])

    def p_axis_path_multi(self, p: yacc.YaccProduction) -> None:
        """axis_path : axis_path DOT IDENTIFIER"""
        p[1].steps.append(p[3])
        p[0] = p[1]

    # ---- Join expression: join("sep", .path) ----

    def p_join_expr(self, p: yacc.YaccProduction) -> None:
        """join_expr : IDENTIFIER LPAREN STRING COMMA axis_path RPAREN"""
        if p[1] != "join":
            raise SyntaxError(f"TTGE: Expected 'join', got '{p[1]}'")
        p[0] = JoinPred(separator=p[3], path=p[5])

    # ---- Error handler ----

    def p_error(self, p: yacc.YaccProduction) -> None:
        if p:
            raise SyntaxError(f"TTGE: Syntax error at '{p.value}' (position {p.lexpos})")
        raise SyntaxError("TTGE: Unexpected end of input")
