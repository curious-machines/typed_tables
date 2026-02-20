"""Parser for TTGC (Typed Tables Graph Config) files."""

from __future__ import annotations

import os

import ply.yacc as yacc

from typed_tables.ttg.ttgc_lexer import TTGCLexer
from typed_tables.ttg.types import GraphConfig

_PARSER_DIR = os.path.dirname(os.path.abspath(__file__))


class TTGCParser:
    """Parser for .ttgc config files."""

    tokens = TTGCLexer.tokens

    def __init__(self) -> None:
        self._lexer = TTGCLexer()
        self._parser: yacc.LRParser | None = None
        self._config: GraphConfig | None = None

    def build(self, **kwargs) -> None:  # type: ignore
        self._lexer.build(debug=False, errorlog=yacc.NullLogger())
        kwargs.setdefault("debug", False)
        kwargs.setdefault("write_tables", True)
        kwargs.setdefault("outputdir", _PARSER_DIR)
        kwargs.setdefault("tabmodule", "typed_tables.ttg._ttgc_parsetab")
        kwargs.setdefault("errorlog", yacc.NullLogger())
        self._parser = yacc.yacc(module=self, **kwargs)

    def parse(self, text: str) -> GraphConfig:
        if self._parser is None:
            self.build()
        self._config = GraphConfig()
        self._parser.parse(text, lexer=self._lexer.lexer)
        return self._config

    # ---- Grammar rules ----

    def p_config_file(self, p: yacc.YaccProduction) -> None:
        """config_file : sections"""
        pass

    def p_sections_empty(self, p: yacc.YaccProduction) -> None:
        """sections : """
        pass

    def p_sections_multi(self, p: yacc.YaccProduction) -> None:
        """sections : sections section"""
        pass

    # ---- Section dispatch ----

    def p_section_selector(self, p: yacc.YaccProduction) -> None:
        """section : SELECTOR LBRACE selector_entries RBRACE"""
        pass

    def p_section_group(self, p: yacc.YaccProduction) -> None:
        """section : GROUP LBRACE group_entries RBRACE"""
        pass

    def p_section_axis(self, p: yacc.YaccProduction) -> None:
        """section : AXIS LBRACE axis_entries RBRACE"""
        pass

    def p_section_reverse(self, p: yacc.YaccProduction) -> None:
        """section : REVERSE LBRACE reverse_entries RBRACE"""
        pass

    def p_section_axis_group(self, p: yacc.YaccProduction) -> None:
        """section : AXIS_GROUP LBRACE group_entries_for_axis RBRACE"""
        pass

    def p_section_identity(self, p: yacc.YaccProduction) -> None:
        """section : IDENTITY LBRACE identity_entries RBRACE"""
        pass

    def p_section_shortcut(self, p: yacc.YaccProduction) -> None:
        """section : SHORTCUT LBRACE shortcut_entries RBRACE"""
        pass

    # ---- Selector entries: name: SchemaType ----

    def p_selector_entries_empty(self, p: yacc.YaccProduction) -> None:
        """selector_entries : """
        pass

    def p_selector_entries_multi(self, p: yacc.YaccProduction) -> None:
        """selector_entries : selector_entries selector_entry"""
        pass

    def p_selector_entry(self, p: yacc.YaccProduction) -> None:
        """selector_entry : IDENTIFIER COLON IDENTIFIER opt_comma"""
        self._config.selectors[p[1]] = p[3]

    # ---- Group entries: name: [item, ...] ----

    def p_group_entries_empty(self, p: yacc.YaccProduction) -> None:
        """group_entries : """
        pass

    def p_group_entries_multi(self, p: yacc.YaccProduction) -> None:
        """group_entries : group_entries group_entry"""
        pass

    def p_group_entry(self, p: yacc.YaccProduction) -> None:
        """group_entry : IDENTIFIER COLON LBRACKET ident_list RBRACKET opt_comma"""
        self._config.groups[p[1]] = p[4]

    # ---- Axis entries: name: selector.field or [selector.field, ...] ----

    def p_axis_entries_empty(self, p: yacc.YaccProduction) -> None:
        """axis_entries : """
        pass

    def p_axis_entries_multi(self, p: yacc.YaccProduction) -> None:
        """axis_entries : axis_entries axis_entry"""
        pass

    def p_axis_entry_single(self, p: yacc.YaccProduction) -> None:
        """axis_entry : IDENTIFIER COLON dotted_name opt_comma"""
        self._config.axes[p[1]] = [p[3]]

    def p_axis_entry_list(self, p: yacc.YaccProduction) -> None:
        """axis_entry : IDENTIFIER COLON LBRACKET dotted_name_list RBRACKET opt_comma"""
        self._config.axes[p[1]] = p[4]

    def p_dotted_name_base(self, p: yacc.YaccProduction) -> None:
        """dotted_name : IDENTIFIER DOT IDENTIFIER"""
        p[0] = f"{p[1]}.{p[3]}"

    def p_dotted_name_extend(self, p: yacc.YaccProduction) -> None:
        """dotted_name : dotted_name DOT IDENTIFIER"""
        p[0] = f"{p[1]}.{p[3]}"

    def p_dotted_name_list_single(self, p: yacc.YaccProduction) -> None:
        """dotted_name_list : dotted_name"""
        p[0] = [p[1]]

    def p_dotted_name_list_multi(self, p: yacc.YaccProduction) -> None:
        """dotted_name_list : dotted_name_list COMMA dotted_name"""
        p[0] = p[1] + [p[3]]

    # ---- Reverse entries: name: forward_axis_name ----

    def p_reverse_entries_empty(self, p: yacc.YaccProduction) -> None:
        """reverse_entries : """
        pass

    def p_reverse_entries_multi(self, p: yacc.YaccProduction) -> None:
        """reverse_entries : reverse_entries reverse_entry"""
        pass

    def p_reverse_entry(self, p: yacc.YaccProduction) -> None:
        """reverse_entry : IDENTIFIER COLON IDENTIFIER opt_comma"""
        self._config.reverses[p[1]] = p[3]

    # ---- Axis group entries (reuses group-like syntax) ----

    def p_group_entries_for_axis_empty(self, p: yacc.YaccProduction) -> None:
        """group_entries_for_axis : """
        pass

    def p_group_entries_for_axis_multi(self, p: yacc.YaccProduction) -> None:
        """group_entries_for_axis : group_entries_for_axis group_entry_for_axis"""
        pass

    def p_group_entry_for_axis(self, p: yacc.YaccProduction) -> None:
        """group_entry_for_axis : IDENTIFIER COLON LBRACKET ident_list RBRACKET opt_comma"""
        self._config.axis_groups[p[1]] = p[4]

    # ---- Identity entries: "default": field or selector: field ----

    def p_identity_entries_empty(self, p: yacc.YaccProduction) -> None:
        """identity_entries : """
        pass

    def p_identity_entries_multi(self, p: yacc.YaccProduction) -> None:
        """identity_entries : identity_entries identity_entry"""
        pass

    def p_identity_entry(self, p: yacc.YaccProduction) -> None:
        """identity_entry : IDENTIFIER COLON IDENTIFIER opt_comma"""
        self._config.identity[p[1]] = p[3]

    # ---- Shortcut entries: STRING: <raw TTG expression> ----

    def p_shortcut_entries_empty(self, p: yacc.YaccProduction) -> None:
        """shortcut_entries : """
        pass

    def p_shortcut_entries_multi(self, p: yacc.YaccProduction) -> None:
        """shortcut_entries : shortcut_entries shortcut_entry"""
        pass

    def p_shortcut_entry(self, p: yacc.YaccProduction) -> None:
        """shortcut_entry : STRING COLON SHORTCUT_VALUE"""
        self._config.shortcuts[p[1]] = p[3]

    # ---- Shared rules ----

    def p_ident_list_single(self, p: yacc.YaccProduction) -> None:
        """ident_list : IDENTIFIER"""
        p[0] = [p[1]]

    def p_ident_list_multi(self, p: yacc.YaccProduction) -> None:
        """ident_list : ident_list COMMA IDENTIFIER"""
        p[0] = p[1] + [p[3]]

    def p_opt_comma_yes(self, p: yacc.YaccProduction) -> None:
        """opt_comma : COMMA"""
        pass

    def p_opt_comma_no(self, p: yacc.YaccProduction) -> None:
        """opt_comma : """
        pass

    def p_error(self, p: yacc.YaccProduction) -> None:
        if p:
            raise SyntaxError(f"TTGC: Syntax error at '{p.value}' (line {p.lineno})")
        raise SyntaxError("TTGC: Unexpected end of input")
