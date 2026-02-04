"""Parser for the type definition DSL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import ply.yacc as yacc

from typed_tables.parsing.type_lexer import TypeLexer
from typed_tables.types import (
    AliasTypeDefinition,
    ArrayTypeDefinition,
    CompositeTypeDefinition,
    FieldDefinition,
    TypeDefinition,
    TypeRegistry,
)


@dataclass
class TypeRef:
    """Reference to a type, possibly as an array."""

    name: str
    is_array: bool = False


@dataclass
class FieldSpec:
    """Specification for a field before resolution."""

    name: str
    type_ref: TypeRef | None = None  # None means type name matches field name


@dataclass
class TypeSpec:
    """Specification for a composite type before resolution."""

    name: str
    fields: list[FieldSpec]


@dataclass
class AliasSpec:
    """Specification for an alias before resolution."""

    name: str
    base_type_ref: TypeRef


class TypeParser:
    """Parser for the type definition DSL."""

    tokens = TypeLexer.tokens

    def __init__(self) -> None:
        self.lexer = TypeLexer()
        self.lexer.build()
        self.parser: yacc.LRParser = None  # type: ignore
        self.registry: TypeRegistry = TypeRegistry()
        self._specs: list[AliasSpec | TypeSpec] = []

    def p_schema(self, p: yacc.YaccProduction) -> None:
        """schema : statement_list"""
        p[0] = p[1]

    def p_statement_list_single(self, p: yacc.YaccProduction) -> None:
        """statement_list : statement"""
        if p[1] is not None:
            p[0] = [p[1]]
        else:
            p[0] = []

    def p_statement_list_multiple(self, p: yacc.YaccProduction) -> None:
        """statement_list : statement_list statement"""
        p[0] = p[1]
        if p[2] is not None:
            p[0].append(p[2])

    def p_statement_alias(self, p: yacc.YaccProduction) -> None:
        """statement : alias_def"""
        p[0] = p[1]

    def p_statement_type(self, p: yacc.YaccProduction) -> None:
        """statement : type_def"""
        p[0] = p[1]

    def p_statement_newline(self, p: yacc.YaccProduction) -> None:
        """statement : NEWLINE"""
        p[0] = None

    def p_alias_def(self, p: yacc.YaccProduction) -> None:
        """alias_def : DEFINE IDENTIFIER AS type_ref"""
        p[0] = AliasSpec(name=p[2], base_type_ref=p[4])

    def p_alias_def_newline(self, p: yacc.YaccProduction) -> None:
        """alias_def : DEFINE IDENTIFIER AS type_ref NEWLINE"""
        p[0] = AliasSpec(name=p[2], base_type_ref=p[4])

    def p_type_def(self, p: yacc.YaccProduction) -> None:
        """type_def : IDENTIFIER LBRACE field_list RBRACE"""
        p[0] = TypeSpec(name=p[1], fields=p[3])

    def p_type_def_newline(self, p: yacc.YaccProduction) -> None:
        """type_def : IDENTIFIER LBRACE field_list RBRACE NEWLINE"""
        p[0] = TypeSpec(name=p[1], fields=p[3])

    def p_type_def_empty(self, p: yacc.YaccProduction) -> None:
        """type_def : IDENTIFIER LBRACE RBRACE"""
        p[0] = TypeSpec(name=p[1], fields=[])

    def p_type_def_empty_newline(self, p: yacc.YaccProduction) -> None:
        """type_def : IDENTIFIER LBRACE RBRACE NEWLINE"""
        p[0] = TypeSpec(name=p[1], fields=[])

    def p_type_def_newlines(self, p: yacc.YaccProduction) -> None:
        """type_def : IDENTIFIER LBRACE NEWLINE field_list RBRACE"""
        p[0] = TypeSpec(name=p[1], fields=p[4])

    def p_type_def_newlines_end(self, p: yacc.YaccProduction) -> None:
        """type_def : IDENTIFIER LBRACE NEWLINE field_list RBRACE NEWLINE"""
        p[0] = TypeSpec(name=p[1], fields=p[4])

    def p_type_def_empty_newlines(self, p: yacc.YaccProduction) -> None:
        """type_def : IDENTIFIER LBRACE NEWLINE RBRACE"""
        p[0] = TypeSpec(name=p[1], fields=[])

    def p_type_def_empty_newlines_end(self, p: yacc.YaccProduction) -> None:
        """type_def : IDENTIFIER LBRACE NEWLINE RBRACE NEWLINE"""
        p[0] = TypeSpec(name=p[1], fields=[])

    def p_field_list_single(self, p: yacc.YaccProduction) -> None:
        """field_list : field"""
        p[0] = [p[1]]

    def p_field_list_multiple(self, p: yacc.YaccProduction) -> None:
        """field_list : field_list field"""
        p[0] = p[1] + [p[2]]

    def p_field_with_type(self, p: yacc.YaccProduction) -> None:
        """field : IDENTIFIER COLON type_ref NEWLINE"""
        p[0] = FieldSpec(name=p[1], type_ref=p[3])

    def p_field_with_type_no_newline(self, p: yacc.YaccProduction) -> None:
        """field : IDENTIFIER COLON type_ref"""
        p[0] = FieldSpec(name=p[1], type_ref=p[3])

    def p_field_implicit_type(self, p: yacc.YaccProduction) -> None:
        """field : IDENTIFIER NEWLINE"""
        p[0] = FieldSpec(name=p[1], type_ref=None)

    def p_field_implicit_type_no_newline(self, p: yacc.YaccProduction) -> None:
        """field : IDENTIFIER"""
        p[0] = FieldSpec(name=p[1], type_ref=None)

    def p_type_ref_simple(self, p: yacc.YaccProduction) -> None:
        """type_ref : IDENTIFIER"""
        p[0] = TypeRef(name=p[1], is_array=False)

    def p_type_ref_array(self, p: yacc.YaccProduction) -> None:
        """type_ref : IDENTIFIER LBRACKET RBRACKET"""
        p[0] = TypeRef(name=p[1], is_array=True)

    def p_error(self, p: yacc.YaccProduction) -> None:
        if p:
            raise SyntaxError(f"Syntax error at '{p.value}' (line {p.lineno})")
        else:
            raise SyntaxError("Syntax error at end of input")

    def build(self, **kwargs: Any) -> None:
        """Build the parser."""
        self.parser = yacc.yacc(module=self, **kwargs)

    def parse(self, data: str) -> TypeRegistry:
        """Parse type definitions and return a populated TypeRegistry."""
        if self.parser is None:
            self.build(debug=False, write_tables=False)

        self.registry = TypeRegistry()
        self._specs = []

        # Parse into specs
        specs = self.parser.parse(data, lexer=self.lexer.lexer)
        if specs is None:
            specs = []
        self._specs = specs

        # Resolve specs into type definitions
        self._resolve_specs()

        return self.registry

    def _resolve_type_ref(self, type_ref: TypeRef) -> TypeDefinition:
        """Resolve a type reference to a type definition."""
        if type_ref.is_array:
            return self.registry.get_array_type(type_ref.name)
        return self.registry.get_or_raise(type_ref.name)

    def _resolve_specs(self) -> None:
        """Resolve all specs into type definitions using two-phase resolution.

        Phase 1: Pre-register stubs for all composite TypeSpecs so that
        self-referential and mutually referential types can resolve.
        Phase 2: Iteratively resolve aliases and populate composite stubs.
        """
        # Phase 1: Pre-register composite stubs
        for spec in self._specs:
            if isinstance(spec, TypeSpec):
                self.registry.register_stub(spec.name)

        # Phase 2: Iteratively resolve
        unresolved: list[AliasSpec | TypeSpec] = []
        for spec in self._specs:
            if isinstance(spec, AliasSpec):
                unresolved.append(spec)
            elif isinstance(spec, TypeSpec):
                unresolved.append(spec)

        max_iterations = len(unresolved) + 1
        for _ in range(max_iterations):
            if not unresolved:
                break

            still_unresolved: list[AliasSpec | TypeSpec] = []
            progress = False

            for spec in unresolved:
                try:
                    if isinstance(spec, AliasSpec):
                        base_type = self._resolve_type_ref(spec.base_type_ref)
                        alias = AliasTypeDefinition(name=spec.name, base_type=base_type)
                        self.registry.register(alias)
                        progress = True
                    elif isinstance(spec, TypeSpec):
                        fields: list[FieldDefinition] = []
                        for field_spec in spec.fields:
                            if field_spec.type_ref is None:
                                field_type = self.registry.get_or_raise(field_spec.name)
                            else:
                                field_type = self._resolve_type_ref(field_spec.type_ref)
                            fields.append(
                                FieldDefinition(name=field_spec.name, type_def=field_type)
                            )
                        # Mutate the existing stub in-place
                        stub = self.registry.get(spec.name)
                        stub.fields = fields
                        progress = True
                except KeyError:
                    # Dependency not yet resolved
                    still_unresolved.append(spec)

            unresolved = still_unresolved

            if not progress and unresolved:
                remaining = [
                    s.name if isinstance(s, TypeSpec) else s.name for s in unresolved
                ]
                raise ValueError(f"Cannot resolve types: {remaining}")
