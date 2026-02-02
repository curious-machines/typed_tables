"""Parser for the TTQ (Typed Tables Query) language."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import ply.yacc as yacc

from typed_tables.parsing.query_lexer import QueryLexer


@dataclass
class SelectField:
    """A field in a SELECT clause."""

    name: str  # Field name or "*"
    aggregate: str | None = None  # count, average, sum, product


@dataclass
class Condition:
    """A WHERE condition."""

    field: str
    operator: str  # =, !=, <, <=, >, >=, starts_with, matches
    value: Any
    negate: bool = False


@dataclass
class CompoundCondition:
    """A compound condition (AND/OR)."""

    left: Condition | CompoundCondition
    operator: str  # and, or
    right: Condition | CompoundCondition


@dataclass
class SelectQuery:
    """A SELECT query."""

    table: str
    fields: list[SelectField] = field(default_factory=list)
    where: Condition | CompoundCondition | None = None
    group_by: list[str] = field(default_factory=list)
    sort_by: list[str] = field(default_factory=list)
    offset: int = 0
    limit: int | None = None


@dataclass
class ShowTablesQuery:
    """A SHOW TABLES query."""

    pass


@dataclass
class DescribeQuery:
    """A DESCRIBE query."""

    table: str


Query = SelectQuery | ShowTablesQuery | DescribeQuery


class QueryParser:
    """Parser for TTQ queries."""

    tokens = QueryLexer.tokens

    # Operator precedence
    precedence = (
        ("left", "OR"),
        ("left", "AND"),
        ("right", "NOT"),
    )

    def __init__(self) -> None:
        self.lexer = QueryLexer()
        self.lexer.build()
        self.parser: yacc.LRParser = None  # type: ignore

    def p_query_select(self, p: yacc.YaccProduction) -> None:
        """query : select_query"""
        p[0] = p[1]

    def p_query_show_tables(self, p: yacc.YaccProduction) -> None:
        """query : SHOW TABLES"""
        p[0] = ShowTablesQuery()

    def p_query_describe(self, p: yacc.YaccProduction) -> None:
        """query : DESCRIBE IDENTIFIER"""
        p[0] = DescribeQuery(table=p[2])

    def p_select_query(self, p: yacc.YaccProduction) -> None:
        """select_query : from_clause select_clause where_clause group_clause sort_clause offset_clause limit_clause"""
        p[0] = SelectQuery(
            table=p[1],
            fields=p[2],
            where=p[3],
            group_by=p[4],
            sort_by=p[5],
            offset=p[6],
            limit=p[7],
        )

    def p_from_clause(self, p: yacc.YaccProduction) -> None:
        """from_clause : FROM IDENTIFIER"""
        p[0] = p[2]

    def p_select_clause_empty(self, p: yacc.YaccProduction) -> None:
        """select_clause : """
        p[0] = [SelectField(name="*")]

    def p_select_clause_star(self, p: yacc.YaccProduction) -> None:
        """select_clause : SELECT STAR"""
        p[0] = [SelectField(name="*")]

    def p_select_clause_fields(self, p: yacc.YaccProduction) -> None:
        """select_clause : SELECT field_list"""
        p[0] = p[2]

    def p_field_list_single(self, p: yacc.YaccProduction) -> None:
        """field_list : select_field"""
        p[0] = [p[1]]

    def p_field_list_multiple(self, p: yacc.YaccProduction) -> None:
        """field_list : field_list COMMA select_field"""
        p[0] = p[1] + [p[3]]

    def p_select_field_name(self, p: yacc.YaccProduction) -> None:
        """select_field : IDENTIFIER"""
        p[0] = SelectField(name=p[1])

    def p_select_field_count(self, p: yacc.YaccProduction) -> None:
        """select_field : COUNT LPAREN RPAREN"""
        p[0] = SelectField(name="*", aggregate="count")

    def p_select_field_aggregate(self, p: yacc.YaccProduction) -> None:
        """select_field : AVERAGE LPAREN IDENTIFIER RPAREN
                        | SUM LPAREN IDENTIFIER RPAREN
                        | PRODUCT LPAREN IDENTIFIER RPAREN"""
        p[0] = SelectField(name=p[3], aggregate=p[1].lower())

    def p_where_clause_empty(self, p: yacc.YaccProduction) -> None:
        """where_clause : """
        p[0] = None

    def p_where_clause(self, p: yacc.YaccProduction) -> None:
        """where_clause : WHERE condition"""
        p[0] = p[2]

    def p_condition_comparison(self, p: yacc.YaccProduction) -> None:
        """condition : IDENTIFIER EQ value
                     | IDENTIFIER NEQ value
                     | IDENTIFIER LT value
                     | IDENTIFIER LTE value
                     | IDENTIFIER GT value
                     | IDENTIFIER GTE value"""
        op_map = {"=": "eq", "!=": "neq", "<": "lt", "<=": "lte", ">": "gt", ">=": "gte"}
        p[0] = Condition(field=p[1], operator=op_map[p[2]], value=p[3])

    def p_condition_starts_with(self, p: yacc.YaccProduction) -> None:
        """condition : IDENTIFIER STARTS WITH STRING"""
        p[0] = Condition(field=p[1], operator="starts_with", value=p[4])

    def p_condition_matches(self, p: yacc.YaccProduction) -> None:
        """condition : IDENTIFIER MATCHES REGEX"""
        p[0] = Condition(field=p[1], operator="matches", value=p[3])

    def p_condition_not(self, p: yacc.YaccProduction) -> None:
        """condition : NOT condition"""
        cond = p[2]
        if isinstance(cond, Condition):
            cond.negate = not cond.negate
        p[0] = cond

    def p_condition_and(self, p: yacc.YaccProduction) -> None:
        """condition : condition AND condition"""
        p[0] = CompoundCondition(left=p[1], operator="and", right=p[3])

    def p_condition_or(self, p: yacc.YaccProduction) -> None:
        """condition : condition OR condition"""
        p[0] = CompoundCondition(left=p[1], operator="or", right=p[3])

    def p_condition_paren(self, p: yacc.YaccProduction) -> None:
        """condition : LPAREN condition RPAREN"""
        p[0] = p[2]

    def p_value_integer(self, p: yacc.YaccProduction) -> None:
        """value : INTEGER"""
        p[0] = p[1]

    def p_value_float(self, p: yacc.YaccProduction) -> None:
        """value : FLOAT"""
        p[0] = p[1]

    def p_value_string(self, p: yacc.YaccProduction) -> None:
        """value : STRING"""
        p[0] = p[1]

    def p_group_clause_empty(self, p: yacc.YaccProduction) -> None:
        """group_clause : """
        p[0] = []

    def p_group_clause(self, p: yacc.YaccProduction) -> None:
        """group_clause : GROUP BY identifier_list"""
        p[0] = p[3]

    def p_sort_clause_empty(self, p: yacc.YaccProduction) -> None:
        """sort_clause : """
        p[0] = []

    def p_sort_clause(self, p: yacc.YaccProduction) -> None:
        """sort_clause : SORT BY identifier_list"""
        p[0] = p[3]

    def p_identifier_list_single(self, p: yacc.YaccProduction) -> None:
        """identifier_list : IDENTIFIER"""
        p[0] = [p[1]]

    def p_identifier_list_multiple(self, p: yacc.YaccProduction) -> None:
        """identifier_list : identifier_list COMMA IDENTIFIER"""
        p[0] = p[1] + [p[3]]

    def p_offset_clause_empty(self, p: yacc.YaccProduction) -> None:
        """offset_clause : """
        p[0] = 0

    def p_offset_clause(self, p: yacc.YaccProduction) -> None:
        """offset_clause : OFFSET INTEGER"""
        p[0] = p[2]

    def p_limit_clause_empty(self, p: yacc.YaccProduction) -> None:
        """limit_clause : """
        p[0] = None

    def p_limit_clause(self, p: yacc.YaccProduction) -> None:
        """limit_clause : LIMIT INTEGER"""
        p[0] = p[2]

    def p_error(self, p: yacc.YaccProduction) -> None:
        if p:
            raise SyntaxError(f"Syntax error at '{p.value}' (position {p.lexpos})")
        else:
            raise SyntaxError("Syntax error at end of input")

    def build(self, **kwargs: Any) -> None:
        """Build the parser."""
        self.parser = yacc.yacc(module=self, **kwargs)

    def parse(self, data: str) -> Query:
        """Parse a query string."""
        if self.parser is None:
            self.build(debug=False, write_tables=False)

        return self.parser.parse(data, lexer=self.lexer.lexer)
