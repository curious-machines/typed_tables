"""Parser for the TTQ (Typed Tables Query) language."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import ply.yacc as yacc

from typed_tables.parsing.query_lexer import QueryLexer


@dataclass
class ArrayTypeSpec:
    """Structured type spec for prefix array syntax: [int32]."""

    element_type: str | ArrayTypeSpec | SetTypeSpec | DictTypeSpec


@dataclass
class SetTypeSpec:
    """Structured type spec for set syntax: {int32}."""

    element_type: str | ArrayTypeSpec | SetTypeSpec | DictTypeSpec


@dataclass
class DictTypeSpec:
    """Structured type spec for dict syntax: {string: int32}."""

    key_type: str | ArrayTypeSpec | SetTypeSpec | DictTypeSpec
    value_type: str | ArrayTypeSpec | SetTypeSpec | DictTypeSpec


# Union type for all type specifications
TypeSpec = str | ArrayTypeSpec | SetTypeSpec | DictTypeSpec


@dataclass
class ArraySlice:
    """Represents an array slice like 0:5 or :5 or 0:."""

    start: int | None = None
    end: int | None = None


@dataclass
class ArrayIndex:
    """Represents array indexing like [0], [-1], [0:5], [-3:], etc."""

    index: int | ArraySlice  # Single index or slice


@dataclass
class SelectField:
    """A field in a SELECT clause."""

    name: str  # Field name or "*" or dotted path like "address.state"
    aggregate: str | None = None  # count, average, sum, product, min, max
    array_index: ArrayIndex | None = None  # Optional array indexing
    post_path: list[str] | None = None  # Path after array index: arr[0].name
    method_name: str | None = None  # e.g., "length", "isEmpty"
    method_args: list[Any] | None = None  # For future methods with args
    method_chain: list[MethodCall] | None = None  # For chained methods

    @property
    def path(self) -> list[str]:
        """Return the field path as a list (e.g., ['address', 'state'])."""
        if self.name == "*":
            return ["*"]
        return self.name.split(".")


@dataclass
class Condition:
    """A WHERE condition."""

    field: str
    operator: str  # =, !=, <, <=, >, >=, starts_with, matches
    value: Any
    negate: bool = False
    method_name: str | None = None  # e.g., "length", "isEmpty"
    method_args: list[Any] | None = None  # For future methods with args
    method_chain: list[MethodCall] | None = None  # For chained methods


@dataclass
class CompoundCondition:
    """A compound condition (AND/OR)."""

    left: Condition | CompoundCondition
    operator: str  # and, or
    right: Condition | CompoundCondition


@dataclass
class SelectQuery:
    """A SELECT query."""

    table: str | None = None
    fields: list[SelectField] = field(default_factory=list)
    where: Condition | CompoundCondition | None = None
    group_by: list[str] = field(default_factory=list)
    sort_by: list[str] = field(default_factory=list)
    offset: int = 0
    limit: int | None = None
    source_var: str | None = None
    variant: str | None = None  # For enum variant queries: from Shape.line select *


@dataclass
class ShowTypesQuery:
    """A SHOW TYPES query."""

    filter: str | None = None  # None, "interfaces", "composites", "enums", "primitives", "aliases"
    sort_by: list[str] = field(default_factory=list)


@dataclass
class ShowReferencesQuery:
    """A SHOW REFERENCES query."""

    type_name: str | None = None  # None = show all edges
    sort_by: list[str] = field(default_factory=list)


@dataclass
class DumpGraphQuery:
    """A DUMP GRAPH query."""

    output_file: str | None = None  # None = TTQ to stdout


@dataclass
class CompactQuery:
    """A COMPACT TO query."""

    output_path: str


@dataclass
class ArchiveQuery:
    """An ARCHIVE TO query."""

    output_file: str | None = None


@dataclass
class RestoreQuery:
    """A RESTORE query."""

    archive_file: str
    output_path: str | None = None


@dataclass
class ExecuteQuery:
    """An EXECUTE query — run statements from a file."""

    file_path: str


@dataclass
class ImportQuery:
    """An IMPORT query — execute a script once per database."""

    file_path: str


@dataclass
class DescribeQuery:
    """A DESCRIBE query."""

    table: str
    sort_by: list[str] = field(default_factory=list)


@dataclass
class UseQuery:
    """A USE query to select a database directory."""

    path: str
    temporary: bool = False


@dataclass
class FieldDef:
    """A field definition for create type."""

    name: str
    type_name: str | ArrayTypeSpec | SetTypeSpec | DictTypeSpec
    default_value: Any = None
    overflow: str | None = None  # "saturating" or "wrapping"


@dataclass
class CreateTypeQuery:
    """A CREATE TYPE query."""

    name: str
    fields: list[FieldDef] = field(default_factory=list)
    parents: list[str] = field(default_factory=list)


@dataclass
class CreateInterfaceQuery:
    """A CREATE INTERFACE query."""

    name: str
    fields: list[FieldDef] = field(default_factory=list)


@dataclass
class ForwardTypeQuery:
    """A FORWARD TYPE query - declares a type name for forward references."""

    name: str


@dataclass
class CreateAliasQuery:
    """A CREATE ALIAS query."""

    name: str
    base_type: str | ArrayTypeSpec | SetTypeSpec | DictTypeSpec


@dataclass
class CompositeRef:
    """A reference to an existing composite instance: TypeName(index)."""

    type_name: str
    index: int


@dataclass
class InlineInstance:
    """An inline instance creation: TypeName(field=value, ...)."""

    type_name: str
    fields: list[FieldValue]
    tag: str | None = None


@dataclass
class FieldValue:
    """A field value for create instance or mutation in update."""

    name: str
    value: Any = None  # Can be literal or FunctionCall; None for mutations
    method_name: str | None = None  # e.g., "reverse", "swap"
    method_args: list[Any] | None = None  # e.g., [0, 3] for swap
    method_chain: list[MethodCall] | None = None  # For chained methods in assignment


@dataclass
class FunctionCall:
    """A function call like uuid()."""

    name: str
    args: list[Any] = field(default_factory=list)


@dataclass
class CreateInstanceQuery:
    """A CREATE instance query."""

    type_name: str
    fields: list[FieldValue] = field(default_factory=list)
    tag: str | None = None


@dataclass
class EvalQuery:
    """A standalone expression evaluation query (SELECT without FROM)."""

    expressions: list[Any] = field(default_factory=list)  # List of (value/FunctionCall, alias) tuples


@dataclass
class DeleteQuery:
    """A DELETE query."""

    table: str
    where: Condition | CompoundCondition | None = None
    force: bool = False


@dataclass
class DropDatabaseQuery:
    """A DROP database query."""

    path: str | None = None
    force: bool = False


@dataclass
class DumpItem:
    """A single item in a dump list."""

    table: str | None = None
    variable: str | None = None


@dataclass
class DumpQuery:
    """A DUMP query to serialize database contents."""

    table: str | None = None
    output_file: str | None = None
    variable: str | None = None
    items: list[DumpItem] | None = None
    pretty: bool = False
    format: str = "ttq"  # "ttq", "yaml", or "json"
    include_system: bool = False


@dataclass
class NullValue:
    """The null literal value."""

    pass


@dataclass
class TagReference:
    """A reference to a declared tag used as a field value."""

    name: str


@dataclass
class VariableReference:
    """A reference to a bound variable: $var."""

    var_name: str


@dataclass
class VariableAssignmentQuery:
    """A variable assignment: $var = create Type(...)."""

    var_name: str
    create_query: CreateInstanceQuery


@dataclass
class CollectSource:
    """A single source in a collect query."""

    table: str | None = None
    variable: str | None = None
    where: Condition | CompoundCondition | None = None


@dataclass
class CollectQuery:
    """A collect query: $var = collect Type where ... sort by ... offset N limit M."""

    var_name: str
    sources: list[CollectSource] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    sort_by: list[str] = field(default_factory=list)
    offset: int = 0
    limit: int | None = None


@dataclass
class UpdateQuery:
    """An UPDATE query to modify fields on existing records."""

    type_name: str
    index: int | None = None
    var_name: str | None = None
    fields: list[FieldValue] = field(default_factory=list)
    where: Condition | CompoundCondition | None = None


@dataclass
class EnumVariantSpec:
    """Specification for an enum variant in CREATE ENUM."""

    name: str
    explicit_value: int | None = None         # C-style: `ok = 200`
    fields: list[FieldDef] | None = None      # Swift-style: `(x: float32, ...)`


@dataclass
class CreateEnumQuery:
    """A CREATE ENUM query."""

    name: str
    variants: list[EnumVariantSpec] = field(default_factory=list)
    backing_type: str | None = None  # e.g. "uint8", "uint16"


@dataclass
class EnumValueExpr:
    """Enum value in instance creation: Color.red or Shape.circle(cx=50, ...).

    When enum_name is None, the shorthand form was used (.red, .circle(cx=50))
    and the enum type must be inferred from the field type at execution time.
    """

    enum_name: str | None
    variant_name: str
    args: list[FieldValue] | None = None  # None for C-style


@dataclass
class SortKeyExpr:
    """A sort key expression: .field, .field desc, or just desc/asc."""

    field_name: str | None = None  # None for bare direction on primitives
    descending: bool = False


@dataclass
class MethodCall:
    """A single method call in a chain."""

    method_name: str
    method_args: list[Any] | None = None


@dataclass
class MethodChainValue:
    """A chained method call expression used as an instance_value.

    Represents expressions like: readings.sort().reverse()
    Used in assignment form: set readings = readings.sort().reverse()
    """

    source_field: str
    chain: list[MethodCall]


@dataclass
class ScopeBlock:
    """A scope block containing statements with shared tag/variable namespace.

    Tags and variables declared within a scope are destroyed when the scope exits.
    Tags can only be used within a scope block.
    """

    statements: list[Any] = field(default_factory=list)


@dataclass
class MethodCallExpr:
    """A method call on an expression: expr.method(args)."""

    target: Any
    method_name: str
    method_args: list[Any] | None = None


@dataclass
class TypedLiteral:
    """A literal with an explicit type suffix: 5i8, 200u16, 3.14f32."""

    value: int | float
    type_name: str  # "int8", "uint16", "float32", etc.


@dataclass
class BinaryExpr:
    """A binary expression: left op right."""

    left: Any
    op: str  # "+", "-", "*", "/", "%", "//", "++"
    right: Any


@dataclass
class UnaryExpr:
    """A unary expression: op operand."""

    op: str  # "-", "+"
    operand: Any


@dataclass
class SetLiteral:
    """A set literal: {1, 2, 3}."""

    elements: list


@dataclass
class DictEntry:
    """A key-value pair in a dict literal."""

    key: Any
    value: Any


@dataclass
class DictLiteral:
    """A dict literal: {"a": 1, "b": 2}."""

    entries: list  # list of DictEntry


@dataclass
class EmptyBraces:
    """Empty braces {} — resolved from field type context."""

    pass


Query = SelectQuery | ShowTypesQuery | ShowReferencesQuery | DescribeQuery | UseQuery | CreateTypeQuery | CreateInterfaceQuery | CreateAliasQuery | CreateInstanceQuery | CreateEnumQuery | EvalQuery | DeleteQuery | DropDatabaseQuery | DumpQuery | DumpGraphQuery | CompactQuery | ArchiveQuery | RestoreQuery | ExecuteQuery | ImportQuery | VariableAssignmentQuery | CollectQuery | UpdateQuery | ScopeBlock


class QueryParser:
    """Parser for TTQ queries."""

    tokens = QueryLexer.tokens

    # Operator precedence
    precedence = (
        ("left", "OR"),
        ("left", "AND"),
        ("right", "NOT"),
        ("left", "CONCAT"),
        ("left", "PLUS", "MINUS"),
        ("left", "STAR", "SLASH", "PERCENT", "DOUBLESLASH"),
        ("right", "UMINUS", "UPLUS"),
    )

    def __init__(self) -> None:
        self.lexer = QueryLexer()
        self.lexer.build()
        self.parser: yacc.LRParser = None  # type: ignore

    # --- Program (multi-statement) productions ---

    def p_program(self, p: yacc.YaccProduction) -> None:
        """program : program_statement_list"""
        p[0] = p[1]

    def p_program_statement_list_single(self, p: yacc.YaccProduction) -> None:
        """program_statement_list : program_statement"""
        p[0] = [p[1]] if p[1] is not None else []

    def p_program_statement_list_multiple(self, p: yacc.YaccProduction) -> None:
        """program_statement_list : program_statement_list program_statement"""
        p[0] = p[1]
        if p[2] is not None:
            p[0].append(p[2])

    def p_program_statement(self, p: yacc.YaccProduction) -> None:
        """program_statement : query
                             | query SEMICOLON"""
        p[0] = p[1]

    def p_program_statement_empty(self, p: yacc.YaccProduction) -> None:
        """program_statement : SEMICOLON"""
        p[0] = None

    def p_query_select(self, p: yacc.YaccProduction) -> None:
        """query : select_query"""
        p[0] = p[1]

    def p_query_show_types(self, p: yacc.YaccProduction) -> None:
        """query : SHOW TYPES sort_clause"""
        p[0] = ShowTypesQuery(sort_by=p[3])

    def p_query_show_system_types(self, p: yacc.YaccProduction) -> None:
        """query : SHOW SYSTEM TYPES sort_clause"""
        p[0] = ShowTypesQuery(filter="system", sort_by=p[4])

    def p_query_show_interfaces(self, p: yacc.YaccProduction) -> None:
        """query : SHOW INTERFACES sort_clause"""
        p[0] = ShowTypesQuery(filter="interfaces", sort_by=p[3])

    def p_query_show_composites(self, p: yacc.YaccProduction) -> None:
        """query : SHOW COMPOSITES sort_clause"""
        p[0] = ShowTypesQuery(filter="composites", sort_by=p[3])

    def p_query_show_enums(self, p: yacc.YaccProduction) -> None:
        """query : SHOW ENUMS sort_clause"""
        p[0] = ShowTypesQuery(filter="enums", sort_by=p[3])

    def p_query_show_primitives(self, p: yacc.YaccProduction) -> None:
        """query : SHOW PRIMITIVES sort_clause"""
        p[0] = ShowTypesQuery(filter="primitives", sort_by=p[3])

    def p_query_show_aliases(self, p: yacc.YaccProduction) -> None:
        """query : SHOW ALIASES sort_clause"""
        p[0] = ShowTypesQuery(filter="aliases", sort_by=p[3])

    def p_query_show_references(self, p: yacc.YaccProduction) -> None:
        """query : SHOW REFERENCES sort_clause"""
        p[0] = ShowReferencesQuery(sort_by=p[3])

    def p_query_show_references_type(self, p: yacc.YaccProduction) -> None:
        """query : SHOW REFERENCES IDENTIFIER sort_clause"""
        p[0] = ShowReferencesQuery(type_name=p[3], sort_by=p[4])

    def p_query_dump_graph(self, p: yacc.YaccProduction) -> None:
        """query : DUMP GRAPH"""
        p[0] = DumpGraphQuery()

    def p_query_dump_graph_to(self, p: yacc.YaccProduction) -> None:
        """query : DUMP GRAPH TO STRING"""
        p[0] = DumpGraphQuery(output_file=p[4])

    def p_query_compact_to(self, p: yacc.YaccProduction) -> None:
        """query : COMPACT TO STRING"""
        p[0] = CompactQuery(output_path=p[3])

    def p_query_archive_to(self, p: yacc.YaccProduction) -> None:
        """query : ARCHIVE TO STRING"""
        p[0] = ArchiveQuery(output_file=p[3])

    def p_query_archive(self, p: yacc.YaccProduction) -> None:
        """query : ARCHIVE"""
        p[0] = ArchiveQuery()

    def p_query_restore_to(self, p: yacc.YaccProduction) -> None:
        """query : RESTORE STRING TO STRING"""
        p[0] = RestoreQuery(archive_file=p[2], output_path=p[4])

    def p_query_restore(self, p: yacc.YaccProduction) -> None:
        """query : RESTORE STRING"""
        p[0] = RestoreQuery(archive_file=p[2])

    def p_query_execute(self, p: yacc.YaccProduction) -> None:
        """query : EXECUTE STRING"""
        p[0] = ExecuteQuery(file_path=p[2])

    def p_query_import(self, p: yacc.YaccProduction) -> None:
        """query : IMPORT STRING"""
        p[0] = ImportQuery(file_path=p[2])

    def p_query_describe(self, p: yacc.YaccProduction) -> None:
        """query : DESCRIBE IDENTIFIER sort_clause
                 | DESCRIBE STRING sort_clause"""
        p[0] = DescribeQuery(table=p[2], sort_by=p[3])

    def p_query_describe_variant(self, p: yacc.YaccProduction) -> None:
        """query : DESCRIBE IDENTIFIER DOT IDENTIFIER sort_clause"""
        p[0] = DescribeQuery(table=f"{p[2]}.{p[4]}", sort_by=p[5])

    def p_query_use_none(self, p: yacc.YaccProduction) -> None:
        """query : USE"""
        p[0] = UseQuery(path="")

    def p_query_use_identifier(self, p: yacc.YaccProduction) -> None:
        """query : USE IDENTIFIER"""
        p[0] = UseQuery(path=p[2])

    def p_query_use_string(self, p: yacc.YaccProduction) -> None:
        """query : USE STRING"""
        p[0] = UseQuery(path=p[2])

    def p_query_use_identifier_temp(self, p: yacc.YaccProduction) -> None:
        """query : USE IDENTIFIER AS TEMPORARY"""
        p[0] = UseQuery(path=p[2], temporary=True)

    def p_query_use_string_temp(self, p: yacc.YaccProduction) -> None:
        """query : USE STRING AS TEMPORARY"""
        p[0] = UseQuery(path=p[2], temporary=True)

    def p_query_create_alias(self, p: yacc.YaccProduction) -> None:
        """query : ALIAS IDENTIFIER EQ type_spec"""
        p[0] = CreateAliasQuery(name=p[2], base_type=p[4])

    def p_query_create_type(self, p: yacc.YaccProduction) -> None:
        """query : TYPE IDENTIFIER type_field_list"""
        p[0] = CreateTypeQuery(name=p[2], fields=p[3])

    def p_query_forward_type(self, p: yacc.YaccProduction) -> None:
        """query : FORWARD IDENTIFIER"""
        p[0] = ForwardTypeQuery(name=p[2])

    def p_query_create_type_inherit(self, p: yacc.YaccProduction) -> None:
        """query : TYPE IDENTIFIER FROM parent_list type_field_list"""
        parents = p[4]
        p[0] = CreateTypeQuery(name=p[2], fields=p[5], parents=parents)

    def p_query_create_type_inherit_empty(self, p: yacc.YaccProduction) -> None:
        """query : TYPE IDENTIFIER FROM parent_list"""
        parents = p[4]
        p[0] = CreateTypeQuery(name=p[2], fields=[], parents=parents)

    def p_parent_list_single(self, p: yacc.YaccProduction) -> None:
        """parent_list : IDENTIFIER"""
        p[0] = [p[1]]

    def p_parent_list_multiple(self, p: yacc.YaccProduction) -> None:
        """parent_list : parent_list COMMA IDENTIFIER"""
        p[0] = p[1] + [p[3]]

    def p_query_create_interface(self, p: yacc.YaccProduction) -> None:
        """query : INTERFACE IDENTIFIER type_field_list"""
        p[0] = CreateInterfaceQuery(name=p[2], fields=p[3])

    def p_query_create_interface_empty(self, p: yacc.YaccProduction) -> None:
        """query : INTERFACE IDENTIFIER"""
        p[0] = CreateInterfaceQuery(name=p[2], fields=[])

    def p_query_create_enum(self, p: yacc.YaccProduction) -> None:
        """query : ENUM IDENTIFIER LBRACE enum_variant_list RBRACE
                 | ENUM IDENTIFIER LBRACE enum_variant_list COMMA RBRACE"""
        p[0] = CreateEnumQuery(name=p[2], variants=p[4])

    def p_query_create_enum_backed(self, p: yacc.YaccProduction) -> None:
        """query : ENUM IDENTIFIER COLON IDENTIFIER LBRACE enum_variant_list RBRACE
                 | ENUM IDENTIFIER COLON IDENTIFIER LBRACE enum_variant_list COMMA RBRACE"""
        p[0] = CreateEnumQuery(name=p[2], variants=p[6], backing_type=p[4])

    def p_enum_variant_list_single(self, p: yacc.YaccProduction) -> None:
        """enum_variant_list : enum_variant"""
        p[0] = [p[1]]

    def p_enum_variant_list_multiple(self, p: yacc.YaccProduction) -> None:
        """enum_variant_list : enum_variant_list COMMA enum_variant"""
        p[0] = p[1] + [p[3]]

    def p_enum_variant_bare(self, p: yacc.YaccProduction) -> None:
        """enum_variant : IDENTIFIER"""
        p[0] = EnumVariantSpec(name=p[1])

    def p_enum_variant_value(self, p: yacc.YaccProduction) -> None:
        """enum_variant : IDENTIFIER EQ INTEGER"""
        p[0] = EnumVariantSpec(name=p[1], explicit_value=p[3])

    def p_enum_variant_fields(self, p: yacc.YaccProduction) -> None:
        """enum_variant : IDENTIFIER LPAREN type_field_items RPAREN
                        | IDENTIFIER LPAREN type_field_items COMMA RPAREN"""
        p[0] = EnumVariantSpec(name=p[1], fields=p[3])

    def p_enum_variant_empty_fields(self, p: yacc.YaccProduction) -> None:
        """enum_variant : IDENTIFIER LPAREN RPAREN"""
        p[0] = EnumVariantSpec(name=p[1], fields=[])

    def p_dump_prefix(self, p: yacc.YaccProduction) -> None:
        """dump_prefix : DUMP
                       | DUMP PRETTY
                       | DUMP YAML
                       | DUMP YAML PRETTY
                       | DUMP PRETTY YAML
                       | DUMP JSON
                       | DUMP JSON PRETTY
                       | DUMP PRETTY JSON
                       | DUMP XML
                       | DUMP XML PRETTY
                       | DUMP PRETTY XML
                       | DUMP ARCHIVE
                       | DUMP ARCHIVE PRETTY
                       | DUMP ARCHIVE YAML
                       | DUMP ARCHIVE YAML PRETTY
                       | DUMP ARCHIVE PRETTY YAML
                       | DUMP ARCHIVE JSON
                       | DUMP ARCHIVE JSON PRETTY
                       | DUMP ARCHIVE PRETTY JSON
                       | DUMP ARCHIVE XML
                       | DUMP ARCHIVE XML PRETTY
                       | DUMP ARCHIVE PRETTY XML"""
        # Returns (pretty: bool, format: str, archive: bool)
        tokens = [p[i].lower() if isinstance(p[i], str) else p[i] for i in range(1, len(p))]
        pretty = "pretty" in tokens
        archive = "archive" in tokens
        if "yaml" in tokens:
            fmt = "yaml"
        elif "json" in tokens:
            fmt = "json"
        elif "xml" in tokens:
            fmt = "xml"
        else:
            fmt = "ttq"
        p[0] = (pretty, fmt, archive)

    def p_query_dump(self, p: yacc.YaccProduction) -> None:
        """query : dump_prefix"""
        pretty, fmt, archive = p[1]
        p[0] = DumpQuery(pretty=pretty, format=fmt, include_system=archive)

    def p_query_dump_table(self, p: yacc.YaccProduction) -> None:
        """query : dump_prefix IDENTIFIER
                 | dump_prefix STRING"""
        pretty, fmt, archive = p[1]
        p[0] = DumpQuery(table=p[2], pretty=pretty, format=fmt, include_system=archive)

    def p_query_dump_to(self, p: yacc.YaccProduction) -> None:
        """query : dump_prefix TO STRING"""
        pretty, fmt, archive = p[1]
        p[0] = DumpQuery(output_file=p[3], pretty=pretty, format=fmt, include_system=archive)

    def p_query_dump_table_to(self, p: yacc.YaccProduction) -> None:
        """query : dump_prefix IDENTIFIER TO STRING
                 | dump_prefix STRING TO STRING"""
        pretty, fmt, archive = p[1]
        p[0] = DumpQuery(table=p[2], output_file=p[4], pretty=pretty, format=fmt, include_system=archive)

    def p_query_dump_variable(self, p: yacc.YaccProduction) -> None:
        """query : dump_prefix VARIABLE"""
        pretty, fmt, archive = p[1]
        p[0] = DumpQuery(variable=p[2], pretty=pretty, format=fmt, include_system=archive)

    def p_query_dump_variable_to(self, p: yacc.YaccProduction) -> None:
        """query : dump_prefix VARIABLE TO STRING"""
        pretty, fmt, archive = p[1]
        p[0] = DumpQuery(variable=p[2], output_file=p[4], pretty=pretty, format=fmt, include_system=archive)

    def p_query_dump_list(self, p: yacc.YaccProduction) -> None:
        """query : dump_prefix LBRACKET dump_item_list RBRACKET"""
        pretty, fmt, archive = p[1]
        p[0] = DumpQuery(items=p[3], pretty=pretty, format=fmt, include_system=archive)

    def p_query_dump_list_to(self, p: yacc.YaccProduction) -> None:
        """query : dump_prefix LBRACKET dump_item_list RBRACKET TO STRING"""
        pretty, fmt, archive = p[1]
        p[0] = DumpQuery(items=p[3], output_file=p[6], pretty=pretty, format=fmt, include_system=archive)

    def p_dump_item_list_single(self, p: yacc.YaccProduction) -> None:
        """dump_item_list : dump_item"""
        p[0] = [p[1]]

    def p_dump_item_list_multiple(self, p: yacc.YaccProduction) -> None:
        """dump_item_list : dump_item_list COMMA dump_item"""
        p[0] = p[1] + [p[3]]

    def p_dump_item_table(self, p: yacc.YaccProduction) -> None:
        """dump_item : IDENTIFIER
                     | STRING"""
        p[0] = DumpItem(table=p[1])

    def p_dump_item_variable(self, p: yacc.YaccProduction) -> None:
        """dump_item : VARIABLE"""
        p[0] = DumpItem(variable=p[1])

    def p_query_collect(self, p: yacc.YaccProduction) -> None:
        """query : VARIABLE EQ COLLECT collect_source_list group_clause sort_clause offset_clause limit_clause"""
        p[0] = CollectQuery(var_name=p[1], sources=p[4], group_by=p[5], sort_by=p[6], offset=p[7], limit=p[8])

    def p_collect_source_list_single(self, p: yacc.YaccProduction) -> None:
        """collect_source_list : collect_source"""
        p[0] = [p[1]]

    def p_collect_source_list_multiple(self, p: yacc.YaccProduction) -> None:
        """collect_source_list : collect_source_list COMMA collect_source"""
        p[0] = p[1] + [p[3]]

    def p_collect_source_table(self, p: yacc.YaccProduction) -> None:
        """collect_source : IDENTIFIER where_clause
                          | STRING where_clause"""
        p[0] = CollectSource(table=p[1], where=p[2])

    def p_collect_source_variable(self, p: yacc.YaccProduction) -> None:
        """collect_source : VARIABLE where_clause"""
        p[0] = CollectSource(variable=p[1], where=p[2])

    def p_query_drop_database(self, p: yacc.YaccProduction) -> None:
        """query : DROP IDENTIFIER
                 | DROP STRING"""
        p[0] = DropDatabaseQuery(path=p[2])

    def p_query_drop_current(self, p: yacc.YaccProduction) -> None:
        """query : DROP"""
        p[0] = DropDatabaseQuery()

    def p_query_drop_force(self, p: yacc.YaccProduction) -> None:
        """query : DROP BANG IDENTIFIER
                 | DROP BANG STRING"""
        p[0] = DropDatabaseQuery(path=p[3], force=True)

    def p_query_drop_force_current(self, p: yacc.YaccProduction) -> None:
        """query : DROP BANG"""
        p[0] = DropDatabaseQuery(force=True)

    def p_query_variable_assignment(self, p: yacc.YaccProduction) -> None:
        """query : VARIABLE EQ CREATE IDENTIFIER LPAREN tagged_instance_field_list RPAREN
                 | VARIABLE EQ CREATE IDENTIFIER LPAREN RPAREN"""
        if len(p) == 8:
            tag_name, fields = p[6]
            p[0] = VariableAssignmentQuery(
                var_name=p[1],
                create_query=CreateInstanceQuery(type_name=p[4], fields=fields, tag=tag_name),
            )
        else:
            p[0] = VariableAssignmentQuery(
                var_name=p[1],
                create_query=CreateInstanceQuery(type_name=p[4], fields=[]),
            )

    def p_query_create_instance(self, p: yacc.YaccProduction) -> None:
        """query : CREATE IDENTIFIER LPAREN tagged_instance_field_list RPAREN"""
        tag_name, fields = p[4]
        p[0] = CreateInstanceQuery(type_name=p[2], fields=fields, tag=tag_name)

    def p_query_create_instance_empty(self, p: yacc.YaccProduction) -> None:
        """query : CREATE IDENTIFIER LPAREN RPAREN"""
        p[0] = CreateInstanceQuery(type_name=p[2], fields=[])

    def p_query_delete(self, p: yacc.YaccProduction) -> None:
        """query : DELETE IDENTIFIER WHERE condition
                 | DELETE STRING WHERE condition"""
        p[0] = DeleteQuery(table=p[2], where=p[4])

    def p_query_delete_all(self, p: yacc.YaccProduction) -> None:
        """query : DELETE IDENTIFIER
                 | DELETE STRING"""
        p[0] = DeleteQuery(table=p[2], where=None)

    def p_query_delete_force(self, p: yacc.YaccProduction) -> None:
        """query : DELETE BANG IDENTIFIER WHERE condition
                 | DELETE BANG STRING WHERE condition"""
        p[0] = DeleteQuery(table=p[3], where=p[5], force=True)

    def p_query_delete_force_all(self, p: yacc.YaccProduction) -> None:
        """query : DELETE BANG IDENTIFIER
                 | DELETE BANG STRING"""
        p[0] = DeleteQuery(table=p[3], where=None, force=True)

    def p_query_update_var(self, p: yacc.YaccProduction) -> None:
        """query : UPDATE VARIABLE SET instance_field_list"""
        p[0] = UpdateQuery(type_name="", var_name=p[2], fields=p[4])

    def p_query_update_ref(self, p: yacc.YaccProduction) -> None:
        """query : UPDATE IDENTIFIER LPAREN INTEGER RPAREN SET instance_field_list"""
        p[0] = UpdateQuery(type_name=p[2], index=p[4], fields=p[7])

    def p_query_update_bulk_where(self, p: yacc.YaccProduction) -> None:
        """query : UPDATE IDENTIFIER SET instance_field_list WHERE condition"""
        p[0] = UpdateQuery(type_name=p[2], fields=p[4], where=p[6])

    def p_query_update_bulk_all(self, p: yacc.YaccProduction) -> None:
        """query : UPDATE IDENTIFIER SET instance_field_list"""
        p[0] = UpdateQuery(type_name=p[2], fields=p[4])

    def p_query_scope(self, p: yacc.YaccProduction) -> None:
        """query : SCOPE LBRACE scope_statement_list RBRACE"""
        p[0] = ScopeBlock(statements=p[3])

    def p_scope_statement_list_single(self, p: yacc.YaccProduction) -> None:
        """scope_statement_list : scope_statement"""
        p[0] = [p[1]]

    def p_scope_statement_list_multiple(self, p: yacc.YaccProduction) -> None:
        """scope_statement_list : scope_statement_list scope_statement"""
        p[0] = p[1] + [p[2]]

    def p_scope_statement(self, p: yacc.YaccProduction) -> None:
        """scope_statement : query
                           | query SEMICOLON"""
        p[0] = p[1]

    def p_type_field_list(self, p: yacc.YaccProduction) -> None:
        """type_field_list : LBRACE type_field_items RBRACE
                           | LBRACE type_field_items COMMA RBRACE
                           | LBRACE RBRACE"""
        if len(p) == 3:
            p[0] = []
        else:
            p[0] = p[2]

    def p_type_field_items_single(self, p: yacc.YaccProduction) -> None:
        """type_field_items : type_field_def"""
        p[0] = [p[1]]

    def p_type_field_items_multiple(self, p: yacc.YaccProduction) -> None:
        """type_field_items : type_field_items COMMA type_field_def"""
        p[0] = p[1] + [p[3]]

    # --- type_spec: recursive type specification for fields and aliases ---

    def p_type_spec_identifier(self, p: yacc.YaccProduction) -> None:
        """type_spec : IDENTIFIER"""
        p[0] = p[1]

    def p_type_spec_postfix_array(self, p: yacc.YaccProduction) -> None:
        """type_spec : IDENTIFIER LBRACKET RBRACKET"""
        p[0] = p[1] + "[]"

    def p_type_spec_prefix_array(self, p: yacc.YaccProduction) -> None:
        """type_spec : LBRACKET type_spec RBRACKET"""
        p[0] = ArrayTypeSpec(element_type=p[2])

    def p_type_spec_set(self, p: yacc.YaccProduction) -> None:
        """type_spec : LBRACE type_spec RBRACE"""
        p[0] = SetTypeSpec(element_type=p[2])

    def p_type_spec_dict(self, p: yacc.YaccProduction) -> None:
        """type_spec : LBRACE type_spec COLON type_spec RBRACE"""
        p[0] = DictTypeSpec(key_type=p[2], value_type=p[4])

    def p_type_field_def(self, p: yacc.YaccProduction) -> None:
        """type_field_def : IDENTIFIER COLON type_spec"""
        p[0] = FieldDef(name=p[1], type_name=p[3])

    def p_type_field_def_default(self, p: yacc.YaccProduction) -> None:
        """type_field_def : IDENTIFIER COLON type_spec EQ instance_value"""
        p[0] = FieldDef(name=p[1], type_name=p[3], default_value=p[5])

    def p_type_field_def_overflow(self, p: yacc.YaccProduction) -> None:
        """type_field_def : IDENTIFIER COLON overflow_modifier type_spec"""
        p[0] = FieldDef(name=p[1], type_name=p[4], overflow=p[3])

    def p_type_field_def_overflow_default(self, p: yacc.YaccProduction) -> None:
        """type_field_def : IDENTIFIER COLON overflow_modifier type_spec EQ instance_value"""
        p[0] = FieldDef(name=p[1], type_name=p[4], default_value=p[6], overflow=p[3])

    def p_overflow_modifier(self, p: yacc.YaccProduction) -> None:
        """overflow_modifier : SATURATING
                             | WRAPPING"""
        p[0] = p[1].lower()

    def p_instance_field_list_single(self, p: yacc.YaccProduction) -> None:
        """instance_field_list : instance_field"""
        p[0] = [p[1]]

    def p_instance_field_list_multiple(self, p: yacc.YaccProduction) -> None:
        """instance_field_list : instance_field_list COMMA instance_field"""
        p[0] = p[1] + [p[3]]

    def p_instance_field(self, p: yacc.YaccProduction) -> None:
        """instance_field : IDENTIFIER EQ instance_value"""
        value = p[3]
        if isinstance(value, MethodChainValue):
            if len(value.chain) == 1:
                mc = value.chain[0]
                p[0] = FieldValue(name=p[1], value=value.source_field,
                                  method_name=mc.method_name, method_args=mc.method_args)
            else:
                p[0] = FieldValue(name=p[1], value=value.source_field, method_chain=value.chain)
        else:
            p[0] = FieldValue(name=p[1], value=value)

    def p_instance_field_mutation(self, p: yacc.YaccProduction) -> None:
        """instance_field : mutation_chain"""
        field_name, chain = p[1]
        if len(chain) == 1:
            mc = chain[0]
            p[0] = FieldValue(name=field_name, method_name=mc.method_name, method_args=mc.method_args)
        else:
            p[0] = FieldValue(name=field_name, method_chain=chain)

    # --- mutation_chain: field.method().method() chains in UPDATE SET ---

    def p_mutation_chain_base_no_args(self, p: yacc.YaccProduction) -> None:
        """mutation_chain : IDENTIFIER DOT method_name LPAREN RPAREN"""
        p[0] = (p[1], [MethodCall(method_name=p[3], method_args=[])])

    def p_mutation_chain_base_with_args(self, p: yacc.YaccProduction) -> None:
        """mutation_chain : IDENTIFIER DOT method_name LPAREN method_arg_list RPAREN"""
        p[0] = (p[1], [MethodCall(method_name=p[3], method_args=p[5])])

    def p_mutation_chain_extend_no_args(self, p: yacc.YaccProduction) -> None:
        """mutation_chain : mutation_chain DOT method_name LPAREN RPAREN"""
        field_name, chain = p[1]
        chain.append(MethodCall(method_name=p[3], method_args=[]))
        p[0] = (field_name, chain)

    def p_mutation_chain_extend_with_args(self, p: yacc.YaccProduction) -> None:
        """mutation_chain : mutation_chain DOT method_name LPAREN method_arg_list RPAREN"""
        field_name, chain = p[1]
        chain.append(MethodCall(method_name=p[3], method_args=p[5]))
        p[0] = (field_name, chain)

    def p_method_name_identifier(self, p: yacc.YaccProduction) -> None:
        """method_name : IDENTIFIER"""
        p[0] = p[1]

    def p_method_name_delete(self, p: yacc.YaccProduction) -> None:
        """method_name : DELETE"""
        p[0] = p[1]

    def p_method_name_sort(self, p: yacc.YaccProduction) -> None:
        """method_name : SORT"""
        p[0] = p[1]

    def p_method_arg_list_single(self, p: yacc.YaccProduction) -> None:
        """method_arg_list : method_arg"""
        p[0] = [p[1]]

    def p_method_arg_list_multiple(self, p: yacc.YaccProduction) -> None:
        """method_arg_list : method_arg_list COMMA method_arg"""
        p[0] = p[1] + [p[3]]

    def p_method_arg_value(self, p: yacc.YaccProduction) -> None:
        """method_arg : instance_value"""
        p[0] = p[1]

    def p_method_arg_sort_key_desc(self, p: yacc.YaccProduction) -> None:
        """method_arg : DOT IDENTIFIER DESC"""
        p[0] = SortKeyExpr(field_name=p[2], descending=True)

    def p_method_arg_sort_key_asc(self, p: yacc.YaccProduction) -> None:
        """method_arg : DOT IDENTIFIER ASC"""
        p[0] = SortKeyExpr(field_name=p[2], descending=False)

    def p_method_arg_bare_desc(self, p: yacc.YaccProduction) -> None:
        """method_arg : DESC"""
        p[0] = SortKeyExpr(field_name=None, descending=True)

    def p_method_arg_bare_asc(self, p: yacc.YaccProduction) -> None:
        """method_arg : ASC"""
        p[0] = SortKeyExpr(field_name=None, descending=False)

    def p_tagged_instance_field_list_with_tag(self, p: yacc.YaccProduction) -> None:
        """tagged_instance_field_list : TAG LPAREN IDENTIFIER RPAREN COMMA instance_field_list"""
        p[0] = (p[3], p[6])  # (tag_name, field_list)

    def p_tagged_instance_field_list_no_tag(self, p: yacc.YaccProduction) -> None:
        """tagged_instance_field_list : instance_field_list"""
        p[0] = (None, p[1])

    def p_instance_value_typed_literal(self, p: yacc.YaccProduction) -> None:
        """instance_value : TYPED_INTEGER
                          | TYPED_FLOAT"""
        value, type_name = p[1]
        p[0] = TypedLiteral(value=value, type_name=type_name)

    def p_instance_value_negative_typed(self, p: yacc.YaccProduction) -> None:
        """instance_value : MINUS TYPED_INTEGER
                          | MINUS TYPED_FLOAT"""
        value, type_name = p[2]
        p[0] = TypedLiteral(value=-value, type_name=type_name)

    def p_instance_value_bool(self, p: yacc.YaccProduction) -> None:
        """instance_value : TRUE
                          | FALSE"""
        p[0] = 1 if p[1].lower() == "true" else 0

    def p_instance_value_literal(self, p: yacc.YaccProduction) -> None:
        """instance_value : STRING
                          | INTEGER
                          | FLOAT"""
        p[0] = p[1]

    def p_instance_value_negative(self, p: yacc.YaccProduction) -> None:
        """instance_value : MINUS INTEGER
                          | MINUS FLOAT"""
        p[0] = -p[2]

    def p_instance_value_func(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER LPAREN RPAREN"""
        p[0] = FunctionCall(name=p[1])

    def p_instance_value_func_with_positional_args(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER LPAREN func_positional_args RPAREN"""
        p[0] = FunctionCall(name=p[1], args=p[3])

    def p_func_positional_args_two(self, p: yacc.YaccProduction) -> None:
        """func_positional_args : instance_value COMMA instance_value"""
        p[0] = [p[1], p[3]]

    def p_func_positional_args_extend(self, p: yacc.YaccProduction) -> None:
        """func_positional_args : func_positional_args COMMA instance_value"""
        p[0] = p[1] + [p[3]]

    def p_instance_value_func_single_string(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER LPAREN STRING RPAREN"""
        p[0] = FunctionCall(name=p[1], args=[p[3]])

    def p_instance_value_composite_ref(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER LPAREN INTEGER RPAREN"""
        p[0] = CompositeRef(type_name=p[1], index=p[3])

    def p_instance_value_inline_instance(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER LPAREN tagged_instance_field_list RPAREN"""
        tag_name, fields = p[3]
        p[0] = InlineInstance(type_name=p[1], fields=fields, tag=tag_name)

    def p_instance_value_enum_bare(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER DOT IDENTIFIER"""
        p[0] = EnumValueExpr(enum_name=p[1], variant_name=p[3])

    def p_instance_value_enum_with_args(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER DOT IDENTIFIER LPAREN instance_field_list RPAREN"""
        p[0] = EnumValueExpr(enum_name=p[1], variant_name=p[3], args=p[5])

    def p_instance_value_enum_with_args_empty(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER DOT IDENTIFIER LPAREN RPAREN"""
        p[0] = EnumValueExpr(enum_name=p[1], variant_name=p[3], args=[])

    def p_instance_value_enum_shorthand_bare(self, p: yacc.YaccProduction) -> None:
        """instance_value : DOT IDENTIFIER"""
        p[0] = EnumValueExpr(enum_name=None, variant_name=p[2])

    def p_instance_value_enum_shorthand_with_args(self, p: yacc.YaccProduction) -> None:
        """instance_value : DOT IDENTIFIER LPAREN instance_field_list RPAREN"""
        p[0] = EnumValueExpr(enum_name=None, variant_name=p[2], args=p[4])

    def p_instance_value_enum_shorthand_with_args_empty(self, p: yacc.YaccProduction) -> None:
        """instance_value : DOT IDENTIFIER LPAREN RPAREN"""
        p[0] = EnumValueExpr(enum_name=None, variant_name=p[2], args=[])

    # --- Method chain values (SORT/DELETE keyword methods + positional args + chaining) ---

    def p_instance_value_method_keyword_bare(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER DOT SORT
                          | IDENTIFIER DOT DELETE"""
        p[0] = MethodChainValue(source_field=p[1], chain=[MethodCall(method_name=p[3])])

    def p_instance_value_method_keyword_no_args(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER DOT SORT LPAREN RPAREN
                          | IDENTIFIER DOT DELETE LPAREN RPAREN"""
        p[0] = MethodChainValue(source_field=p[1], chain=[MethodCall(method_name=p[3], method_args=[])])

    def p_instance_value_method_keyword_with_args(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER DOT SORT LPAREN method_arg_list RPAREN
                          | IDENTIFIER DOT DELETE LPAREN method_arg_list RPAREN"""
        p[0] = MethodChainValue(source_field=p[1], chain=[MethodCall(method_name=p[3], method_args=p[5])])

    def p_instance_value_method_positional_args(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER DOT IDENTIFIER LPAREN method_arg_list RPAREN"""
        p[0] = MethodChainValue(source_field=p[1], chain=[MethodCall(method_name=p[3], method_args=p[5])])

    def p_instance_value_chain_extend_no_args(self, p: yacc.YaccProduction) -> None:
        """instance_value : instance_value DOT method_name LPAREN RPAREN"""
        base = p[1]
        if isinstance(base, MethodChainValue):
            base.chain.append(MethodCall(method_name=p[3], method_args=[]))
            p[0] = base
        elif isinstance(base, EnumValueExpr) and base.enum_name is not None:
            p[0] = MethodChainValue(
                source_field=base.enum_name,
                chain=[MethodCall(method_name=base.variant_name, method_args=base.args),
                       MethodCall(method_name=p[3], method_args=[])])
        else:
            raise SyntaxError(f"Cannot chain method call on {type(base).__name__}")

    def p_instance_value_chain_extend_with_args(self, p: yacc.YaccProduction) -> None:
        """instance_value : instance_value DOT method_name LPAREN method_arg_list RPAREN"""
        base = p[1]
        if isinstance(base, MethodChainValue):
            base.chain.append(MethodCall(method_name=p[3], method_args=p[5]))
            p[0] = base
        elif isinstance(base, EnumValueExpr) and base.enum_name is not None:
            p[0] = MethodChainValue(
                source_field=base.enum_name,
                chain=[MethodCall(method_name=base.variant_name, method_args=base.args),
                       MethodCall(method_name=p[3], method_args=p[5])])
        else:
            raise SyntaxError(f"Cannot chain method call on {type(base).__name__}")

    def p_instance_value_tag_reference(self, p: yacc.YaccProduction) -> None:
        """instance_value : IDENTIFIER"""
        p[0] = TagReference(name=p[1])

    def p_instance_value_null(self, p: yacc.YaccProduction) -> None:
        """instance_value : NULL"""
        p[0] = NullValue()

    def p_instance_value_variable(self, p: yacc.YaccProduction) -> None:
        """instance_value : VARIABLE"""
        p[0] = VariableReference(var_name=p[1])

    def p_instance_value_array(self, p: yacc.YaccProduction) -> None:
        """instance_value : LBRACKET array_elements RBRACKET
                          | LBRACKET RBRACKET"""
        if len(p) == 4:
            p[0] = p[2]
        else:
            p[0] = []

    def p_instance_value_empty_braces(self, p: yacc.YaccProduction) -> None:
        """instance_value : LBRACE RBRACE"""
        p[0] = EmptyBraces()

    def p_instance_value_empty_set(self, p: yacc.YaccProduction) -> None:
        """instance_value : LBRACE COMMA RBRACE"""
        p[0] = SetLiteral(elements=[])

    def p_instance_value_empty_dict(self, p: yacc.YaccProduction) -> None:
        """instance_value : LBRACE COLON RBRACE"""
        p[0] = DictLiteral(entries=[])

    def p_instance_value_set(self, p: yacc.YaccProduction) -> None:
        """instance_value : LBRACE set_elements RBRACE"""
        p[0] = SetLiteral(elements=p[2])

    def p_instance_value_set_trailing(self, p: yacc.YaccProduction) -> None:
        """instance_value : LBRACE set_elements COMMA RBRACE"""
        p[0] = SetLiteral(elements=p[2])

    def p_instance_value_dict(self, p: yacc.YaccProduction) -> None:
        """instance_value : LBRACE dict_entries RBRACE"""
        p[0] = DictLiteral(entries=p[2])

    def p_instance_value_dict_trailing(self, p: yacc.YaccProduction) -> None:
        """instance_value : LBRACE dict_entries COMMA RBRACE"""
        p[0] = DictLiteral(entries=p[2])

    def p_set_elements_single(self, p: yacc.YaccProduction) -> None:
        """set_elements : instance_value"""
        p[0] = [p[1]]

    def p_set_elements_multiple(self, p: yacc.YaccProduction) -> None:
        """set_elements : set_elements COMMA instance_value"""
        p[0] = p[1] + [p[3]]

    def p_dict_entries_single(self, p: yacc.YaccProduction) -> None:
        """dict_entries : instance_value COLON instance_value"""
        p[0] = [DictEntry(key=p[1], value=p[3])]

    def p_dict_entries_multiple(self, p: yacc.YaccProduction) -> None:
        """dict_entries : dict_entries COMMA instance_value COLON instance_value"""
        p[0] = p[1] + [DictEntry(key=p[3], value=p[5])]

    def p_array_elements_single(self, p: yacc.YaccProduction) -> None:
        """array_elements : array_element"""
        p[0] = [p[1]]

    def p_array_elements_multiple(self, p: yacc.YaccProduction) -> None:
        """array_elements : array_elements COMMA array_element"""
        p[0] = p[1] + [p[3]]

    def p_array_element_typed(self, p: yacc.YaccProduction) -> None:
        """array_element : TYPED_INTEGER
                         | TYPED_FLOAT"""
        value, type_name = p[1]
        p[0] = TypedLiteral(value=value, type_name=type_name)

    def p_array_element_bool(self, p: yacc.YaccProduction) -> None:
        """array_element : TRUE
                         | FALSE"""
        p[0] = 1 if p[1].lower() == "true" else 0

    def p_array_element(self, p: yacc.YaccProduction) -> None:
        """array_element : STRING
                         | INTEGER
                         | FLOAT"""
        p[0] = p[1]

    def p_array_element_negative(self, p: yacc.YaccProduction) -> None:
        """array_element : MINUS INTEGER
                         | MINUS FLOAT"""
        p[0] = -p[2]

    def p_array_element_inline_instance(self, p: yacc.YaccProduction) -> None:
        """array_element : IDENTIFIER LPAREN tagged_instance_field_list RPAREN"""
        tag_name, fields = p[3]
        p[0] = InlineInstance(type_name=p[1], fields=fields, tag=tag_name)

    def p_array_element_tag_reference(self, p: yacc.YaccProduction) -> None:
        """array_element : IDENTIFIER"""
        p[0] = TagReference(name=p[1])

    def p_array_element_null(self, p: yacc.YaccProduction) -> None:
        """array_element : NULL"""
        p[0] = NullValue()

    def p_array_element_variable(self, p: yacc.YaccProduction) -> None:
        """array_element : VARIABLE"""
        p[0] = VariableReference(var_name=p[1])

    def p_query_eval(self, p: yacc.YaccProduction) -> None:
        """query : eval_expr_list"""
        p[0] = EvalQuery(expressions=p[1])

    def p_eval_expr_list_single(self, p: yacc.YaccProduction) -> None:
        """eval_expr_list : eval_expr_with_alias"""
        p[0] = [p[1]]

    def p_eval_expr_list_multiple(self, p: yacc.YaccProduction) -> None:
        """eval_expr_list : eval_expr_list COMMA eval_expr_with_alias"""
        p[0] = p[1] + [p[3]]

    def p_eval_expr_with_alias(self, p: yacc.YaccProduction) -> None:
        """eval_expr_with_alias : eval_expr NAMED STRING
                                | eval_expr"""
        if len(p) == 4:
            p[0] = (p[1], p[3])  # (expression, alias)
        else:
            p[0] = (p[1], None)  # (expression, no alias)

    def p_eval_expr_typed_literal(self, p: yacc.YaccProduction) -> None:
        """eval_expr : TYPED_INTEGER
                     | TYPED_FLOAT"""
        value, type_name = p[1]
        p[0] = TypedLiteral(value=value, type_name=type_name)

    def p_eval_expr_bool(self, p: yacc.YaccProduction) -> None:
        """eval_expr : TRUE
                     | FALSE"""
        p[0] = 1 if p[1].lower() == "true" else 0

    def p_eval_expr_literal(self, p: yacc.YaccProduction) -> None:
        """eval_expr : STRING
                     | INTEGER
                     | FLOAT"""
        p[0] = p[1]

    def p_eval_expr_func(self, p: yacc.YaccProduction) -> None:
        """eval_expr : IDENTIFIER LPAREN RPAREN"""
        p[0] = FunctionCall(name=p[1])

    def p_eval_expr_func_with_args(self, p: yacc.YaccProduction) -> None:
        """eval_expr : IDENTIFIER LPAREN eval_arg_list RPAREN"""
        p[0] = FunctionCall(name=p[1], args=p[3])

    def p_eval_arg_list_single(self, p: yacc.YaccProduction) -> None:
        """eval_arg_list : eval_expr"""
        p[0] = [p[1]]

    def p_eval_arg_list_multiple(self, p: yacc.YaccProduction) -> None:
        """eval_arg_list : eval_arg_list COMMA eval_expr"""
        p[0] = p[1] + [p[3]]

    def p_eval_expr_array(self, p: yacc.YaccProduction) -> None:
        """eval_expr : LBRACKET eval_expr_items RBRACKET
                     | LBRACKET RBRACKET"""
        p[0] = p[2] if len(p) == 4 else []

    def p_eval_expr_items_single(self, p: yacc.YaccProduction) -> None:
        """eval_expr_items : eval_expr"""
        p[0] = [p[1]]

    def p_eval_expr_items_multiple(self, p: yacc.YaccProduction) -> None:
        """eval_expr_items : eval_expr_items COMMA eval_expr"""
        p[0] = p[1] + [p[3]]

    def p_eval_expr_binary(self, p: yacc.YaccProduction) -> None:
        """eval_expr : eval_expr PLUS eval_expr
                     | eval_expr MINUS eval_expr
                     | eval_expr STAR eval_expr
                     | eval_expr SLASH eval_expr
                     | eval_expr PERCENT eval_expr
                     | eval_expr DOUBLESLASH eval_expr
                     | eval_expr CONCAT eval_expr"""
        p[0] = BinaryExpr(left=p[1], op=p[2], right=p[3])

    def p_eval_expr_unary_minus(self, p: yacc.YaccProduction) -> None:
        """eval_expr : MINUS eval_expr %prec UMINUS"""
        p[0] = UnaryExpr(op="-", operand=p[2])

    def p_eval_expr_unary_plus(self, p: yacc.YaccProduction) -> None:
        """eval_expr : PLUS eval_expr %prec UPLUS"""
        p[0] = UnaryExpr(op="+", operand=p[2])

    def p_eval_expr_method_call(self, p: yacc.YaccProduction) -> None:
        """eval_expr : eval_expr DOT method_name LPAREN RPAREN"""
        p[0] = MethodCallExpr(target=p[1], method_name=p[3])

    def p_eval_expr_method_call_with_args(self, p: yacc.YaccProduction) -> None:
        """eval_expr : eval_expr DOT method_name LPAREN eval_arg_list RPAREN"""
        p[0] = MethodCallExpr(target=p[1], method_name=p[3], method_args=p[5])

    def p_eval_expr_paren(self, p: yacc.YaccProduction) -> None:
        """eval_expr : LPAREN eval_expr RPAREN"""
        p[0] = p[2]

    def p_select_query(self, p: yacc.YaccProduction) -> None:
        """select_query : from_clause select_clause where_clause group_clause sort_clause offset_clause limit_clause"""
        from_val = p[1]
        if isinstance(from_val, VariableReference):
            p[0] = SelectQuery(
                source_var=from_val.var_name,
                fields=p[2],
                where=p[3],
                group_by=p[4],
                sort_by=p[5],
                offset=p[6],
                limit=p[7],
            )
        elif isinstance(from_val, tuple):
            # Variant query: from Type.variant select ...
            type_name, variant_name = from_val
            p[0] = SelectQuery(
                table=type_name,
                variant=variant_name,
                fields=p[2],
                where=p[3],
                group_by=p[4],
                sort_by=p[5],
                offset=p[6],
                limit=p[7],
            )
        else:
            p[0] = SelectQuery(
                table=from_val,
                fields=p[2],
                where=p[3],
                group_by=p[4],
                sort_by=p[5],
                offset=p[6],
                limit=p[7],
            )

    def p_from_clause(self, p: yacc.YaccProduction) -> None:
        """from_clause : FROM IDENTIFIER
                       | FROM STRING"""
        p[0] = p[2]

    def p_from_clause_variant(self, p: yacc.YaccProduction) -> None:
        """from_clause : FROM IDENTIFIER DOT IDENTIFIER"""
        p[0] = (p[2], p[4])  # (type_name, variant_name) tuple

    def p_from_clause_variable(self, p: yacc.YaccProduction) -> None:
        """from_clause : FROM VARIABLE"""
        p[0] = VariableReference(var_name=p[2])

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
        """select_field : field_path"""
        p[0] = SelectField(name=p[1])

    def p_select_field_with_index(self, p: yacc.YaccProduction) -> None:
        """select_field : field_path LBRACKET array_index_item RBRACKET"""
        p[0] = SelectField(name=p[1], array_index=ArrayIndex(index=p[3]))

    def p_select_field_with_index_and_path(self, p: yacc.YaccProduction) -> None:
        """select_field : field_path LBRACKET array_index_item RBRACKET DOT field_path"""
        p[0] = SelectField(
            name=p[1],
            array_index=ArrayIndex(index=p[3]),
            post_path=p[6].split("."),
        )

    # --- method_chain_expr: accumulates chained method calls ---

    # Known aggregate function names
    _AGGREGATE_NAMES = {"count", "average", "sum", "product", "min", "max"}

    def p_method_chain_base_no_args(self, p: yacc.YaccProduction) -> None:
        """method_chain_expr : field_path LPAREN RPAREN"""
        path = p[1]
        parts = path.rsplit(".", 1)
        if len(parts) == 2:
            p[0] = (parts[0], [MethodCall(method_name=parts[1])])
        elif path.lower() in self._AGGREGATE_NAMES:
            p[0] = ("__aggregate__", path.lower(), None)  # (sentinel, name, field)
        else:
            raise SyntaxError(f"Method call requires a field: {path}()")

    def p_method_chain_base_with_args(self, p: yacc.YaccProduction) -> None:
        """method_chain_expr : field_path LPAREN method_arg_list RPAREN"""
        path = p[1]
        parts = path.rsplit(".", 1)
        if len(parts) == 2:
            p[0] = (parts[0], [MethodCall(method_name=parts[1], method_args=p[3])])
        elif path.lower() in self._AGGREGATE_NAMES:
            # Extract field name from first arg (TagReference)
            args = p[3]
            if len(args) == 1 and isinstance(args[0], TagReference):
                p[0] = ("__aggregate__", path.lower(), args[0].name)
            else:
                raise SyntaxError(f"{path}() requires a single field name argument in SELECT")
        else:
            raise SyntaxError(f"Method call requires a field: {path}()")

    def p_method_chain_extend_no_args(self, p: yacc.YaccProduction) -> None:
        """method_chain_expr : method_chain_expr DOT method_name LPAREN RPAREN"""
        field_name, chain = p[1]
        chain.append(MethodCall(method_name=p[3]))
        p[0] = (field_name, chain)

    def p_method_chain_extend_with_args(self, p: yacc.YaccProduction) -> None:
        """method_chain_expr : method_chain_expr DOT method_name LPAREN method_arg_list RPAREN"""
        field_name, chain = p[1]
        chain.append(MethodCall(method_name=p[3], method_args=p[5]))
        p[0] = (field_name, chain)

    # --- SELECT field rules using method_chain_expr ---

    def p_select_field_method_chain(self, p: yacc.YaccProduction) -> None:
        """select_field : method_chain_expr"""
        result = p[1]
        if len(result) == 3 and result[0] == "__aggregate__":
            _, agg_name, field_name = result
            p[0] = SelectField(name=field_name or "*", aggregate=agg_name)
        else:
            field_name, chain = result
            if len(chain) == 1:
                mc = chain[0]
                p[0] = SelectField(name=field_name, method_name=mc.method_name, method_args=mc.method_args)
            else:
                p[0] = SelectField(name=field_name, method_chain=chain)

    def p_select_field_method_call_with_index(self, p: yacc.YaccProduction) -> None:
        """select_field : field_path LBRACKET array_index_item RBRACKET DOT IDENTIFIER LPAREN RPAREN"""
        p[0] = SelectField(
            name=p[1],
            array_index=ArrayIndex(index=p[3]),
            method_name=p[6],
        )

    def p_signed_int(self, p: yacc.YaccProduction) -> None:
        """signed_int : INTEGER"""
        p[0] = p[1]

    def p_signed_int_negative(self, p: yacc.YaccProduction) -> None:
        """signed_int : MINUS INTEGER"""
        p[0] = -p[2]

    def p_array_index_item_single(self, p: yacc.YaccProduction) -> None:
        """array_index_item : signed_int"""
        p[0] = p[1]

    def p_array_index_item_slice_full(self, p: yacc.YaccProduction) -> None:
        """array_index_item : signed_int COLON signed_int"""
        p[0] = ArraySlice(start=p[1], end=p[3])

    def p_array_index_item_slice_start(self, p: yacc.YaccProduction) -> None:
        """array_index_item : signed_int COLON"""
        p[0] = ArraySlice(start=p[1], end=None)

    def p_array_index_item_slice_end(self, p: yacc.YaccProduction) -> None:
        """array_index_item : COLON signed_int"""
        p[0] = ArraySlice(start=None, end=p[2])

    def p_field_path_single(self, p: yacc.YaccProduction) -> None:
        """field_path : IDENTIFIER"""
        p[0] = p[1]

    def p_field_path_dotted(self, p: yacc.YaccProduction) -> None:
        """field_path : field_path DOT IDENTIFIER
                      | field_path DOT SORT
                      | field_path DOT DELETE"""
        p[0] = f"{p[1]}.{p[3]}"

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

    # --- WHERE condition rules using method_chain_expr ---

    def p_condition_method_chain_comparison(self, p: yacc.YaccProduction) -> None:
        """condition : method_chain_expr EQ value
                     | method_chain_expr NEQ value
                     | method_chain_expr LT value
                     | method_chain_expr LTE value
                     | method_chain_expr GT value
                     | method_chain_expr GTE value"""
        if len(p[1]) == 3 and p[1][0] == "__aggregate__":
            raise SyntaxError("Aggregate functions cannot be used in WHERE clauses")
        field_name, chain = p[1]
        op_map = {"=": "eq", "!=": "neq", "<": "lt", "<=": "lte", ">": "gt", ">=": "gte"}
        if len(chain) == 1:
            mc = chain[0]
            p[0] = Condition(field=field_name, operator=op_map[p[2]], value=p[3],
                             method_name=mc.method_name, method_args=mc.method_args)
        else:
            p[0] = Condition(field=field_name, operator=op_map[p[2]], value=p[3],
                             method_chain=chain)

    def p_condition_method_chain_boolean(self, p: yacc.YaccProduction) -> None:
        """condition : method_chain_expr"""
        if len(p[1]) == 3 and p[1][0] == "__aggregate__":
            raise SyntaxError("Aggregate functions cannot be used in WHERE clauses")
        field_name, chain = p[1]
        if len(chain) == 1:
            mc = chain[0]
            p[0] = Condition(field=field_name, operator="eq", value=True,
                             method_name=mc.method_name, method_args=mc.method_args)
        else:
            p[0] = Condition(field=field_name, operator="eq", value=True,
                             method_chain=chain)

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

    def p_value_typed_literal(self, p: yacc.YaccProduction) -> None:
        """value : TYPED_INTEGER
                 | TYPED_FLOAT"""
        value, type_name = p[1]
        p[0] = TypedLiteral(value=value, type_name=type_name)

    def p_value_negative_typed(self, p: yacc.YaccProduction) -> None:
        """value : MINUS TYPED_INTEGER
                 | MINUS TYPED_FLOAT"""
        value, type_name = p[2]
        p[0] = TypedLiteral(value=-value, type_name=type_name)

    def p_value_bool(self, p: yacc.YaccProduction) -> None:
        """value : TRUE
                 | FALSE"""
        p[0] = 1 if p[1].lower() == "true" else 0

    def p_value_integer(self, p: yacc.YaccProduction) -> None:
        """value : INTEGER"""
        p[0] = p[1]

    def p_value_float(self, p: yacc.YaccProduction) -> None:
        """value : FLOAT"""
        p[0] = p[1]

    def p_value_string(self, p: yacc.YaccProduction) -> None:
        """value : STRING"""
        p[0] = p[1]

    def p_value_negative(self, p: yacc.YaccProduction) -> None:
        """value : MINUS INTEGER
                 | MINUS FLOAT"""
        p[0] = -p[2]

    def p_value_null(self, p: yacc.YaccProduction) -> None:
        """value : NULL"""
        p[0] = NullValue()

    def p_value_enum_shorthand_bare(self, p: yacc.YaccProduction) -> None:
        """value : DOT IDENTIFIER"""
        p[0] = EnumValueExpr(enum_name=None, variant_name=p[2])

    def p_value_enum_shorthand_with_args(self, p: yacc.YaccProduction) -> None:
        """value : DOT IDENTIFIER LPAREN instance_field_list RPAREN"""
        p[0] = EnumValueExpr(enum_name=None, variant_name=p[2], args=p[4])

    def p_value_enum_shorthand_with_args_empty(self, p: yacc.YaccProduction) -> None:
        """value : DOT IDENTIFIER LPAREN RPAREN"""
        p[0] = EnumValueExpr(enum_name=None, variant_name=p[2], args=[])

    def p_value_enum_qualified_bare(self, p: yacc.YaccProduction) -> None:
        """value : IDENTIFIER DOT IDENTIFIER"""
        p[0] = EnumValueExpr(enum_name=p[1], variant_name=p[3])

    def p_value_enum_qualified_with_args(self, p: yacc.YaccProduction) -> None:
        """value : IDENTIFIER DOT IDENTIFIER LPAREN instance_field_list RPAREN"""
        p[0] = EnumValueExpr(enum_name=p[1], variant_name=p[3], args=p[5])

    def p_value_enum_qualified_with_args_empty(self, p: yacc.YaccProduction) -> None:
        """value : IDENTIFIER DOT IDENTIFIER LPAREN RPAREN"""
        p[0] = EnumValueExpr(enum_name=p[1], variant_name=p[3], args=[])

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
        """identifier_list : sort_key"""
        p[0] = [p[1]]

    def p_identifier_list_multiple(self, p: yacc.YaccProduction) -> None:
        """identifier_list : identifier_list COMMA sort_key"""
        p[0] = p[1] + [p[3]]

    def p_sort_key_identifier(self, p: yacc.YaccProduction) -> None:
        """sort_key : IDENTIFIER"""
        p[0] = p[1]

    def p_sort_key_reserved(self, p: yacc.YaccProduction) -> None:
        """sort_key : TYPE"""
        p[0] = p[1].lower()

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
        self.parser = yacc.yacc(module=self, start="program", **kwargs)

    def parse(self, data: str) -> Query:
        """Parse a single query string."""
        if self.parser is None:
            self.build(debug=False, write_tables=False)

        results = self.parser.parse(data, lexer=self.lexer.lexer)
        if not results:
            raise SyntaxError("Empty input")
        if len(results) > 1:
            raise SyntaxError("Multiple statements found; use parse_program()")
        return results[0]

    def parse_program(self, data: str) -> list[Query]:
        """Parse multiple statements."""
        if self.parser is None:
            self.build(debug=False, write_tables=False)

        results = self.parser.parse(data, lexer=self.lexer.lexer)
        return results if results else []
