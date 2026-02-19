"""TTG types — result types, config dataclasses, and AST nodes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Union


# ---- Result types (returned to caller) ----


@dataclass
class GraphEdge:
    """A single edge in the graph result."""
    source: str
    label: str
    target: str


@dataclass
class GraphResult:
    """Result of a TTG expression evaluation — edges and isolated nodes."""
    edges: list[GraphEdge] = field(default_factory=list)
    isolated_nodes: list[str] = field(default_factory=list)
    node_kinds: dict[str, str] = field(default_factory=dict)  # node_id → selector/kind
    node_displays: dict[str, str] = field(default_factory=dict)  # node_id → display label override


@dataclass
class FileResult:
    """Result of a TTG expression with file output."""
    path: str
    edge_count: int


@dataclass
class ShowResult:
    """Tabular result from a show command."""
    columns: list[str]
    rows: list[dict[str, str]]


# ---- Config dataclasses (parsed from .ttgc) ----


@dataclass
class GraphConfig:
    """Parsed config from a .ttgc file."""
    selectors: dict[str, str] = field(default_factory=dict)       # name → schema type
    groups: dict[str, list[str]] = field(default_factory=dict)     # name → [selector/group names]
    axes: dict[str, list[str]] = field(default_factory=dict)       # name → [selector.field, ...]
    reverses: dict[str, str] = field(default_factory=dict)         # name → forward axis name
    axis_groups: dict[str, list[str]] = field(default_factory=dict)  # name → [axis names]
    identity: dict[str, str] = field(default_factory=dict)         # "default" or selector → field
    shortcuts: dict[str, str] = field(default_factory=dict)        # name → raw TTG expression string


# ---- Predicate value types ----


@dataclass
class NameTerm:
    """A single term in a name matching expression."""
    negated: bool
    name: str  # simple identifier, or None if grouped


@dataclass
class GroupedNameTerm:
    """A grouped name term: !(expr) or (expr)."""
    negated: bool
    expr: NamePred


@dataclass
class NamePred:
    """Name matching predicate — OR'd terms."""
    terms: list[Union[NameTerm, GroupedNameTerm]]


@dataclass
class AxisPathPred:
    """Axis path predicate value (e.g., .name, .fields.name)."""
    steps: list[str]  # ["name"] or ["fields", "name"]


@dataclass
class JoinPred:
    """Join aggregation predicate value."""
    separator: str
    path: AxisPathPred | None = None  # Single path (legacy)
    paths: list[AxisPathPred] = field(default_factory=list)  # Multi-path: join(".", .a, .b)


@dataclass
class IntPred:
    """Integer predicate value."""
    value: int


@dataclass
class InfPred:
    """Infinity predicate value (for depth=inf)."""
    pass


@dataclass
class BoolPred:
    """Boolean predicate value."""
    value: bool


@dataclass
class StringPred:
    """String literal predicate value."""
    value: str


PredValue = Union[NamePred, AxisPathPred, JoinPred, IntPred, InfPred, BoolPred, StringPred]


# ---- Expression AST nodes ----


@dataclass
class SelectorExpr:
    """A selector choosing nodes by kind, optionally filtered."""
    name: str
    predicates: dict[str, PredValue] | None = None


@dataclass
class SetExpr:
    """A set literal: {expr, expr, ...}."""
    members: list[Expr]


@dataclass
class ParenExpr:
    """A parenthesized expression."""
    expr: Expr


@dataclass
class AxisRef:
    """A single axis reference with optional predicates."""
    name: str
    predicates: dict[str, PredValue] | None = None


@dataclass
class DotExpr:
    """Dot chaining: base.axis1.axis2 — replaces current set with targets."""
    base: Expr
    axes: list[AxisRef]  # chain of axes: .a.b.c → [a, b, c]


@dataclass
class CompoundAxisOperand:
    """Compound axis set: {.axis1, .axis2}."""
    axes: list[AxisRef]


@dataclass
class SingleAxisOperand:
    """Single axis chain: .axis1.axis2."""
    axes: list[AxisRef]


AxisOperand = Union[CompoundAxisOperand, SingleAxisOperand]


@dataclass
class ChainOp:
    """A chain operation: + axis, / axis, or - axis/atom."""
    op: str  # "+", "/", "-"
    operand: Union[AxisOperand, Expr]  # AxisOperand for +//, Expr or AxisOperand for -


@dataclass
class ChainExpr:
    """Chain expression: base + .axis / .axis - selector."""
    base: Expr
    ops: list[ChainOp]


@dataclass
class UnionExpr:
    """Union of two expressions: left | right."""
    left: Expr
    right: Expr


@dataclass
class IntersectExpr:
    """Intersection of two expressions: left & right."""
    left: Expr
    right: Expr


# Union of all expression types
Expr = Union[
    SelectorExpr, SetExpr, ParenExpr, DotExpr,
    ChainExpr, UnionExpr, IntersectExpr,
]


# ---- Statement AST nodes ----


@dataclass
class ConfigStmt:
    """graph config "file.ttgc" """
    file_path: str


@dataclass
class MetaConfigStmt:
    """graph metadata config "file.ttgc" """
    file_path: str


@dataclass
class StyleStmt:
    """graph style "file" [{...}] or graph style {...}."""
    file_path: str | None = None
    inline: list[tuple[str, str]] | None = None


@dataclass
class MetaStyleStmt:
    """graph metadata style "file" [{...}] or graph metadata style {...}."""
    file_path: str | None = None
    inline: list[tuple[str, str]] | None = None


@dataclass
class ExecuteStmt:
    """graph execute "file.ttg" """
    file_path: str


@dataclass
class ExprStmt:
    """An expression statement with optional metadata prefix, sort, and output."""
    metadata: bool = False  # True if "metadata" prefix
    expression: Expr | None = None
    sort_by: list[str] = field(default_factory=list)  # ["source", "label", "target"]
    output_file: str | None = None


@dataclass
class ShowStmt:
    """show <category> [<name>]"""
    category: str | None  # None = list categories; else "selector", "group", etc.
    name: str | None    # None = list all, str = single entry
    metadata: bool      # True if prefixed with "metadata"


# Union of all statement types
Stmt = Union[ConfigStmt, MetaConfigStmt, StyleStmt, MetaStyleStmt, ExecuteStmt, ExprStmt, ShowStmt]
