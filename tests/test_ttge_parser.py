"""Tests for the TTGE expression/statement parser."""

import pytest

from typed_tables.ttge.ttge_parser import TTGEParser
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


@pytest.fixture
def parser():
    p = TTGEParser()
    p.build(debug=False, write_tables=False)
    return p


# ---- Statement parsing ----


class TestConfigStmt:
    def test_config(self, parser):
        r = parser.parse('config "meta-schema.ttgc"')
        assert isinstance(r, ConfigStmt)
        assert r.file_path == "meta-schema.ttgc"

    def test_meta_config(self, parser):
        r = parser.parse('metadata config "meta-schema.ttgc"')
        assert isinstance(r, MetaConfigStmt)
        assert r.file_path == "meta-schema.ttgc"

    def test_execute(self, parser):
        r = parser.parse('execute "setup.ttg"')
        assert isinstance(r, ExecuteStmt)
        assert r.file_path == "setup.ttg"


class TestStyleStmt:
    def test_style_file(self, parser):
        r = parser.parse('style "dark.ttgs"')
        assert isinstance(r, StyleStmt)
        assert r.file_path == "dark.ttgs"
        assert r.inline is None

    def test_style_file_inline(self, parser):
        r = parser.parse('style "base.ttgs" {"direction": "TB"}')
        assert isinstance(r, StyleStmt)
        assert r.file_path == "base.ttgs"
        assert r.inline == [("direction", "TB")]

    def test_style_inline_only(self, parser):
        r = parser.parse('style {"direction": "LR", "composite.color": "#FF0000"}')
        assert isinstance(r, StyleStmt)
        assert r.file_path is None
        assert r.inline == [("direction", "LR"), ("composite.color", "#FF0000")]

    def test_meta_style_file(self, parser):
        r = parser.parse('metadata style "dark.ttgs"')
        assert isinstance(r, MetaStyleStmt)
        assert r.file_path == "dark.ttgs"

    def test_meta_style_inline(self, parser):
        r = parser.parse('metadata style {"direction": "TB"}')
        assert isinstance(r, MetaStyleStmt)
        assert r.inline == [("direction", "TB")]


# ---- Expression statement with sort/output ----


class TestExprStmt:
    def test_bare_expression(self, parser):
        r = parser.parse("composites")
        assert isinstance(r, ExprStmt)
        assert isinstance(r.expression, SelectorExpr)
        assert r.sort_by == []
        assert r.output_file is None
        assert r.metadata is False

    def test_with_sort(self, parser):
        r = parser.parse("composites sort by source")
        assert isinstance(r, ExprStmt)
        assert r.sort_by == ["source"]

    def test_with_multi_sort(self, parser):
        r = parser.parse("composites sort by source, label, target")
        assert isinstance(r, ExprStmt)
        assert r.sort_by == ["source", "label", "target"]

    def test_with_output(self, parser):
        r = parser.parse('composites > "types.dot"')
        assert isinstance(r, ExprStmt)
        assert r.output_file == "types.dot"

    def test_with_sort_and_output(self, parser):
        r = parser.parse('composites sort by source > "types.dot"')
        assert isinstance(r, ExprStmt)
        assert r.sort_by == ["source"]
        assert r.output_file == "types.dot"

    def test_metadata_prefix(self, parser):
        r = parser.parse("metadata composites")
        assert isinstance(r, ExprStmt)
        assert r.metadata is True
        assert isinstance(r.expression, SelectorExpr)


# ---- Selector expressions ----


class TestSelectorExpr:
    def test_bare_selector(self, parser):
        r = parser.parse("composites")
        assert isinstance(r, ExprStmt)
        sel = r.expression
        assert isinstance(sel, SelectorExpr)
        assert sel.name == "composites"
        assert sel.predicates is None

    def test_selector_with_pred(self, parser):
        r = parser.parse("composites{name=Person}")
        sel = r.expression
        assert isinstance(sel, SelectorExpr)
        assert sel.name == "composites"
        assert "name" in sel.predicates
        pred = sel.predicates["name"]
        assert isinstance(pred, NamePred)
        assert len(pred.terms) == 1
        assert pred.terms[0].name == "Person"

    def test_selector_name_or(self, parser):
        r = parser.parse("composites{name=Person|Employee}")
        sel = r.expression
        pred = sel.predicates["name"]
        assert isinstance(pred, NamePred)
        assert len(pred.terms) == 2
        assert pred.terms[0].name == "Person"
        assert pred.terms[1].name == "Employee"

    def test_selector_name_negated(self, parser):
        r = parser.parse("composites{name=!Root}")
        sel = r.expression
        pred = sel.predicates["name"]
        term = pred.terms[0]
        assert isinstance(term, NameTerm)
        assert term.negated is True
        assert term.name == "Root"

    def test_selector_name_grouped_negated(self, parser):
        r = parser.parse("composites{name=!(Root|Base)}")
        sel = r.expression
        pred = sel.predicates["name"]
        term = pred.terms[0]
        assert isinstance(term, GroupedNameTerm)
        assert term.negated is True
        assert len(term.expr.terms) == 2

    def test_selector_multiple_preds(self, parser):
        r = parser.parse('composites{name=Person, depth=2}')
        sel = r.expression
        assert "name" in sel.predicates
        assert "depth" in sel.predicates
        assert isinstance(sel.predicates["depth"], IntPred)
        assert sel.predicates["depth"].value == 2


# ---- Predicate values ----


class TestPredValues:
    def test_integer_pred(self, parser):
        r = parser.parse("composites{depth=3}")
        assert isinstance(r.expression.predicates["depth"], IntPred)
        assert r.expression.predicates["depth"].value == 3

    def test_inf_pred(self, parser):
        r = parser.parse("composites{depth=inf}")
        assert isinstance(r.expression.predicates["depth"], InfPred)

    def test_infinity_pred(self, parser):
        r = parser.parse("composites{depth=infinity}")
        assert isinstance(r.expression.predicates["depth"], InfPred)

    def test_bool_true_pred(self, parser):
        r = parser.parse("fields{declared=true}")
        assert isinstance(r.expression.predicates["declared"], BoolPred)
        assert r.expression.predicates["declared"].value is True

    def test_bool_false_pred(self, parser):
        r = parser.parse("fields{declared=false}")
        assert isinstance(r.expression.predicates["declared"], BoolPred)
        assert r.expression.predicates["declared"].value is False

    def test_string_pred(self, parser):
        r = parser.parse('composites{label="hello"}')
        assert isinstance(r.expression.predicates["label"], StringPred)
        assert r.expression.predicates["label"].value == "hello"

    def test_axis_path_pred(self, parser):
        r = parser.parse("composites{label=.name}")
        pred = r.expression.predicates["label"]
        assert isinstance(pred, AxisPathPred)
        assert pred.steps == ["name"]

    def test_axis_path_multi_step(self, parser):
        r = parser.parse("composites{result=.fields.type}")
        pred = r.expression.predicates["result"]
        assert isinstance(pred, AxisPathPred)
        assert pred.steps == ["fields", "type"]

    def test_join_pred(self, parser):
        r = parser.parse('composites{label=join(", ", .fields.name)}')
        pred = r.expression.predicates["label"]
        assert isinstance(pred, JoinPred)
        assert pred.separator == ", "
        assert pred.path.steps == ["fields", "name"]


# ---- Dot expressions ----


class TestDotExpr:
    def test_single_dot(self, parser):
        r = parser.parse("composites.fields")
        expr = r.expression
        assert isinstance(expr, DotExpr)
        assert isinstance(expr.base, SelectorExpr)
        assert expr.base.name == "composites"
        assert len(expr.axes) == 1
        assert expr.axes[0].name == "fields"

    def test_multi_dot(self, parser):
        r = parser.parse("composites.fields.type")
        expr = r.expression
        assert isinstance(expr, DotExpr)
        assert len(expr.axes) == 2
        assert expr.axes[0].name == "fields"
        assert expr.axes[1].name == "type"

    def test_dot_with_pred(self, parser):
        r = parser.parse("composites.fields{name=age}")
        expr = r.expression
        assert isinstance(expr, DotExpr)
        assert expr.axes[0].predicates is not None
        assert "name" in expr.axes[0].predicates


# ---- Chain expressions ----


class TestChainExpr:
    def test_chain_plus(self, parser):
        r = parser.parse("composites + .fields")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        assert isinstance(expr.base, SelectorExpr)
        assert len(expr.ops) == 1
        assert expr.ops[0].op == "+"
        assert isinstance(expr.ops[0].operand, SingleAxisOperand)
        assert expr.ops[0].operand.axes[0].name == "fields"

    def test_chain_multi_plus(self, parser):
        r = parser.parse("composites + .fields + .type")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        assert len(expr.ops) == 2
        assert expr.ops[0].op == "+"
        assert expr.ops[1].op == "+"

    def test_chain_slash(self, parser):
        r = parser.parse("composites / .fields")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        assert expr.ops[0].op == "/"

    def test_chain_minus_axis(self, parser):
        r = parser.parse("composites - .fields")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        assert expr.ops[0].op == "-"
        assert isinstance(expr.ops[0].operand, SingleAxisOperand)

    def test_chain_minus_atom(self, parser):
        r = parser.parse("composites - interfaces")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        assert expr.ops[0].op == "-"
        assert isinstance(expr.ops[0].operand, SelectorExpr)

    def test_chain_plus_with_pred(self, parser):
        r = parser.parse("composites + .fields{label=.name, result=.type}")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        axis = expr.ops[0].operand.axes[0]
        assert axis.predicates is not None
        assert "label" in axis.predicates
        assert "result" in axis.predicates

    def test_chain_plus_multi_axis(self, parser):
        r = parser.parse("composites + .fields.type")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        axis_op = expr.ops[0].operand
        assert isinstance(axis_op, SingleAxisOperand)
        assert len(axis_op.axes) == 2


# ---- Compound axis ----


class TestCompoundAxis:
    def test_compound(self, parser):
        r = parser.parse("composites + {.fields, .extends, .interfaces}")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        axis_op = expr.ops[0].operand
        assert isinstance(axis_op, CompoundAxisOperand)
        assert len(axis_op.axes) == 3
        assert axis_op.axes[0].name == "fields"
        assert axis_op.axes[1].name == "extends"
        assert axis_op.axes[2].name == "interfaces"


# ---- Set expressions ----


class TestSetExpr:
    def test_set_literal(self, parser):
        r = parser.parse("{composites, interfaces}")
        expr = r.expression
        assert isinstance(expr, SetExpr)
        assert len(expr.members) == 2

    def test_set_with_preds(self, parser):
        r = parser.parse("{composites{name=Person}, interfaces{name=Sizeable}}")
        expr = r.expression
        assert isinstance(expr, SetExpr)
        assert len(expr.members) == 2


# ---- Union and intersection ----


class TestSetOperators:
    def test_union(self, parser):
        r = parser.parse("composites | interfaces")
        expr = r.expression
        assert isinstance(expr, UnionExpr)

    def test_intersection(self, parser):
        r = parser.parse("composites & interfaces")
        expr = r.expression
        assert isinstance(expr, IntersectExpr)

    def test_precedence_union_intersect(self, parser):
        """& binds tighter than |."""
        r = parser.parse("a | b & c")
        expr = r.expression
        assert isinstance(expr, UnionExpr)
        assert isinstance(expr.right, IntersectExpr)

    def test_precedence_chain_binds_tighter(self, parser):
        """+ binds tighter than &."""
        r = parser.parse("a + .b & c")
        expr = r.expression
        assert isinstance(expr, IntersectExpr)
        assert isinstance(expr.left, ChainExpr)
        assert isinstance(expr.right, SelectorExpr)


# ---- Parenthesized expressions ----


class TestParenExpr:
    def test_paren(self, parser):
        r = parser.parse("(composites | interfaces) + .fields")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        assert isinstance(expr.base, ParenExpr)
        assert isinstance(expr.base.expr, UnionExpr)


# ---- Depth predicate ----


class TestDepthPred:
    def test_depth_integer(self, parser):
        r = parser.parse("composites + .extends{depth=2}")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        axis = expr.ops[0].operand.axes[0]
        assert axis.predicates["depth"].value == 2

    def test_depth_inf(self, parser):
        r = parser.parse("composites + .extends{depth=inf}")
        expr = r.expression
        axis = expr.ops[0].operand.axes[0]
        assert isinstance(axis.predicates["depth"], InfPred)


# ---- Complex expressions from design doc ----


class TestComplexExpressions:
    def test_path_to_pattern(self, parser):
        """Path-to via intersection from the design doc."""
        r = parser.parse(
            "composites{name=Boss} + .all{depth=inf} "
            "& interfaces{name=Entity} + .allReverse{depth=inf}"
        )
        expr = r.expression
        assert isinstance(expr, IntersectExpr)
        assert isinstance(expr.left, ChainExpr)
        assert isinstance(expr.right, ChainExpr)

    def test_compact_field_type_edges(self, parser):
        """Compact form: field dissolved into edge label."""
        r = parser.parse("composites + .fields{label=.name, result=.type}")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        axis = expr.ops[0].operand.axes[0]
        assert isinstance(axis.predicates["label"], AxisPathPred)
        assert isinstance(axis.predicates["result"], AxisPathPred)

    def test_full_overview_shortcut(self, parser):
        """The empty shortcut expression from meta-schema.ttgc."""
        r = parser.parse("types + .fields{label=.name, result=.type} + .extends + .interfaces")
        expr = r.expression
        assert isinstance(expr, ChainExpr)
        assert expr.base.name == "types"
        assert len(expr.ops) == 3

    def test_union_with_expansion(self, parser):
        """Union of path-to with target expansion."""
        r = parser.parse(
            "(composites{name=Boss} + .all{depth=inf} "
            "& interfaces{name=Entity} + .allReverse{depth=inf}) "
            "| (interfaces{name=Entity} + .fields{label=.name, result=.type})"
        )
        expr = r.expression
        assert isinstance(expr, UnionExpr)


# ---- Program parsing (multiple statements) ----


class TestProgramParsing:
    def test_multi_statements(self, parser):
        stmts = parser.parse_program(
            'config "meta.ttgc"; composites + .fields'
        )
        assert len(stmts) == 2
        assert isinstance(stmts[0], ConfigStmt)
        assert isinstance(stmts[1], ExprStmt)

    def test_with_comments(self, parser):
        stmts = parser.parse_program(
            "-- Load config\n"
            'config "meta.ttgc";\n'
            "-- Show composites\n"
            "composites\n"
        )
        assert len(stmts) == 2
