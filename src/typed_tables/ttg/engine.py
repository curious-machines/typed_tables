"""TTG engine — evaluates TTG statements against a database."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from typed_tables.ttg.types import (
    AxisPathPred,
    AxisRef,
    BoolPred,
    ChainExpr,
    ChainOp,
    CompoundAxisOperand,
    ConfigStmt,
    DotExpr,
    ExprStmt,
    ExecuteStmt,
    Expr,
    FileResult,
    GraphConfig,
    GraphEdge,
    GraphResult,
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
    ShowResult,
    ShowStmt,
    SingleAxisOperand,
    Stmt,
    StringPred,
    StyleStmt,
    UnionExpr,
)


# ---- Internal result set ----

@dataclass
class ResultSet:
    """Internal evaluation state: set of nodes + set of edges."""
    nodes: set[str] = field(default_factory=set)
    edges: set[tuple[str, str, str]] = field(default_factory=set)  # (source, label, target)

    def union(self, other: ResultSet) -> ResultSet:
        return ResultSet(
            nodes=self.nodes | other.nodes,
            edges=self.edges | other.edges,
        )

    def intersect(self, other: ResultSet) -> ResultSet:
        shared_nodes = self.nodes & other.nodes
        # Keep edges where both endpoints survive
        shared_edges = {
            e for e in self.edges | other.edges
            if e[0] in shared_nodes and e[2] in shared_nodes
        }
        return ResultSet(nodes=shared_nodes, edges=shared_edges)

    def subtract_nodes(self, to_remove: set[str]) -> ResultSet:
        remaining = self.nodes - to_remove
        pruned_edges = {
            e for e in self.edges
            if e[0] in remaining and e[2] in remaining
        }
        return ResultSet(nodes=remaining, edges=pruned_edges)

    def to_graph_result(
        self, sort_by: list[str] | None = None, node_kinds: dict[str, str] | None = None
    ) -> GraphResult:
        edges = [GraphEdge(source=s, label=l, target=t) for s, l, t in self.edges]
        if sort_by:
            def sort_key(e: GraphEdge) -> tuple:
                return tuple(getattr(e, k, "") for k in sort_by)
            edges.sort(key=sort_key)
        # Isolated nodes: in node set but not any edge endpoint
        edge_nodes = {e.source for e in edges} | {e.target for e in edges}
        isolated = sorted(self.nodes - edge_nodes)
        # Filter node_kinds to only include nodes in this result
        all_nodes = self.nodes | edge_nodes
        kinds = {}
        if node_kinds:
            kinds = {n: node_kinds[n] for n in all_nodes if n in node_kinds}
        return GraphResult(edges=edges, isolated_nodes=isolated, node_kinds=kinds)


class TTGEngine:
    """TTG expression engine — config, style, session state, evaluation."""

    def __init__(self, storage: Any, registry: Any) -> None:
        self.storage = storage
        self.registry = registry

        # Lazy-initialized parsers and providers
        self._parser: Any = None
        self._ttgc_parser: Any = None
        self._meta_provider: Any = None

        # Session state — two contexts (data and metadata)
        self._data_config: GraphConfig | None = None
        self._data_style: dict[str, str] = {}
        self._meta_config: GraphConfig | None = None
        self._meta_style: dict[str, str] = {}

        # Script execution state
        self._script_stack: list[Path] = []
        self._loaded_scripts: set[str] = set()

        # Load built-in meta-schema config
        self._meta_config = self._load_builtin_meta_config()

    # ---- Public API ----

    def execute(self, raw_text: str) -> GraphResult | FileResult | str:
        """Execute a single TTG statement from raw text."""
        parser = self._get_parser()
        stmt = parser.parse(raw_text)
        if stmt is None:
            raise SyntaxError(f"TTG: failed to parse: {raw_text!r}")
        return self._execute_stmt(stmt)

    def execute_stmt(self, stmt: Stmt) -> GraphResult | FileResult | str:
        """Execute a pre-parsed TTG statement."""
        return self._execute_stmt(stmt)

    def reset_session(self) -> None:
        """Clear all session state (called on database switch)."""
        self._data_config = None
        self._data_style = {}
        self._meta_config = self._load_builtin_meta_config()
        self._meta_style = {}
        self._meta_provider = None
        self._script_stack = []
        self._loaded_scripts = set()

    # ---- Statement dispatch ----

    def _execute_stmt(self, stmt: Stmt) -> GraphResult | FileResult | ShowResult | str:
        if isinstance(stmt, ConfigStmt):
            return self._execute_config(stmt)
        elif isinstance(stmt, MetaConfigStmt):
            return self._execute_meta_config(stmt)
        elif isinstance(stmt, StyleStmt):
            return self._execute_style(stmt)
        elif isinstance(stmt, MetaStyleStmt):
            return self._execute_meta_style(stmt)
        elif isinstance(stmt, ExecuteStmt):
            return self._execute_execute(stmt)
        elif isinstance(stmt, ShowStmt):
            return self._execute_show(stmt)
        elif isinstance(stmt, ExprStmt):
            return self._execute_expr(stmt)
        else:
            raise ValueError(f"TTG: unknown statement type: {type(stmt).__name__}")

    # ---- Config commands ----

    def _execute_config(self, stmt: ConfigStmt) -> str:
        config = self._load_config_file(stmt.file_path)
        self._data_config = config
        return f"TTG: loaded config '{stmt.file_path}'"

    def _execute_meta_config(self, stmt: MetaConfigStmt) -> str:
        config = self._load_config_file(stmt.file_path)
        self._meta_config = config
        self._meta_provider = None  # Invalidate cache
        return f"TTG: loaded metadata config '{stmt.file_path}'"

    def _execute_show(self, stmt: ShowStmt) -> ShowResult:
        """Execute a show command — list or look up config entries."""
        if stmt.metadata:
            config = self._meta_config
        else:
            config = self._data_config
        if config is None:
            if stmt.metadata:
                raise RuntimeError("TTG: no metadata config loaded")
            else:
                raise RuntimeError(
                    "TTG: no data config loaded. Use 'graph config \"file.ttgc\"' first."
                )

        category = stmt.category
        name = stmt.name

        if category is None:
            # List available categories with their entry counts
            categories = [
                ("selector", len(config.selectors)),
                ("group", len(config.groups)),
                ("axis", len(config.axes)),
                ("reverse", len(config.reverses)),
                ("axis_group", len(config.axis_groups)),
                ("identity", len(config.identity)),
                ("shortcut", len(config.shortcuts)),
            ]
            return ShowResult(
                columns=["category", "entries"],
                rows=[{"category": c, "entries": str(n)} for c, n in categories],
            )

        # Map category to config attribute and column definitions
        if category == "selector":
            data = config.selectors
            columns = ["name", "type"]
            def format_entry(k: str, v: str) -> dict[str, str]:
                return {"name": k, "type": v}
        elif category == "group":
            data = config.groups
            columns = ["name", "members"]
            def format_entry(k: str, v: list[str]) -> dict[str, str]:
                return {"name": k, "members": ", ".join(v)}
        elif category == "axis":
            data = config.axes
            columns = ["name", "paths"]
            def format_entry(k: str, v: list[str]) -> dict[str, str]:
                return {"name": k, "paths": ", ".join(v)}
        elif category == "reverse":
            data = config.reverses
            columns = ["name", "axis"]
            def format_entry(k: str, v: str) -> dict[str, str]:
                return {"name": k, "axis": v}
        elif category == "axis_group":
            data = config.axis_groups
            columns = ["name", "axes"]
            def format_entry(k: str, v: list[str]) -> dict[str, str]:
                return {"name": k, "axes": ", ".join(v)}
        elif category == "identity":
            data = config.identity
            columns = ["selector", "field"]
            def format_entry(k: str, v: str) -> dict[str, str]:
                return {"selector": k, "field": v}
        elif category == "shortcut":
            data = config.shortcuts
            columns = ["name", "expression"]
            def format_entry(k: str, v: str) -> dict[str, str]:
                return {"name": k, "expression": v}
        else:
            raise RuntimeError(f"TTG: unknown show category '{category}'")

        if name is not None:
            if name not in data:
                raise RuntimeError(f"TTG: {category} '{name}' not found")
            rows = [format_entry(name, data[name])]
        else:
            rows = [format_entry(k, v) for k, v in data.items()]

        return ShowResult(columns=columns, rows=rows)

    def _load_config_file(self, file_path: str) -> GraphConfig:
        resolved = self._resolve_path(file_path)
        if not os.path.exists(resolved):
            raise FileNotFoundError(f"TTG: config file not found: {resolved}")
        with open(resolved, "r") as f:
            text = f.read()
        parser = self._get_ttgc_parser()
        config = parser.parse(text)
        if config is None:
            raise SyntaxError(f"TTG: failed to parse config: {resolved}")
        return config

    # ---- Style commands ----

    def _execute_style(self, stmt: StyleStmt) -> str:
        self._apply_style(stmt.file_path, stmt.inline, self._data_style, "data")
        return "TTG: style updated"

    def _execute_meta_style(self, stmt: MetaStyleStmt) -> str:
        self._apply_style(stmt.file_path, stmt.inline, self._meta_style, "metadata")
        return "TTG: metadata style updated"

    def _apply_style(
        self,
        file_path: str | None,
        inline: list[tuple[str, str]] | None,
        target: dict[str, str],
        context_name: str,
    ) -> None:
        if file_path is not None:
            resolved = self._resolve_path(file_path)
            if not os.path.exists(resolved):
                raise FileNotFoundError(f"TTG: style file not found: {resolved}")
            for key, value in self._parse_style_file(resolved):
                target[key] = value
        if inline is not None:
            for key, value in inline:
                target[key] = value

    def _parse_style_file(self, path: str) -> list[tuple[str, str]]:
        with open(path, "r") as f:
            text = f.read()
        lines = []
        for line in text.split("\n"):
            stripped = line.split("--")[0]
            lines.append(stripped)
        clean = "\n".join(lines).strip()
        if not clean.startswith("{") or not clean.endswith("}"):
            raise SyntaxError(f"TTG: style file must contain a dict literal: {path}")
        parser = self._get_parser()
        return parser._parse_dict_literal(clean)

    # ---- Execute command (scripts) ----

    def _execute_execute(self, stmt: ExecuteStmt) -> str:
        resolved = self._resolve_path(stmt.file_path)
        if not os.path.exists(resolved):
            if not resolved.endswith(".ttg") and not resolved.endswith(".ttg.gz"):
                for ext in (".ttg", ".ttg.gz"):
                    candidate = resolved + ext
                    if os.path.exists(candidate):
                        resolved = candidate
                        break
        if not os.path.exists(resolved):
            raise FileNotFoundError(f"TTG: script not found: {resolved}")
        abs_path = os.path.abspath(resolved)
        if abs_path in self._loaded_scripts:
            raise RuntimeError(f"TTG: script already loaded (cycle): {resolved}")
        self._loaded_scripts.add(abs_path)
        self._script_stack.append(Path(resolved).parent)
        try:
            if resolved.endswith(".gz"):
                import gzip
                with gzip.open(resolved, "rt") as f:
                    text = f.read()
            else:
                with open(resolved, "r") as f:
                    text = f.read()
            parser = self._get_parser()
            stmts = parser.parse_program(text)
            for s in stmts:
                self._execute_stmt(s)
            return f"TTG: executed '{stmt.file_path}'"
        finally:
            self._script_stack.pop()

    # ---- Expression evaluation ----

    def _execute_expr(self, stmt: ExprStmt) -> GraphResult | FileResult | str:
        """Evaluate a TTG expression statement."""
        if stmt.metadata:
            config = self._meta_config
            provider = self._get_meta_provider()
        else:
            config = self._data_config
            provider = self._get_meta_provider()  # For now, always use meta provider

        if config is None:
            if not stmt.metadata and self._meta_config is not None:
                config = self._meta_config
            else:
                context_name = "metadata" if stmt.metadata else "data"
                raise RuntimeError(
                    f"TTG: no config loaded for {context_name} context. "
                    f"Use 'graph config \"file.ttgc\"' first."
                )

        if provider is None:
            return GraphResult(edges=[], isolated_nodes=[])

        # Handle shortcuts: if the expression is a bare selector matching a shortcut name
        expr = stmt.expression
        if expr is not None and isinstance(expr, SelectorExpr) and expr.predicates is None:
            if expr.name in config.shortcuts:
                shortcut_text = config.shortcuts[expr.name]
                parser = self._get_parser()
                shortcut_stmt = parser.parse(shortcut_text)
                if isinstance(shortcut_stmt, ExprStmt) and shortcut_stmt.expression:
                    expr = shortcut_stmt.expression

        if expr is None:
            # Empty expression — check for empty shortcut
            if "" in config.shortcuts:
                shortcut_text = config.shortcuts[""]
                parser = self._get_parser()
                shortcut_stmt = parser.parse(shortcut_text)
                if isinstance(shortcut_stmt, ExprStmt) and shortcut_stmt.expression:
                    expr = shortcut_stmt.expression

        if expr is None:
            return GraphResult(edges=[], isolated_nodes=[])

        result_set = self._eval_expr(expr, config, provider)

        # Collect node kinds from the provider
        node_kinds = self._collect_node_kinds(result_set, provider)

        # Apply sort and convert to GraphResult
        sort_by = stmt.sort_by if stmt.sort_by else None
        graph_result = result_set.to_graph_result(sort_by=sort_by, node_kinds=node_kinds)

        # Handle file output
        if stmt.output_file:
            style = self._meta_style if stmt.metadata else self._data_style
            return self._write_output(stmt.output_file, graph_result, style)

        return graph_result

    # ---- Expression evaluator dispatch ----

    def _eval_expr(self, expr: Expr, config: GraphConfig, provider: Any) -> ResultSet:
        if isinstance(expr, SelectorExpr):
            return self._eval_selector(expr, config, provider)
        elif isinstance(expr, DotExpr):
            return self._eval_dot(expr, config, provider)
        elif isinstance(expr, ChainExpr):
            return self._eval_chain(expr, config, provider)
        elif isinstance(expr, UnionExpr):
            return self._eval_union(expr, config, provider)
        elif isinstance(expr, IntersectExpr):
            return self._eval_intersect(expr, config, provider)
        elif isinstance(expr, SetExpr):
            return self._eval_set(expr, config, provider)
        elif isinstance(expr, ParenExpr):
            return self._eval_expr(expr.expr, config, provider)
        else:
            raise ValueError(f"TTG: unknown expression type: {type(expr).__name__}")

    # ---- Selector evaluation ----

    def _eval_selector(self, sel: SelectorExpr, config: GraphConfig, provider: Any) -> ResultSet:
        """Evaluate a selector expression: resolve name to nodes."""
        # Check if it's a shortcut (only bare selectors without predicates)
        if sel.predicates is None and sel.name in config.shortcuts:
            shortcut_text = config.shortcuts[sel.name]
            parser = self._get_parser()
            shortcut_stmt = parser.parse(shortcut_text)
            if isinstance(shortcut_stmt, ExprStmt) and shortcut_stmt.expression:
                return self._eval_expr(shortcut_stmt.expression, config, provider)

        # Resolve selector name to leaf selectors
        leaf_selectors = self._resolve_selector_name(sel.name, config)
        if not leaf_selectors:
            return ResultSet()

        # Collect all nodes matching the leaf selectors
        nodes: set[str] = set()
        for leaf in leaf_selectors:
            nodes |= provider.get_nodes_for_selector(leaf)

        # Apply predicate filtering
        if sel.predicates:
            nodes = self._filter_nodes(nodes, sel.predicates, provider)

        return ResultSet(nodes=nodes)

    def _resolve_selector_name(self, name: str, config: GraphConfig) -> list[str]:
        """Resolve a selector/group name to leaf selector names."""
        if name in config.selectors:
            return [name]
        if name in config.groups:
            result: list[str] = []
            for member in config.groups[name]:
                result.extend(self._resolve_selector_name(member, config))
            return result
        # Unknown selector — return empty
        return []

    def _filter_nodes(self, nodes: set[str], predicates: dict[str, Any], provider: Any) -> set[str]:
        """Filter nodes by predicates."""
        filtered = set()
        for node_id in nodes:
            if self._node_matches_predicates(node_id, predicates, provider):
                filtered.add(node_id)
        return filtered

    def _node_matches_predicates(self, node_id: str, predicates: dict[str, Any], provider: Any) -> bool:
        """Check if a node matches all predicates."""
        for key, pred in predicates.items():
            if key in ("label", "result", "depth"):
                continue  # These are axis predicates, not node filters
            if key == "name":
                node_name = provider.get_node_property(node_id, "name")
                if node_name is None:
                    node_name = node_id
                if isinstance(pred, NamePred):
                    if not self._name_matches(node_name, pred):
                        return False
                elif isinstance(pred, StringPred):
                    if node_name != pred.value:
                        return False
            elif isinstance(pred, BoolPred):
                val = provider.get_node_property(node_id, key)
                if val != pred.value:
                    return False
            elif isinstance(pred, IntPred):
                val = provider.get_node_property(node_id, key)
                if val != pred.value:
                    return False
            elif isinstance(pred, StringPred):
                val = provider.get_node_property(node_id, key)
                if val != pred.value:
                    return False
        return True

    def _name_matches(self, name: str, pred: NamePred) -> bool:
        """Check if a name matches a NamePred (OR of terms)."""
        for term in pred.terms:
            if isinstance(term, NameTerm):
                match = (name == term.name)
                if term.negated:
                    match = not match
                if match:
                    return True
            elif isinstance(term, GroupedNameTerm):
                # Grouped: !(A|B) or (A|B)
                inner_match = self._name_matches(name, term.expr)
                if term.negated:
                    if not inner_match:
                        return True
                else:
                    if inner_match:
                        return True
        # If all terms are negated and none returned True, need special handling
        # For negated terms: ALL must match (AND semantics for negation)
        all_negated = all(
            (isinstance(t, NameTerm) and t.negated) or
            (isinstance(t, GroupedNameTerm) and t.negated)
            for t in pred.terms
        )
        if all_negated and pred.terms:
            # All negated: check that name doesn't match ANY of them
            for term in pred.terms:
                if isinstance(term, NameTerm):
                    if name == term.name:
                        return False
                elif isinstance(term, GroupedNameTerm):
                    if self._name_matches(name, term.expr):
                        return False
            return True
        return False

    # ---- Dot expression evaluation ----

    def _eval_dot(self, dot: DotExpr, config: GraphConfig, provider: Any) -> ResultSet:
        """Dot chaining: pipe semantics — each step replaces the current set."""
        current = self._eval_expr(dot.base, config, provider)
        for axis_ref in dot.axes:
            # Pipe: replace nodes with traversal targets
            traversal = self._traverse_axis(
                current.nodes, axis_ref, config, provider
            )
            current = ResultSet(nodes=traversal.nodes, edges=set())
        return current

    # ---- Chain expression evaluation ----

    def _eval_chain(self, chain: ChainExpr, config: GraphConfig, provider: Any) -> ResultSet:
        """Chain expression: base op1 operand1 op2 operand2 ..."""
        current = self._eval_expr(chain.base, config, provider)

        for chain_op in chain.ops:
            if chain_op.op == "+":
                # Add: union current with traversal results
                traversal = self._eval_chain_operand(
                    current.nodes, chain_op.operand, config, provider
                )
                current = current.union(traversal)
            elif chain_op.op == "/":
                # Pipe: replace current with traversal results
                traversal = self._eval_chain_operand(
                    current.nodes, chain_op.operand, config, provider
                )
                current = ResultSet(nodes=traversal.nodes, edges=set())
            elif chain_op.op == "-":
                # Subtract
                if isinstance(chain_op.operand, (SingleAxisOperand, CompoundAxisOperand)):
                    # Traverse then remove results
                    traversal = self._eval_chain_operand(
                        current.nodes, chain_op.operand, config, provider
                    )
                    current = current.subtract_nodes(traversal.nodes)
                else:
                    # Subtract an expression (selector, etc.)
                    other = self._eval_expr(chain_op.operand, config, provider)
                    current = current.subtract_nodes(other.nodes)

        return current

    def _eval_chain_operand(
        self, source_nodes: set[str], operand: Any, config: GraphConfig, provider: Any
    ) -> ResultSet:
        """Evaluate a chain operand (axis traversal from source nodes)."""
        if isinstance(operand, SingleAxisOperand):
            # Chain of axes: .a.b.c — pipe through each
            current_nodes = source_nodes
            all_edges: set[tuple[str, str, str]] = set()
            for i, axis_ref in enumerate(operand.axes):
                traversal = self._traverse_axis(current_nodes, axis_ref, config, provider)
                if i == len(operand.axes) - 1:
                    # Last axis: keep its edges
                    all_edges |= traversal.edges
                # Move to next level
                current_nodes = traversal.nodes
            return ResultSet(nodes=current_nodes, edges=all_edges)
        elif isinstance(operand, CompoundAxisOperand):
            # Multiple axes combined: {.a, .b, .c}
            combined = ResultSet()
            for axis_ref in operand.axes:
                traversal = self._traverse_axis(source_nodes, axis_ref, config, provider)
                combined = combined.union(traversal)
            return combined
        else:
            # It's an expression (for - operator with selector)
            return self._eval_expr(operand, config, provider)

    # ---- Set operators ----

    def _eval_union(self, expr: UnionExpr, config: GraphConfig, provider: Any) -> ResultSet:
        left = self._eval_expr(expr.left, config, provider)
        right = self._eval_expr(expr.right, config, provider)
        return left.union(right)

    def _eval_intersect(self, expr: IntersectExpr, config: GraphConfig, provider: Any) -> ResultSet:
        left = self._eval_expr(expr.left, config, provider)
        right = self._eval_expr(expr.right, config, provider)
        return left.intersect(right)

    def _eval_set(self, expr: SetExpr, config: GraphConfig, provider: Any) -> ResultSet:
        combined = ResultSet()
        for member in expr.members:
            combined = combined.union(self._eval_expr(member, config, provider))
        return combined

    # ---- Axis traversal ----

    def _traverse_axis(
        self,
        source_nodes: set[str],
        axis_ref: AxisRef,
        config: GraphConfig,
        provider: Any,
    ) -> ResultSet:
        """Traverse an axis from source nodes, applying predicates."""
        axis_name = axis_ref.name
        preds = axis_ref.predicates or {}

        # Get depth
        depth = 1
        if "depth" in preds:
            depth_pred = preds["depth"]
            if isinstance(depth_pred, IntPred):
                depth = depth_pred.value
            elif isinstance(depth_pred, InfPred):
                depth = -1  # Sentinel for infinity

        if depth == 0:
            return ResultSet()

        # Get label override
        label_pred = preds.get("label")
        # Get result projection
        result_pred = preds.get("result")
        # Get name filter
        name_pred = preds.get("name")

        # Resolve axis to leaf axes (handle axis groups)
        leaf_axes = self._resolve_axis_name(axis_name, config)
        if not leaf_axes:
            return ResultSet()

        # Perform traversal with depth
        all_nodes: set[str] = set()
        all_edges: set[tuple[str, str, str]] = set()
        current_sources = source_nodes
        visited: set[str] = set(source_nodes)
        iteration = 0

        while True:
            iteration += 1
            if depth > 0 and iteration > depth:
                break

            new_nodes: set[str] = set()
            for leaf_axis in leaf_axes:
                edges = provider.get_edges_for_axis(leaf_axis, current_sources)
                # Also check reverse axes
                if leaf_axis in config.reverses:
                    fwd_axis = config.reverses[leaf_axis]
                    edges = self._get_reverse_edges(fwd_axis, current_sources, provider)

                for edge in edges:
                    target = edge.target_id
                    source = edge.source_id

                    # Apply name filter on target
                    if name_pred and isinstance(name_pred, NamePred):
                        target_name = provider.get_node_property(target, "name") or target
                        if not self._name_matches(target_name, name_pred):
                            continue

                    # Determine the edge label
                    edge_label = axis_name  # Default: axis name
                    if label_pred is not None:
                        edge_label = self._resolve_label(
                            label_pred, target, provider
                        )

                    # Determine which node enters the result
                    if result_pred is not None:
                        # Follow result path from target
                        result_nodes = self._resolve_result_path(
                            result_pred, target, config, provider
                        )
                        for rn in result_nodes:
                            all_edges.add((source, edge_label, rn))
                            new_nodes.add(rn)
                    else:
                        all_edges.add((source, edge_label, target))
                        new_nodes.add(target)

            # Remove already-visited nodes for depth continuation
            truly_new = new_nodes - visited
            if not truly_new:
                break  # No new nodes discovered — stable
            visited |= truly_new
            all_nodes |= new_nodes
            current_sources = truly_new

            if depth > 0 and iteration >= depth:
                break

        return ResultSet(nodes=all_nodes, edges=all_edges)

    def _resolve_axis_name(self, name: str, config: GraphConfig) -> list[str]:
        """Resolve an axis/reverse/axis_group name to leaf forward axis names."""
        # Check forward axes
        if name in config.axes:
            return [name]
        # Check reverse axes
        if name in config.reverses:
            return [name]  # Handled specially during traversal
        # Check axis groups
        if name in config.axis_groups:
            result: list[str] = []
            for member in config.axis_groups[name]:
                result.extend(self._resolve_axis_name(member, config))
            return result
        return []

    def _get_reverse_edges(self, fwd_axis: str, target_nodes: set[str], provider: Any) -> list:
        """Get reverse edges: find forward edges where target is in target_nodes."""
        from typed_tables.ttg.provider import EdgeInfo
        all_fwd_edges = provider._axis_edges.get(fwd_axis, [])
        result = []
        for edge in all_fwd_edges:
            if edge.target_id in target_nodes:
                # Reverse: swap source and target
                result.append(EdgeInfo(
                    source_id=edge.target_id,
                    target_id=edge.source_id,
                    axis_name=edge.axis_name,
                    label=edge.label,
                ))
        return result

    # ---- Label and result resolution ----

    def _resolve_label(self, label_pred: Any, target_id: str, provider: Any) -> str:
        """Resolve a label= predicate to a string."""
        if isinstance(label_pred, StringPred):
            return label_pred.value
        elif isinstance(label_pred, AxisPathPred):
            # Follow path from target node
            current = target_id
            for step in label_pred.steps:
                edges = provider.get_edges_for_axis(step, {current})
                if edges:
                    current = edges[0].target_id
                else:
                    prop = provider.get_node_property(current, step)
                    if prop is not None:
                        return str(prop)
                    return current
            # Return the identity of the final node
            name = provider.get_node_property(current, "name")
            return name if name is not None else current
        elif isinstance(label_pred, JoinPred):
            # Collect values from path and join
            values = self._collect_path_values(
                label_pred.path, target_id, provider
            )
            return label_pred.separator.join(values)
        return ""

    def _resolve_result_path(
        self, result_pred: Any, start_id: str, config: GraphConfig, provider: Any
    ) -> set[str]:
        """Follow a result= path from a node, returning the endpoint nodes."""
        if isinstance(result_pred, AxisPathPred):
            current_nodes = {start_id}
            for step in result_pred.steps:
                next_nodes: set[str] = set()
                for node in current_nodes:
                    edges = provider.get_edges_for_axis(step, {node})
                    for e in edges:
                        next_nodes.add(e.target_id)
                current_nodes = next_nodes
                if not current_nodes:
                    break
            return current_nodes
        return {start_id}

    def _collect_path_values(self, path: AxisPathPred, start_id: str, provider: Any) -> list[str]:
        """Collect display values by following a path from a node."""
        current_nodes = {start_id}
        for step in path.steps:
            next_nodes: set[str] = set()
            for node in current_nodes:
                edges = provider.get_edges_for_axis(step, {node})
                for e in edges:
                    next_nodes.add(e.target_id)
            current_nodes = next_nodes
            if not current_nodes:
                break

        # Get names of final nodes
        values = []
        for node_id in sorted(current_nodes):
            name = provider.get_node_property(node_id, "name")
            values.append(name if name is not None else node_id)
        return values

    # ---- Node kind collection ----

    def _collect_node_kinds(self, result_set: ResultSet, provider: Any) -> dict[str, str]:
        """Collect selector/kind info for all nodes in a result set."""
        kinds: dict[str, str] = {}
        edge_nodes = {s for s, _, _ in result_set.edges} | {t for _, _, t in result_set.edges}
        all_nodes = result_set.nodes | edge_nodes
        for node_id in all_nodes:
            node_info = provider.get_node(node_id)
            if node_info is not None:
                kinds[node_id] = node_info.selector
        return kinds

    # ---- File output ----

    def _write_output(
        self, file_path: str, result: GraphResult, style: dict[str, str]
    ) -> FileResult:
        """Write graph result to a file. Format determined by extension."""
        resolved = self._resolve_path(file_path)
        # Auto-extension: if no extension, default to .dot
        _, ext = os.path.splitext(resolved)
        if not ext:
            resolved += ".dot"
            ext = ".dot"

        if ext == ".dot":
            content = self._format_dot(result, style)
        elif ext == ".ttq":
            content = self._format_ttq(result, style)
        else:
            content = self._format_dot(result, style)

        with open(resolved, "w") as f:
            f.write(content)
        return FileResult(path=resolved, edge_count=len(result.edges))

    # ---- DOT output ----

    # Selector → (shape, default_color)
    _SELECTOR_STYLES: dict[str, tuple[str, str]] = {
        "composites": ("box", "#4A90D9"),
        "interfaces": ("box", "#7B68EE"),
        "enums": ("box", "#66BB6A"),
        "aliases": ("box", "#B0BEC5"),
        "fields": ("ellipse", "#E0E0E0"),
        "variants": ("ellipse", "#A5D6A7"),
        "arrays": ("ellipse", "#FFB74D"),
        "sets": ("ellipse", "#FFB74D"),
        "dictionaries": ("ellipse", "#FFB74D"),
        "overflows": ("box", "#B0BEC5"),
        "boolean": ("ellipse", "#FFF59D"),
        "string": ("ellipse", "#CE93D8"),
        "fraction": ("ellipse", "#F48FB1"),
        "bigint": ("ellipse", "#EF9A9A"),
        "biguint": ("ellipse", "#EF9A9A"),
    }

    # Style key → selector
    _STYLE_KEY_MAP: dict[str, str] = {
        "composite.color": "composites",
        "interface.color": "interfaces",
        "enum.color": "enums",
        "alias.color": "aliases",
        "field.color": "fields",
        "variant.color": "variants",
        "array.color": "arrays",
        "set.color": "sets",
        "dictionary.color": "dictionaries",
        "primitive.color": "_primitives",
    }

    # Default primitive style
    _PRIMITIVE_STYLE = ("ellipse", "#FFF9C4")

    def _format_dot(self, result: GraphResult, style: dict[str, str]) -> str:
        """Format a GraphResult as DOT for Graphviz."""
        lines: list[str] = []
        lines.append("digraph types {")

        direction = style.get("direction", "LR")
        lines.append(f"    rankdir={direction};")
        lines.append('    bgcolor="#FAFAFA";')
        lines.append('    fontname="Helvetica";')
        lines.append(
            "    node [style=\"filled,rounded\", fontname=\"Helvetica\","
            " fontsize=11, fontcolor=\"#333333\", penwidth=0.8];"
        )
        lines.append(
            "    edge [fontname=\"Helvetica\", fontsize=9,"
            ' color="#666666", fontcolor="#444444"];'
        )

        title = style.get("title")
        if title:
            escaped = title.replace('"', '\\"')
            lines.append(f'    label="{escaped}";')
            lines.append("    labelloc=t;")
            lines.append("    fontsize=18;")

        lines.append("")

        # Build effective style overrides
        color_overrides: dict[str, str] = {}
        for style_key, selector in self._STYLE_KEY_MAP.items():
            if style_key in style:
                color_overrides[selector] = style[style_key]

        # Collect all nodes (from edges + isolated)
        all_nodes: set[str] = set()
        for edge in result.edges:
            all_nodes.add(edge.source)
            all_nodes.add(edge.target)
        for node in result.isolated_nodes:
            all_nodes.add(node)

        # Emit node definitions
        for name in sorted(all_nodes):
            kind = result.node_kinds.get(name, "")
            shape, color = self._SELECTOR_STYLES.get(kind, self._PRIMITIVE_STYLE)
            # Apply color overrides
            if kind in color_overrides:
                color = color_overrides[kind]
            elif "_primitives" in color_overrides and kind not in self._SELECTOR_STYLES:
                color = color_overrides["_primitives"]
            lines.append(f'    "{name}" [shape={shape}, fillcolor="{color}"];')

        lines.append("")

        # Emit edges
        for e in result.edges:
            label = e.label.replace('"', '\\"')
            if label == "extends":
                lines.append(
                    f'    "{e.source}" -> "{e.target}"'
                    f' [style=dashed, color="#1565C0", arrowhead=empty];'
                )
            elif label == "implements":
                lines.append(
                    f'    "{e.source}" -> "{e.target}"'
                    f' [style=dotted, color="#7B68EE", arrowhead=empty];'
                )
            elif label == "alias":
                lines.append(
                    f'    "{e.source}" -> "{e.target}"'
                    f' [style=dashed, color="#78909C", arrowhead=empty];'
                )
            elif label:
                lines.append(f'    "{e.source}" -> "{e.target}" [label="{label}"];')
            else:
                lines.append(f'    "{e.source}" -> "{e.target}";')

        lines.append("}")
        lines.append("")
        return "\n".join(lines)

    # ---- TTQ output ----

    def _format_ttq(self, result: GraphResult, style: dict[str, str]) -> str:
        """Format a GraphResult as a TTQ script."""
        lines: list[str] = []
        title = style.get("title", "Type reference graph")
        lines.append(f"-- {title}")
        lines.append("enum NodeRole { focus, context, endpoint, leaf }")
        lines.append("type TypeNode { name: string, kind: string, role: NodeRole }")
        lines.append("type Edge { source: TypeNode, target: TypeNode, field_name: string }")
        lines.append("")

        # Collect all nodes
        all_nodes: list[str] = []
        seen: set[str] = set()
        for edge in result.edges:
            for name in (edge.source, edge.target):
                if name not in seen:
                    all_nodes.append(name)
                    seen.add(name)
        for name in result.isolated_nodes:
            if name not in seen:
                all_nodes.append(name)
                seen.add(name)

        node_index = {name: i for i, name in enumerate(all_nodes)}

        # Source nodes in the graph (non-leaf)
        source_names = {e.source for e in result.edges}

        for name in all_nodes:
            kind = result.node_kinds.get(name, "unknown")
            if name in source_names:
                role = "context"
            else:
                role = "leaf"
            lines.append(
                f'create TypeNode(name="{name}", kind="{kind}", role=.{role})'
            )

        if all_nodes:
            lines.append("")

        for e in result.edges:
            src_idx = node_index[e.source]
            tgt_idx = node_index[e.target]
            field_name = e.label.replace('"', '\\"')
            lines.append(
                f'create Edge(source=TypeNode({src_idx}), '
                f'target=TypeNode({tgt_idx}), field_name="{field_name}")'
            )

        lines.append("")
        return "\n".join(lines)

    # ---- Provider access ----

    def _get_meta_provider(self) -> Any:
        """Get or create the meta-schema provider."""
        if self._meta_provider is None and self.registry is not None:
            from typed_tables.ttg.provider import MetaSchemaProvider
            self._meta_provider = MetaSchemaProvider(self.registry)
        return self._meta_provider

    # ---- Path resolution ----

    def _resolve_path(self, file_path: str) -> str:
        if os.path.isabs(file_path):
            return file_path
        if self._script_stack:
            return str(self._script_stack[-1] / file_path)
        return file_path

    # ---- Parser lazy initialization ----

    def _get_parser(self) -> Any:
        if self._parser is None:
            from typed_tables.ttg.ttg_parser import TTGParser
            self._parser = TTGParser()
            self._parser.build(debug=False, write_tables=False)
        return self._parser

    def _get_ttgc_parser(self) -> Any:
        if self._ttgc_parser is None:
            from typed_tables.ttg.ttgc_parser import TTGCParser
            self._ttgc_parser = TTGCParser()
            self._ttgc_parser.build(debug=False, write_tables=False)
        return self._ttgc_parser

    # ---- Built-in meta-schema config ----

    def _load_builtin_meta_config(self) -> GraphConfig:
        config = GraphConfig()
        config.selectors = {
            "composites": "CompositeDef",
            "interfaces": "InterfaceDef",
            "enums": "EnumDef",
            "fields": "FieldDef",
            "variants": "VariantDef",
            "aliases": "AliasDef",
            "arrays": "ArrayDef",
            "sets": "SetDef",
            "dictionaries": "DictDef",
            "overflows": "OverflowDef",
            "bit": "BitDef",
            "character": "CharacterDef",
            "uint8": "UInt8Def",
            "int8": "Int8Def",
            "uint16": "UInt16Def",
            "int16": "Int16Def",
            "uint32": "UInt32Def",
            "int32": "Int32Def",
            "uint64": "UInt64Def",
            "int64": "Int64Def",
            "uint128": "UInt128Def",
            "int128": "Int128Def",
            "float16": "Float16Def",
            "float32": "Float32Def",
            "float64": "Float64Def",
            "boolean": "BooleanDef",
            "string": "StringDef",
            "bigint": "BigIntDef",
            "biguint": "BigUIntDef",
            "fraction": "FractionDef",
        }
        config.groups = {
            "integers": [
                "uint8", "int8", "uint16", "int16", "uint32", "int32",
                "uint64", "int64", "uint128", "int128", "bigint", "biguint",
            ],
            "floats": ["float16", "float32", "float64"],
            "primitives": ["integers", "floats", "bit", "character"],
            "types": [
                "composites", "interfaces", "enums", "aliases", "arrays",
                "sets", "dictionaries", "overflows", "primitives", "boolean",
                "string", "fraction",
            ],
            "all": ["types", "fields", "variants"],
        }
        config.axes = {
            "fields": ["composites.fields", "interfaces.fields", "variants.fields"],
            "extends": ["composites.parent", "interfaces.extends"],
            "interfaces": ["composites.interfaces"],
            "variants": ["enums.variants"],
            "backing": ["enums.backing_type"],
            "type": ["fields.type"],
            "alias": ["aliases.base_type"],
            "base": ["overflows.base_type"],
            "element": ["arrays.element_type", "sets.element_type"],
            "key": ["dictionaries.key_type"],
            "value": ["dictionaries.value_type"],
        }
        config.reverses = {
            "children": "extends",
            "implementers": "interfaces",
            "owner": "fields",
            "enum": "variants",
            "typedBy": "type",
            "aliasedBy": "alias",
            "backedBy": "backing",
            "wrappedBy": "base",
            "elementOf": "element",
            "keyOf": "key",
            "valueOf": "value",
        }
        config.axis_groups = {
            "all": [
                "fields", "extends", "interfaces", "variants", "backing",
                "type", "alias", "base", "element", "key", "value",
            ],
            "allReverse": [
                "children", "implementers", "owner", "enum", "typedBy",
                "aliasedBy", "backedBy", "wrappedBy", "elementOf", "keyOf", "valueOf",
            ],
            "referencedBy": [
                "typedBy", "aliasedBy", "backedBy", "wrappedBy",
                "elementOf", "keyOf", "valueOf",
            ],
        }
        config.identity = {"default": "name"}
        config.shortcuts = {
            "all": "types + .fields{label=.name, result=.type} + .extends + .interfaces",
        }
        return config
