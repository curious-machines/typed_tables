# Graph Expression Language — Memory Pointer

**This file is superseded by the full design document:**
[scratch/graphs/graph-expression-language.md](../../../../../scratch/graphs/graph-expression-language.md)

## Quick Summary

XPath-inspired expression language for the `graph` command. Replaces the current keyword-driven syntax (structure, declared, stored, showing, excluding, path-to, depth) with composable expressions.

**Key concepts:**
- **Selectors** choose nodes by kind (`composites`, `interfaces`, etc.)
- **Axes** traverse relationships (`.fields`, `.extends`, `.type`, etc.) — forward and reverse
- **Predicates** filter, label, project, and control depth (`{name=X, label=.name, result=.type, depth=inf}`)
- **Operators**: `.` (dot chain/pipe), `+` (add), `/` (pipe), `-` (subtract), `&` (intersect), `|` (union)
- **Schema-driven**: all identifiers resolved at runtime against the loaded schema (D22)
- **Shortcuts**: schema-defined AST rewrites for common patterns (D23)

30 decided items (D1–D30). BNF grammar deferred. Design discussion ongoing.
