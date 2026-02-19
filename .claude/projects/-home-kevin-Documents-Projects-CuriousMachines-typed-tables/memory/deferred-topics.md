# Deferred Topics Index

Consolidated index of all topics explicitly deferred for future discussion. Each entry links to its source file.

---

## Graph Expression Language

Source: [scratch/graphs/graph-expression-language.md](../../../../../scratch/graphs/graph-expression-language.md)

| Topic | Summary | Notes |
|-------|---------|-------|
| D2 — Syntactic sugar | Shorthand for common graph patterns (`graph Person` etc.) | Build explicit language first, sugar later |
| D6 — Expression variables | Naming intermediate graph results for reuse | Deferred to future scripting/language design discussion |
| D10 — Node display properties | How nodes look when rendered (residual after D12 resolved compact/expanded) | Fold into rendering/style discussion |
| D19 — Data query generalization | Could expression language extend to data traversal? | Reframed by D22: not about extending the language, but plugging in a different schema |
| Axis aliases | `axis_alias { field_types: fields{edge=.name, result=.type}, descendants: children{depth=inf} }` — bundle an axis name with default predicates. Any predicates supported (edge, result, display, depth, name, etc.). Explicit predicates at use site merge/override defaults. | Distinct from shortcuts (expression-level) and axis_groups (no predicates) |
| DOT node sorting | User-controlled node sort order in DOT output. E.g., `sort by node` could sort by kind then name. Currently nodes are always alphabetical. Needs a way for the user to specify sort criteria — not a hardcoded default. | Could extend `sort by` clause or be a style property |
| DOT subgraph clustering | Group nodes into Graphviz `subgraph cluster_X` blocks by kind or other criteria. User-controlled — when to cluster, which kinds get clusters. Clustering constrains layout significantly so must be opt-in. | Could be a style property (`cluster=true`) or expression-level directive |

## Typed Math

Source: MEMORY.md, dev-notes/features.md

| Topic | Summary | Notes |
|-------|---------|-------|
| Phase 7 — Bitwise operators | `&`, `|`, `^`, `~` | Grammar conflict with graph `&` operator TBD |
| Phase 8 — Lambda expressions | `map(lambda)`, `filter(lambda)` for arrays | |

## Type System & Storage

Source: MEMORY.md, dev-notes/features.md, dev-notes/known-bugs.md

| Topic | Summary | Notes |
|-------|---------|-------|
| Indexes | Database indexing for sets, dicts, and queries | |
| Nested arrays | General `int32[][]` support; only `string[]` works today | Needs recursive element pre-storage |

## Planned Capabilities (Unscoped)

Source: CLAUDE.md ("To be determined" section)

| Topic | Summary |
|-------|---------|
| Transitive closure | Return a type and all types it references, recursively |
| Value location | Given a value, emit its table and index |
| Reverse reference lookup | Given a table and index, find all composites that reference it |
| Storage size calculation | Calculate total bytes for a type instance (with/without metadata) |

## For Consideration (Exploratory)

Source: dev-notes/features.md

| Topic | Summary |
|-------|---------|
| Executor as VM | Turn executor into bytecode-based VM with instruction set |
| Reduce keyword count | Simplify the grammar |
| Packed bit arrays | Handle bit streams across byte boundaries |
| Relational DB mode | Allow tables to be used relationally |
| Complete trig functions | Full set of trigonometric functions |

---

*Last updated: 2026-02-19*
