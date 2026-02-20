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

## REPL & Scripting

| Topic | Summary | Notes |
|-------|---------|-------|
| Startup script | Script that runs automatically on REPL startup or database load. Open questions: (1) Should the script be per-database (e.g., `_startup.ttq` in data dir) or session-level (e.g., `~/.ttqrc`)? Could support both. (2) Should the script be pure TTQ, or should it also support REPL commands (like `set max_width`)? `set` is already valid TTQ syntax, but future REPL-only commands might not be. Example use case: `set max_width 120` to apply user preferences on startup. | |

## JSON Import & Transformation

Source: conversation 2026-02-19, scratch/json/json_schema.ttq

| Topic | Summary | Notes |
|-------|---------|-------|
| JSON → generic schema import | Import any JSON file into the existing `JsonValue` enum schema (`json_schema.ttq`). Output is a `.ttq` script with `create JsonDocument(...)` statements. | First step — handles any JSON but no typed structure |
| JSON → typed schema inference | Infer typed_tables types from JSON structure: objects → composites, arrays → typed arrays, numbers → int64/float64 heuristic. Schema deduplication for structurally identical nested objects. | More advanced — produces proper typed schemas |
| Dynamic key detection | Heuristic to distinguish fixed-key objects (→ composite type) from dynamic-key objects (→ `{string: ValueType}` dictionary) | Part of typed inference |
| Heterogeneous array handling | JSON arrays with mixed types → Swift-style enum or reject. Could reuse `JsonValue` for mixed arrays within an otherwise typed schema | Part of typed inference |
| JSON transformation | Transform JSON-schema database into a typed database using user-defined mappings | Depends on typed inference and a mapping language |

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
