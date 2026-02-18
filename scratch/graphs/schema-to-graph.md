# Schema-to-Graph Mapping — Design Discussion

## Status: Active Discussion (2026-02-18)

## Context

The current TTGE (Typed Tables Graph Expression) language design (see `graph-expression-language.md`) operates on an **implied source schema** — the type system's own meta-schema — and produces an **implied target schema** — the graph schema (nodes and edges). Both are hardcoded.

This document captures the design discussion around making the **source schema explicit** so that TTGE can operate on arbitrary database schemas, not just the built-in type-system schema. The target schema (graph) remains implicit for now.

## Key Insight: TTGE as Data Transformation

TTGE is a **data transformation language**, not a schema transformation language. It operates on instances (the type definitions stored in a database) and produces instances (nodes and edges). The schema mapping is currently hardcoded: "things selected become nodes, traversals become edges."

The transformation has two layers:

1. **Schema mapping** (structural) — which source types can become nodes, which relationships become edges, what properties are available. This is what the config defines.

2. **Data transformation** (per-query) — which specific instances get transformed and how. This is what TTGE expressions control, including dynamic decisions like whether a FieldDef becomes a node or is dissolved into an edge via `label=`/`result=`.

## Two Implied Schemas

### Source Schema (to be made explicit)

Currently the built-in meta-schema. Defined in `meta-schema.ttq`:

- Entity types: `CompositeDef`, `InterfaceDef`, `EnumDef`, `FieldDef`, `VariantDef`, `AliasDef`, `ArrayDef`, `SetDef`, `DictDef`, `OverflowDef`, and all primitive defs
- Relationships: `CompositeDef.fields → FieldDef[]`, `FieldDef.type → TypeDef`, `CompositeDef.parent → CompositeDef`, etc.
- Properties: `name`, `has_explicit_values`, `default_value`, etc.

### Target Schema (remains implicit)

The graph schema — what TTGE produces:

```ttq
type Node {
    name: string,
    kind: string,
    label: string
}

type Edge {
    source: Node,
    target: Node,
    label: string
}
```

This is essentially what the current table output produces (`source | edge | target`) and what DOT rendering consumes. Making this explicit is a future step toward general-purpose schema-to-schema transformation.

## What the Config Defines

The config provides the **vocabulary** that TTGE expressions use. It maps concepts from the source schema to TTGE constructs:

### 1. Entity Kinds (Selectors)

Which types from the source schema are graph-selectable, and what name refers to them.

For the meta-schema:
- `CompositeDef` → selector `composites`
- `InterfaceDef` → selector `interfaces`
- `EnumDef` → selector `enums`
- `FieldDef` → selector `fields`
- `VariantDef` → selector `variants`
- `AliasDef` → selector `aliases`
- `ArrayDef` → selector `arrays`
- `SetDef` → selector `sets`
- `DictDef` → selector `dictionaries`
- Each primitive def → its own selector (e.g., `UInt8Def` → `uint8`)

### 2. Selector Groupings

Named unions of selectors:
- `integers` = all integer def selectors + `bigint` + `biguint`
- `floats` = `float16`, `float32`, `float64`
- `primitives` = `integers` + `floats` + `bit` + `character`
- `types` = everything that implements `TypeDef`
- `all` = every selectable entity kind

### 3. Forward Axes

Per entity kind, which fields are traversable as named axes. The axis name can differ from the field name.

For `CompositeDef`:
- `.fields` → follows `CompositeDef.fields` field → produces `FieldDef` nodes
- `.extends` → follows `CompositeDef.parent` field → produces `CompositeDef` nodes
- `.interfaces` → follows `CompositeDef.interfaces` field → produces `InterfaceDef` nodes

For `FieldDef`:
- `.type` → follows `FieldDef.type` field → produces `TypeDef` nodes

For `EnumDef`:
- `.variants` → follows `EnumDef.variants` field → produces `VariantDef` nodes
- `.backing` → follows `EnumDef.backing_type` field → produces `IntegerDef` nodes

(etc. — see full axis table in `graph-expression-language.md`)

### 4. Reverse Axes

Inverses of forward axes:
- `.children` = inverse of `.extends` (who extends me?)
- `.implementers` = inverse of `.interfaces` (who implements me?)
- `.owner` = inverse of `.fields` (who owns this field?)
- `.typedBy` = inverse of `.type` (which fields have me as their type?)

(etc.)

### 5. Axis Groupings

Named sets of axes:
- `.all` = all forward axes
- `.allReverse` = all reverse axes
- `.referencedBy` = subset of reverse axes (`.typedBy`, `.aliasedBy`, `.elementOf`, `.keyOf`, `.valueOf`, `.backedBy`)

### 6. Shortcuts

Named expression templates, rewritten at the AST level:
- `""` (bare `graph`) → full graph template
- `"all"` → all nodes, no edges

### 7. Predicates / Attributes

Implicit: any scalar field on a schema type that is not mapped to an axis becomes a predicate automatically. For example, `EnumDef.has_explicit_values`, `FieldDef.default_value`, `FieldDef.name`.

Fields that are mapped to axes are still accessible as predicates — they serve dual roles. This may prove to be a design blemish, but syntactic position (selectors at expression start, axes after `.`) disambiguates.

**Gap:** Computed predicates like `declared` and `stored` on fields (is this field declared on the owning type vs inherited from parent) cannot be expressed yet. These require language support for expressions that reference the traversal context (the owning type). Noted for future work.

### 8. Node Identity

What constitutes "same node" for deduplication. Defined per selector with a default. For the meta-schema, the default is `name`. Per-selector overrides allow different entity kinds to use different identity fields. Fields used for identity are also available as predicates.

**Validation:** The config loader must verify that every selector's schema type has the identity field (or a per-selector override). This check runs at config load time, before any graph expressions are evaluated.

For data graphs, identity would differ (e.g., type + instance index).

## The FieldDef Case

`FieldDef` illustrates why TTGE is a data transformation language, not just a config-driven renderer.

In the meta-schema, `FieldDef` is a full composite type with `name`, `type`, and `default_value` fields. In the graph, it can appear as:

- **A node** (via `+ .fields`): two-hop traversal, Composite → Field → Type
- **A named edge** (via `+ .fields{label=.name, result=.type}`): Field is dissolved, its `name` becomes the edge label, its `type` becomes the edge target

This is a **per-query** decision, not a config decision. The config only says "FieldDef is selectable and has these axes." The TTGE expression decides how it appears in the output. This is the right separation — the config defines what's possible, the expression defines what happens.

## TTGE as a Self-Contained Language

All lines starting with `graph` are passed entirely to the TTGE infrastructure. The TTQ parser does not parse any part of a graph command — it recognizes the `graph` keyword and hands off the full line. This means:

- `graph config`, `graph style`, and `graph <expression>` are all TTGE commands
- TTGE manages its own command parsing, config state, and expression evaluation
- TTGE scripts (`.ttge` files) can contain graph commands without involving the TTQ parser
- The TTQ executor simply delegates to the TTGE subsystem

### Config and Style as Session State

Config and style are **session state**, not persisted per-database. There is no registry. The user sets config and style in the REPL or in TTGE scripts before running expressions.

**Two independent contexts:** The `metadata` prefix determines which context a command targets:

- **Data context** — the active database. Config and style start empty; user must set them.
- **Metadata context** — the active database's schema. Config and style have built-in defaults (`meta-schema.ttgc` and a default style, bundled with the package).

**Switching databases** (`use otherdb` in TTQ) **clears both contexts** — config and style for data and metadata are reset. The metadata context reinitializes with built-in defaults for the new database.

```
-- From the REPL (graph prefix required in TTQ context)
graph config "social-graph.ttgc"                       -- set data config
graph style "light.tts"                              -- set data style
graph style { "direction": "LR" }                      -- amend data style
graph metadata style { "direction": "LR" }             -- amend meta-schema style
graph users + .friends{label=.name}                    -- data query
graph metadata composites + .fields{label=.name, result=.type}  -- schema query
```

**Style commands** support three forms, in both data and metadata contexts:

1. **`[metadata] style "file"`** — load file, replace current style entirely.
2. **`[metadata] style "file" { "key": "val" }`** — load file, then apply inline overrides on top.
3. **`[metadata] style { "key": "val" }`** — amend the current style in place. Does not reload any file. Error if no config is active in that context.

Inline properties use TTQ dictionary syntax. Style persists for the session (until database switch or explicit replacement). This replaces the `@{...}` metadata syntax entirely.

**Config commands** work the same way:

- **`config "file.ttgc"`** — set data config (validated against active database's schema).
- **`metadata config "file.ttgc"`** — override the built-in meta-schema config (rarely needed).

**Error if no config.** Running a data expression without a config is an error. Metadata expressions always have the built-in config available.

### TTGE Scripts

TTGE scripts (`.ttge` files) bundle config, style, and expressions. They are executed via `graph execute` from TTQ context, or just `execute` from within TTGE context (including other TTGE scripts).

```
-- schema-report.ttge
config "meta-schema.ttgc"
style "dark.tts"
metadata composites + .fields{label=.name, result=.type} > "composites.dot"
metadata enums + .variants > "enums.dot"
```

```
-- data-report.ttge (assumes config/style already set by caller)
users + .friends{label=.name} > "social.dot"
```

```
-- From the REPL
use mydb
graph execute "schema-report.ttge"

-- Or set config interactively, then run a simpler script
graph config "social-graph.ttgc"
graph style "light.tts"
graph execute "data-report.ttge"
```

Scripts can execute other scripts. A script assumes a database is already selected (database selection is a TTQ concern). Scripts stop on errors.

Inside `.ttge` files, no `graph` prefix — everything is TTGE context. Commands available: `config`, `style`, `metadata config`, `metadata style`, `metadata <expr>`, `<expr>`, `execute`.

### Edge Labels

Default edge labels come from the **axis name**. The `.fields` axis produces edges labeled "fields", the `.extends` axis produces edges labeled "extends", etc. Users can override with `label=` predicates on any axis. No separate label configuration is needed.

### Meta-Schema Database Generation

- The TTGE engine internally generates a meta-schema database from the active database's `_metadata.json`, populated with instances of `CompositeDef`, `InterfaceDef`, `FieldDef`, etc.
- Generation happens on demand. When the schema is stale (metadata has changed since last generation), the meta-schema database is deleted and regenerated fresh.
- The meta-schema database is stored inside the active database's folder (location TBD — must not conflict with `compact` or `archive` operations).
- The user never sees or manages the meta-schema database directly.

### `meta-schema.ttq` Generation

The `meta-schema.ttq` file is **generated programmatically** from the type system, not maintained as a static file. This ensures it stays in sync as new built-in types are added. The `.ttgc` config file is bundled with the package as a static file (it rarely changes since it maps the stable meta-schema structure to graph concepts).

### Full Statement Structure

From TTQ context (REPL or TTQ scripts), all prefixed with `graph`:
```
graph config "file.ttgc"
graph [metadata] style "file.tts" [{ "key": "value", ... }]
graph [metadata] style { "key": "value", ... }
graph metadata config "file.ttgc"
graph [metadata] <expression> [sort by ...] [> "file"]
graph execute "file.ttge"
```

From TTGE context (`.ttge` scripts), no `graph` prefix:
```
config "file.ttgc"
[metadata] style "file.tts" [{ "key": "value", ... }]
[metadata] style { "key": "value", ... }
metadata config "file.ttgc"
[metadata] <expression> [sort by ...] [> "file"]
execute "file.ttge"
```

### TTGE→Caller Interface

TTGE returns its own result type, defined in the TTGE module with no dependency on TTQ or the REPL:

```python
@dataclass
class GraphEdge:
    source: str      # node identity (e.g., type name)
    label: str       # axis name or custom label
    target: str      # node identity

@dataclass
class GraphResult:
    edges: list[GraphEdge]       # sorted if sort by was specified
    isolated_nodes: list[str]    # nodes with no edges
```

Two output paths:
- **File output** (`> "file.dot"`): TTGE serializes to DOT/TTQ and writes the file. Returns a status/confirmation.
- **No file output**: TTGE returns a `GraphResult`. The caller (TTQ executor/REPL) renders it as a table, mapping `source`→column 1, `label`→column 2, `target`→column 3.

`sort by` is part of the TTGE expression. TTGE sorts `GraphResult.edges` before returning. Sort fields: `source`, `label`, `target`. No default sort order.

Dependency direction is strictly one-way: TTQ/REPL → TTGE → storage. TTGE does not depend on TTQ or the REPL.

### Future Considerations

- **Multiple active databases / cross-database queries:** Being able to reference multiple databases in a single TTGE expression could enable cross-database graph joins. Deferred.
- **TTGE script database selection:** Currently, TTGE scripts operate on the currently active database (selected via TTQ). If TTGE evolves into a richer scripting language, it may need its own database selection mechanism. Deferred.

## Parser Architecture

Two parsers (the TTGC parser and the TTGE parser) plus delegation from TTQ:

| Parser | Language | Purpose |
|--------|----------|---------|
| **TTQ parser** | TTQ | Queries, type definitions, CRUD; delegates all `graph` lines to TTGE |
| **TTGC parser** | `.ttgc` | Graph config file format (declarative, block-based) |
| **TTGE parser** | TTGE | Graph commands (`config`, `style`) and expressions (selectors, axes, operators, predicates) |

The TTQ parser recognizes `graph` and passes the entire line to the TTGE subsystem. The TTGE parser handles all graph-related commands internally. The TTGC parser is invoked by the TTGE subsystem when loading config files.

The parsers are independent — no shared token types or grammar rules. They live in separate modules under `src/typed_tables/parsing/`.

## Future: General-Purpose Transformation

The current scope makes the source schema explicit while keeping the target schema (graph) implicit. The next step would be making the target schema explicit too, enabling arbitrary schema-to-schema transformations:

- CSV schema → domain schema
- Domain schema → API schema
- Domain schema → graph schema (what TTGE does today)

TTGE's expression language — selectors, axes, predicates, `label=`, `result=` — may serve as the foundation for a general transformation language. The key extension would be specifying the target schema and how source fields map to target fields.

This is deferred until we have a second concrete use case to validate against.

## Config Format

**File extension:** `.ttgc` (typed tables graph config)

**Structure:** Block-based declarative format with named sections. Each section uses `keyword { key: value, ... }` syntax. See `meta-schema.ttgc` for the complete example.

### Sections

| Section | Purpose | Value Format |
|---------|---------|-------------|
| `selector` | Map selector names to schema types | `name: SchemaType` |
| `group` | Named unions of selectors | `name: [selector, ...]` (can include other groups) |
| `axis` | Forward axes — named traversals | `name: selector.field` or `name: [selector.field, ...]` |
| `reverse` | Reverse axes — inverses of forward axes | `name: forward_axis_name` |
| `axis_group` | Named sets of axes | `name: [axis, ...]` |
| `identity` | Node identity field per selector | `default: field` with per-selector overrides |
| `shortcut` | Named TTGE expression templates | `"name": expression` |

### Design Decisions

- **Predicates are implicit.** Any scalar field on a schema type that isn't mapped to an axis is automatically available as a predicate.
- **Identity is per selector** with a `default`. Fields used for identity are also available as predicates. Config loader validates all selectables have an identity field.
- **Axis names can differ from field names.** E.g., the `.extends` axis follows the `parent` field on composites but the `extends` field on interfaces.
- **Same axis, multiple entity kinds.** E.g., `.fields` follows `fields` on composites, interfaces, and variants — expressed as a list.
- **Groups can reference other groups.** E.g., `primitives: [integers, floats, bit, character]` where `integers` and `floats` are themselves groups.
- **Only shortcuts contain TTGE expressions.** Everything else is declarative vocabulary.

### Parsing Strategy

The `.ttgc` parser uses PLY (same infrastructure as TTQ). Key design choices:

- **Comments:** `--` to end of line (same as TTQ)
- **Blocks:** `keyword { key: value, ... }` with comma-separated entries
- **Values:** identifiers, dotted identifiers (`composites.fields`), lists (`[a, b, c]`), strings
- **Shortcut expressions are raw strings.** The `.ttgc` lexer does NOT tokenize TTGE expressions. In the `shortcut` block, after `STRING COLON`, the lexer consumes everything to end of line as a raw string. Continuation lines (lines that don't start with `"` or `}`) are appended.
- **Whitespace normalization:** Multiline shortcut expressions are collapsed to a single line — newlines and leading whitespace replaced with a single space. The TTGE parser receives a clean single-line expression.
- **No TTGE token types in the .ttgc lexer.** This keeps the two parsers fully independent. The `.ttgc` parser produces raw expression strings; the TTGE parser handles them separately.

AST output is simple dataclasses:
```python
@dataclass
class GraphConfig:
    selectors: dict[str, str]           # name → schema type
    groups: dict[str, list[str]]        # name → [selector/group names]
    axes: dict[str, list[str]]          # name → [selector.field, ...]
    reverses: dict[str, str]            # name → forward axis name
    axis_groups: dict[str, list[str]]   # name → [axis names]
    identity: dict[str, str]            # "default" or selector → field
    shortcuts: dict[str, str]           # name → raw TTGE expression string
```

The data structure can be refined when TTGE implementation begins.

### Known Gaps

- **Computed predicates** (e.g., `declared`, `stored` on fields) cannot be expressed. These depend on the traversal context (which type owns the field) and need language support for context-aware expressions.

## Related Documents

- `graph-expression-language.md` — TTGE expression language design (selectors, axes, predicates, operators)
- `meta-schema.ttq` — the meta-schema that describes database schemas (the first concrete source schema)
- `meta-schema.ttgc` — graph config for the meta-schema (first concrete config example)
