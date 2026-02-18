# TTGE (Typed Tables Graph Expression) Language — Design Document

## Status: Active Discussion (2026-02-18)

## Motivation

The current graph command has grown organically with keyword-based modifiers (`structure`, `declared`, `stored`, `showing`, `excluding`, `path-to`, `depth`). While functional (212 tests), it's becoming a bag of special cases. Each new capability requires a new keyword and grammar rule.

The goal is a **composable expression language** that gives users fine-grained control over what appears in the graph. The key insight: we are maintaining a **set of nodes and edges**, and operations on the expression progressively build or refine that set.

## Inspiration

XPath-like semantics — not the syntax, but the idea of:
- **Selectors** that choose initial nodes
- **Axes** that traverse relationships from current nodes
- **Predicates** that filter results
- **Operators** that combine or refine result sets

---

## Entity Catalog

**Note:** This catalog describes the built-in meta-schema entities. Per D22 (Schema-Driven Identifiers), all selectors, axes, and predicates are resolved at runtime against the loaded config. The catalog below is the concrete instance for the type-system schema (`meta-schema.ttgc`). Other schemas would define their own entity kinds and relationships.

### Entity Kinds

These are the concrete node types that can appear in a graph (for the meta-schema):

| Entity Kind | Description | Has Outgoing Edges? |
|-------------|-------------|---------------------|
| **Composite** | User-defined struct-like type | Yes |
| **Interface** | User-defined interface type | Yes |
| **Enum** | User-defined enumeration | Yes |
| **Variant** | A single variant of an enum | Yes (Swift-style) or No (C-style) |
| **Alias** | Named type alias | Yes (points to base) |
| **Overflow** | Overflow-wrapped type (e.g., `saturating int8`) | Yes (points to base + policy) |
| **Array** | Array type (e.g., `uint8[]`) | Yes (element type) |
| **Set** | Set type (e.g., `{string}`) | Yes (element type) |
| **Dictionary** | Dictionary type (e.g., `{string: int32}`) | Yes (key + value types) |
| **Field** | A field on a composite/interface/variant | Yes (points to its type) |
| **String** | Built-in string type | No (leaf) |
| **Boolean** | Built-in boolean type | No (leaf) |
| **Bit** | Built-in bit type | No (leaf) |
| **Character** | Built-in character type | No (leaf) |
| **BigInt** | Arbitrary-precision signed integer | No (leaf) |
| **BigUInt** | Arbitrary-precision unsigned integer | No (leaf) |
| **Fraction** | Exact rational number | No (leaf) |
| **uint8, int8, uint16, int16, uint32, int32, uint64, int64, uint128, int128** | Integer primitives | No (leaf) |
| **float16, float32, float64** | Float primitives | No (leaf) |

### Groupings

Groupings are selector shortcuts that match multiple entity kinds:

| Grouping | Expands To |
|----------|-----------|
| `integers` | uint8, int8, uint16, int16, uint32, int32, uint64, int64, uint128, int128, bigint, biguint |
| `floats` | float16, float32, float64 |
| `primitives` | integers + floats + bit + character |
| `types` | Everything including Interface (any type in the system) |
| `all` | Every entity kind |

**Note:** Interface is included in the `types` grouping because interfaces can be used as field types. The system supports polymorphic field references — an interface-typed field stores a tagged reference `[uint16 type_id][uint32 index]` that identifies the concrete implementing type.

### Edges Per Entity (Single-Step Traversals)

Each entity kind has a fixed set of outgoing edge types. These are the relationships that axis traversals follow.

**Composite:**
| Edge | Target | Cardinality | Description |
|------|--------|-------------|-------------|
| extends | Composite | 0..1 | Parent composite type |
| implements | Interface | 0..n | Interfaces this composite implements |
| has | Field | 0..n | Fields declared on this composite |

**Interface:**
| Edge | Target | Cardinality | Description |
|------|--------|-------------|-------------|
| extends | Interface | 0..n | Parent interfaces |
| has | Field | 0..n | Fields declared on this interface |

**Enum:**
| Edge | Target | Cardinality | Description |
|------|--------|-------------|-------------|
| variant | Variant | 1..n | Variants of this enum |
| backing_type | integer primitive | 0..1 | Backing integer type (e.g., `enum Color : uint8`) |

**Variant:**
| Edge | Target | Cardinality | Description |
|------|--------|-------------|-------------|
| has | Field | 0..n | Fields of this variant (Swift-style only) |

C-style variants have no outgoing edges — they are leaf-like nodes with a name and a discriminant value. Display of C-style variants (name + value) is deferred to when we design graph rendering customization.

**Alias:**
| Edge | Target | Cardinality | Description |
|------|--------|-------------|-------------|
| resolves | any type | 1 | Base type (can be another Alias) |

**Overflow:**
| Edge | Target | Cardinality | Description |
|------|--------|-------------|-------------|
| base | integer primitive | 1 | Base integer type |

Overflow entities also carry a `policy` attribute (`saturating` or `wrapping`) that is not an edge but may be displayed as a node label.

**Array:**
| Edge | Target | Cardinality | Description |
|------|--------|-------------|-------------|
| element | any type | 1 | Element type |

**Set:**
| Edge | Target | Cardinality | Description |
|------|--------|-------------|-------------|
| element | any type | 1 | Element type |

**Dictionary:**
| Edge | Target | Cardinality | Description |
|------|--------|-------------|-------------|
| key | any type | 1 | Key type |
| value | any type | 1 | Value type |

**Field:**
| Edge | Target | Cardinality | Description |
|------|--------|-------------|-------------|
| type | any type | 1 | The type of this field |

Field entities also carry attributes (name, default value, overflow policy) that are not edges but may be displayed as node labels.

**All leaf types** (primitives, String, Boolean, BigInt, BigUInt, Fraction, Bit, Character):
No outgoing edges.

---

## Core Language

### Step 1: Selectors

The expression starts by choosing elements to work with. Initially, the result set is empty.

**Kind selectors** choose nodes by their entity kind:

```
graph composites                              -- all composite types
graph interfaces                              -- all interface types
graph enums                                   -- all enum types
graph aliases                                 -- all alias types
graph arrays                                  -- all array types
graph sets                                    -- all set types
graph dictionaries                            -- all dictionary types
```

**Grouping selectors** expand to multiple kinds:

```
graph integers                                -- all integer types
graph floats                                  -- all float types
graph primitives                              -- all primitive types
graph types                                   -- all types in the system
graph all                                     -- every entity kind
```

**Predicate filtering** narrows a selector:

```
graph composites{name=Person}                 -- single composite by name
graph composites{name=Person|Root}            -- multiple composites (OR)
graph composites{name=!Root}                  -- all composites except Root
graph composites{name=!(Root|Base)}           -- all composites except Root and Base
graph interfaces{name=Sizeable}               -- single interface
```

**Set notation** combines multiple selectors:

```
graph {composites{name=Person}, interfaces{name=Sizeable}}
```

### Step 2: Operators

Operators build and combine result sets. There are three categories: **dot chaining** for navigation, **explicit operators** for building/refining, and **set operators** for combining sub-expressions.

**Dot chaining** (`.axis` — tightest binding):

Dot notation chains axes directly onto selectors or other axes. Dot chaining has **pipe semantics** — each step replaces the current node set with the traversal results.

```
composites{name=Person}.fields.type        -- navigate: Person → Fields → Types (only Types remain)
```

**Explicit operators** (current set OP axis):

| Operator | Node Set | Edge Set | Use For |
|----------|---------|---------|---------|
| `+` (add) | Adds traversal target nodes | Adds traversal edges | Building up the graph (keep intermediates) |
| `/` (pipe) | Replaced by traversal target nodes | Cleared — edges discarded since source nodes are replaced out | Shifting focus (same as dot, but explicit) |
| `-` (subtract) | Removes matched nodes | Prunes edges where either endpoint was removed | Excluding things |

All three chaining operators accept axes consistently. `-` with an axis traverses from the current set, then removes the results. Subtracting nodes that aren't in the set is a no-op (standard set subtraction). `-` also accepts selectors: `- primitives` removes all primitive nodes from the set.

**Set operators** (expression OP expression):

| Operator | Result | Use For |
|----------|--------|---------|
| `\|` (union) | All nodes/edges from both sides | Merging independent subgraphs |
| `&` (intersection) | Only nodes in both sides; edges where both endpoints survive | Finding paths between types |

**Dot vs `+` vs `/`:**
- `.axis` is equivalent to `/ .axis` — navigate, replacing the current set
- `+ .axis` — accumulate, keeping the current set and adding traversal results
- Use `.` for navigation chains; use `+` when you want to keep intermediate nodes visible

```
-- Dot: navigate to field types (only types in result)
composites{name=Person}.fields.type

-- Plus: build visible path (Person + Fields + Types all in result)
composites{name=Person} + .fields + .type
```

**Dot chaining after `+`:** Because `.` binds tighter than `+`, a dot chain after `+` is evaluated as a unit. The `+` adds only the endpoint of the chain:

```
-- Dot chain after +: adds only types (Fields are navigated through, not kept)
composites + .fields.type
-- parses as: composites + (.fields.type)
-- Result: {composites, types} — no Field nodes

-- Separate + operators: adds everything at each step
composites + .fields + .type
-- parses as: (composites + .fields) + .type
-- Result: {composites, Fields, types} — all intermediates visible
```

**Parentheses** `()` group sub-expressions to override operator precedence:

```
(composites{name=Person} + .fields) | (composites{name=Root} + .fields)
```

**Edge survival:** Edges only survive if both endpoints are in the result set. With dot/pipe, replaced nodes take their edges with them. `composites{name=Person}.fields.type` yields only type nodes with no edges — Person and Field nodes were navigated through and discarded. Use `+` to keep intermediates and their edges visible.

**There is one result set, not separate "working" and "display" sets.** The expression describes exactly what appears in the final graph. Nothing is implicitly accumulated. Edges only enter the set through explicit axis traversal — co-presence in the node set does NOT imply edges.

### Step 3: Axes

Axes traverse relationships from nodes in the current node set. Each traversal produces new **nodes** and new **edges**.

**Forward axes:**

| Axis | Traverses Edge | From Entity Kinds | Description |
|------|---------------|-------------------|-------------|
| `.fields` | has | Composite, Interface, Variant | Produces Field nodes |
| `.extends` | extends | Composite, Interface | Produces parent type nodes |
| `.interfaces` | implements | Composite | Produces Interface nodes |
| `.variants` | variant | Enum | Produces Variant nodes |
| `.backing` | backing_type | Enum | Produces integer primitive node |
| `.alias` | resolves | Alias | Produces base type node |
| `.base` | base | Overflow | Produces base integer type node |
| `.element` | element | Array, Set | Produces element type node |
| `.key` | key | Dictionary | Produces key type node |
| `.value` | value | Dictionary | Produces value type node |
| `.type` | type | Field | Produces the field's type node |

**Reverse axes:**

| Axis | Meaning | Available On | Inverse Of |
|------|---------|-------------|------------|
| `.children` | who extends me? | Composite, Interface | `.extends` |
| `.implementers` | who implements me? | Interface | `.interfaces` |
| `.owner` | who owns this field? | Field | `.fields` |
| `.enum` | which enum owns this variant? | Variant | `.variants` |
| `.aliasedBy` | which aliases resolve to me? | any type | `.alias` |
| `.elementOf` | which Array/Set has me as element? | any type | `.element` |
| `.keyOf` | which Dict has me as key? | any type | `.key` |
| `.valueOf` | which Dict has me as value? | any type | `.value` |
| `.typedBy` | which Fields have me as their type? | any type | `.type` |
| `.backedBy` | which Enums use me as backing type? | integer primitives | `.backing` |
| `.wrappedBy` | which Overflow types wrap me? | integer primitives | `.base` |

**Axis groupings** (analogous to type groupings like `integers`, `floats`):

| Grouping | Expands To | Meaning |
|----------|-----------|---------|
| `.all` | `{.fields, .extends, .interfaces, .variants, .backing, .type, .alias, .base, .element, .key, .value}` | Every forward axis |
| `.allReverse` | `{.children, .implementers, .owner, .enum, .typedBy, .aliasedBy, .backedBy, .wrappedBy, .elementOf, .keyOf, .valueOf}` | Every reverse axis |
| `.referencedBy` | `{.typedBy, .aliasedBy, .backedBy, .wrappedBy, .elementOf, .keyOf, .valueOf}` | Everything that references me through any edge |

More axis groupings can be added as patterns emerge.

Axes that don't apply to a node's entity kind are **silently ignored**. This is important for mixed-kind sets.

### Step 4: Axis Predicates

Axes accept `{key=value}` predicates that serve four purposes: **filtering**, **labeling**, **projection**, and **depth control**.

#### Filtering

Narrow which nodes the axis traverses to:

```
.fields{name=age}                   -- only the 'age' field
.fields{name=age|name}              -- 'age' or 'name' fields
.fields{name=!age}                  -- all fields except 'age'
.fields{name=!(age|name)}           -- all fields except 'age' and 'name'
.interfaces{name=Sizeable}          -- only Sizeable interface
.fields{declared=true}              -- only fields declared on this type (not inherited)
.fields{stored=true}                -- all fields including inherited
```

**Predicate value expressions** support `|` (OR), `!` (NOT), and `()` for grouping. The negation goes inside the value (`name=!X`), keeping the dictionary-like `key=value` structure. The `-` (subtract) operator remains available for more complex exclusion patterns at the expression level.

**Bare axes** (no predicate) default to all matches: `.fields` = `.fields{name=*}`.

#### Labeling (`label=`)

Control what label appears on the produced edges. Without `label=`, edges are labeled with the edge type name (e.g., "has", "extends").

```
.fields{label=.name}               -- label edge with the field's name ("age", "name", etc.)
.fields{label="has"}               -- literal label
.extends{label="inherits"}         -- override default label
.variants{label=join(", ", .fields.name)}  -- aggregate: "cx, cy, r"
```

**Label expressions** (mini-language):
- `.axis` — follow an axis from the traversal target, use the resulting node's display name
- `.axis.axis` — path chaining (follow multiple axes)
- `"literal"` — literal string value
- `join(separator, expr)` — aggregate multiple values into one string

#### Projection (`result=`)

Control what nodes actually enter the result set. Without `result=`, the direct traversal targets are added. With `result=`, follow a sub-path from each target and add those nodes instead. The original targets become **transient** — used for computation but not in the final result.

```
.fields{result=.type}              -- add the field's type node instead of the Field node
.fields{label=.name, result=.type} -- compact form: Person --name--> string
.variants{result=.fields.type}     -- skip through variant fields to their types
```

**Result expressions** (mini-language):
- `.axis` — follow an axis, add those nodes instead
- `.axis.axis` — path chaining (follow multiple axes)

**Key example** — collapsing Field into a single-hop edge:
```
-- Without result=: two hops with Field as intermediate node
graph composites{name=Person} + .fields + .type
-- Result: Person → [Field:name] → string, Person → [Field:age] → uint8

-- With result=: single hop, Field is transient
graph composites{name=Person} + .fields{label=.name, result=.type}
-- Result: Person --name--> string, Person --age--> uint8
```

#### Depth Control (`depth=`)

Control how many times the axis traversal repeats (recursion). Without `depth=`, the axis is applied once. With `depth=N`, the axis is re-applied to the newly added nodes up to N times. `depth=infinity` (or `depth=inf`) repeats until no new nodes are discovered.

```
.extends{depth=2}                  -- follow extends chain up to 2 levels
.extends{depth=inf}                -- full inheritance chain
.fields{label=.name, result=.type, depth=inf}  -- transitive closure of field references
.referencedBy{depth=inf}           -- everything that references me, transitively
```

**Depth semantics:**
- `depth=0` — no-op (zero applications, axis does nothing)
- `depth=1` — same as bare axis (one application, the default)
- `depth=2` — apply axis, then apply again on results
- `depth=N` — repeat N times total
- `depth=infinity` / `depth=inf` — repeat until stable (no new nodes)

When `depth=` is used with `result=`, each depth level is one complete application of the axis including the projection. For example, `.fields{result=.type, depth=2}` means: get field types (depth 1), then get those types' field types (depth 2), stop.

**Cycle detection** is inherent in the `depth=inf` semantics: "repeat until no new nodes are discovered." If a cycle is encountered, the already-visited nodes are not new, so the recursion terminates naturally. Implementations should track visited nodes to ensure this.

### Compound Axis Expressions

Set notation applies multiple axes in one step using explicit operators:

```
graph composites{name=Root} + {.fields, .extends, .interfaces}     -- plus: keep Root, add all three
graph composites{name=Root} / {.fields, .extends, .interfaces}     -- pipe: replace Root with all three
```

Dot chaining does not support branching — use `/` with set notation instead. This keeps axis syntax consistent (always `.name`).

---

## Examples

### Basic — Building Up with `+`

```
-- Just show composite nodes, no edges
graph composites

-- Person with its field nodes (intermediate Field entities visible)
graph composites{name=Person} + .fields

-- Person fields resolved to their types (Person + Fields + Types all visible)
graph composites{name=Person} + .fields + .type

-- Person with structure (extends + implements)
graph composites{name=Person} + {.extends, .interfaces}

-- Person with only declared fields (not inherited)
graph composites{name=Person} + .fields{declared=true}

-- Person with all stored fields (inherited + own)
graph composites{name=Person} + .fields{stored=true}

-- Two composites with their fields
graph composites{name=Person|Root} + .fields

-- A composite and an interface together
graph {composites{name=Person}, interfaces{name=Sizeable}} + .fields

-- All composites with their fields, minus primitives
graph composites + .fields + .type - primitives
```

### Navigation with Dot Chaining

```
-- Navigate to Person's field types (only types remain, Person and Fields gone)
graph composites{name=Person}.fields.type

-- Navigate to Person's parent interfaces (only interfaces remain)
graph composites{name=Person}.extends.interfaces

-- Chain with predicates
graph composites{name=Person}.fields{declared=true}.type

-- Dot chaining in path-to (equivalent to / form)
composites{name=Boss}.all{depth=inf}
& interfaces{name=Entity}.allReverse{depth=inf}
```

### Enums

```
-- Enum with its variants
graph enums{name=Shape} + .variants

-- Enum variants with their fields and field types
graph enums{name=Shape} + .variants + .fields + .type

-- Enum variants collapsed to their field types
graph enums{name=Shape} + .variants{label=.name, result=.fields.type}
```

### Compact Form (label= and result=)

```
-- Collapse Field into edge label (reproduces current graph behavior)
graph composites{name=Person} + .fields{label=.name, result=.type}
-- Result: Person --name--> string, Person --age--> uint8

-- Transitive closure of field references (compact, recursive)
graph composites{name=Person} + .fields{label=.name, result=.type, depth=inf}
-- Result: Person --name--> string, Person --age--> uint8,
--         Person --address--> Address, Address --street--> string, etc.
```

### Depth and Recursion

```
-- Root's full inheritance chain
graph composites{name=Root} + .extends{depth=inf}

-- Alias chain: follow alias → alias → ... → base type
graph aliases{name=uuid} + .alias{depth=inf}

-- Root's interfaces, including ancestor interfaces
graph composites{name=Root} + .interfaces{depth=inf}
```

### Reverse Traversal

```
-- Everything that references string, transitively
graph primitives{name=string} + .referencedBy{depth=inf}

-- What composites implement Sizeable?
graph interfaces{name=Sizeable} + .implementers

-- What extends Base, recursively?
graph composites{name=Base} + .children{depth=inf}
```

### Set Operations — Union

```
-- Merge two independently built subgraphs
(composites{name=Person} + .fields{label=.name, result=.type})
| (composites{name=Root} + .fields{label=.name, result=.type})
-- Result: both Person and Root with their field edges, merged together

-- Path with target expansion (see Path-To below)
```

### Set Operations — Intersection (Path-To)

Finding paths between types uses intersection of forward and backward expansions. Given this schema:

```ttq
interface Entity { id: uuid }
interface Combatant from Entity { hp: int32 }
type Warrior from Combatant { weapon: string }
type Boss from Warrior { level: uint8 }
```

**Find the structural path from Boss to Entity:**

```
composites{name=Boss} + .all{depth=inf}
& interfaces{name=Entity} + .allReverse{depth=inf}
```

How it works:
1. **Left side** — forward from Boss with `.all{depth=inf}`:
   - Boss → .extends → Warrior → .interfaces → Combatant → .extends → Entity
   - Also reaches: Field nodes, uint8, string, int32, uuid (via .fields + .type)
   - Full set: {Boss, Warrior, Combatant, Entity, Field:level, Field:weapon, Field:hp, Field:id, uint8, string, int32, uuid, ...}

2. **Right side** — backward from Entity with `.allReverse{depth=inf}`:
   - Entity → .children → Combatant → .implementers → Warrior → .children → Boss
   - Full set: {Entity, Combatant, Warrior, Boss, ...plus anything else that references Entity}

3. **Intersection** — nodes present in both sides:
   - {Boss, Warrior, Combatant, Entity}
   - Field nodes and leaf types are pruned (only in the left side)
   - Surviving edges: Boss→Warrior (extends), Warrior→Combatant (implements), Combatant→Entity (extends)

Result: the structural path with all routes between Boss and Entity. Multiple paths are preserved if they exist.

**Path from Boss to Entity, with Entity's fields expanded** (matches current `graph Boss to Entity` behavior):

```
(composites{name=Boss} + .all{depth=inf}
 & interfaces{name=Entity} + .allReverse{depth=inf})
| (interfaces{name=Entity} + .fields{label=.name, result=.type})
```

How it works:
1. **Left of `|`** — the intersection produces the path: {Boss, Warrior, Combatant, Entity} with structural edges
2. **Right of `|`** — Entity's field expansion built independently: {Entity, uuid} with Entity --id--> uuid edge
3. **Union** — merges both: {Boss, Warrior, Combatant, Entity, uuid} with all edges from both sides

Entity appears in both sides, naturally bridging the path to the field expansion. The `|` operator is essential here — it builds the target expansion independently so that only Entity's fields are expanded, not every type on the path.

**Find how Person relates to string (through field references):**

```
composites{name=Person} + .all{depth=inf}
& primitives{name=string} + .allReverse{depth=inf}
```

How it works:
1. **Left** — forward from Person reaches: Person, Field:name, string, Field:age, uint8, ...
2. **Right** — backward from string reaches: string, Field:name (and other string-typed fields), Person, Address, Root, ...
3. **Intersection**: {Person, Field:name, string} — the path through the name field, with edges Person→Field:name (has) and Field:name→string (type)

This works for any relationship type, not just structural — field references, alias chains, collection containment, etc.

---

## Mapping to Current Features

| Current Syntax | Proposed Expression | Notes |
|---------------|-------------------|-------|
| `graph` | `graph` (empty shortcut → full graph template) | Schema shortcut (D23) |
| `graph Person` | `graph composites{name=Person} + .fields{label=.name, result=.type} + .extends + .interfaces` | Sugar deferred (D2) |
| `graph [Person, Employee]` | `graph composites{name=Person\|Employee} + .fields{label=.name, result=.type} + .extends + .interfaces` | Direct mapping |
| `graph all Composites` | `graph composites + .fields{label=.name, result=.type} + .extends + .interfaces` | Direct mapping |
| `graph all Interfaces` | `graph interfaces + .fields{label=.name, result=.type} + .extends` | Direct mapping |
| `graph Person structure` | `graph composites{name=Person} + {.extends, .interfaces}` | Structure only |
| `graph Person declared` | `graph composites{name=Person} + .fields{declared=true, label=.name, result=.type}` | Declared fields |
| `graph Person stored` | `graph composites{name=Person} + .fields{stored=true, label=.name, result=.type}` | Stored fields |
| `graph Person depth 2` | `graph composites{name=Person} + .fields{depth=2, label=.name, result=.type}` | Per-axis depth |
| `graph showing type string` | `graph primitives{name=string} + .referencedBy{depth=inf}` | Reverse axis grouping |
| `graph excluding type [uint8, uint16]` | `... - primitives{name=uint8\|uint16}` | Subtract |
| `graph Boss to Entity` | `(composites{name=Boss} + .all{depth=inf} & interfaces{name=Entity} + .allReverse{depth=inf}) \| (interfaces{name=Entity} + .fields{label=.name, result=.type})` | Intersection + union |
| `graph > "file.dot"` | `graph > "file.dot"` (empty shortcut + output) | Schema shortcut (D23) |
| `graph{"title":"X"} > "f.dot"` | `graph style { "title": "X" }` then `graph > "f.dot"` (empty shortcut + session style) | Session-level style (D3) |

---

## Decided

### D1: Single Result Set (no display/working split)

There is one result set containing nodes and edges. The expression describes exactly what goes into the final graph — nothing more, nothing less.

- `.axis` (dot chaining) navigates — replaces current nodes with traversal targets (equivalent to `/`).
- `+` adds traversal target nodes and edges to the set (keeps intermediates).
- `/` replaces: target nodes become the new node set, edges are cleared (same as dot, but explicit).
- `-` removes nodes and prunes any edges that reference removed nodes.

Edges only enter the set through explicit axis traversal. Co-presence in the node set does NOT imply edges — `composites{name=Person} + composites{name=Root}` gives two isolated nodes with no edges between them.

### D2: Sugar Deferred

Build the explicit expression language first. Sugar for common patterns is a later concern — only after the core model is proven.

### D4: Depth Control

Use `depth=N` only — no separate `recursive=true` keyword. `depth=infinity` (or `depth=inf`) means "repeat until no new nodes are discovered." Depth is per-axis since different axes may need different limits.

When used with `result=`, each depth level is one complete application of the axis including the projection. This solves the two-hop recursion problem:
```
graph composites{name=Person} + .fields{label=.name, result=.type, depth=inf}
```

### D5: Reverse Axes and Axis Groupings

Named reverse axes for every forward axis. Full table in the Axes section. Names can be refined later.

Axis groupings parallel type groupings:
- `.all` — every forward axis
- `.allReverse` — every reverse axis
- `.referencedBy` — `{.typedBy, .aliasedBy, .elementOf, .keyOf, .valueOf, .backedBy}`

More groupings can be added as patterns emerge.

### D8: Path-To via Set Operations

Path-to queries are expressed using intersection (`&`) and union (`|`) rather than a dedicated operator. This composes from existing primitives.

**Finding paths:** Intersect forward expansion from source with backward expansion from target. Only nodes reachable from BOTH survive — these are exactly the nodes on paths between them.

**Expanding the target:** Use union (`|`) to merge the path with an independently built target expansion. This ensures only the target type's fields are expanded, not every type on the path.

**Cycle detection:** Built into `depth=inf` semantics — already-visited nodes are not "new," so recursion terminates naturally.

### D9: Bare Selectors = Nodes Only

Bare selectors give you nodes with no edges. Everything is explicit. Follows directly from D1.

### D11: Entity Catalog

Field and Variant are first-class entity kinds in the graph. The full catalog is documented above.

**Field as entity:** The `.fields` axis on a Composite/Interface produces Field nodes. To reach the field's type, chain `.type`. This gives two hops: `composites{name=Person} + .fields + .type` yields Person → [name] → string, Person → [age] → uint8, etc. How Field nodes are rendered in the graph (compact edge vs expanded node) is deferred to the rendering/display design.

**Variant as entity:** The `.variants` axis on an Enum produces Variant nodes. C-style variants are leaf-like (name + discriminant value). Swift-style variants can be further traversed via `.fields` to reach their associated value types.

**Interface as valid field type:** Confirmed — the system supports polymorphic interface-typed fields via 6-byte tagged references `[uint16 type_id][uint32 index]`. Interface is included in the `types` grouping.

### D12: Edge Labeling and Projection

Axis predicates support `label=` and `result=` options for controlling edge labels and which nodes enter the result set. Full specification in the Axis Predicates section above.

- `label=` controls edge display text (default: edge type name)
- `result=` controls which nodes are added (default: direct traversal targets)
- Both accept axis path expressions (`.name`, `.type`, `.fields.name`)
- `label=` additionally supports literals (`"has"`) and aggregation (`join(", ", expr)`)

Key pattern — collapsing Field to reproduce current compact graph:
```
.fields{label=.name, result=.type}  -- Person --name--> string (one hop, not two)
```

### D3: Config, Style, and the `metadata` Prefix

Configuration and style are **session state**, not persisted per-database. There is no registry. The user sets config and style before running expressions.

**Two independent contexts**, selected by the `metadata` prefix:

- **Data context** (no prefix) — the active database. Config and style start empty; must be set explicitly.
- **Metadata context** (`metadata` prefix) — the active database's schema. Has built-in defaults (`meta-schema.ttgc` and a default style, bundled with the package).

**Switching databases** (TTQ `use`) clears both contexts. Metadata reinitializes with built-in defaults for the new database.

**Config** validates against the target database's schema at load time:
```
graph config "social-graph.ttgc"              -- set data config (from TTQ/REPL)
graph metadata config "custom-meta.ttgc"      -- override built-in meta-schema config (rare)
```

**Style** supports three forms, in both data and metadata contexts:
1. `[metadata] style "file"` — load file, replace current style entirely.
2. `[metadata] style "file" { "key": "val" }` — load file, then apply inline overrides.
3. `[metadata] style { "key": "val" }` — amend current style in place. Error if no config active.

```
-- From the REPL
graph config "social-graph.ttgc"
graph style "light.ttgs"
graph style { "direction": "LR" }                      -- amend data style
graph metadata style { "direction": "LR" }             -- amend meta-schema style
graph users + .friends{label=.name}                    -- data query
graph metadata composites + .fields{label=.name, result=.type}  -- schema query
```

This separates concerns: the expression defines what's in the graph, `>` defines where it goes, and session-level config/style define how it's rendered.

### D6: Expression Variables

Deferred to a future scripting/language design discussion. Expression variables push the graph command from "single expression" into "mini-script" territory (multiple lines of expressions), which doesn't fit the REPL model well. Note: TTGE scripts (D31) partially address the multi-line need by allowing sequences of commands.

### D31: TTGE Scripts

TTGE scripts (`.ttg` files) bundle config, style, and expressions into executable files. They provide the multi-line capability that D6 deferred, without requiring expression variables.

**Invocation:**
- From TTQ context: `graph execute "file.ttg"`
- From TTGE context (inside `.ttg` files): `execute "file.ttg"`

**Inside `.ttg` files**, no `graph` prefix — everything is TTGE context. Available commands: `config`, `style`, `metadata config`, `metadata style`, `metadata <expr>`, `<expr>`, `execute`.

**Scripts assume a database is already selected** (database selection is a TTQ concern). Scripts can set their own config/style, or inherit whatever the caller set. Scripts can execute other scripts. Scripts stop on errors.

**Cycle detection and relative paths** follow the same patterns as TTQ's `execute` command.

```
-- schema-report.ttg
metadata style { "direction": "LR" }
metadata composites + .fields{label=.name, result=.type} > "composites.dot"
metadata enums + .variants > "enums.dot"
```

### D7: Result Type and Table Rendering

**TTGE's native result type** (`GraphResult`) is defined in the TTGE module with no dependency on TTQ or the REPL:

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

**Two output paths:**

1. **File output** (`> "file.dot"`): TTGE serializes to DOT/TTQ format and writes the file. Returns a status message (e.g., `FileResult(path, edge_count)`) or `None`.
2. **No file output**: TTGE returns a `GraphResult`. The caller (TTQ executor or REPL) renders it as a table, mapping `source`→column 1, `label`→column 2, `target`→column 3.

**Dependency direction is strictly one-way:**
- TTQ/REPL → calls TTGE, reads `GraphResult`
- TTGE → uses storage layer (reads databases)
- TTGE → does NOT depend on TTQ or REPL

TTGE does not know about tables. It returns its native result type. The caller decides how to display it.

**Entity kind qualification:** When Field entities appear in results, they are prefixed with their entity kind to avoid ambiguity with type names (e.g., `Field:name` not just `name`). This qualification is part of the node identity string in `GraphResult`.

**Examples** (as rendered by the REPL):

Nodes only (`graph composites`):
```
source | label | target
-------+-------+-------
Base   |       |
Root   |       |
Person |       |
```

Two-hop with Field entities (`graph composites{name=Person} + .fields + .type`):
```
source         | label | target
---------------+-------+-----------
Person         | has   | Field:name
Person         | has   | Field:id
Person         | has   | Field:age
Person         | has   | Field:nicknames
Person         | has   | Field:code
Field:name     | type  | string
Field:id       | type  | uuid
Field:age      | type  | uint8
Field:nicknames| type  | {string}
Field:code     | type  | [uint8]
```

Compact form (`graph composites{name=Person} + .fields{label=.name, result=.type}`):
```
source | label     | target
-------+-----------+---------
Person | name      | string
Person | id        | uuid
Person | age       | uint8
Person | nicknames | {string}
Person | code      | [uint8]
```

Mixed — isolated nodes appear with empty label and target (`graph composites + .extends`):
```
source | label   | target
-------+---------+--------
Root   | extends | Base
Base   |         |
Person |         |
```

### D10: Field and Variant Rendering

Largely resolved by D12 (`label=` / `result=`). The compact vs expanded choice is handled by axis predicates — `.fields{label=.name, result=.type}` collapses Field into an edge label, while `.fields + .type` keeps Field as a visible node. The residual question of node display properties (how nodes look when they ARE in the result set) can be folded into the broader rendering/style discussion.

### D13: Set Operations

Full set algebra on result sets:
- `|` (union) — merge two independently built subgraphs
- `&` (intersection) — keep only nodes/edges present in both sides

These enable path-to queries (D8) and combining independent graph fragments. Set operators work on complete sub-expressions, unlike chaining operators (`+`, `/`, `-`) which work with axes from the current node set.

### D14: Operator Precedence

Four levels, tightest to loosest:

| Level | Operators | Associativity | Role |
|-------|-----------|---------------|------|
| 0 (tightest) | `.` (dot chain) | left-to-right | Navigation: traverse axes from current set |
| 1 | `+`, `/`, `-` | left-to-right | Build/refine: accumulate, pipe, or subtract |
| 2 | `&` | left-to-right | Intersection: combine two chains |
| 3 (loosest) | `\|` | left-to-right | Union: merge independent subgraphs |

`.` binds tightest — `composites{name=Person}.fields.type` is a single unit. `&` above `|` follows boolean AND/OR convention. Parentheses available for explicit grouping.

This eliminates mandatory parentheses in path-to expressions:
```
composites{name=Boss} + .all{depth=inf}
& interfaces{name=Entity} + .allReverse{depth=inf}
| interfaces{name=Entity} + .fields{label=.name, result=.type}
```

### D30: depth=0

`depth=0` is allowed as a no-op (zero applications — the axis does nothing). Mathematically consistent and potentially useful if depth values are computed or parameterized.

### D29: Default Sort Order

Unspecified when no `sort by` is given. If predictable output is needed, use `sort by`. This keeps the implementation free to use whatever internal representation is efficient. Can be refined after implementation experience.

### D28: Deduplication and Node Identity

The result set uses **set semantics** — nodes and edges are deduplicated. No duplicate rows in table output, no duplicate nodes in DOT output.

**Node identity is schema-defined.** For the type-system schema, identity = type name. One `float32` node exists in the graph; multiple fields pointing to it correctly shows "these all use the same type." Edge labels (`label=.name`) preserve field-level distinction even when targets are shared.

For data graphs, identity would differ — e.g., (type, instance index) or (type, value) — so distinct values of the same type become separate nodes. The expression language doesn't change; the schema determines what "same node" means. This is another reason the schema-driven architecture (D22) is the right approach.

### D27: Subtract With Axes

`-` works with axes just like `+` and `/` — all three chaining operators accept axes consistently. `- .axis` traverses the axis from the current set and removes the results. Subtracting nodes not in the set is a no-op (standard set subtraction). `-` also accepts selectors directly: `- primitives`.

### D26: Parentheses

Standard grouping with `()` to override operator precedence. Any sub-expression can be wrapped in parentheses.

### D25: Backward Compatibility

Clean break — old keyword-driven syntax (`graph Person structure depth 2 showing type string`) is removed when the new expression language ships. Tag the release before implementation begins so the old behavior is accessible for comparison if needed.

### D24: Predicate Negation

Predicate values support `!` (NOT), `|` (OR), and `()` for grouping. Negation goes inside the value expression (`name=!X`), preserving the dictionary-like `key=value` structure.

```
name=Person              -- match Person
name=Person|Root         -- match Person or Root
name=!Root               -- match anything except Root
name=!(Person|Root)      -- match anything except Person or Root
```

The `-` (subtract) operator remains for complex exclusion at the expression level. `&` (AND) in predicate values deferred — not useful for name matching, can be added for future predicate keys if needed.

### D23: Schema-Defined Shortcuts

Shortcuts are named expression templates defined by the schema. Before evaluation, the shortcut is rewritten to its template expression at the AST level, then evaluated normally. The expression language has no special cases — shortcuts are a schema concern.

Two shortcuts for the type-system schema:
- `""` (bare `graph` with no expression) → full-graph template (all types with all edges)
- `"all"` → all-nodes template (every node, no edges)

Other schemas can define their own shortcuts for their common patterns. This is how sugar (D2) gets implemented: not as parser special cases, but as schema-provided AST rewrites.

### D21: Error Handling

Axes that find no targets return a silent empty set — this is normal (e.g., `.extends` on a type with no parent). Selectors with `name=` predicates that match nothing emit a **warning** (likely a typo) but the expression still completes. The final result may be empty, rendering as an empty table or empty DOT file.

### D22: Schema-Driven Identifiers

The expression language grammar is **generic** — selectors, axes, and predicate keys are all identifiers resolved at runtime against the loaded schema. No words are reserved in the expression grammar.

```
selector    = IDENTIFIER [predicate_dict]
axis        = '.' IDENTIFIER [predicate_dict]
predicate   = IDENTIFIER '=' value
```

The schema defines what's valid:
- Selector names (`composites`, `interfaces`, ...)
- Axis names per entity kind (`.fields` on Composite, `.element` on Array, ...)
- Predicate keys (`name=`, `declared=`, `depth=`, ...)

Our type-system schema is one instance of this — the built-in one. The same expression language can drive any domain with a schema definition (social network graph, dependency tree, etc.). Error messages for unknown names come from the schema, not the parser.

This eliminates the selector keyword conflict concern — `composites` is just an identifier. It also reframes D19: generalizing isn't about extending the language, it's about plugging in a different schema.

### D20: Dot Chaining

Dot notation chains axes directly onto selectors: `composites{name=Person}.fields.type`. Dot has **pipe semantics** (`/`) — each step replaces the current node set with the traversal results. This contrasts with `+` which accumulates.

```
-- Dot: navigate to field types (only types in result)
composites{name=Person}.fields.type

-- Plus: build visible path (Person + Fields + Types all in result)
composites{name=Person} + .fields + .type
```

Dot chaining is linear only — no branching. For multi-axis traversal, use `/ {.fields, .extends, .interfaces}` instead. This keeps axis syntax consistent (always `.name`).

Dot binds tightest of all operators (precedence level 0). See D14.

### D19: Generalization to Data Queries

Deferred entirely. Schema traversal and data traversal are fundamentally different problems (scale, cardinality, evaluation strategy, result shape). The conceptual vocabulary (selectors, axes, predicates) could inform future data query design, but the implementation and semantics would be separate. The current `from ... select ... where` with dot notation already handles tabular data traversal well.

### D18: Performance

Eager evaluation — no lazy optimization needed. The expression language operates on the schema (type definitions), which is small (tens to hundreds of types). Even worst-case expressions (`all + .all{depth=inf}`) evaluate in microseconds. If the language generalizes to data traversal (actual instances), that would need its own performance design.

### D17: Style Files

Style file format (TTQ dict syntax) unchanged. Style files are loaded via session-level style commands (see D3). Three forms: file only, file with inline overrides, inline amendment. The `metadata` prefix targets the meta-schema style context. Key set expanded to cover all entity kinds:

```
{
    "direction": "LR",
    "composite.color": "#4A90D9",
    "interface.color": "#7B68EE",
    "enum.color": "#50C878",
    "variant.color": "#90EE90",
    "field.color": "#AAAAAA",
    "alias.color": "#DDA0DD",
    "array.color": "#FFB347",
    "set.color": "#FFB347",
    "dictionary.color": "#FFB347",
    "primitive.color": "#D3D3D3"
}
```

`focus.color` dropped — the new expression model has no implicit focus/context roles. Entity kind colors provide visual grouping. Node highlighting can be revisited as a rendering customization if needed.

### D16: Output File and Format

`> "file"` after the expression, format determined by extension (`.dot` for DOT, `.ttq` for TTQ, no extension defaults to `.ttq`). No `>` means table output to stdout.

Full statement structure from TTQ context (REPL or TTQ scripts), prefixed with `graph`:
```
graph config "file.ttgc"
graph [metadata] style "file.ttgs" [{ "key": "value", ... }]
graph [metadata] style { "key": "value", ... }
graph metadata config "file.ttgc"
graph [metadata] <expression> [sort by ...] [> "file"]
graph execute "file.ttg"
```

From TTGE context (`.ttg` scripts), no `graph` prefix:
```
config "file.ttgc"
[metadata] style "file.ttgs" [{ "key": "value", ... }]
[metadata] style { "key": "value", ... }
metadata config "file.ttgc"
[metadata] <expression> [sort by ...] [> "file"]
execute "file.ttg"
```

`sort by` only applies when no file output (no `>`) — it orders the `GraphResult.edges` list that TTGE returns to the caller. Rendering metadata (title, direction, colors) is set via session-level config and style commands (see D3), not per-expression. TTQ output schema details (adapting for Field/Variant entities) are an implementation concern, not a language design question.

### D15: Sorting

`sort by field[, field]` after the expression, before `>` (if present). Ascending default. Fields correspond to `GraphEdge` attributes: `source`, `label`, `target`.

```
graph composites + .fields{label=.name, result=.type} sort by source
graph composites + .fields sort by source, label
```

TTGE applies the sort to its `GraphResult.edges` list before returning it. No default sort order — results come in whatever order the evaluation produces (D29). Silently ignored when outputting to a file. `asc`/`desc` modifiers deferred — add only if needed.

---

## Formal Grammar (EBNF)

This grammar serves as the implementation specification for the TTGE parser. Notation: `{ x }` means zero or more repetitions, `[ x ]` means optional, `|` separates alternatives, `(* ... *)` are comments. Terminal tokens are in `CAPS` or quoted strings.

### Statement Grammar

A TTGE statement is the unit parsed by the TTGE parser. From TTQ context, the `graph` prefix has already been stripped before reaching TTGE.

```ebnf
statement       = config_stmt
                | meta_config_stmt
                | style_stmt
                | meta_style_stmt
                | execute_stmt
                | meta_expr_stmt
                | expr_stmt ;

config_stmt     = "config" , STRING ;
meta_config_stmt = "metadata" , "config" , STRING ;

style_stmt      = "style" , style_args ;
meta_style_stmt = "metadata" , "style" , style_args ;

style_args      = STRING , [ dict_literal ]
                | dict_literal ;

execute_stmt    = "execute" , STRING ;

meta_expr_stmt  = "metadata" , expression , [ sort_clause ] , [ output_clause ] ;
expr_stmt       = expression , [ sort_clause ] , [ output_clause ] ;

sort_clause     = "sort" , "by" , sort_key , { "," , sort_key } ;
sort_key        = "source" | "label" | "target" ;

output_clause   = ">" , STRING ;

dict_literal    = "{" , [ dict_entry , { "," , dict_entry } ] , "}" ;
dict_entry      = STRING , ":" , STRING ;
```

### Expression Grammar

Precedence from tightest to loosest: `.` (dot) → `+` `/` `-` (chain) → `&` (intersection) → `|` (union). All left-to-right associative.

```ebnf
expression      = union_expr ;

union_expr      = isect_expr , { "|" , isect_expr } ;

isect_expr      = chain_expr , { "&" , chain_expr } ;

chain_expr      = dot_expr , { chain_op } ;

chain_op        = "+" , axis_operand
                | "/" , axis_operand
                | "-" , subtract_operand ;

(* "-" can subtract axis results OR selector/set nodes *)
subtract_operand = axis_operand
                 | atom ;

dot_expr        = atom , { "." , axis } ;

atom            = selector
                | "(" , expression , ")"
                | set_literal ;

set_literal     = "{" , expression , { "," , expression } , "}" ;

selector        = IDENTIFIER , [ pred_dict ] ;
```

### Axis Grammar

```ebnf
axis            = IDENTIFIER , [ pred_dict ] ;

(* Single axis chain or compound axis set *)
axis_operand    = "." , axis , { "." , axis }
                | "{" , "." , axis , { "," , "." , axis } , "}" ;
```

### Predicate Grammar

Predicate values use a generic syntax. The TTGE engine validates value types against the loaded config (D22 — schema-driven identifiers, no reserved words in expressions).

```ebnf
pred_dict       = "{" , predicate , { "," , predicate } , "}" ;

predicate       = IDENTIFIER , "=" , pred_value ;

pred_value      = name_expr
                | axis_path
                | join_expr
                | INTEGER
                | INFINITY
                | BOOLEAN
                | STRING ;

(* Name matching with OR and NOT — used for filtering predicates *)
name_expr       = name_term , { "|" , name_term } ;

name_term       = "!" , name_atom
                | name_atom ;

name_atom       = IDENTIFIER
                | "(" , name_expr , ")" ;

(* Axis path — used for label= and result= *)
axis_path       = "." , IDENTIFIER , { "." , IDENTIFIER } ;

(* Aggregation — used for label= *)
join_expr       = "join" , "(" , STRING , "," , axis_path , ")" ;
```

### Terminals

```ebnf
IDENTIFIER      = letter , { letter | digit | "_" } ;
STRING          = '"' , { any_char - '"' } , '"' ;
INTEGER         = digit , { digit } ;
INFINITY        = "inf" | "infinity" ;
BOOLEAN         = "true" | "false" ;
COMMENT         = "--" , { any_char - newline } , newline ;
```

### Disambiguation Rules

Curly braces `{` have four roles depending on context. These are syntactically unambiguous:

| Context | Meaning | Distinguished By |
|---------|---------|-----------------|
| After IDENTIFIER (no whitespace) | Predicate dict | Contents: `IDENTIFIER = ...` |
| After `+` `/` `-` | Compound axis set | Contents start with `.` |
| As atom (start of expression, after operator, after `(`, after `,` in set) | Set literal | Contents are expressions (no leading `.`, no `=`) |
| After `style` or as `style` argument | Dict literal | Contents: `STRING : STRING` |

In an LALR(1) parser, the key disambiguations:
- **pred_dict vs set_literal:** `{` immediately following an IDENTIFIER is always a pred_dict (part of the `selector` or `axis` production). Standalone `{` is a set_literal.
- **compound axis set vs set_literal:** After a chain operator, `{` followed by `.` is a compound axis set. `{` followed by IDENTIFIER is a set_literal (for `- {composites, interfaces}`).
- **dict_literal:** Only appears in `style_args`, never in expression context.

### Contextual Keywords

Per D22, no words are reserved in the expression grammar — all selector names, axis names, and predicate keys are identifiers resolved at runtime against the loaded config.

The following are **contextual keywords**, recognized only in specific positions:

| Keyword | Context |
|---------|---------|
| `config`, `style`, `execute`, `metadata` | Statement start only |
| `sort`, `by` | After expression, before `>` |
| `source`, `label`, `target` | After `sort by` |
| `inf`, `infinity` | Predicate value position |
| `true`, `false` | Predicate value position |
| `join` | Predicate value position |

All of these can be used as identifiers in other positions (e.g., `composites{name=sort}` is valid).

---

## Implementation Notes

Details to resolve during implementation, not requiring design decisions:

- **Predicate syntax:** Multiple predicates are comma-separated within `{}` (e.g., `{label=.name, result=.type, depth=inf}`). Duplicate keys within a single predicate dict should be an error.

- **Predicate value types are key-dependent.** `name` expects identifiers (with `|`, `!`, `()` for boolean logic). `depth` expects integers or `inf`. `label` expects label expressions (axis paths, literals, `join()`). `result` expects axis path expressions. `declared`/`stored` expect booleans. For arbitrary schemas (D22), the schema defines what value types each predicate key accepts.

- **TTQ output schema** needs updating — the current `enum NodeRole { focus, context, endpoint, leaf }` doesn't apply to the new model (no implicit focus/context roles).

- **Edge identity for deduplication:** Two edges are the same when they share (source, target, label). Person→string labeled "name" and Person→string labeled "nicknames" are different edges; duplicate (source, target, label) triples are deduplicated.

- **`result=` can produce multiple nodes** from a single traversal step. E.g., `.variants{result=.fields.type}` on a Swift-style variant with multiple fields produces multiple type nodes. Each becomes a separate edge from the source. Works naturally with set semantics.

- **`{}` parsing context:** See Disambiguation Rules in the Formal Grammar section for the four roles of `{` and how they are syntactically distinguished.

---

## Things Not Yet Discussed

- ~~**Sorting:**~~ Decided — see D15
- ~~**Output file and format:**~~ Decided — see D16
- ~~**Style files:**~~ Decided — see D17
- ~~**Generalization to data queries:**~~ Deferred — see D19
- ~~**Performance:**~~ Decided — see D18
- ~~**Operator precedence:**~~ Decided — see D14

---

## Revision History

- 2026-02-15: Initial document created from discussion
- 2026-02-15: Added entity catalog (D11), decided D1/D2/D9, restructured axes around Field/Variant entities
- 2026-02-15: Decided D5 — named reverse axes for all forward axes, axis groupings (`.referencedBy`)
- 2026-02-15: Decided D4 — depth=N only (depth=inf for unbounded), decided D12 — label=/result= axis predicates
- 2026-02-15: Decided D8 — path-to via intersection/union, decided D13 — set operations (`&`, `|`), added `.all`/`.allReverse` axis groupings
- 2026-02-15: Decided D6 — deferred to future scripting discussion; D7 — `source | edge | target` table format with entity kind qualification; D10 — largely resolved by D12
- 2026-02-15: Decided D3 — trailing `@{...}` metadata after output clause
- 2026-02-15: Decided D14 — operator precedence: chaining > `&` > `|`
- 2026-02-15: Decided D15 — `sort by` after expression, table output only, ascending default
- 2026-02-15: Decided D16 — output file/format unchanged, full statement structure documented
- 2026-02-15: Decided D17 — style files: expanded key set for all entity kinds, dropped focus.color
- 2026-02-15: Decided D18 — eager evaluation, no lazy optimization needed for schema graphs
- 2026-02-15: Decided D19 — data query generalization deferred, fundamentally different problem
- 2026-02-15: Decided D20 — dot chaining with pipe semantics (linear only, no branching); updated D14 precedence to 4 levels
- 2026-02-16: Decided D21 — error handling: silent empty for axes, warning for unmatched name= selectors
- 2026-02-16: Decided D22 — schema-driven identifiers: all selectors/axes/predicates are runtime-resolved identifiers, no reserved words in expression grammar
- 2026-02-16: Decided D23 — schema-defined shortcuts: named expression templates (e.g., "" and "all") rewritten at AST level before evaluation
- 2026-02-16: Decided D24 — predicate negation: `!` and `()` inside values, preserving `key=value` structure
- 2026-02-16: Decided D25 — clean break from old syntax, tag release before implementation
- 2026-02-16: Decided D26 — parentheses for explicit grouping
- 2026-02-16: Decided D27 — `-` works with axes consistently (traverse then remove)
- 2026-02-16: Decided D28 — set semantics for deduplication; node identity is schema-defined (type name for schema graphs)
- 2026-02-16: Decided D29 — default sort order unspecified; use `sort by` for predictable output
- 2026-02-16: Decided D30 — depth=0 allowed as no-op
- 2026-02-16: Fixed mapping table (bare `graph` uses "" shortcut, not `all`); added Implementation Notes section
- 2026-02-18: Renamed GQL to TTGE; replaced @{...} metadata with global `graph config` and `graph style` commands; added three-parser architecture (TTQ, TTGC, TTGE)
- 2026-02-18: Reconciliation pass — D3 rewritten for session-state config/style with `metadata` prefix and two contexts; D16 updated with full statement structure for TTQ and TTGE contexts; D17 updated for three style forms; D31 added for TTGE scripts; added Overflow entity to catalog and axes; added `.base`/`.wrappedBy` axes; entity catalog noted as schema-driven per D22
- 2026-02-18: D7 rewritten — TTGE returns `GraphResult` (edges + isolated nodes), not a table; one-way dependency (TTQ/REPL → TTGE, not reverse); table column renamed `edge` → `label` to match `GraphEdge.label`; D15 updated for `GraphResult` sorting semantics; D16 updated to reference `GraphResult`
- 2026-02-18: Formal EBNF grammar added — statement grammar (config, style, metadata, execute, expressions), expression grammar (union/intersection/chain/dot precedence), axis grammar, predicate grammar with generic value syntax, disambiguation rules for `{}`, contextual keywords table
