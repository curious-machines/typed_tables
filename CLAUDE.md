# Typed Tables

A Python library that provides a typed, file-based database for structured data.

## Overview

Typed Tables is a live persistence layer. Types are defined in a DSL. Their format is similar to how a structure is defined in other languages. Only values can be defined, no methods.

In order to improve queries and to keep type table files small, built-in types may be used to define a new type. The representation remains the same, but the name changes. Each type will have its own table, so the new name creates a new table. For example, there is a uint128 type. All uint128 values will be stored in the uint128 table. UUIDs are 128 bits, so we could define `alias UUID = uint128`. When we create a UUID, it will be stored in a UUID table where each entry follows the same format as a uint128.

TTQ is both the query language and the type definition language.

## Key Features

- **Type Safety**: Define schemas using a custom DSL
- **File-Based Storage**: Data stored as binary files for compact and quick retrieval
- **Simple API**: CRUD operations with a clean, Pythonic interface
- **No External Dependencies**: Works without a separate database server
- **Query Support**: Filter and search types by field values in a custom query DSL (planned)

## Type System

The built-in primitive types that are supported: bit, character, uint8, int8, uint16, int16, uint32, int32, uint64, int64, uint128, int128, float16, float32, float64

Additional built-in types:
- `string` — stored as `character[]` but displayed as a joined string (e.g., `"Alice"`). A bare `character[]` is displayed as an array of individual characters (e.g., `['A', 'l', 'i', 'c', 'e']`). Aliases to `string` inherit string display behavior; aliases to `character[]` do not.
- `boolean` — stored as `bit`, displayed as `true`/`false`.
- `bigint` — arbitrary-precision signed integer, stored as variable-length byte sequence.
- `biguint` — arbitrary-precision unsigned integer, stored as variable-length byte sequence.
- `fraction` — exact rational number (numerator/denominator), stored as two variable-length byte sequences. Construction: `fraction(355, 113)` or `fraction(3)`.

All types have array variants which is indicated by the type name followed by square brackets. For example, `character[]`

TTQ syntax is used to describe the schema. Types are defined with the `type` keyword followed by a name and a body surrounded by curly braces. The body contains a list of name:type pairs. An example follows:

```ttq
alias uuid = uint128

type Person {
  id: uuid,
  name: string
}
```

In this example, we use `alias` to create a new type name from a primitive type. We have a uuid type which is stored as a uint128.

We next create a new composite type named Person using the `type` keyword. It consists of two fields: id and name. `id` is of type uuid and `name` is of type `string`.

Composite types may be nested.

## Data Storage

Each composite type is stored in its own table as a `.bin` file. A table is an array of fixed-size records. In memory we reference a record by its type name and index.

**Composite record layout:** `[null_bitmap] [field0_data] [field1_data] ...`

- **Null bitmap**: `ceil(N/8)` bytes at the start, one bit per field. Bit set = field is null, with the field's data area zeroed.
- **Primitive/alias-to-primitive fields**: actual value stored inline (1–16 bytes depending on type). No separate table is created for primitive fields.
- **Array fields**: `(start_index, length)` stored inline (8 bytes) — `start_index` is a uint32 index into the element table, `length` is a uint32 element count.
- **Composite ref fields**: uint32 index (4 bytes) into the referenced type's table.
- **Enum fields**: stored inline. C-style: discriminant only (1/2/4 bytes based on max value). Swift-style: `[discriminant (1-4 bytes)][uint32 variant_table_index]` (disc + 4 bytes); variant fields are stored in per-variant `.bin` tables in `{data_dir}/{enum_name}/`.
- **Set fields**: stored identically to array fields — `(start_index, length)` inline (8 bytes). Uniqueness is enforced at creation/mutation time, not at the storage level.
- **Dict fields**: stored as `(start_index, length)` inline (8 bytes) pointing into an array of uint32 entry composite indices. Each entry is a synthetic composite type (e.g., `Dict_string_float64`) with `key` and `value` fields, stored in its own `.bin` table.
- **BigInt/BigUInt fields**: `(start_index, length)` stored inline (8 bytes) — bytes stored in shared `bigint.bin` / `biguint.bin` element tables. Little-endian encoding; signed uses two's complement.
- **Fraction fields**: `(num_start, num_len, den_start, den_len)` stored inline (16 bytes) — numerator bytes in `_frac_num.bin` (signed), denominator bytes in `_frac_den.bin` (unsigned). Auto-normalized by Python's `fractions.Fraction`.

Array elements are stored in element tables (e.g., `name.bin`). The composite record directly contains the `(start_index, length)` pair needed to locate the elements.

**Type-based querying**: `from <non-composite-type> select *` scans all composites containing a field of that type and extracts values, returning `_source`, `_index`, `_field`, and value columns.

Metadata about types is stored in `_metadata.json` in the data directory.

## Query Language

Newlines are treated as whitespace, so queries can be formatted freely across multiple lines. Semicolons are optional everywhere — statements are separated by keyword boundaries. In the REPL, a query can also be submitted by pressing Enter on an empty line during continuation. Semicolons are still accepted for backward compatibility.

**Backtick-quoted identifiers**: Names that clash with reserved keywords can be wrapped in backticks to force identifier treatment. This works anywhere a name is expected — type names, field names, enum variant names, references:
```ttq
enum State { WA, `OR`, CA, `AS` }
create Person(state=.`OR`)
create Person(state=State.`OR`)
from Person select * where state = .`OR`
```
The `dump` command automatically backtick-escapes names that are reserved keywords.

### Select Database

This will create a new database if one does not exist already

```ttq
use example_data
```

Mark a database as temporary — it will be automatically deleted when the REPL exits:
```ttq
use test_db as temporary
```

### Delete Database

```ttq
drop example_data
```

### Create Types

Type definitions use `{ }` with comma-separated fields:

```ttq
type Address {
    number: string,
    street: string,
    city: string,
    state: string,
    zipcode: string
}
```

```ttq
type Person {
    id: uuid,
    name: string,
    age: uint8,
    address: Address
}
```

```ttq
type Employee from Person {
    department: string,
    title: string
}
```

Types with array fields:
```ttq
type Sensor { name: string, readings: int8[] }
```

Types with set fields (unique elements, `{type}` syntax):
```ttq
type Student { name: string, tags: {string} }
type Data { nums: {int32} }
```

Types with dictionary fields (unique keys, `{keytype: valuetype}` syntax):
```ttq
type Student { name: string, scores: {string: float64} }
type Lookup { data: {int32: string} }
```

Trailing commas are allowed:
```ttq
type Point { x: float32, y: float32, }
```

### Default Values

Fields can have default values that are used when the field is omitted during instance creation:
```ttq
type Person {
    name: string,
    age: uint8 = 0,
    status: string = "active"
}

create Person(name="Alice")  -- age defaults to 0, status to "active"
```

Default values are supported for primitive types, strings, arrays, and enums (both C-style and Swift-style). Enum defaults use dot notation:
```ttq
enum Color { red, green, blue }
type Pixel { x: uint16 = 0, y: uint16 = 0, color: Color = .red }
create Pixel()  -- all fields use defaults
```

Interface fields can also have defaults, which are inherited by implementing types:
```ttq
interface Positioned { x: float32 = 0.0, y: float32 = 0.0 }
type Point from Positioned { label: string }
create Point(label="origin")  -- x and y default to 0.0
```

Default values must be static (no function calls like `uuid()`, no inline instances, no composite references). If no default is specified, the field defaults to NULL (preserving existing behavior).

### Self-Referential Types

Types can reference themselves, useful for tree and graph structures:
```ttq
type Node { value: uint8, children: Node[] }
create Node(value=0, children=[Node(value=1, children=[]), Node(value=2, children=[])])
```

Direct self-reference (linked list):
```ttq
type LinkedNode { value: uint8, next: LinkedNode }
create LinkedNode(value=2, next=LinkedNode(value=1, next=LinkedNode(0)))
```

### Forward Declarations (Mutual References)

For mutually referential types, use forward declarations:
```ttq
forward B
type A { value: uint8, b: B }
type B { value: uint8, a: A }
```

The `forward B` registers an empty stub. The third statement populates it with fields. This allows A and B to reference each other.

### NULL Values

Composite fields can be set to `null` to indicate the absence of a value. Null is tracked via a null bitmap at the start of each composite record. Fields omitted during creation default to null.

```ttq
type Node { value: uint8, next: Node }
create Node(value=1, next=null)
create Node(value=2)              -- next defaults to null
```

NULL values display as `NULL` in select results and as `null` in dump output.

### Aliases

```ttq
alias uuid = uint128
```

### Enumerations

Enumerations use a unified syntax that covers both C-style (bare variants) and Swift-style (variants with associated values).

C-style enums with auto-assigned discriminants:
```ttq
enum Color { red, green, blue }
```

C-style enums with explicit backing values:
```ttq
enum HttpStatus { ok = 200, not_found = 404, internal_error = 500 }
```

Swift-style enums with associated values:
```ttq
enum Shape {
    none,
    line(x1: float32, y1: float32, x2: float32, y2: float32),
    circle(cx: float32, cy: float32, r: float32)
}
```

**Restriction:** Explicit integer discriminants (`= 200`) and associated values (`(x: float32)`) cannot coexist in the same enum.

Enum values are used in instance creation with dot notation:
```ttq
type Pixel { x: uint16, y: uint16, color: Color }
create Pixel(x=0, y=0, color=Color.red)

type Canvas { name: string, bg: Shape }
create Canvas(name="test", bg=Shape.circle(cx=50, cy=50, r=25))
```

When the enum type can be inferred from the field, a shorthand form is available using a leading dot:
```ttq
create Pixel(x=0, y=0, color=.red)
create Canvas(name="test", bg=.circle(cx=50, cy=50, r=25))
create Canvas(name="empty", bg=.none)
```

Both forms can be mixed freely. The `dump` command always outputs the fully-qualified form.

**Storage:** C-style enums store only the discriminant inline (1/2/4 bytes). Swift-style enums store `[discriminant][uint32 variant_table_index]` inline; variant fields live in per-variant `.bin` tables under `{data_dir}/{enum_name}/`. Bare variants (no fields) use a sentinel index (`NULL_REF`).

**Querying enums:**
```ttq
-- Overview: shows _variant column, no WHERE allowed
from Shape select *

-- Variant-specific: associated values as columns, WHERE allowed
from Shape.circle select *
from Shape.circle select cx, cy where r > 10

-- Describe enum
describe Shape
describe Shape.circle
```

### Typed Math Expressions

Math operations are type-checked at the schema level. No implicit type casting — operands must be the same type, and literals auto-size from field context.

#### Overflow Policy

Fields can specify overflow behavior with a modifier before the type:

```ttq
type Sensor {
    reading: saturating int8,        -- clamp to -128..127
    count: wrapping uint16,          -- modular arithmetic (C-style)
    temperature: float32             -- default: error on overflow
}
```

Three policies: **error** (default, no keyword), **saturating** (clamp to min/max), **wrapping** (modular arithmetic). Overflow modifiers are not allowed on float types.

#### Type-Annotated Literals

Literals can carry explicit type suffixes:

```ttq
5i8, 5i16, 5i32, 5i64         -- signed integers
5u8, 5u16, 5u32, 5u64, 5u128  -- unsigned integers
5.0f32, 5.0f64                 -- floats
0xFFu8, 0b1010i8               -- hex/binary with suffix
```

#### Literal Auto-Sizing

Bare literals (no suffix) adopt the type of the field they're used with:

```ttq
-- reading is int8 (-128..127)
from Sensor select reading + 1       -- 1 becomes int8, ok
from Sensor select reading + 200     -- error: 200 doesn't fit int8

-- bare eval, no field context
5 + 3                                -- arbitrary precision, no overflow
```

If a literal doesn't fit in the contextual type, it's always an error regardless of overflow policy. This is distinct from runtime overflow (e.g., `127 + 1` where both operands are valid int8 but the result overflows — governed by the field's policy).

#### Type Conversion

Primitive type names work as conversion functions:

```ttq
int16(value)           -- convert scalar to int16
int16(readings)        -- element-wise: int8[] -> int16[]
int16([1, 2, 3])       -- typed array literal
float64(age)           -- uint8 -> float64
int8(200)              -- error: 200 overflows int8 (narrowing always errors on overflow)
bigint(42)             -- convert to arbitrary-precision signed integer
biguint(42)            -- convert to arbitrary-precision unsigned integer
fraction(355, 113)     -- exact rational 355/113
fraction(3)            -- exact rational 3/1
boolean(1)             -- true; boolean(0) → false
string(42)             -- "42" (convert any value to string)
```

#### Expression Type Rules

Both operands must have the same type. Mixed-type expressions require explicit casting:

```ttq
-- reading is int8, count is uint16
reading + count                         -- error: type mismatch
int16(reading) + int16(count) + 1       -- ok: all int16, literal adopts int16
```

Same rules apply in WHERE clauses — no special widening:

```ttq
from Sensor select * where reading > 200          -- error: 200 doesn't fit int8
from Sensor select * where int16(reading) > 200   -- ok: both int16
```

Only bare eval expressions (no field context) use arbitrary precision.

#### Division

Both `/` and `//` perform floor division (toward negative infinity). Use `float64(x) / float64(y)` for true division.

#### C-Style Enum Arithmetic

Enums can specify a backing integer type:

```ttq
enum Color : uint8 { red, green, blue }
enum HttpStatus : uint16 { ok = 200, not_found = 404, internal_error = 500 }
```

Enum values participate in arithmetic as their backing type. The result is the integer type, not the enum:

```ttq
Color.red + 1          -- uint8 value 1
```

#### Enum Conversion

The enum name works as a conversion function, accepting integers or strings:

```ttq
Color(0)               -- Color.red (by discriminant)
Color("red")           -- Color.red (by variant name)
Color(5)               -- error: no variant with discriminant 5
Color("purple")        -- error: no variant named "purple"
```

For Swift-style enums, string conversion only works for bare variants (no associated values).

#### Bit Type

Bit values are not numeric. Use boolean functions:

```ttq
and(a, b), or(a, b), not(a), xor(a, b)
```

Bit values can be cast to integer types (produces 0 or 1) and back (only 0 or 1 accepted):

```ttq
uint8(flag)            -- 0 or 1 as uint8
bit(1)                 -- ok
bit(2)                 -- error: only 0 or 1
```

### Create Entry

```ttq
create Person(name="Kevin", id=uuid(), age=32, address=Address(1))
```

With inline nested instances:
```ttq
create Person(name="Alice", address=Address(street="123 Main", city="Springfield"))
```

With array values:
```ttq
create Sensor(name="temperature", readings=[25, 26, 24, 27])
```

With set values (duplicate elements are rejected):
```ttq
create Student(name="Alice", tags={"math", "science"})
create Data(nums={,})       -- empty set
```

With dictionary values (duplicate keys are rejected):
```ttq
create Student(name="Alice", scores={"midterm": 92.5, "final": 88.0})
create Student(name="Bob", scores={:})  -- empty dict
```

`{}` infers set or dict from the field type.

### Variable Bindings

Variables are immutable bindings to created instances:
```ttq
$addr = create Address(street="123 Main", city="Springfield")
create Person(name="Alice", address=$addr)
create Person(name="Bob", address=$addr)
```

Variables can be used in array elements:
```ttq
$e1 = create Employee(name="Alice")
$e2 = create Employee(name="Bob")
create Team(members=[$e1, $e2])
```

Variables can also collect sets of record indices:
```ttq
$seniors = collect Person where age >= 65
$top10 = collect Score sort by value limit 10
$all = collect Person
```

Multi-source collect unions multiple sources (same type, deduplication built in):
```ttq
$combined = collect Person where age >= 65, Person where age = 30
$subset = collect $seniors where city = "Springfield"
$union = collect $seniors, $young
```

### Delete Entry

```ttq
delete Person where name="Kevin"
```

### Update Entry

Modify fields on an existing record. The target can be a variable or a direct type reference with index.

```ttq
update $n1 set next=$n2
update Node(0) set value=42
update $n set value=10, next=null
```

Bulk update with a WHERE clause updates all matching records:
```ttq
update Pixel set color=.blue where color=.green
update Style set fill=.hex(value="#00FF00") where fill=.hex(value="#87CEEB")
```

Bulk update without WHERE updates all records of the type:
```ttq
update Pixel set color=.red
```

Enum values (both shorthand and fully-qualified) are supported in WHERE conditions:
```ttq
update Pixel set color=.blue where color=.green
update Pixel set color=Color.blue where color=Color.red
```

### Cyclic Data Structures

Cycles in composite references (e.g., linked lists, graphs) are supported using **tag syntax** within **scope blocks**. Tags allow creating cycles by declaring a name for the record being created that nested records can reference.

Self-referencing (node points to itself):
```ttq
type Node { value: uint8, next: Node }
scope { create Node(tag(SELF), value=42, next=SELF) }
```

Two-node cycle (A→B→A):
```ttq
type Node { name: string, child: Node }
scope { create Node(tag(A), name="A", child=Node(name="B", child=A)) }
```

Four-node cycle (A→B→C→D→A):
```ttq
scope { create Node(tag(A), name="A", child=Node(name="B", child=Node(name="C", child=Node(name="D", child=A)))) }
```

Tags require a scope block and are scoped to that block. Tags and variables declared inside a scope are destroyed when the scope exits. Tags cannot be redefined within a scope.

For multi-statement cycles, use `null` + `update`:

```ttq
$n1 = create Node(value=1, next=null)
$n2 = create Node(value=2, next=$n1)
update $n1 set next=$n2
-- Now: n1 -> n2 -> n1 (cycle)
```

The `dump` command is cycle-aware: it automatically emits scope blocks with tag syntax when serializing cyclic data, ensuring roundtrip fidelity.

### Selection
The query language, TTQ, requires a `from` clause followed by a `select` clause:

```ttq
from Person select *
```

This example will return all Person entries.

A variable can also be used as the source:
```ttq
$seniors = collect Person where age >= 65
from $seniors select name, age sort by age
from $seniors select average(age)
```

### Naming Fields

```ttq
uuid() named "One", uuid() named "Two"
```

### Limiting Selection
The return selection can be limited using `offset` and `limit` phrases. If `limit` is used without `offset`, then `offset` defaults to 0.

```ttq
from Person select * offset 10 limit 10
```

This will skip the first 10 Person entries, and then display the next 10 entries

### Filtering Selection
The `where` clause can be used to filter the selected table

```ttq
from Person select name, age where name starts with "K"
```

or alternately, using regular expressions

```ttq
from Person select * where name matches /^K/
```

Comparisons are also available for filtering

```ttq
from Person select * where age >= 18
```

### Array Indexing

Array fields can be indexed to select specific elements:

```ttq
from Sensor select name, readings[0]
```

Negative indices access elements from the end of the array:
```ttq
from Sensor select readings[-1]
from Sensor select readings[-3]
```

Slices can extract a range of elements, and support negative indices:
```ttq
from Sensor select readings[0:5]
from Sensor select readings[5:]
from Sensor select readings[:3]
from Sensor select readings[-3:]
from Sensor select readings[:-1]
from Sensor select readings[1:-1]
```

Post-index dot notation accesses fields of composite array elements:
```ttq
from Team select employees[0].name
from Team select employees[0].address.city
```

Array projection maps a field path across all elements of a composite array:
```ttq
from Team select employees.name
```

### Dictionary Bracket Access

Dictionary fields can be accessed by key using string bracket syntax:
```ttq
from Student select scores["midterm"]
from Student select scores["final"]
```

Missing keys return NULL.

### Collection Methods

Arrays, sets, and dictionaries support method calls in SELECT and WHERE clauses.

**Shared read-only methods** (arrays, sets, dicts):
```ttq
from X select tags.length()         -- number of elements
from X select tags.isEmpty()        -- true if empty
from X select tags.contains(val)    -- true if element/key exists
```

**Set-specific methods** (return a new set):
```ttq
from X select tags.add(5)                        -- add element (no-op if present)
from X select tags.union({3, 4})                  -- elements in either
from X select tags.intersect({1, 2})              -- elements in both
from X select tags.difference({2})                -- elements in this but not other
from X select tags.symmetric_difference({2, 3})   -- elements in either but not both
```

**Dict-specific methods**:
```ttq
from X select scores.hasKey("math")   -- true if key exists
from X select scores.keys()           -- set of all keys
from X select scores.values()         -- list of all values
from X select scores.entries()        -- list of {key, value} pairs
from X select scores.remove("math")   -- new dict without key
```

**Method chaining** works on sets and dicts:
```ttq
from X select tags.add(5).sort()
from X select scores.keys().length()
from X select scores.remove("a").length()
```

**Collection methods in WHERE**:
```ttq
from X select * where tags.contains(5)
from X select * where scores.hasKey("math")
from X select * where tags.length() > 2
```

**Set/dict mutations in UPDATE**:
```ttq
update $x set tags.add("new")
update $x set tags.union({"a", "b"})
update $x set tags.intersect({"a"})
update $x set tags.difference({"old"})
update $x set tags.symmetric_difference({"a", "b"})
update $x set scores.remove("midterm")
```

Assignment chaining works for sets:
```ttq
update $x set tags = tags.add(5).sort()
```

### Grouping
Results can be grouped

```ttq
from Person select age group by age
```

Since there may be more than one Person that has the same age, the Person with the lowest id will be displayed for that group

```ttq
from Person select age, count() group by age
```

### Aggregation
```ttq
from Person select average(age)
```

```ttq
from Person select sum(age)
```

```ttq
from Person select product(age)
```

```ttq
from Person select min(age)
```

```ttq
from Person select max(age)
```

```ttq
from Person select name, age sort by age, name
```

Aggregate functions (`count`, `average`, `sum`, `product`, `min`, `max`) are not reserved keywords — they can also be used as field names:
```ttq
type Stats { count: uint32, sum: float64 }
```

In eval expressions (bare expressions without FROM), aggregates operate on arrays:
```ttq
sum([1, 2, 3])       -- 6
average([10, 20])     -- 15.0
min(5, 3)             -- 3 (multi-arg)
max([5, 3, 7])        -- 7
```

### Special Queries

```ttq
show types
```

List all type names

```ttq
describe Person
```

This shows the type and all of its properties along with their types

### Graph

> **Legacy documentation**: The keyword-driven graph command described below has been removed and replaced by TTGE (Typed Tables Graph Expression) language. This section is kept as reference for the TTGE implementation. The `graph` keyword now delegates to the TTGE engine.

Explore the type reference graph. The `graph` command is a unified tool for viewing schema structure as a table, DOT file, or TTQ script.

**Basic usage** — table output with columns `kind`, `source`, `field`, `target`:

```ttq
graph                            -- all type edges
graph Person                     -- edges involving Person (as source or target)
graph [Person, Employee]         -- edges involving either type (multi-focus)
graph all Interfaces             -- all interfaces expanded (focus by kind)
graph sort by source             -- sort by column
```

**Focus by kind** — expand all types in a category:

```ttq
graph all Composites             -- all composite types expanded
graph all Interfaces             -- all interfaces expanded
graph all Enums                  -- all enums expanded
graph all Aliases                -- alias→target forest
graph all Primitives             -- all used primitives
graph all Arrays                 -- all array types
graph all Sets                   -- all set types
graph all Dictionaries           -- all dictionary types
```

Singular forms also accepted (`graph all Composite`). Singleton kinds (String, Boolean, Fraction, BigInt, BigUInt) are not valid — use the type name directly (e.g., `graph string`).

**File output** — DOT or TTQ format determined by extension:

```ttq
graph > "types.dot"              -- DOT format (for Graphviz)
graph > "types.ttq"              -- TTQ format
graph > "types"                  -- no extension → assumes TTQ, appends .ttq
graph Person > "person.dot"      -- focus type to file
```

DOT output can be rendered with Graphviz:
```sh
dot -Tsvg types.dot -o types.svg
```

In DOT output, inheritance edges are unlabeled and distinguished by line style: **dashed** for `extends`, **dotted** for `implements`. Field edges always show the field name as a label.

**View modes** — control which edges are included:

```ttq
graph Person structure           -- only extends/implements edges (no field→type)
graph Person declared            -- only fields Person itself defines
graph Person stored              -- all fields on Person (inherited + own)
```

**Depth control** — number of edges to traverse from focus type:

```ttq
graph Person depth 0             -- focus node only (no edges)
graph Person depth 1             -- direct edges only (fields, extends, implements)
graph Person depth 2             -- direct edges + 1 level of expansion
graph Person structure depth 2   -- structure view, 2 levels deep
graph Person stored depth 1      -- field edges only (aliases not expanded)
graph Person stored depth 2      -- field edges + 1 level of alias resolution
graph Person stored              -- field edges + full alias resolution (default)
```

**Filters** — include or exclude by type, field, or kind:

```ttq
graph showing type string                    -- paths leading to string
graph showing field [name, age]              -- paths to edges with those fields
graph showing kind Interface                 -- paths leading to any interface
graph showing kind Primitive                 -- paths leading to any primitive
graph excluding type [uint8, uint16]         -- hide specific types
graph Person showing type float32 excluding field speed
```

`showing type/field/kind` finds matching edges and walks backward to show all paths leading to them. `showing kind X` expands to all types of that kind (e.g., `showing kind Primitive` = `showing type [uint8, int8, ...]`).

**Path-to queries** — find inheritance paths between types:

```ttq
graph Boss to Entity                         -- path + target expansion
graph Boss to [Entity, Combatant]            -- paths to multiple targets
graph Boss to Entity depth 0                 -- linear path only (no expansion)
graph Boss to Entity depth 1                 -- expand one level from target
graph Boss to Entity > "path.dot"            -- output path as DOT
```

Target types are expanded with their full transitive closure by default. Use `depth 0` for just the linear path (no target expansion), or `depth N` to traverse N edges from each target.

**Metadata dict** — customize DOT output with `graph{...}` syntax:

```ttq
graph{"title": "My Schema"} > "types.dot"
graph{"style": "custom.ttgs"} > "types.dot"
graph{"title": "Schema", "style": "dark.ttgs"} > "types.dot"
graph{"direction": "TB", "composite.color": "#FF0000"} > "types.dot"
graph{"style": "base.ttgs", "direction": "TB"} > "types.dot"  -- TB overrides file
graph{"title": "Boss"} Boss > "out.dot"
```

Keys and values are quoted strings. Entries are processed in order — later entries override earlier ones. The `style` key loads an external style file; other keys set properties directly.

Style files use TTQ dictionary syntax:
```ttq
-- Graph style (comments allowed)
{
    "direction": "LR",
    "composite.color": "#4A90D9",
    "interface.color": "#7B68EE",
    "focus.color": "#FFD700"
}
```

The TTQ output defines `enum NodeRole { focus, context, endpoint, leaf }`, `type TypeNode { name: string, kind: string, role: NodeRole }`, and `type Edge { source: TypeNode, target: TypeNode, field_name: string }`.

### Execute Script

Execute queries from a file. This is a proper TTQ statement that works both in
the REPL and inside other scripts:
```ttq
execute "setup.ttq"
execute "./path/to/script.ttq"
execute "setup.ttq.gz"          -- execute from a gzip-compressed file
```

The `.ttq` or `.ttq.gz` extension is auto-appended if the file is not found and
has no extension.

**Relative paths** resolve relative to the directory of the calling script, not
the current working directory. This allows scripts to reference sibling files
reliably.

**Cycle detection**: Each script can only be loaded once per session. Re-executing
an already-loaded script (directly or indirectly) is an error.

**REPL vs script context**: When `execute` is used in the REPL, scripts may
contain `use`, `drop`, and `restore` — the REPL adopts the script's final
database state. When `execute` is used inside another script (nested execute),
these lifecycle commands are not allowed.

### Import Script (Execute Once)

Import a script that will only execute once per database. Subsequent imports
of the same file are silently skipped:
```ttq
import "setup.ttq"
import "setup.ttq"          -- silently skipped (already imported)
import "setup.ttq.gz"       -- gzip-compressed files supported
```

Import tracking is stored in the database itself. Dropping and recreating the
database resets import history. The stored path preserves whatever the user
provided (relative or absolute), normalized so that `setup.ttq` and
`./setup.ttq` are treated as the same import.

### System Types

Types starting with `_` are reserved for internal use and hidden from
`show types` and `dump`:
```ttq
show system types            -- show internal system types
delete _ImportRecord         -- blocked (system type)
delete! _ImportRecord        -- force-delete (! = you know what you're doing)
```

Use `dump archive` to include system types in a dump (equivalent to archiving
the full database state):
```ttq
dump archive                 -- dump including system types
dump archive > "full.ttq"    -- dump archive to file
dump archive yaml            -- dump archive as YAML
```

### Dump Database

Serialize database contents as an executable TTQ script:
```ttq
dump              -- dump entire database
dump Person       -- dump single table
dump > "backup.ttq"           -- dump entire database to file
dump Person > "person.ttq"    -- dump single table to file
dump $var                     -- dump records referenced by a variable
dump $var > "backup.ttq"      -- dump variable records to file
dump [Person, $seniors, Employee]             -- dump a list of tables/variables
dump [Person, $seniors, Employee] > "backup.ttq"  -- dump list to file
dump > "backup"               -- no extension → appends .ttq (or .yaml/.json/.xml)
dump > "backup.ttq.gz"       -- gzip-compressed output (any format)
dump yaml > "backup.yaml.gz"
```

YAML format is also supported using anchors and aliases for references:
```ttq
dump yaml                     -- dump as YAML
dump yaml pretty              -- pretty-print YAML
dump yaml > "backup.yaml"     -- dump YAML to file
```

Example YAML output with cyclic references:
```yaml
Node:
  - &Node_0
    name: "A"
    child: *Node_1
  - &Node_1
    name: "B"
    child: *Node_0
```

JSON format is supported using `$id` and `$ref` for references:
```ttq
dump json                     -- dump as JSON
dump json pretty              -- pretty-print JSON
dump json > "backup.json"     -- dump JSON to file
```

Example JSON output with cyclic references:
```json
{
  "Node": [
    {"$id": "Node_0", "name": "A", "child": {"$ref": "Node_1"}},
    {"$id": "Node_1", "name": "B", "child": {"$ref": "Node_0"}}
  ]
}
```

XML format is supported using `id` and `ref="#id"` attributes for references:
```ttq
dump xml                      -- dump as XML
dump xml pretty               -- pretty-print XML
dump xml > "backup.xml"       -- dump XML to file
```

Example XML output with cyclic references:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<database name="mydb">
  <Nodes>
    <Node id="Node_0">
      <name>A</name>
      <child ref="#Node_1"/>
    </Node>
    <Node id="Node_1">
      <name>B</name>
      <child ref="#Node_0"/>
    </Node>
  </Nodes>
</database>
```

### Compact Database

Create a compacted copy of the database, removing deleted records (tombstones) and unreferenced data:
```ttq
compact > "path/to/output"
```

The original database is left untouched. The output path must not already exist. All three table types are compacted:
- **Composite tables**: tombstoned records removed, remaining records renumbered
- **Array element tables**: unreferenced elements removed, start indices remapped
- **Variant tables**: unreferenced variant records removed, variant indices remapped

All reference types (composite refs, interface refs, array refs, Swift-style enum refs) are remapped to the new indices. If a live record references a deleted record, the reference becomes null.

### Archive Database

Bundle the current database into a single binary archive file (`.ttar`). The database is automatically compacted before archiving:
```ttq
archive                       -- archive to <database_name>.ttar
archive > "backup.ttar"
archive > "backup.ttar.gz"    -- gzip-compressed archive
```

The `.ttar` extension is added automatically if not present. If the target file already exists, the REPL prompts for confirmation before overwriting.

### Restore Database

Extract a `.ttar` archive into a new database directory:
```ttq
restore "backup.ttar" to "restored_db"
restore "backup.ttar.gz" to "restored_db"   -- restore from gzip-compressed archive
restore "backup.ttar"                        -- restores to "backup" directory
restore "backup.ttar.gz"                     -- restores to "backup" directory
```

The `to` clause is optional. When omitted, the output directory is derived from the archive filename by stripping `.ttar` and `.gz` extensions. The output path must not already exist.

Restore does not require a database to be currently loaded.

To be determined, but here is a list of features that will be expected to be supported:


### Notes
- We should be able to return the transitive closure of a type; its type and all types it uses and all types they use, etc.
- Starting from a value, we should be able to emit its table and index
- Given a table and index, we should be able to find all composite types that reference it
- We should be able to calculate the total number of bytes required to store a type instance
    - Perhaps one version includes metadata needed to represent the type
    - Another version would calculate the space taken on disk to store the type, including array entries for array types

## Parsing

The TTQ language is defined using PLY (Python Lex-Yacc).

## Project Structure

```
typed_tables/
├── src/
│   └── typed_tables/
│       ├── __init__.py        # Public API exports
│       ├── types.py           # Type definitions (PrimitiveType, CompositeType, etc.)
│       ├── table.py           # Binary table storage with mmap
│       ├── array_table.py     # Array table specialization
│       ├── storage.py         # Storage manager for all tables
│       ├── schema.py          # Schema API (parse, create_instance)
│       ├── instance.py        # InstanceRef for referencing stored values
│       ├── dump.py            # CLI tool for dumping table contents
│       ├── query_executor.py  # TTQ query execution engine
│       ├── repl.py            # Interactive TTQ REPL
│       └── parsing/
│           ├── __init__.py
│           ├── query_lexer.py # PLY lexer for TTQ (types + queries)
│           └── query_parser.py # PLY parser for TTQ (types + queries)
├── tests/
│   ├── test_types.py
│   ├── test_parser.py
│   └── test_storage.py
├── pyproject.toml
├── example.py
└── CLAUDE.md
```

## Usage Example

```python
from typed_tables import Schema

# Define types using TTQ syntax
types = '''
alias uuid = uint128

type Person {
  id: uuid,
  name: string,
  age: uint8
}
'''

# Parse the schema and create storage in a data directory
with Schema.parse(types, "./data") as schema:
    # Create an instance of Person
    person = schema.create_instance(
        "Person",
        {
            "id": 0x12345678_12345678_12345678_12345678,
            "name": ["B", "i", "l", "l"],
        },
    )

    # Load the instance back
    data = person.load()
    print(data)  # {'id': 0x12345678..., 'name': ['B', 'i', 'l', 'l']}
```

## Development Commands

```sh
# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with verbose output
pytest -v

# Dump table contents
tt-dump <data_dir>                    # List all tables
tt-dump <data_dir> <table_name>       # Show table with resolved values
tt-dump <data_dir> <table_name> -r    # Show raw indices/references
tt-dump <data_dir> <table_name> -j    # Output as JSON
tt-dump <data_dir> <table_name> -n 10 # Limit to 10 records

# TTQ REPL (interactive query shell)
ttq <data_dir>                        # Start interactive REPL
ttq <data_dir> -c "from Person select *"  # Execute single query
ttq -f script.ttq                     # Execute queries from file
ttq -f script.ttq.gz                  # Execute from gzip-compressed file
ttq <data_dir> -f script.ttq          # Execute with initial database
ttq -f script.ttq -v                  # Verbose mode (print each query)
```

## Design Principles

1. **Explicit over implicit**: All data types must be declared
2. **Simplicity**: Minimal API surface, easy to understand
3. **Type-first**: Creates a custom type system built from primitive types
4. **Reference-based**: Composite types store references, enabling data deduplication and efficient queries