# Typed Tables

A Python library that provides a typed, file-based database for structured data.

## Overview

Typed Tables is a live persistence layer. Types are defined in a DSL. Their format is similar to how a structure is defined in other languages. Only values can be defined, no methods.

In order to improve queries and to keep type table files small, built-in types may be used to define a new type. The representation remains the same, but the name changes. Each type will have its own table, so the new name creates a new table. For example, there is a uint128 type. All uint128 values will be stored in the uint128 table. UUIDs are 128 bits, so we could define `define UUID as uint128`. When we create a UUID, it will be stored in a UUID table where each entry follows the same format as a uint128.

Eventually, the type DSL will be extended to a language, but for now, this is merely for declaring data structures to be stored on disk.

## Key Features

- **Type Safety**: Define schemas using a custom DSL
- **File-Based Storage**: Data stored as binary files for compact and quick retrieval
- **Simple API**: CRUD operations with a clean, Pythonic interface
- **No External Dependencies**: Works without a separate database server
- **Query Support**: Filter and search types by field values in a custom query DSL (planned)

## Type System

The built-in primitive types that are supported: bit, character, uint8, int8, uint16, int16, uint32, int32, uint64, int64, uint128, int128, float32, float64

There is also a built-in `string` type. It is stored as `character[]` but always displayed as a joined string (e.g., `"Alice"`). A bare `character[]` is displayed as an array of individual characters (e.g., `['A', 'l', 'i', 'c', 'e']`). Aliases to `string` inherit string display behavior; aliases to `character[]` do not.

All types have array variants which is indicated by the type name followed by square brackets. For example, `character[]`

A custom DSL is used to describe the schema. Types are by a capitalized name with a body surrounded by curly braces. The body contains a list of name:type pairs. An example follows:

```
define uuid as uint128
define name as character[]

Person {
  id: uuid,
  name
}
```

In this example, we create new types from primitive types. We have a uuid type which is stored as a uint128. We have a name type, which is stored as an array of unicode characters.

We next create a new composite type named Person. It consists of two fields: id and name. `id` is of type uuid and `name` is of type `name`. Notice that if the field name and type name match, we do not need to specify the type.

Composite types may be nested.

## Data Storage

Each composite type is stored in its own table as a `.bin` file. A table is an array of fixed-size records. In memory we reference a record by its type name and index.

**Composite record layout:** `[null_bitmap] [field0_data] [field1_data] ...`

- **Null bitmap**: `ceil(N/8)` bytes at the start, one bit per field. Bit set = field is null, with the field's data area zeroed.
- **Primitive/alias-to-primitive fields**: actual value stored inline (1–16 bytes depending on type). No separate table is created for primitive fields.
- **Array fields**: `(start_index, length)` stored inline (8 bytes) — `start_index` is a uint32 index into the element table, `length` is a uint32 element count.
- **Composite ref fields**: uint32 index (4 bytes) into the referenced type's table.
- **Enum fields**: stored inline. C-style: discriminant only (1/2/4 bytes based on max value). Swift-style: discriminant + variant payload padded to largest variant's size. Variant payloads serialize fields identically to composite field data.

Array elements are stored in element tables (e.g., `name.bin`). The composite record directly contains the `(start_index, length)` pair needed to locate the elements.

**Type-based querying**: `from <non-composite-type> select *` scans all composites containing a field of that type and extracts values, returning `_source`, `_index`, `_field`, and value columns.

Metadata about types is stored in `_metadata.json` in the data directory.

## Query Language

Newlines are treated as whitespace, so queries can be formatted freely across multiple lines. Semicolons are optional everywhere — statements are separated by keyword boundaries. In the REPL, a query can also be submitted by pressing Enter on an empty line during continuation. Semicolons are still accepted for backward compatibility.

### Select Database

This will create a new database if one does not exist already

```ttq
use example_data
```

### Delete Database

```ttq
drop example_data
```

### Create Types

Type definitions use `{ }` with comma-separated fields:

```ttq
create type Address {
    number: string,
    street: string,
    city: string,
    state: string,
    zipcode: string
}
```

```ttq
create type Person {
    id: uuid,
    name: string,
    age: uint8,
    address: Address
}
```

```ttq
create Employee from Person {
    department: string,
    title: string
}
```

Types with array fields:
```ttq
create type Sensor { name: string, readings: int8[] }
```

Trailing commas are allowed:
```ttq
create type Point { x: float32, y: float32, }
```

### Self-Referential Types

Types can reference themselves, useful for tree and graph structures:
```ttq
create type Node { value: uint8, children: Node[] }
create Node(value=0, children=[Node(value=1, children=[]), Node(value=2, children=[])])
```

Direct self-reference (linked list):
```ttq
create type LinkedNode { value: uint8, next: LinkedNode }
create LinkedNode(value=2, next=LinkedNode(value=1, next=LinkedNode(0)))
```

### Forward Declarations (Mutual References)

For mutually referential types, use forward declarations:
```ttq
forward type B
create type A { value: uint8, b: B }
create type B { value: uint8, a: A }
```

The `forward type B` registers an empty stub. The third statement populates it with fields. This allows A and B to reference each other.

### NULL Values

Composite fields can be set to `null` to indicate the absence of a value. Null is tracked via a null bitmap at the start of each composite record. Fields omitted during creation default to null.

```ttq
create type Node { value: uint8, next: Node }
create Node(value=1, next=null)
create Node(value=2)              -- next defaults to null
```

NULL values display as `NULL` in select results and as `null` in dump output.

### Create Aliases

```ttq
create alias uuid as uint128
```

### Create Enumerations

Enumerations use a unified syntax that covers both C-style (bare variants) and Swift-style (variants with associated values).

C-style enums with auto-assigned discriminants:
```ttq
create enum Color { red, green, blue }
```

C-style enums with explicit backing values:
```ttq
create enum HttpStatus { ok = 200, not_found = 404, internal_error = 500 }
```

Swift-style enums with associated values:
```ttq
create enum Shape {
    none,
    line(x1: float32, y1: float32, x2: float32, y2: float32),
    circle(cx: float32, cy: float32, r: float32)
}
```

**Restriction:** Explicit integer discriminants (`= 200`) and associated values (`(x: float32)`) cannot coexist in the same enum.

Enum values are used in instance creation with dot notation:
```ttq
create type Pixel { x: uint16, y: uint16, color: Color }
create Pixel(x=0, y=0, color=Color.red)

create type Canvas { name: string, bg: Shape }
create Canvas(name="test", bg=Shape.circle(cx=50, cy=50, r=25))
```

When the enum type can be inferred from the field, a shorthand form is available using a leading dot:
```ttq
create Pixel(x=0, y=0, color=.red)
create Canvas(name="test", bg=.circle(cx=50, cy=50, r=25))
create Canvas(name="empty", bg=.none)
```

Both forms can be mixed freely. The `dump` command always outputs the fully-qualified form.

**Storage:** Enums are stored inline in composite records. C-style enums store only the discriminant (1/2/4 bytes). Swift-style enums store discriminant + variant payload padded to the largest variant's size.

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

### Cyclic Data Structures

Cycles in composite references (e.g., linked lists, graphs) are supported using **tag syntax** within **scope blocks**. Tags allow creating cycles by declaring a name for the record being created that nested records can reference.

Self-referencing (node points to itself):
```ttq
create type Node { value: uint8, next: Node }
scope { create Node(tag(SELF), value=42, next=SELF) }
```

Two-node cycle (A→B→A):
```ttq
create type Node { name: string, child: Node }
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
select uuid() as "One", uuid() as "Two"
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

Slices can extract a range of elements:
```ttq
from Sensor select readings[0:5]
from Sensor select readings[5:]
from Sensor select readings[:3]
```

Multiple indices and slices can be combined:
```ttq
from Sensor select readings[0, 2, 4]
from Sensor select readings[0, 5:10, 15]
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
from Person select name, age sort by age, name
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

### Execute Script

Execute queries from a file within the REPL:
```ttq
execute "setup.ttq"
execute "./path/to/script.ttq"
```

### Dump Database

Serialize database contents as an executable TTQ script:
```ttq
dump              -- dump entire database
dump Person       -- dump single table
dump to "backup.ttq"          -- dump entire database to file
dump Person to "person.ttq"   -- dump single table to file
dump $var                     -- dump records referenced by a variable
dump $var to "backup.ttq"     -- dump variable records to file
dump [Person, $seniors, Employee]             -- dump a list of tables/variables
dump [Person, $seniors, Employee] to "backup.ttq"  -- dump list to file
```

YAML format is also supported using anchors and aliases for references:
```ttq
dump yaml                     -- dump as YAML
dump yaml pretty              -- pretty-print YAML
dump yaml to "backup.yaml"    -- dump YAML to file
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
dump json to "backup.json"    -- dump JSON to file
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
dump xml to "backup.xml"      -- dump XML to file
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

To be determined, but here is a list of features that will be expected to be supported:


### Notes
- We should be able to return the transitive closure of a type; its type and all types it uses and all types they use, etc.
- Starting from a value, we should be able to emit its table and index
- Given a table and index, we should be able to find all composite types that reference it
- We should be able to calculate the total number of bytes required to store a type instance
    - Perhaps one version includes metadata needed to represent the type
    - Another version would calculate the space taken on disk to store the type, including array entries for array types

## Parsing

The languages for type definition and for querying are defined using PLY.

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
│           ├── type_lexer.py  # PLY lexer for type DSL
│           ├── type_parser.py # PLY parser for type DSL
│           ├── query_lexer.py # PLY lexer for TTQ
│           └── query_parser.py # PLY parser for TTQ
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

# Define a data structure of types
types = '''
define uuid as uint128
define name as character[]
define age as uint8

Person {
  id: uuid,
  name,
  age
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

    # At this point, tables have been created:
    # - uuid.bin: stores the uuid value
    # - name.bin: stores array headers (start_index, length)
    # - name.bin: stores the actual characters
    # - Person.bin: stores Person records with references to uuid and name

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
ttq <data_dir> -f script.ttq          # Execute with initial database
ttq -f script.ttq -v                  # Verbose mode (print each query)
```

## Design Principles

1. **Explicit over implicit**: All data types must be declared
2. **Simplicity**: Minimal API surface, easy to understand
3. **Type-first**: Creates a custom type system built from primitive types
4. **Reference-based**: Composite types store references, enabling data deduplication and efficient queries