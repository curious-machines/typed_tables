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

All types have array variants which is indicated by the type name followed by square brackets. For example, `character[]`

A custom DSL is used to describe the schema. Types are by a capitalized name with a body surrounded by curly braces. The body contains a list of name:type pairs. An example follows:

```
define uuid as uint128
define name as character[]

Person {
  id: uuid
  name
}
```

In this example, we create new types from primitive types. We have a uuid type which is stored as a uint128. We have a name type, which is stored as an array of unicode characters.

We next create a new composite type named Person. It consists of two fields: id and name. `id` is of type uuid and `name` is of type `name`. Notice that if the field name and type name match, we do not need to specify the type.

Composite types may be nested.

## Data Storage

Each type is stored in its own table. It is a requirement that a table be thought of as an array of fixed type members. In memory we reference a type by its name and index. The name matches the type name, which is used to name the data file on disk. The index points to the entry within that data file.

**Composite types store references, not values.** When a Person is created, the uuid value is stored in the `uuid.bin` table, and the name characters are stored in the `name_elements.bin` table with a header in `name.bin`. The Person record in `Person.bin` stores only indices pointing to those values:
- All fields: uint32 index (4 bytes) into the field's type table
- For array fields, this index points to the array's header table (e.g., `name.bin`), which contains (start_index, length)

Tables for array types work a little differently. Remember, that it is a requirement that all entries in a table must be of a fixed size in order to gain quick access to the member. The array table consists of entries that could be defined like so:

```
UInt8Array {
    startingIndex: uint32
    length: uint32
}
```

So, in order to load a UInt8Array, we must have an index into the UInt8Array table. The entry pointed to tells us what the starting and ending indices are for the uint8 table containing its values.

Metadata about types is stored in `_metadata.json` in the data directory.

## Query Language

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

```ttq
create type Address
  number: string
  street: string
  city: string
  state: string
  zipcode: string
```

```ttq
create type Person
  id: uuid
  name: string
  age: uint8
  address: Address
```

```ttq
create Employee from Person
  department: string
  title: string
```

Types with array fields:
```ttq
create type Sensor
  name: string
  readings: int8[]
```

### Create Aliases

```ttq
create alias uuid as uint128
```

### Create Entry

```ttq
create Person(name="Kevin", id=uuid(), age=32, address=Address(1))
```

With array values:
```ttq
create Sensor(name="temperature", readings=[25, 26, 24, 27])
```

### Delete Entry

```ttq
delete Person
where name="Kevin"
```

### Selection
The query language, TTQ, works by first selecting a type:

```ttq
from Person
```

This example will return all Person entries. This is equivalent to `from Person select *`

### Naming Fields

```ttq
select uuid() as "One", uuid() as "Two"
```

### Limiting Selection
The return selection can be limited using `offset` and `limit` phrases. If `limit` is used without `offset`, then `offset` defaults to 0.

```ttq
from Person
select *
offset 10
limit 10
```

This will skip the first 10 Person entries, and then display the next 10 entries

### Filtering Selection
The `where` clause can be used to filter the selected table

```ttq
from Person select name, age where name starts with "K"
```

or alternately, using regular expressions

```ttq
from Person where name matches /^K/
```

In this example, notice that the `select` class has been omitted. This defaults to `select *`

Comparisons are also available for filtering

```ttq
from Person where age >= 18
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

### Grouping
Results can be grouped

```ttq
from Person
select age
group by age
```

Since there may be more than one Person that has the same age, the Person with the lowest id will be displayed for that group

```ttq
from Person
select age, count()
group by age
```

### Aggregation
```ttq
from Person
select average(age)
```

```ttq
from Person
select sum(age)
```

```ttq
from Person
select product(age)
```

```ttq
from Person
select name, age
sort by age, name
```

### Special Queries

```ttq
show tables
```

List all type table names

```ttq
describe Person
```

This shows the type and all of its properties along with their types

### Execute Script

Execute queries from a file within the REPL:
```ttq
execute setup.ttq
execute "./path/to/script.ttq"
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
  id: uuid
  name
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
    # - name_elements.bin: stores the actual characters
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
ttq <data_dir> -c "from Person"       # Execute single query
ttq -f script.ttq                     # Execute queries from file
ttq <data_dir> -f script.ttq          # Execute with initial database
ttq -f script.ttq -v                  # Verbose mode (print each query)
```

## Design Principles

1. **Explicit over implicit**: All data types must be declared
2. **Simplicity**: Minimal API surface, easy to understand
3. **Type-first**: Creates a custom type system built from primitive types
4. **Reference-based**: Composite types store references, enabling data deduplication and efficient queries