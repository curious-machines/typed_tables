# Typed Tables

A Python library that provides a typed, file-based database for structured data.

## Overview

Typed Tables is a live persistance layer. Types are defined in a DSL. Their format is similar to how a structure is defined in other languages. Only values can be defined, no methods.

In order to improve queries and to keep type table files small, built-in types may be used to define a new type. The representation remains the same, but the name changes. Each type will have its own table, so the new name creates a new table. For example, there is a uint128 type. All uint128 values will be stored in the uint128 table. UUIDs are 128 bits, so we could define `define UUID as uint128`. When we create a UUID, it will be stored in a UUID table where each entry follows the same format as a uint128.

Eventually, the type DSL will be extended to a language, but for now, this is merely for declaring data structures to be stored on disk.

## Key Features

- **Type Safety**: Define schemas using a custom DSL
- **File-Based Storage**: Data stored as binary files for compact and quick retrieval
- **Simple API**: CRUD operations with a clean, Pythonic interface
- **No External Dependencies**: Works without a separate database server
- **Query Support**: Filter and search types by field values in a custom query DSL

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

Composite types may be nested

## Data Storage

Each type is stored in its own table. It is a requirement that a table be thought of as an array of fixed type members. In memory we reference a type by its name and index. The name matches the type name, which is used to name the data file on disk. The index points to the entry within that data file.

Tables for array types worked a little differently. Remember, that it is a requirement that all entries in a table must be of a fixed size in order to gain quick access to the member. The array table consists of entries that could be defined like so:

```
UInt8Array {
    startingIndex: uint32
    length: uint32
}
```

So, in order to load a UInt8Array, we must have an index into the UInt8Array table. The entry pointed to tells use what the starting and ending indices are for the uint8 table containing its values

You'll notice that we don't know which table UInt8Array needs to access. This implies that we need to maintain metadata, on disk, about each of the type tables. If we can use the same system that we use to represent types to represent this metadata, that would be great. But, if that causes a bootstrapping problem, we can make an exception for the metadata. However, a user may want to load this metadata, so it needs to be representable in a TypeTable structure even if it is not stored on disk in that format.

## Query Language

To be determined, but here is a list of features that will be expected to be supported:

- We should be able to query a type and list its members and their types
- We should be able to return the transitive closure of a type; its type and all types it uses and all types they use, etc.
- Starting from a value, we should be able to emit its table and index
- Given a table and index, we should be able to find all composite types that reference it
- We should be able to calculate the total number of bytes required to store a type instance
    - Perhaps one version includes metadata needed to represent the type
    - Another version would calculate the space taken on disk to store the type, including array entries for array types
- We should be able to query types by comparison. For example, select all uint8's that are equal to or less than 5
    - This list of instances could then be used for another query, for example, to calculate how much space each instance requires on the drive
- We should have aggregate functions like average, sum, product, group by, sort, etc.

## Parsing

The languages for type definition and for querying are defined using Ply.

## Project Structure

```
typed_tables/
├── src/
│   ├── typed_tables/
│   │   ├── __init__.py
│   │   ├── table.py
│   │   ├── array_table.py
│   └── ├── type.py
├───├── parsing/
│   │   ├── __init__.py
│   │   ├── type_parser.py
│   │   ├── query_parser.py
│   │   ├── type_lexer.py
│   │   ├── query_lexer.py
├── tests/
├── pyproject.toml
└── README.md
```

## Usage Example

```python
from typed_tables import Parser

# define a data structure of types
types = '''
define uuid as uint128
define name as string

Person {
  id: uuid
  name
}
'''

# parse the schema and build an in-memory representation of the types and their associated tables
schema = Parser.parse(types)

# create an instance of one of the types in the schema
person = schema.create_instance(Person, ("0000-0000-0000-0000", "Bill"))

# at this point, tables have been created for UUIDs and for Names.
# The values in the constructor have been saved into those tables and likely cached in memory
# Each value in `person` points to its table and item index in that table
```

## Development Commands

## Design Principles

1. **Explicit over implicit**: All data types must be declared
2. **Simplicity**: Minimal API surface, easy to understand
4. **Type-first**: Creates a custom type system built from primitive types
