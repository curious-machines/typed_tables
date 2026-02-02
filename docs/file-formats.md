# Typed Tables File Formats

This document describes the binary file formats used by Typed Tables to store data on disk.

## Overview

Typed Tables uses a file-per-type storage model. Each type has its own binary file(s):

- **Primitive/Alias types**: `<type_name>.bin`
- **Composite types**: `<type_name>.bin`
- **Array types**: `<type_name>.bin` (headers) + `<type_name>_elements.bin` (elements)
- **Metadata**: `_metadata.json`

All binary data is stored in **little-endian** byte order.

## Table File Format

All table files share the same basic structure: an 8-byte header followed by fixed-size records.

```
+------------------+
|  Header (8 bytes)|
+------------------+
|  Record 0        |
+------------------+
|  Record 1        |
+------------------+
|  ...             |
+------------------+
|  Record N-1      |
+------------------+
|  (unused space)  |
+------------------+
```

### Header

| Offset | Size | Type   | Description                    |
|--------|------|--------|--------------------------------|
| 0      | 8    | uint64 | Record count (number of records)|

### File Growth

- Initial file size: 4096 bytes
- Growth factor: 2x when capacity is exceeded
- Capacity = (file_size - 8) / record_size

### Record Offset Calculation

```
offset(index) = 8 + (index × record_size)
```

## Primitive Type Storage

Each primitive type is stored with a fixed size:

| Type      | Size (bytes) | Format              | Description                |
|-----------|--------------|---------------------|----------------------------|
| bit       | 1            | uint8 (0 or 1)      | Boolean value              |
| character | 4            | uint32              | Unicode code point (UTF-32)|
| uint8     | 1            | uint8               | Unsigned 8-bit integer     |
| int8      | 1            | int8                | Signed 8-bit integer       |
| uint16    | 2            | uint16              | Unsigned 16-bit integer    |
| int16     | 2            | int16               | Signed 16-bit integer      |
| uint32    | 4            | uint32              | Unsigned 32-bit integer    |
| int32     | 4            | int32               | Signed 32-bit integer      |
| uint64    | 8            | uint64              | Unsigned 64-bit integer    |
| int64     | 8            | int64               | Signed 64-bit integer      |
| uint128   | 16           | uint64[2]           | Low 64 bits, then high 64  |
| int128    | 16           | uint64[2]           | Low 64 bits, then high 64  |
| float32   | 4            | IEEE 754 single     | 32-bit floating point      |
| float64   | 8            | IEEE 754 double     | 64-bit floating point      |

### 128-bit Integer Layout

```
Byte:   0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15
      |<-------- low 64 bits -------->|<-------- high 64 bits ------->|
```

## Composite Type Storage

Composite types store **references** to values, not the values themselves. Each field value is stored in its own type's table, and the composite record stores only indices pointing to those values.

### Record Layout

```
+------------------------+------------------------+-----+------------------------+
| Field 0 Index (uint32) | Field 1 Index (uint32) | ... | Field N-1 Index        |
+------------------------+------------------------+-----+------------------------+
```

### Field Reference Size

All fields use 4 bytes (uint32) to store an index into the field's type table:

| Field Type    | Reference Size | Format                              |
|---------------|----------------|-------------------------------------|
| Primitive     | 4 bytes        | uint32 index into primitive table   |
| Alias         | 4 bytes        | uint32 index into alias table       |
| Array         | 4 bytes        | uint32 index into array header table|
| Composite     | 4 bytes        | uint32 index into composite table   |

### Example: Person Type

```
define uuid as uint128
define name as character[]
define age as uint8

Person {
  id: uuid      -- 4 bytes (index into uuid.bin)
  name          -- 4 bytes (index into name.bin header table)
  age           -- 4 bytes (index into age.bin)
}
```

Person record size: 4 + 4 + 4 = 12 bytes

```
Byte:   0   1   2   3   4   5   6   7   8   9  10  11
      |<-- id index -->|<- name index -->|<- age index ->|
```

To resolve the `name` field:
1. Read the name index from Person.bin (e.g., index 5)
2. Look up index 5 in name.bin to get (start_index, length)
3. Read `length` elements starting at `start_index` from name_elements.bin

## Array Type Storage

Array types use two files:

1. **Header table** (`<name>.bin`): Stores (start_index, length) pairs
2. **Element table** (`<name>_elements.bin`): Stores actual element values

### Header Record Format

| Offset | Size | Type   | Description                              |
|--------|------|--------|------------------------------------------|
| 0      | 4    | uint32 | Start index in element table             |
| 4      | 4    | uint32 | Number of elements                       |

Total header size: 8 bytes

### Example: character[] (string)

For the string "Hello":

**name_elements.bin** (character table):
```
Index  Value (uint32 code point)
0      0x00000048  ('H')
1      0x00000065  ('e')
2      0x0000006C  ('l')
3      0x0000006C  ('l')
4      0x0000006F  ('o')
```

**name.bin** (header table):
```
Index  start_index  length
0      0            5
```

### Empty Arrays

Empty arrays are stored with:
- start_index = 0
- length = 0

## Soft Delete

Deleted records are "soft deleted" by filling them with 0xFF bytes. The record count is not decremented to maintain referential integrity.

The 0xFF marker is used instead of 0x00 because a valid record at index 0 might have all field indices as 0, which would be indistinguishable from a zeroed-out deletion marker.

A record is considered deleted if all its bytes are 0xFF:
```
is_deleted(index) = record[index] == b'\xff' × record_size
```

Deleted records are skipped when iterating through tables.

## Metadata File

Type definitions are stored in `_metadata.json` for persistence across sessions.

### Format

```json
{
  "types": {
    "type_name": {
      "kind": "primitive" | "alias" | "array" | "composite",
      ...type-specific fields...
    }
  }
}
```

### Primitive Type Entry

```json
{
  "kind": "primitive",
  "primitive": "uint32"
}
```

### Alias Type Entry

```json
{
  "kind": "alias",
  "base_type": "uint128"
}
```

### Array Type Entry

```json
{
  "kind": "array",
  "element_type": "character"
}
```

### Composite Type Entry

```json
{
  "kind": "composite",
  "fields": [
    {"name": "id", "type": "uuid"},
    {"name": "name", "type": "name"},
    {"name": "age", "type": "age"}
  ]
}
```

## File Operations

### Reading a Record

1. Calculate offset: `offset = 8 + (index × record_size)`
2. Seek to offset in memory-mapped file
3. Read `record_size` bytes
4. Deserialize based on type

### Writing a Record

1. Check if file needs to grow (count >= capacity)
2. If growing: close mmap, extend file, reopen mmap
3. Calculate offset: `offset = 8 + (count × record_size)`
4. Serialize value to bytes
5. Write bytes at offset
6. Increment count in header
7. Flush mmap

### Deleting a Record

1. Calculate offset: `offset = 8 + (index × record_size)`
2. Write `record_size` zero bytes at offset
3. Flush mmap
4. (Record count unchanged)

## Memory Mapping

Tables use memory-mapped files (mmap) for efficient I/O:

- Files are opened in read-write mode
- Changes are flushed after each write operation
- Files are properly closed when tables are closed

## Example File Structure

For a database with Person type:

```
data_directory/
├── _metadata.json      # Type definitions
├── Person.bin          # Person records (references only)
├── uuid.bin            # UUID values (uint128)
├── name.bin            # String headers (start_index, length)
├── name_elements.bin   # String characters (uint32 code points)
└── age.bin             # Age values (uint8)
```
