# TTQ Extension Features

Complete reference for the TTQ Language Support VS Code extension.

## Syntax Highlighting

### Comments
Line comments starting with `--`.
```ttq
-- this is a comment
```

### Strings
Double-quoted strings with escape sequences.
```ttq
"hello world"
"escaped \"quote\""
```

### Regular Expressions
Regex literals following the `matches` keyword.
```ttq
from Person select * where name matches /^K/
```

### Numbers
Integer, float, and hexadecimal literals.
```ttq
42
3.14
0xFF_AB
```

### Variables
Identifiers prefixed with `$`.
```ttq
$var
$seniors
```

### Keywords

| Category | Keywords |
|---|---|
| Commands | `create`, `delete`, `update`, `set`, `use`, `drop`, `dump`, `compact`, `archive`, `restore`, `execute`, `import`, `show`, `describe`, `collect`, `forward`, `define`, `scope` |
| Query | `from`, `select`, `where`, `sort`, `by`, `group`, `offset`, `limit`, `and`, `or`, `not`, `as`, `to`, `starts`, `with`, `matches`, `temp`, `types`, `interfaces`, `composites`, `enums`, `primitives`, `aliases`, `references`, `graph`, `system` |
| Type | `type`, `enum`, `interface`, `alias` |
| Format | `yaml`, `json`, `xml`, `pretty` |

### Built-in Types
```
bit  character  string  path
uint8   int8    uint16  int16
uint32  int32   uint64  int64
uint128 int128  float32 float64
```

### Functions
Highlighted when followed by `(`: `count`, `average`, `sum`, `product`, `uuid`.

### Type Names
Capitalized identifiers are highlighted as type names: `Person`, `Address`, `Node`.

### Enum Access
Both fully-qualified and shorthand enum member access.
```ttq
Color.red       -- fully qualified
.red            -- shorthand
```

### Tag Expressions
```ttq
tag(SELF)
tag(NODE_A)
```

### Operators
`=`, `!=`, `<`, `<=`, `>`, `>=`, `*`

### Constants
`null`, `true`, `false`

### Punctuation
Braces `{}`, brackets `[]`, parentheses `()`, commas, colons, semicolons, dots, and `!` (force operator).

## Language Configuration

### Comment Toggling
`Ctrl+/` toggles `--` line comments.

### Bracket Matching
Matching pairs: `{ }`, `[ ]`, `( )`

### Auto-closing Pairs
Typing an opening bracket or quote automatically inserts the closing counterpart:
- `{` → `{}`
- `[` → `[]`
- `(` → `()`
- `"` → `""` (not inside an existing string)

### Surrounding Pairs
Selecting text and typing a bracket or quote wraps the selection:
- `{`, `[`, `(`, `"`

### Auto-indentation
- Lines ending with `{` increase the indent level.
- Lines starting with `}` decrease the indent level.

## Code Folding

### Bracket Folding
`{ }` blocks are foldable automatically. Click the fold arrow in the gutter next to any opening brace.

```ttq
create type Person {    -- [fold arrow here]
    name: string,
    age: uint8
}
```

### Region Folding
Use `-- region` and `-- endregion` comments to define custom foldable sections.

```ttq
-- region Type Definitions
create type Person { name: string, age: uint8 }
create type Address { street: string, city: string }
-- endregion

-- #region Queries (alternate syntax)
from Person select *
from Address select *
-- #endregion
```

Both `-- region` and `-- #region` forms are supported.

## Snippets

Type a prefix and press `Tab` to expand. Tab stops (`$1`, `$2`, ...) let you cycle through placeholders with `Tab`.

### Type Definitions

| Prefix | Expands to |
|---|---|
| `type` | `create type Name { field: type }` |
| `typefrom` | `create type Name from Parent { field: type }` |
| `enum` | `create enum Name { a, b, c }` |
| `enumsw` | `create enum Name { bare, variant(field: type) }` |
| `interface` | `create interface Name { field: type }` |
| `alias` | `create alias name as base_type` |
| `forward` | `forward type Name` |

### Instances and Data

| Prefix | Expands to |
|---|---|
| `create` | `create Type(field=value)` |
| `cvar` | `$var = create Type(field=value)` |
| `scope` | `scope { ... }` |

### Queries

| Prefix | Expands to |
|---|---|
| `from` | `from Type select *` |
| `fromw` | `from Type select * where field = value` |
| `froms` | `from Type select * sort by field` |
| `fromg` | `from Type select field, count() group by field` |

### Variables

| Prefix | Expands to |
|---|---|
| `collect` | `$var = collect Type where condition` |

### Operations

| Prefix | Expands to |
|---|---|
| `update` | `update Type set field=value where condition` |
| `delete` | `delete Type where condition` |

### Database and I/O

| Prefix | Expands to |
|---|---|
| `use` | `use db_name` |
| `dump` | `dump to "file.ttq"` |
| `execute` | `execute "file.ttq"` |
| `import` | `import "file.ttq"` |
| `describe` | `describe Type` |
