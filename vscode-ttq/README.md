# TTQ Language Support for VS Code

Syntax highlighting for Typed Tables Query (`.ttq`) files.

## Features

- Syntax highlighting for all TTQ keywords, types, operators, and literals
- Line comment toggling (`--`) with Ctrl+/
- Bracket matching and auto-closing for `{}`, `[]`, `()`, `""`
- Auto-indentation for `{ }` blocks

## Installation

### From the extension folder

```sh
code --install-extension /path/to/vscode-ttq
```

### For development

1. Open the `vscode-ttq/` folder in VS Code
2. Press F5 to launch the Extension Development Host
3. Open any `.ttq` file to see syntax highlighting

## Highlighted Elements

| Element | Examples |
|---|---|
| Comments | `-- this is a comment` |
| Strings | `"hello world"` |
| Regex | `matches /^pattern$/` |
| Numbers | `42`, `3.14`, `0xFF` |
| Variables | `$var`, `$seniors` |
| Commands | `create`, `delete`, `update`, `dump`, ... |
| Query keywords | `from`, `select`, `where`, `sort by`, ... |
| Type keywords | `type`, `enum`, `interface`, `alias` |
| Built-in types | `uint8`, `string`, `float64`, ... |
| Functions | `count()`, `average()`, `uuid()` |
| Type names | `Person`, `Address` (capitalized identifiers) |
| Enum access | `Color.red`, `.red` |
| Operators | `=`, `!=`, `<`, `<=`, `>`, `>=` |
