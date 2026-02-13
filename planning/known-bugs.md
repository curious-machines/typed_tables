# Known Bugs

## 1. ~~`string[]` fields fail on instance creation~~ (FIXED)

**Status:** Fixed
**Severity:** Medium — affects any type with a `string[]` field

### Reproduction

```ttq
type C { tags: string[] }
create C(tags=["a", "b"])
-- Error: not enough values to unpack (expected 2, got 1)
```

This fails even without interfaces — it's a general bug with `string[]` (array of strings).

### Root Cause

`string` is `StringTypeDefinition(ArrayTypeDefinition)` — it's already an array type (character[]). So `string[]` is an array of arrays. During instance creation in `_create_instance`, the array field handling block stores raw string values directly into the element table, but the element table expects `(start_index, length)` tuples because each element is itself an array.

The error trace is:
```
_create_instance → array_table.insert(field_value)
  → element_table.insert(element)    # element is "a", not a (start, length) tuple
    → _serialize(value)
      → start_index, length = value  # FAILS
```

### Fix Pattern

The fix already exists in 2+ other places in the codebase:

**In SetTypeDefinition handling** (`_create_instance`, ~line 3415):
```python
if is_string_type(field_base.element_type):
    char_table = self.storage.get_array_table_for_type(field_base.element_type)
    elements = [char_table.insert(list(e) if isinstance(e, str) else e) for e in elements]
```

**In UPDATE operations** (~line 2012):
```python
if is_string_type(field_base.element_type):
    char_table = self.storage.get_array_table_for_type(field_base.element_type)
    elements = [char_table.insert(list(e) if isinstance(e, str) else e) for e in elements]
```

The same pattern needs to be applied in the array field branch of `_create_instance` (~line 3475). Before inserting into the array table, check if the element type is a string and pre-store each string element into the character table, replacing raw strings with `(start_index, length)` tuples.

### Scope

- `string[]` — confirmed broken
- Potentially any nested array type (array of arrays)
- Affects `create` statements; `update` already has the fix
- `{string}` (string sets) already work — they have the fix

### Fix Applied

The fix targets `string[]` specifically using `is_string_type(field_base.element_type)` checks — the same pattern already used for `{string}` sets. It was applied to the write path (`_create_instance`) and all read paths (7 resolution/dump functions).

**Limitation:** This fix only handles one level of nesting where the element type is `string`. It does **not** generalize to arbitrary nested arrays:
- `string[][]` — not handled (`string[]` is `ArrayTypeDefinition`, not `StringTypeDefinition`)
- `int32[][]` — not handled (no string involved)
- `character[][]` — not handled (`character[]` is not a `StringTypeDefinition`)

A general nested-array solution would require recursive element pre-storage and resolution for any `ArrayTypeDefinition` element type. This is deferred since `string[]` is the only practical nested-array case today.


## 2. ~~Polymorphic queries don't traverse interface inheritance~~ (FIXED)

**Status:** Fixed (Option B — traverse at query time with lazy cached descendant sets)
**Severity:** Low — workaround available, and the feature is new

### Reproduction

```ttq
interface Identifiable { name: string }
interface Entity from Identifiable { id: uint32 }
interface Combatant from Entity { attack: uint16 }
type Creature from Combatant { speed: float32 }

from Combatant select *      -- Returns Creature (correct)
from Entity select *          -- Returns nothing (wrong)
from Identifiable select *    -- Returns nothing (wrong)
```

`Creature` implements `Combatant`, but queries against `Combatant`'s ancestors (`Entity`, `Identifiable`) return nothing.

### Root Cause

When a composite type is created with `type Creature from Combatant`, the `interfaces` list only stores direct interface parents: `["Combatant"]`. It does NOT include ancestor interfaces of `Combatant` (i.e., `Entity`, `Identifiable`).

At query time, `find_implementing_types("Identifiable")` checks `"Identifiable" in composite.interfaces` — an exact match that fails because only `"Combatant"` is in the list.

**Relevant code locations:**

| Location | File | What happens |
|----------|------|-------------|
| `_execute_create_type` | `query_executor.py:1402` | Only adds direct interface parent to `interface_names` |
| `find_implementing_types` | `types.py:734` | Only does exact match: `interface_name in td.interfaces` |
| `_load_records_by_interface` | `query_executor.py:4415` | Delegates to `find_implementing_types` |

### Fix Options

**Option A: Expand at creation time** — When a composite implements interface B which extends A, also add A (and all ancestors) to the composite's `interfaces` list.

```python
# In _execute_create_type, when processing interface parents:
if isinstance(parent_base, InterfaceTypeDefinition):
    interface_names.append(parent_name)
    # Also add all ancestor interfaces transitively
    for ancestor in self._collect_ancestor_interfaces(parent_name):
        interface_names.append(ancestor)
```

Pros: queries stay simple and fast. Cons: stores redundant data that becomes stale if the interface hierarchy is ever modified (no `alter interface` today, but a future concern).

**Option B: Traverse at query time** (chosen) — When querying interface A, find all interfaces that transitively extend A, then find composites implementing any of them.

Pros: no storage changes, no stale data. Cons: traversal cost (mitigated by lazy caching).

### Fix Applied

Implemented Option B in `TypeRegistry` (`types.py`):
- `_build_interface_descendant_cache()`: builds a mapping from each interface to all its transitive descendant interfaces (BFS traversal of parent→child edges)
- `_get_descendant_interfaces(name)`: returns cached descendant set, rebuilding lazily if invalidated
- `_interface_descendant_cache`: set to `None` by `_invalidate_caches()`, which is called from `register()`, `get_array_type()`, `get_or_create_set_type()`, `get_or_create_dict_type()`, and all `register_*_stub()` methods
- `find_implementing_types()`: uses `_get_descendant_interfaces()` to match composites implementing any descendant interface, not just exact match

**Future improvement:** Cache invalidation is currently coarse — `_invalidate_caches()` is called on every type registration, including array types, set types, dict types, enum stubs, and composite stubs, none of which affect the interface descendant graph. A more targeted approach would only invalidate when an `InterfaceTypeDefinition` or `CompositeTypeDefinition` with interfaces is registered or modified.
