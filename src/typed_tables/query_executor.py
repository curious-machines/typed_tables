"""Query executor for TTQ queries."""

from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import struct
import tempfile
import uuid as uuid_module
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from typed_tables.parsing.query_parser import (
    ArchiveQuery,
    ArrayIndex,
    ArraySlice,
    ArrayTypeSpec,
    BinaryExpr,
    CollectQuery,
    CollectSource,
    CompactQuery,
    CompoundCondition,
    CompositeRef,
    Condition,
    CreateAliasQuery,
    CreateEnumQuery,
    CreateInstanceQuery,
    CreateInterfaceQuery,
    CreateTypeQuery,
    DeleteQuery,
    DictEntry,
    DictLiteral,
    DictTypeSpec,
    GraphFilter,
    GraphQuery,
    EmptyBraces,
    EnumValueExpr,
    ExecuteQuery,
    ForwardTypeQuery,
    ImportQuery,
    DescribeQuery,
    DropDatabaseQuery,
    DumpItem,
    DumpQuery,
    EvalQuery,
    FieldValue,
    FunctionCall,
    InlineInstance,
    MethodCall,
    MethodCallExpr,
    MethodChainValue,
    NullValue,
    SetLiteral,
    SetTypeSpec,
    Query,
    QueryParser,
    RestoreQuery,
    ScopeBlock,
    SelectField,
    SortKeyExpr,
    SelectQuery,
    ShowTypesQuery,
    TagReference,
    TypedLiteral,
    UnaryExpr,
    UpdateQuery,
    UseQuery,
    VariableAssignmentQuery,
    VariableReference,
)
from typed_tables.storage import StorageManager
from fractions import Fraction
from typed_tables.types import (
    PRIMITIVE_TYPE_NAMES,
    AliasTypeDefinition,
    ArrayTypeDefinition,
    BigInt,
    BigIntTypeDefinition,
    BigUInt,
    BigUIntTypeDefinition,
    BooleanTypeDefinition,
    CompositeTypeDefinition,
    DictionaryTypeDefinition,
    EnumTypeDefinition,
    EnumValue,
    EnumVariantDefinition,
    FieldDefinition,
    FractionTypeDefinition,
    InterfaceTypeDefinition,
    NULL_REF,
    PrimitiveType,
    PrimitiveTypeDefinition,
    SetTypeDefinition,
    SetValue,
    StringTypeDefinition,
    TypeDefinition,
    TypeRegistry,
    TypedValue,
    _type_def_to_type_string,
    is_bigint_type,
    is_biguint_type,
    is_boolean_type,
    is_dict_type,
    is_fraction_type,
    is_set_type,
    is_string_type,
    type_range,
)


# String-only method names — dispatched to _apply_string_method before array methods
_STRING_ONLY_METHODS = frozenset({
    "uppercase", "lowercase", "capitalize",
    "trim", "trimStart", "trimEnd",
    "startsWith", "endsWith",
    "indexOf", "lastIndexOf",
    "padStart", "padEnd",
    "repeat", "split",
    "match",
})

# String mutation methods that can be used in UPDATE SET field.method() form
_STRING_MUTATION_METHODS = frozenset({
    "uppercase", "lowercase", "capitalize",
    "trim", "trimStart", "trimEnd",
    "padStart", "padEnd", "repeat",
})


def _fraction_encode(frac: Fraction, storage: StorageManager) -> tuple[int, int, int, int]:
    """Encode Fraction → (num_start, num_len, den_start, den_len)."""
    n, d = frac.numerator, frac.denominator
    if n == 0:
        num_bytes = [0]
    else:
        num_bytes = list(n.to_bytes((n.bit_length() + 8) // 8, 'little', signed=True))
    den_bytes = list(d.to_bytes((d.bit_length() + 7) // 8, 'little', signed=False))
    num_start, num_len = storage.get_fraction_num_table().insert(num_bytes)
    den_start, den_len = storage.get_fraction_den_table().insert(den_bytes)
    return (num_start, num_len, den_start, den_len)


def _fraction_decode(storage: StorageManager, num_start: int, num_len: int, den_start: int, den_len: int) -> Fraction:
    """Decode (num_start, num_len, den_start, den_len) → Fraction."""
    if num_len == 0:
        return Fraction(0)
    nt = storage.get_fraction_num_table()
    num_bytes = bytes(nt.element_table.get(num_start + j) for j in range(num_len))
    numerator = int.from_bytes(num_bytes, 'little', signed=True)
    dt = storage.get_fraction_den_table()
    den_bytes = bytes(dt.element_table.get(den_start + j) for j in range(den_len))
    denominator = int.from_bytes(den_bytes, 'little', signed=False)
    return Fraction(numerator, denominator)


@dataclass
class QueryResult:
    """Result of a query execution."""

    columns: list[str]
    rows: list[dict[str, Any]]
    message: str | None = None


@dataclass
class UseResult(QueryResult):
    """Result of a USE query - signals REPL to switch databases."""

    path: str = ""
    temporary: bool = False


@dataclass
class CreateResult(QueryResult):
    """Result of a CREATE query."""

    type_name: str = ""
    index: int | None = None


@dataclass
class DeleteResult(QueryResult):
    """Result of a DELETE query."""

    deleted_count: int = 0


@dataclass
class DropResult(QueryResult):
    """Result of a DROP database query."""

    path: str | None = None


@dataclass
class DumpResult(QueryResult):
    """Result of a DUMP query."""

    script: str = ""
    output_file: str | None = None


@dataclass
class CompactResult(QueryResult):
    """Result of a COMPACT query."""

    output_path: str = ""
    records_before: int = 0
    records_after: int = 0


@dataclass
class ArchiveResult(QueryResult):
    """Result of an ARCHIVE query."""

    output_file: str = ""
    file_count: int = 0
    total_bytes: int = 0
    exists: bool = False


@dataclass
class RestoreResult(QueryResult):
    """Result of a RESTORE query."""

    output_path: str = ""
    file_count: int = 0


@dataclass
class ExecuteResult(QueryResult):
    """Result of an EXECUTE query."""

    file_path: str = ""
    statements_executed: int = 0


@dataclass
class ImportResult(QueryResult):
    """Result of an IMPORT query."""

    file_path: str = ""
    skipped: bool = False


@dataclass
class VariableAssignmentResult(QueryResult):
    """Result of a variable assignment."""

    var_name: str = ""
    type_name: str = ""
    index: int | None = None


@dataclass
class CollectResult(QueryResult):
    """Result of a collect query."""

    var_name: str = ""
    type_name: str = ""
    count: int = 0


@dataclass
class UpdateResult(QueryResult):
    """Result of an UPDATE query."""

    type_name: str = ""
    index: int | None = None


@dataclass
class ScopeResult(QueryResult):
    """Result of executing a scope block."""

    statement_count: int = 0


@dataclass
class ScopeState:
    """State for a single scope level.

    Holds tags and variables declared within the scope.
    Destroyed when the scope exits.
    """

    tag_bindings: dict[str, tuple[str, int]]  # tag_name → (type_name, index)
    deferred_patches: list[tuple[str, int, str, str]]  # (type_name, record_idx, field_name, tag_name)
    variables: dict[str, tuple[str, int | list[int]]]  # var_name → (type_name, index_or_indices)


class QueryExecutor:
    """Executes TTQ queries against storage."""

    def __init__(self, storage: StorageManager, registry: TypeRegistry) -> None:
        self.storage = storage
        self.registry = registry
        # Session-level variables (outside any scope)
        self.variables: dict[str, tuple[str, int | list[int]]] = {}  # name → (type_name, index_or_indices)
        # Scope stack for tags and scoped variables
        self._scope_stack: list[ScopeState] = []
        # Script execution tracking
        self._script_stack: list[Path] = []  # stack of script directories for relative path resolution
        self._loaded_scripts: set[str] = set()  # absolute paths of scripts loaded via execute
        # Current overflow policy context (set during CREATE/UPDATE per target field)
        self._current_overflow_policy: str | None = None

    @staticmethod
    def _sort_rows(rows: list[dict[str, Any]], sort_by: list[str], defaults: list[str] | None = None) -> list[dict[str, Any]]:
        """Sort rows by the given column names, falling back to defaults."""
        keys = sort_by if sort_by else (defaults or [])
        if not keys:
            return rows
        def sort_key(row: dict[str, Any]) -> tuple:
            return tuple(
                (0, str(row.get(k, ""))) if row.get(k) is not None else (1, "")
                for k in keys
            )
        return sorted(rows, key=sort_key)

    def execute(self, query: Query) -> QueryResult:
        """Execute a query and return results."""
        if isinstance(query, ShowTypesQuery):
            return self._execute_show_types(query)
        elif isinstance(query, GraphQuery):
            return self._execute_graph(query)
        elif isinstance(query, DescribeQuery):
            return self._execute_describe(query)
        elif isinstance(query, SelectQuery):
            return self._execute_select(query)
        elif isinstance(query, UseQuery):
            return self._execute_use(query)
        elif isinstance(query, CreateInterfaceQuery):
            return self._execute_create_interface(query)
        elif isinstance(query, CreateTypeQuery):
            return self._execute_create_type(query)
        elif isinstance(query, ForwardTypeQuery):
            return self._execute_forward_type(query)
        elif isinstance(query, CreateAliasQuery):
            return self._execute_create_alias(query)
        elif isinstance(query, CreateEnumQuery):
            return self._execute_create_enum(query)
        elif isinstance(query, CreateInstanceQuery):
            return self._execute_create_instance(query)
        elif isinstance(query, EvalQuery):
            return self._execute_eval(query)
        elif isinstance(query, DeleteQuery):
            return self._execute_delete(query)
        elif isinstance(query, DropDatabaseQuery):
            return self._execute_drop_database(query)
        elif isinstance(query, DumpQuery):
            return self._execute_dump(query)
        elif isinstance(query, VariableAssignmentQuery):
            return self._execute_variable_assignment(query)
        elif isinstance(query, CollectQuery):
            return self._execute_collect(query)
        elif isinstance(query, UpdateQuery):
            return self._execute_update(query)
        elif isinstance(query, ScopeBlock):
            return self._execute_scope_block(query)
        elif isinstance(query, CompactQuery):
            return self._execute_compact(query)
        elif isinstance(query, ArchiveQuery):
            return self._execute_archive(query)
        elif isinstance(query, RestoreQuery):
            return execute_restore(query)
        elif isinstance(query, ExecuteQuery):
            return self._execute_execute(query)
        elif isinstance(query, ImportQuery):
            return self._execute_import(query)
        else:
            raise ValueError(f"Unknown query type: {type(query)}")

    # --- Scope management ---

    def _in_scope(self) -> bool:
        """Return True if currently inside a scope block."""
        return len(self._scope_stack) > 0

    def _current_scope(self) -> ScopeState | None:
        """Return the current (innermost) scope, or None if not in a scope."""
        return self._scope_stack[-1] if self._scope_stack else None

    def _push_scope(self) -> None:
        """Enter a new scope."""
        self._scope_stack.append(ScopeState(
            tag_bindings={},
            deferred_patches=[],
            variables={},
        ))

    def _pop_scope(self) -> ScopeState:
        """Exit the current scope and return its state."""
        return self._scope_stack.pop()

    def _lookup_variable(self, var_name: str) -> tuple[str, int | list[int]] | None:
        """Look up a variable, checking scopes from innermost to outermost, then session."""
        # Check scope stack from innermost to outermost
        for scope in reversed(self._scope_stack):
            if var_name in scope.variables:
                return scope.variables[var_name]
        # Check session-level variables
        return self.variables.get(var_name)

    def _define_variable(self, var_name: str, type_name: str, index: int | list[int]) -> None:
        """Define a variable in the current scope (or session if not in a scope)."""
        if self._in_scope():
            self._current_scope().variables[var_name] = (type_name, index)
        else:
            self.variables[var_name] = (type_name, index)

    def _lookup_tag(self, tag_name: str) -> tuple[str, int] | None:
        """Look up a tag in the current scope stack."""
        for scope in reversed(self._scope_stack):
            if tag_name in scope.tag_bindings:
                return scope.tag_bindings[tag_name]
        return None

    def _define_tag(self, tag_name: str, type_name: str, index: int) -> None:
        """Define a tag in the current scope. Error if not in a scope or if redefined."""
        if not self._in_scope():
            raise ValueError("Tags can only be used within a scope block")
        scope = self._current_scope()
        if tag_name in scope.tag_bindings:
            raise ValueError(f"Tag '{tag_name}' already defined in this scope")
        scope.tag_bindings[tag_name] = (type_name, index)

    def _add_deferred_patch(self, type_name: str, record_idx: int, field_name: str, tag_name: str) -> None:
        """Add a deferred patch for a forward tag reference."""
        if not self._in_scope():
            raise ValueError("Tags can only be used within a scope block")
        self._current_scope().deferred_patches.append((type_name, record_idx, field_name, tag_name))

    def _apply_deferred_patches(self) -> list[str]:
        """Apply all deferred patches in the current scope. Returns list of errors for unresolved tags."""
        if not self._in_scope():
            return []

        scope = self._current_scope()
        errors = []

        for type_name, record_idx, field_name, tag_name in scope.deferred_patches:
            tag_binding = self._lookup_tag(tag_name)
            if tag_binding is None:
                errors.append(f"Undefined tag: {tag_name}")
                continue

            target_type, target_idx = tag_binding
            # Patch the record
            table = self.storage.get_table(type_name)
            record = table.get(record_idx)
            record[field_name] = target_idx
            table.update(record_idx, record)

        return errors

    def _execute_scope_block(self, query: ScopeBlock) -> QueryResult:
        """Execute a scope block - enter scope, execute statements, exit scope."""
        self._push_scope()

        results = []
        errors = []

        try:
            for stmt in query.statements:
                result = self.execute(stmt)
                results.append(result)
                if result.message and "error" in result.message.lower():
                    errors.append(result.message)

            # Apply deferred patches at scope end
            patch_errors = self._apply_deferred_patches()
            errors.extend(patch_errors)

        finally:
            # Always pop scope, even on error
            self._pop_scope()

        if errors:
            return ScopeResult(
                columns=[],
                rows=[],
                message=f"Scope completed with errors: {'; '.join(errors)}",
                statement_count=len(results),
            )

        return ScopeResult(
            columns=[],
            rows=[],
            message=f"Scope completed: {len(results)} statement(s) executed",
            statement_count=len(results),
        )

    def _execute_show_types(self, query: ShowTypesQuery) -> QueryResult:
        """Execute SHOW TYPES query, optionally filtered by kind."""
        # Collect primitive/alias names referenced by user-defined types
        referenced_primitives: set[str] = set()
        referenced_aliases: set[str] = set()
        for type_name in self.registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self.registry.get(type_name)
            if type_def is None:
                continue
            base = type_def.resolve_base_type()
            # Scan fields of composites and interfaces for referenced types
            if isinstance(base, (CompositeTypeDefinition, InterfaceTypeDefinition)):
                for f in base.fields:
                    self._collect_referenced_types(f.type_def, referenced_primitives, referenced_aliases)
            # Scan enum variant fields
            if isinstance(base, EnumTypeDefinition):
                for v in base.variants:
                    for f in v.fields:
                        self._collect_referenced_types(f.type_def, referenced_primitives, referenced_aliases)

        rows = []
        for type_name in self.registry.list_types():
            type_def = self.registry.get(type_name)
            if type_def is None:
                continue

            # Filter system types (prefixed with _)
            if query.filter == "system":
                if not type_name.startswith("_"):
                    continue
            elif type_name.startswith("_"):
                continue

            base = type_def.resolve_base_type()

            # Classify the type
            if isinstance(type_def, InterfaceTypeDefinition):
                kind = "Interface"
                impl_types = self.registry.find_implementing_types(type_name)
                count = 0
                for impl_name, _ in impl_types:
                    impl_file = self.storage.data_dir / f"{impl_name}.bin"
                    if impl_file.exists():
                        try:
                            count += self.storage.get_table(impl_name).count
                        except Exception:
                            pass
            elif isinstance(type_def, EnumTypeDefinition):
                kind = "Enum"
                count = len(type_def.variants)
            elif isinstance(type_def, AliasTypeDefinition):
                if type_name not in referenced_aliases:
                    continue
                kind = "Alias"
                count = None
            elif isinstance(type_def, FractionTypeDefinition):
                if type_name not in referenced_primitives:
                    continue
                kind = "Fraction"
                count = None
            elif isinstance(type_def, (BigIntTypeDefinition, BigUIntTypeDefinition)):
                if type_name not in referenced_primitives:
                    continue
                kind = "BigInt" if isinstance(type_def, BigIntTypeDefinition) else "BigUInt"
                count = None
            elif isinstance(type_def, StringTypeDefinition):
                if type_name not in referenced_primitives:
                    continue
                kind = "String"
                count = None
            elif isinstance(type_def, SetTypeDefinition):
                kind = "Set"
                count = None
            elif isinstance(type_def, DictionaryTypeDefinition):
                kind = "Dictionary"
                count = None
            elif isinstance(type_def, ArrayTypeDefinition):
                # Skip array internal types
                continue
            elif isinstance(type_def, PrimitiveTypeDefinition):
                if type_name not in referenced_primitives:
                    continue
                kind = "Primitive"
                count = None
            elif isinstance(type_def, CompositeTypeDefinition):
                table_file = self.storage.data_dir / f"{type_name}.bin"
                if not table_file.exists():
                    continue
                kind = "Composite"
                try:
                    count = self.storage.get_table(type_name).count
                except Exception:
                    count = 0
            else:
                continue

            # Apply kind filter (skip for "system" — already filtered by prefix above)
            if query.filter is not None and query.filter != "system":
                _KIND_TO_FILTER = {
                    "Interface": "interfaces",
                    "Composite": "composites",
                    "Enum": "enums",
                    "Primitive": "primitives",
                    "Alias": "aliases",
                }
                if _KIND_TO_FILTER.get(kind) != query.filter:
                    continue

            rows.append({
                "type": type_name,
                "kind": kind,
                "count": count,
            })

        rows = self._sort_rows(rows, query.sort_by, defaults=["type"])

        return QueryResult(
            columns=["type", "kind", "count"],
            rows=rows,
        )

    def _collect_referenced_types(
        self,
        type_def: TypeDefinition,
        primitives: set[str],
        aliases: set[str],
    ) -> None:
        """Recursively collect primitive and alias type names referenced by a field type."""
        if isinstance(type_def, AliasTypeDefinition):
            aliases.add(type_def.name)
            self._collect_referenced_types(type_def.base_type, primitives, aliases)
        elif isinstance(type_def, FractionTypeDefinition):
            primitives.add(type_def.name)
        elif isinstance(type_def, (BigIntTypeDefinition, BigUIntTypeDefinition)):
            # BigInt/BigUInt are special array types — collect like primitives
            primitives.add(type_def.name)
        elif isinstance(type_def, StringTypeDefinition):
            # String is a special array type — collect it like a primitive
            primitives.add(type_def.name)
        elif isinstance(type_def, SetTypeDefinition):
            self._collect_referenced_types(type_def.element_type, primitives, aliases)
        elif isinstance(type_def, DictionaryTypeDefinition):
            self._collect_referenced_types(type_def.key_type, primitives, aliases)
            self._collect_referenced_types(type_def.value_type, primitives, aliases)
        elif isinstance(type_def, PrimitiveTypeDefinition):
            primitives.add(type_def.name)
        elif isinstance(type_def, ArrayTypeDefinition):
            self._collect_referenced_types(type_def.element_type, primitives, aliases)

    def _classify_type(self, type_def: TypeDefinition) -> str:
        """Return the kind label for a type definition."""
        if isinstance(type_def, InterfaceTypeDefinition):
            return "Interface"
        if isinstance(type_def, EnumTypeDefinition):
            return "Enum"
        if isinstance(type_def, AliasTypeDefinition):
            return "Alias"
        if isinstance(type_def, StringTypeDefinition):
            return "String"
        if isinstance(type_def, BooleanTypeDefinition):
            return "Boolean"
        if isinstance(type_def, FractionTypeDefinition):
            return "Fraction"
        if isinstance(type_def, BigIntTypeDefinition):
            return "BigInt"
        if isinstance(type_def, BigUIntTypeDefinition):
            return "BigUInt"
        if isinstance(type_def, SetTypeDefinition):
            return "Set"
        if isinstance(type_def, DictionaryTypeDefinition):
            return "Dictionary"
        if isinstance(type_def, ArrayTypeDefinition):
            return "Array"
        if isinstance(type_def, CompositeTypeDefinition):
            return "Composite"
        if isinstance(type_def, PrimitiveTypeDefinition):
            return "Primitive"
        return "Unknown"

    def _build_type_graph(self) -> list[dict[str, str]]:
        """Build the type reference graph as a list of edge dicts.

        Each edge is {"source": str, "kind": str, "target": str, "field": str}.
        Arrow direction: referrer → referent (source depends on target).
        System types (_-prefixed) are excluded. Aliases are only included if
        referenced by at least one user-defined type.
        """
        # Pre-collect aliases referenced by user-defined types
        referenced_aliases: set[str] = set()
        for type_name in self.registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self.registry.get(type_name)
            if type_def is None:
                continue
            base = type_def.resolve_base_type()
            if isinstance(base, (CompositeTypeDefinition, InterfaceTypeDefinition)):
                for f in base.fields:
                    self._collect_referenced_types(f.type_def, set(), referenced_aliases)
            elif isinstance(base, EnumTypeDefinition):
                for v in base.variants:
                    for f in v.fields:
                        self._collect_referenced_types(f.type_def, set(), referenced_aliases)

        seen_edges: set[tuple[str, str, str]] = set()
        edges: list[dict[str, str]] = []
        visited_array_types: set[str] = set()

        def add_edge(source: str, kind: str, target: str, field: str) -> None:
            key = (source, target, field)
            if key not in seen_edges:
                seen_edges.add(key)
                edges.append({"source": source, "kind": kind, "target": target, "field": field})

        def process_field_type(owner: str, kind: str, field_name: str, type_def: TypeDefinition) -> None:
            """Add edge from owner to field's type, and handle array/set/dict element edges."""
            add_edge(owner, kind, type_def.name, field_name)
            base = type_def.resolve_base_type()
            if isinstance(base, SetTypeDefinition) and type_def.name not in visited_array_types:
                visited_array_types.add(type_def.name)
                set_kind = self._classify_type(self.registry.get(type_def.name) or base)
                add_edge(type_def.name, set_kind, base.element_type.name, "{}")
            elif isinstance(base, DictionaryTypeDefinition) and type_def.name not in visited_array_types:
                visited_array_types.add(type_def.name)
                dict_kind = self._classify_type(self.registry.get(type_def.name) or base)
                add_edge(type_def.name, dict_kind, base.key_type.name, "{key}")
                add_edge(type_def.name, dict_kind, base.value_type.name, "{value}")
                add_edge(type_def.name, dict_kind, base.entry_type.name, "(entry)")
            elif isinstance(base, ArrayTypeDefinition) and type_def.name not in visited_array_types:
                visited_array_types.add(type_def.name)
                arr_kind = self._classify_type(self.registry.get(type_def.name) or base)
                add_edge(type_def.name, arr_kind, base.element_type.name, "[]")

        for type_name in self.registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self.registry.get(type_name)
            if type_def is None:
                continue

            kind = self._classify_type(type_def)

            if isinstance(type_def, AliasTypeDefinition):
                if type_name in referenced_aliases:
                    add_edge(type_name, kind, type_def.base_type.name, "(alias)")
            elif isinstance(type_def, EnumTypeDefinition):
                for variant in type_def.variants:
                    for f in variant.fields:
                        process_field_type(type_name, kind, f"{variant.name}.{f.name}", f.type_def)
            elif isinstance(type_def, InterfaceTypeDefinition):
                # Skip inherited fields — they belong to parent interfaces
                inherited_fields: set[str] = set()
                for iface_name in type_def.interfaces:
                    iface_td = self.registry.get(iface_name)
                    if iface_td and isinstance(iface_td, InterfaceTypeDefinition):
                        for f in iface_td.fields:
                            inherited_fields.add(f.name)
                for f in type_def.fields:
                    if f.name not in inherited_fields:
                        process_field_type(type_name, kind, f.name, f.type_def)
                # Inheritance edges
                for iface_name in type_def.interfaces:
                    add_edge(type_name, kind, iface_name, "(extends)")
            elif isinstance(type_def, SetTypeDefinition):
                add_edge(type_name, kind, type_def.element_type.name, "{}")
            elif isinstance(type_def, DictionaryTypeDefinition):
                add_edge(type_name, kind, type_def.key_type.name, "{key}")
                add_edge(type_name, kind, type_def.value_type.name, "{value}")
                add_edge(type_name, kind, type_def.entry_type.name, "(entry)")
            elif isinstance(type_def, CompositeTypeDefinition):
                # Collect inherited field names so we only emit own-field edges
                inherited_fields: set[str] = set()
                if type_def.parent:
                    parent_td = self.registry.get(type_def.parent)
                    if parent_td and isinstance(parent_td, CompositeTypeDefinition):
                        for f in parent_td.fields:
                            inherited_fields.add(f.name)
                for iface_name in type_def.interfaces:
                    iface_td = self.registry.get(iface_name)
                    if iface_td and isinstance(iface_td, InterfaceTypeDefinition):
                        for f in iface_td.fields:
                            inherited_fields.add(f.name)
                for f in type_def.fields:
                    if f.name not in inherited_fields:
                        process_field_type(type_name, kind, f.name, f.type_def)
                # Inheritance edges
                if type_def.parent:
                    add_edge(type_name, kind, type_def.parent, "(extends)")
                for iface_name in type_def.declared_interfaces:
                    add_edge(type_name, kind, iface_name, "(implements)")
            elif isinstance(type_def, FractionTypeDefinition):
                pass
            elif isinstance(type_def, (BigIntTypeDefinition, BigUIntTypeDefinition)):
                # Only show if referenced by user types (like string/path)
                pass
            elif isinstance(type_def, ArrayTypeDefinition) and type_name not in visited_array_types:
                visited_array_types.add(type_name)
                add_edge(type_name, kind, type_def.element_type.name, "[]")

        return edges

    def _filter_edges_by_type(self, edges: list[dict[str, str]], type_name: str,
                              depth: int | None = None) -> list[dict[str, str]]:
        """Filter edges to those involving a type, expanding outward via BFS.

        Depth = number of edges to traverse from the focus type:
        depth 0 = focus node only (no edges), depth 1 = direct edges only, etc.

        The BFS follows inheritance and alias edges. Each edge traversal counts
        against the depth budget.
        """
        if depth is not None and depth <= 0:
            return []
        names = {type_name, type_name + "[]"}
        filtered = [e for e in edges if e["source"] in names or e["target"] in names]
        if depth is not None and depth <= 1:
            return filtered
        # Build a quick lookup: source → list of edges
        edges_by_source: dict[str, list[dict[str, str]]] = {}
        for e in edges:
            edges_by_source.setdefault(e["source"], []).append(e)
        # BFS expansion: follow inheritance/alias targets outward
        # depth 1 = focus edges only (already collected above)
        # depth 2+ = one more level per depth increment
        expanded: set[str] = set(names)
        # Seed frontier with all targets reachable via structural edges from focus
        frontier: set[str] = set()
        for e in filtered:
            if e["field"] in ("(extends)", "(implements)", "(alias)") and e["source"] in names:
                frontier.add(e["target"])
            # Also follow targets that are aliases (they need expansion at depth 2)
            target = e["target"]
            if target not in names:
                td = self.registry.get(target)
                if td and isinstance(td, AliasTypeDefinition):
                    frontier.add(target)
        current_depth = 1  # We've already traversed 1 edge (focus → targets)
        while frontier:
            current_depth += 1
            if depth is not None and current_depth > depth:
                break
            next_frontier: set[str] = set()
            for node_name in frontier:
                if node_name in expanded:
                    continue
                expanded.add(node_name)
                expanded.add(node_name + "[]")
                # Add outgoing edges from the pre-built graph
                for e in edges_by_source.get(node_name, []):
                    if e not in filtered:
                        filtered.append(e)
                    if e["field"] in ("(extends)", "(implements)", "(alias)"):
                        next_frontier.add(e["target"])
                # If this node is an alias, generate an alias→base edge
                td = self.registry.get(node_name)
                if td and isinstance(td, AliasTypeDefinition):
                    kind = self._classify_type(td)
                    base_name = td.base_type.name
                    ae = {"kind": kind, "source": node_name, "field": "(alias)", "target": base_name}
                    if ae not in filtered:
                        filtered.append(ae)
                    if base_name not in expanded:
                        next_frontier.add(base_name)
            frontier = next_frontier
        return filtered

    def _execute_graph(self, query: GraphQuery) -> QueryResult | DumpResult:
        """Execute GRAPH query — unified schema exploration."""
        from pathlib import Path

        # Validate constraints
        if query.view_mode in ("declared", "stored") and not query.focus_type:
            return QueryResult(columns=[], rows=[],
                               message=f"'{query.view_mode}' view requires a focus type (e.g., graph MyType {query.view_mode})")
        if query.show_origin and query.view_mode != "stored":
            return QueryResult(columns=[], rows=[],
                               message="'origin' modifier is only valid with 'stored' view mode")
        focus_types = query.focus_type  # list[str] | None

        # Handle path-to queries
        if query.path_to is not None:
            if not focus_types:
                return QueryResult(columns=[], rows=[],
                                   message="'to' requires a focus type (e.g., graph MyType to Target)")
            edges: list[dict[str, str]] = []
            seen: set[tuple[str, str, str]] = set()
            errors: list[str] = []
            for ft in focus_types:
                path_edges = self._build_path_to(ft, query.path_to)
                if isinstance(path_edges, str):
                    errors.append(path_edges)
                    continue
                for e in path_edges:
                    key = (e["source"], e["target"], e["field"])
                    if key not in seen:
                        seen.add(key)
                        edges.append(e)
            if errors and not edges:
                return QueryResult(columns=[], rows=[], message="; ".join(errors))
            # Expand targets (depth controls how many edges from each target)
            all_edges = self._build_type_graph()
            for target in query.path_to:
                target_edges = self._filter_edges_by_type(all_edges, target, query.depth)
                for e in target_edges:
                    key = (e["source"], e["target"], e["field"])
                    if key not in seen:
                        seen.add(key)
                        edges.append(e)
        # Build the graph based on view mode
        elif query.view_mode == "structure":
            if not focus_types or len(focus_types) == 1:
                edges = self._build_structure_graph(focus_types[0] if focus_types else None, query.depth)
            else:
                edges = []
                seen = set()
                for ft in focus_types:
                    for e in self._build_structure_graph(ft, query.depth):
                        key = (e["source"], e["target"], e["field"])
                        if key not in seen:
                            seen.add(key)
                            edges.append(e)
        elif query.view_mode == "declared":
            edges = []
            seen = set()
            for ft in (focus_types or []):
                for e in self._build_declared_graph(ft, query.depth):
                    key = (e["source"], e["target"], e["field"])
                    if key not in seen:
                        seen.add(key)
                        edges.append(e)
        elif query.view_mode == "stored":
            edges = []
            seen = set()
            for ft in (focus_types or []):
                for e in self._build_stored_graph(ft, query.show_origin, query.depth):
                    key = (e["source"], e["target"], e["field"])
                    if key not in seen:
                        seen.add(key)
                        edges.append(e)
        else:
            # full view (default)
            edges = self._build_type_graph()
            if focus_types:
                combined = []
                seen = set()
                for ft in focus_types:
                    for e in self._filter_edges_by_type(edges, ft, query.depth):
                        key = (e["source"], e["target"], e["field"])
                        if key not in seen:
                            seen.add(key)
                            combined.append(e)
                edges = combined

        # Apply showing/excluding filters
        if query.showing or query.excluding:
            edges = self._apply_graph_filters(edges, query.showing, query.excluding)

        if query.output_file:
            # File output: DOT or TTQ
            nodes = self._collect_graph_nodes(edges)
            # Ensure focus types always appear as nodes (even with depth 0 / no edges)
            if focus_types:
                for ft in focus_types:
                    if ft not in nodes:
                        td = self.registry.get(ft)
                        nodes[ft] = self._classify_type(td) if td else "Unknown"
            ext = Path(query.output_file).suffix.lower()
            if ext == ".dot":
                script = self._format_graph_dot(nodes, edges, query)
            else:
                if not ext:
                    query.output_file += ".ttq"
                title = next((v for k, v in query.metadata if k == "title"), None)
                script = self._format_graph_ttq(nodes, edges, title=title, query=query)
            return DumpResult(columns=[], rows=[], script=script, output_file=query.output_file)
        else:
            # Table output
            columns = self._graph_table_columns(query)
            edges = self._sort_rows(edges, query.sort_by, defaults=self._graph_default_sort(query))
            message = None
            if not edges and focus_types and query.depth == 0:
                message = f"{', '.join(focus_types)} (no edges at depth 0)"
            return QueryResult(columns=columns, rows=edges, message=message)

    def _graph_table_columns(self, query: GraphQuery) -> list[str]:
        """Determine table columns based on view mode."""
        if query.view_mode in ("declared", "stored") and query.show_origin:
            return ["kind", "source", "field", "target", "origin"]
        return ["kind", "source", "field", "target"]

    def _graph_default_sort(self, query: GraphQuery) -> list[str]:
        """Default sort keys based on view mode."""
        return ["target", "source"]

    def _apply_graph_filters(self, edges: list[dict[str, str]],
                              showing: list[GraphFilter],
                              excluding: list[GraphFilter]) -> list[dict[str, str]]:
        """Apply showing/excluding filters to graph edges.

        showing narrows to matching edges (plus structural path).
        excluding removes matching edges. showing applied first, then excluding.
        """
        if showing:
            edges = self._apply_showing(edges, showing)
        if excluding:
            edges = self._apply_excluding(edges, excluding)
        return edges

    def _apply_showing(self, edges: list[dict[str, str]],
                        filters: list[GraphFilter]) -> list[dict[str, str]]:
        """Keep matched edges plus edges on paths leading to them.

        1. Find directly matched edges.
        2. Walk backward from matched sources to find types that lead to them.
        3. Return matched edges + edges whose target is in the reachable set.
        """
        # Step 1: Find matched edges, seed reachable from their sources only
        matched: list[dict[str, str]] = []
        reachable: set[str] = set()
        for e in edges:
            if self._edge_matches_any_filter(e, filters):
                matched.append(e)
                reachable.add(e["source"])

        # Step 2: Walk backward through all edges from matched sources
        changed = True
        while changed:
            changed = False
            for e in edges:
                if e["target"] in reachable and e["source"] not in reachable:
                    reachable.add(e["source"])
                    changed = True

        # Step 3: Matched edges + path edges leading to matched sources
        matched_set = set(id(e) for e in matched)
        return matched + [e for e in edges
                          if id(e) not in matched_set and e["target"] in reachable]

    def _apply_excluding(self, edges: list[dict[str, str]],
                          filters: list[GraphFilter]) -> list[dict[str, str]]:
        """Remove edges matching any excluding filter."""
        return [e for e in edges
                if not self._edge_matches_any_filter(e, filters)]

    def _edge_matches_any_filter(self, edge: dict[str, str],
                                  filters: list[GraphFilter]) -> bool:
        """Check if an edge matches any of the given filters."""
        for f in filters:
            if self._edge_matches_filter(edge, f):
                return True
        return False

    def _edge_matches_filter(self, edge: dict[str, str],
                              filt: GraphFilter) -> bool:
        """Check if an edge matches a single filter."""
        if filt.dimension == "type":
            return edge.get("target", "") in filt.values
        elif filt.dimension == "field":
            return edge.get("field", "") in filt.values
        elif filt.dimension == "kind":
            # Case-insensitive match for kind values
            edge_kind = edge.get("kind", "")
            return any(edge_kind.lower() == v.lower() for v in filt.values)
        return False

    def _build_declared_structural_edges(self) -> list[dict[str, str]]:
        """Build structural edges using only directly declared relationships.

        For composites: only the direct parent (extends) and interfaces
        explicitly listed in the type definition (not inherited from parent).
        For interfaces: only directly listed parent interfaces.
        """
        edges: list[dict[str, str]] = []

        for name in self.registry.list_types():
            if name.startswith("_"):
                continue
            type_def = self.registry.get(name)
            if type_def is None:
                continue
            kind = self._classify_type(type_def)
            base = type_def.resolve_base_type() if hasattr(type_def, 'resolve_base_type') else type_def

            if isinstance(base, CompositeTypeDefinition):
                if base.parent:
                    edges.append({"source": name, "kind": kind, "field": "(extends)", "target": base.parent})
                # Compute directly declared interfaces (not inherited from parent)
                parent_ifaces: set[str] = set()
                if base.parent:
                    parent_def = self.registry.get(base.parent)
                    if parent_def:
                        parent_base = parent_def.resolve_base_type() if hasattr(parent_def, 'resolve_base_type') else parent_def
                        if isinstance(parent_base, CompositeTypeDefinition):
                            parent_ifaces = set(parent_base.interfaces)
                for iface in base.interfaces:
                    if iface not in parent_ifaces:
                        edges.append({"source": name, "kind": kind, "field": "(implements)", "target": iface})
            elif isinstance(base, InterfaceTypeDefinition):
                for iface in base.interfaces:
                    edges.append({"source": name, "kind": kind, "field": "(extends)", "target": iface})

        return edges

    def _build_path_to(self, focus_type: str, targets: list[str]) -> list[dict[str, str]] | str:
        """Build a path-to graph: declared structural edges between focus and targets.

        Returns edges on success, or an error message string on failure.
        Uses BFS on the declared inheritance graph (only direct parent/interface
        relationships, not inherited ones) to find the path from focus type
        to each target.
        """
        structural = self._build_declared_structural_edges()

        # Build adjacency: source → list of (target, edge)
        adj: dict[str, list[tuple[str, dict[str, str]]]] = {}
        for e in structural:
            adj.setdefault(e["source"], []).append((e["target"], e))

        result_edges: list[dict[str, str]] = []
        seen_edges: set[int] = set()  # Track by id to avoid duplicates
        errors: list[str] = []

        for target in targets:
            if self.registry.get(target) is None:
                errors.append(f"Unknown type '{target}'")
                continue

            # BFS from focus to target
            queue: list[tuple[str, list[dict[str, str]]]] = [(focus_type, [])]
            visited: set[str] = {focus_type}
            found = False

            while queue:
                current, path = queue.pop(0)
                if current == target:
                    for e in path:
                        eid = id(e)
                        if eid not in seen_edges:
                            seen_edges.add(eid)
                            result_edges.append(e)
                    found = True
                    break
                for neighbor, edge in adj.get(current, []):
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append((neighbor, path + [edge]))

            if not found:
                errors.append(f"No inheritance path from '{focus_type}' to '{target}'")

        if errors:
            return "; ".join(errors)

        return result_edges

    def _build_structure_graph(self, focus_type: str | None, depth: int | None = None) -> list[dict[str, str]]:
        """Build structure view: only inheritance edges, no field→type edges."""
        all_edges = self._build_type_graph()
        structural = [e for e in all_edges if e["field"] in ("(extends)", "(implements)")]
        if focus_type:
            structural = self._filter_edges_by_type_structure(structural, focus_type, depth)
        return structural

    def _filter_edges_by_type_structure(self, edges: list[dict[str, str]], type_name: str,
                                         depth: int | None = None) -> list[dict[str, str]]:
        """Filter structural edges to those reachable from a focus type.

        Depth = number of edges to traverse from the focus type:
        depth 0 = focus node only (no edges), depth 1 = direct inheritance only, etc.
        """
        if depth is not None and depth <= 0:
            return []
        # Build adjacency: source → edges
        edges_by_source: dict[str, list[dict[str, str]]] = {}
        for e in edges:
            edges_by_source.setdefault(e["source"], []).append(e)
        # BFS from focus type through (extends)/(implements)
        visited: set[str] = set()
        frontier = {type_name}
        result: list[dict[str, str]] = []
        current_depth = 0
        while frontier:
            current_depth += 1
            if depth is not None and current_depth > depth:
                break
            next_frontier: set[str] = set()
            for name in frontier:
                if name in visited:
                    continue
                visited.add(name)
                for e in edges_by_source.get(name, []):
                    result.append(e)
                    next_frontier.add(e["target"])
            frontier = next_frontier
        return result

    def _build_declared_graph(self, focus_type: str, depth: int | None = None) -> list[dict[str, str]]:
        """Build declared view: only fields declared by the focus type itself.

        Depth controls alias expansion beyond the initial field listing:
        depth 0 = focus node only (no edges), depth 1 = field edges only,
        None (unlimited) = resolve all aliases to storage types,
        depth 2+ = resolve N-1 levels of alias chains.
        """
        if depth is not None and depth <= 0:
            return []
        type_def = self.registry.get(focus_type)
        if type_def is None:
            return []
        base = type_def.resolve_base_type()

        # Determine which fields are inherited
        inherited_fields: set[str] = set()
        if isinstance(base, CompositeTypeDefinition):
            if base.parent:
                parent_td = self.registry.get(base.parent)
                if parent_td and isinstance(parent_td, CompositeTypeDefinition):
                    for f in parent_td.fields:
                        inherited_fields.add(f.name)
            for iface_name in base.interfaces:
                iface_td = self.registry.get(iface_name)
                if iface_td and isinstance(iface_td, InterfaceTypeDefinition):
                    for f in iface_td.fields:
                        inherited_fields.add(f.name)
        elif isinstance(base, InterfaceTypeDefinition):
            for iface_name in base.interfaces:
                iface_td = self.registry.get(iface_name)
                if iface_td and isinstance(iface_td, InterfaceTypeDefinition):
                    for f in iface_td.fields:
                        inherited_fields.add(f.name)

        # Only own fields
        own_fields = [f for f in base.fields if f.name not in inherited_fields] if hasattr(base, 'fields') else []

        kind = self._classify_type(type_def)
        edges: list[dict[str, str]] = []
        for f in own_fields:
            edges.append({"kind": kind, "source": focus_type, "field": f.name, "target": f.type_def.name})
        return self._expand_view_aliases(edges, depth)

    def _build_stored_graph(self, focus_type: str,
                             show_origin: bool, depth: int | None = None) -> list[dict[str, str]]:
        """Build stored view: all fields on the type's record (inherited + own).

        Depth controls alias expansion beyond the initial field listing:
        depth 0 = focus node only (no edges), depth 1 = field edges only,
        None (unlimited) = resolve all aliases to storage types,
        depth 2+ = resolve N-1 levels of alias chains.
        """
        if depth is not None and depth <= 0:
            return []
        type_def = self.registry.get(focus_type)
        if type_def is None:
            return []
        base = type_def.resolve_base_type()
        if not hasattr(base, 'fields'):
            return []

        kind = self._classify_type(type_def)
        edges: list[dict[str, str]] = []
        origins = self._compute_field_origins(base, focus_type) if show_origin else {}
        for f in base.fields:
            edge: dict[str, str] = {"kind": kind, "source": focus_type, "field": f.name, "target": f.type_def.name}
            if show_origin:
                edge["origin"] = origins.get(f.name, focus_type)
            edges.append(edge)
        return self._expand_view_aliases(edges, depth)

    def _expand_alias_edges(self, edges: list[dict[str, str]]) -> list[dict[str, str]]:
        """Append alias→base edges for any alias targets in the edge list.

        Recursively expands through alias chains (alias of alias).
        """
        seen: set[str] = set()
        targets = {e["target"] for e in edges}
        queue = list(targets)
        while queue:
            name = queue.pop()
            if name in seen:
                continue
            seen.add(name)
            td = self.registry.get(name)
            if td is None:
                continue
            if isinstance(td, AliasTypeDefinition):
                kind = self._classify_type(td)
                base_name = td.base_type.name
                edges.append({"kind": kind, "source": name, "field": "(alias)", "target": base_name})
                if base_name not in seen:
                    queue.append(base_name)
        return edges

    def _expand_view_aliases(self, edges: list[dict[str, str]],
                              depth: int | None) -> list[dict[str, str]]:
        """Expand alias chains in declared/stored views.

        Field edges count as depth 1. Each additional depth level resolves
        one level of alias chains. None (unlimited) resolves all aliases
        to their storage types.
        """
        if depth is not None and depth <= 1:
            return edges
        # remaining_depth: how many alias levels to follow
        remaining_depth = None if depth is None else depth - 1
        seen: set[str] = set()
        targets = {e["target"] for e in edges}
        queue = list(targets)
        levels_expanded = 0
        while queue:
            if remaining_depth is not None and levels_expanded >= remaining_depth:
                break
            levels_expanded += 1
            next_queue: list[str] = []
            for name in queue:
                if name in seen:
                    continue
                seen.add(name)
                td = self.registry.get(name)
                if td is None:
                    continue
                if isinstance(td, AliasTypeDefinition):
                    kind = self._classify_type(td)
                    base_name = td.base_type.name
                    edges.append({"kind": kind, "source": name, "field": "(alias)", "target": base_name})
                    if base_name not in seen:
                        next_queue.append(base_name)
            queue = next_queue
        return edges

    def _compute_field_origins(self, base_type: Any, focus_name: str) -> dict[str, str]:
        """Compute origin (defining type) for each field.

        Recursively walks the parent/interface chain to find the type
        that first declared each field.
        """
        origins: dict[str, str] = {}

        if isinstance(base_type, CompositeTypeDefinition):
            # Recursively walk parent chain
            if base_type.parent:
                parent_td = self.registry.get(base_type.parent)
                if parent_td and isinstance(parent_td, CompositeTypeDefinition):
                    parent_origins = self._compute_field_origins(parent_td, base_type.parent)
                    origins.update(parent_origins)
            # Walk interfaces
            for iface_name in base_type.interfaces:
                iface_td = self.registry.get(iface_name)
                if iface_td and isinstance(iface_td, InterfaceTypeDefinition):
                    iface_origins = self._compute_interface_field_origins(iface_td, iface_name)
                    for fname, origin in iface_origins.items():
                        if fname not in origins:
                            origins[fname] = origin
        elif isinstance(base_type, InterfaceTypeDefinition):
            iface_origins = self._compute_interface_field_origins(base_type, focus_name)
            origins.update(iface_origins)

        # Own fields not yet in origins belong to the focus type
        for f in base_type.fields:
            if f.name not in origins:
                origins[f.name] = focus_name

        return origins

    def _compute_interface_field_origins(self, iface: InterfaceTypeDefinition, iface_name: str) -> dict[str, str]:
        """Trace field origins through interface inheritance chain."""
        origins: dict[str, str] = {}
        # Process parent interfaces first (depth-first)
        for parent_name in iface.interfaces:
            parent_td = self.registry.get(parent_name)
            if parent_td and isinstance(parent_td, InterfaceTypeDefinition):
                parent_origins = self._compute_interface_field_origins(parent_td, parent_name)
                for fname, origin in parent_origins.items():
                    if fname not in origins:
                        origins[fname] = origin
        # Own fields
        inherited_set: set[str] = set()
        for parent_name in iface.interfaces:
            parent_td = self.registry.get(parent_name)
            if parent_td and isinstance(parent_td, InterfaceTypeDefinition):
                for f in parent_td.fields:
                    inherited_set.add(f.name)
        for f in iface.fields:
            if f.name not in inherited_set:
                origins.setdefault(f.name, iface_name)
        return origins

    def _collect_graph_nodes(self, edges: list[dict[str, str]]) -> dict[str, str]:
        """Collect unique node names and their kinds from edges + registry."""
        names: set[str] = set()
        for e in edges:
            names.add(e["source"])
            names.add(e["target"])

        nodes: dict[str, str] = {}
        for name in sorted(names):
            type_def = self.registry.get(name)
            nodes[name] = self._classify_type(type_def) if type_def else "Unknown"
        return nodes

    def _format_graph_ttq(self, nodes: dict[str, str], edges: list[dict[str, str]],
                          title: str | None = None,
                          query: GraphQuery | None = None) -> str:
        """Format type graph as a TTQ script."""
        lines: list[str] = []
        effective_title = title
        if not effective_title and query:
            effective_title = self._build_graph_default_title(query)
        lines.append(f"-- {effective_title or 'Type reference graph'}")
        lines.append("enum NodeRole { focus, context, endpoint, leaf }")
        lines.append("type TypeNode { name: string, kind: string, role: NodeRole }")
        lines.append("type Edge { source: TypeNode, target: TypeNode, field_name: string }")
        lines.append("")

        # Compute node roles
        focus_types = set(query.focus_type) if query and query.focus_type else set()
        path_targets = set(query.path_to) if query and query.path_to else set()
        # Sources in the graph (non-leaf nodes)
        source_names = {e["source"] for e in edges}

        # Assign indices to nodes
        node_list = list(nodes.keys())
        node_index = {name: i for i, name in enumerate(node_list)}

        for name in node_list:
            kind = nodes[name]
            if name in focus_types:
                role = "focus"
            elif name in path_targets:
                role = "endpoint"
            elif name in source_names:
                role = "context"
            else:
                role = "leaf"
            lines.append(f'create TypeNode(name="{name}", kind="{kind}", role=.{role})')

        if node_list:
            lines.append("")

        for e in edges:
            src_idx = node_index[e["source"]]
            tgt_idx = node_index[e["target"]]
            field = e["field"].replace('"', '\\"')
            lines.append(f'create Edge(source=TypeNode({src_idx}), target=TypeNode({tgt_idx}), field_name="{field}")')

        lines.append("")
        return "\n".join(lines)

    def _build_graph_default_title(self, query: GraphQuery) -> str:
        """Build a descriptive default title from query parameters."""
        parts: list[str] = []

        # Focus type(s)
        focus = query.focus_type
        if focus:
            parts.append(", ".join(focus))

        # Path-to
        if query.path_to:
            targets = ", ".join(query.path_to)
            parts.append(f"to {targets}")
        # View mode
        elif query.view_mode != "full":
            mode = query.view_mode
            if query.show_origin:
                mode += " origin"
            parts.append(mode)

        # Depth
        if query.depth is not None:
            parts.append(f"depth {query.depth}")

        # Filters
        for filt in query.showing:
            vals = ", ".join(filt.values)
            parts.append(f"showing {filt.dimension} {vals}")
        for filt in query.excluding:
            vals = ", ".join(filt.values)
            parts.append(f"excluding {filt.dimension} {vals}")

        if parts:
            return "graph " + " ".join(parts)
        return "graph"

    def _load_style_file(self, style_path: str) -> dict[str, str]:
        """Load a style file containing a TTQ dict literal.

        Style files contain a single TTQ dictionary expression:
            {
                "direction": "LR",
                "composite.color": "#4A90D9",
                "interface.color": "#7B68EE",
                "focus.color": "#FFD700"
            }

        Lines starting with -- are comments (standard TTQ comments).
        """
        import os
        styles: dict[str, str] = {}
        # Resolve relative to calling script dir (if running from a script),
        # otherwise relative to data dir
        if not os.path.isabs(style_path):
            if self._script_stack:
                style_path = os.path.join(str(self._script_stack[-1]), style_path)
            else:
                style_path = os.path.join(str(self.storage.data_dir), style_path)
        try:
            with open(style_path) as f:
                content = f.read()
        except FileNotFoundError:
            return styles
        # Parse as TTQ dict literal by wrapping in a create expression
        # (bare {…} is ambiguous with type body at top level)
        try:
            from typed_tables.parsing.query_parser import QueryParser, DictLiteral
            parser = QueryParser()
            parser.build()
            wrapped = f"create _S(x={content})"
            stmts = parser.parse_program(wrapped)
            if stmts and len(stmts) == 1:
                field_val = stmts[0].fields[0].value
                if isinstance(field_val, DictLiteral):
                    for entry in field_val.entries:
                        styles[str(entry.key)] = str(entry.value)
        except Exception:
            pass  # Silently ignore parse errors in style files
        return styles

    def _format_graph_dot(self, nodes: dict[str, str], edges: list[dict[str, str]],
                          query: GraphQuery | None = None) -> str:
        """Format type graph as a DOT file for Graphviz."""
        # Load style overrides from ordered metadata
        user_styles: dict[str, str] = {}
        title = None
        if query:
            for key, value in query.metadata:
                if key == "title":
                    title = value
                elif key == "style":
                    file_styles = self._load_style_file(value)
                    user_styles.update(file_styles)
                else:
                    user_styles[key] = value
            if title is None:
                title = self._build_graph_default_title(query)

        lines: list[str] = []
        lines.append("digraph types {")
        direction = user_styles.get("direction", "LR")
        lines.append(f"    rankdir={direction};")
        lines.append('    node [style=filled];')
        if title:
            escaped = title.replace('"', '\\"')
            lines.append(f'    label="{escaped}";')
            lines.append("    labelloc=t;")
            lines.append("    fontsize=18;")

        lines.append("")

        kind_styles = {
            "Composite": ('box', '#ADD8E6'),
            "Interface": ('box', '#FFB347'),
            "Enum": ('box', '#90EE90'),
            "Alias": ('box', '#D3D3D3'),
            "Primitive": ('ellipse', '#FFFFE0'),
            "String": ('ellipse', '#FFFFE0'),
            "Boolean": ('ellipse', '#FFFFE0'),
            "Array": ('ellipse', '#FFD700'),
            "Set": ('ellipse', '#FFD700'),
            "Dictionary": ('ellipse', '#FFD700'),
            "Fraction": ('ellipse', '#DDA0DD'),
            "BigInt": ('ellipse', '#DDA0DD'),
            "BigUInt": ('ellipse', '#DDA0DD'),
            "Unknown": ('box', '#FFFFFF'),
        }

        # Apply style overrides for kind colors
        style_key_map = {
            "composite.color": "Composite",
            "interface.color": "Interface",
            "enum.color": "Enum",
            "alias.color": "Alias",
            "primitive.color": "Primitive",
        }
        for style_key, kind_name in style_key_map.items():
            if style_key in user_styles and kind_name in kind_styles:
                shape, _ = kind_styles[kind_name]
                kind_styles[kind_name] = (shape, user_styles[style_key])

        # Focus type styling
        focus_color = user_styles.get("focus.color")

        for name in nodes:
            kind = nodes[name]
            shape, color = kind_styles.get(kind, ('box', '#FFFFFF'))
            extra = ""
            if query and query.focus_type and name in query.focus_type:
                if focus_color:
                    color = focus_color
                extra = ', penwidth=3'
            lines.append(f'    "{name}" [shape={shape}, fillcolor="{color}"{extra}];')

        lines.append("")

        for e in edges:
            if e["field"] == "(extends)":
                lines.append(f'    "{e["source"]}" -> "{e["target"]}" [style=dashed];')
            elif e["field"] == "(implements)":
                lines.append(f'    "{e["source"]}" -> "{e["target"]}" [style=dotted];')
            elif e["field"] == "(alias)":
                lines.append(f'    "{e["source"]}" -> "{e["target"]}" [style=dashed, arrowhead=empty];')
            else:
                label = e["field"].replace('"', '\\"')
                origin_suffix = ""
                if "origin" in e and e["origin"] != e.get("source", ""):
                    origin_suffix = f"\\n(from {e['origin']})"
                lines.append(f'    "{e["source"]}" -> "{e["target"]}" [label="{label}{origin_suffix}"];')

        lines.append("}")
        lines.append("")
        return "\n".join(lines)

    def _execute_describe(self, query: DescribeQuery) -> QueryResult:
        """Execute DESCRIBE query."""
        # Handle variant-specific describe: describe Shape.circle
        if "." in query.table:
            parts = query.table.split(".", 1)
            enum_name, variant_name = parts[0], parts[1]
            enum_def = self.registry.get(enum_name)
            if enum_def is None:
                return QueryResult(columns=[], rows=[], message=f"Unknown type: {enum_name}")
            enum_base = enum_def.resolve_base_type()
            if not isinstance(enum_base, EnumTypeDefinition):
                return QueryResult(columns=[], rows=[], message=f"Not an enum type: {enum_name}")
            variant = enum_base.get_variant(variant_name)
            if variant is None:
                return QueryResult(columns=[], rows=[], message=f"Unknown variant '{variant_name}' on enum '{enum_name}'")
            rows = [{
                "property": "(variant)",
                "type": f"{enum_name}.{variant_name}",
                "size": sum(f.type_def.reference_size for f in variant.fields),
            }]
            for f in variant.fields:
                rows.append({"property": f.name, "type": f.type_def.name, "size": f.type_def.reference_size})
            if query.sort_by:
                rows = self._sort_rows(rows, query.sort_by)
            return QueryResult(columns=["property", "type", "size"], rows=rows)

        type_def = self.registry.get(query.table)
        if type_def is None:
            return QueryResult(
                columns=[],
                rows=[],
                message=f"Unknown type: {query.table}",
            )

        rows = []
        base = type_def.resolve_base_type()

        # Add type info
        rows.append({
            "property": "(type)",
            "type": type_def.__class__.__name__.replace("TypeDefinition", ""),
            "size": type_def.size_bytes,
        })

        if isinstance(type_def, AliasTypeDefinition):
            rows.append({
                "property": "(alias_of)",
                "type": type_def.base_type.name,
                "size": type_def.base_type.size_bytes,
            })

        if isinstance(base, EnumTypeDefinition):
            for variant in base.variants:
                if variant.fields:
                    field_strs = [f"{f.name}: {f.type_def.name}" for f in variant.fields]
                    rows.append({
                        "property": variant.name,
                        "type": f"({', '.join(field_strs)})",
                        "size": sum(f.type_def.reference_size for f in variant.fields),
                    })
                else:
                    rows.append({
                        "property": variant.name,
                        "type": f"= {variant.discriminant}",
                        "size": 0,
                    })
        elif isinstance(base, InterfaceTypeDefinition):
            for field in base.fields:
                default_val = self._format_default_for_dump(field.default_value, field.type_def) if field.default_value is not None else None
                rows.append({
                    "property": field.name,
                    "type": field.type_def.name,
                    "size": field.type_def.reference_size,
                    "default": default_val,
                    "overflow": field.overflow or "",
                })
            # List parent interfaces
            if base.interfaces:
                for iface_name in base.interfaces:
                    rows.append({
                        "property": "(extends)",
                        "type": iface_name,
                        "size": 0,
                        "default": "",
                        "overflow": "",
                    })
            # List implementing types
            impl_types = self.registry.find_implementing_types(query.table)
            if impl_types:
                for impl_name, _ in impl_types:
                    rows.append({
                        "property": "(implements)",
                        "type": impl_name,
                        "size": 0,
                        "default": "",
                        "overflow": "",
                    })
        elif isinstance(base, CompositeTypeDefinition):
            for field in base.fields:
                field_base = field.type_def.resolve_base_type()
                default_val = self._format_default_for_dump(field.default_value, field.type_def) if field.default_value is not None else None
                rows.append({
                    "property": field.name,
                    "type": field.type_def.name,
                    "size": field.type_def.reference_size,
                    "default": default_val,
                    "overflow": field.overflow or "",
                })
            if base.parent:
                rows.append({
                    "property": "(parent)",
                    "type": base.parent,
                    "size": 0,
                    "default": "",
                    "overflow": "",
                })
            if base.interfaces:
                for iface_name in base.interfaces:
                    rows.append({
                        "property": "(interface)",
                        "type": iface_name,
                        "size": 0,
                        "default": "",
                        "overflow": "",
                    })
        elif isinstance(base, FractionTypeDefinition):
            rows.append({
                "property": "(precision)",
                "type": "exact rational",
                "size": 16,
            })
        elif isinstance(base, (BigIntTypeDefinition, BigUIntTypeDefinition)):
            rows.append({
                "property": "(precision)",
                "type": "arbitrary",
                "size": 8,  # inline reference size: (start_index, length)
            })
        elif isinstance(base, SetTypeDefinition):
            rows.append({
                "property": "(element_type)",
                "type": base.element_type.name,
                "size": base.element_type.size_bytes,
            })
        elif isinstance(base, DictionaryTypeDefinition):
            rows.append({
                "property": "(key_type)",
                "type": base.key_type.name,
                "size": base.key_type.size_bytes,
            })
            rows.append({
                "property": "(value_type)",
                "type": base.value_type.name,
                "size": base.value_type.size_bytes,
            })
            rows.append({
                "property": "(entry_type)",
                "type": base.entry_type.name,
                "size": base.entry_type.size_bytes,
            })
        elif isinstance(base, ArrayTypeDefinition):
            rows.append({
                "property": "(element_type)",
                "type": base.element_type.name,
                "size": base.element_type.size_bytes,
            })

        columns = ["property", "type", "size"]
        if any(row.get("default") is not None for row in rows):
            columns.append("default")
        if any(row.get("overflow") for row in rows):
            columns.append("overflow")

        if query.sort_by:
            rows = self._sort_rows(rows, query.sort_by)

        return QueryResult(
            columns=columns,
            rows=rows,
        )

    def _execute_use(self, query: UseQuery) -> UseResult:
        """Execute USE query - returns path for REPL to switch databases."""
        return UseResult(
            columns=[],
            rows=[],
            path=query.path,
            temporary=query.temporary,
            message=f"Switching to database: {query.path}",
        )

    def _execute_drop_database(self, query: DropDatabaseQuery) -> DropResult:
        """Execute DROP database query - returns path for REPL to delete database."""
        return DropResult(
            columns=[],
            rows=[],
            path=query.path,
            message=f"Dropping database: {query.path}",
        )

    def _resolve_type_spec(self, spec: str | ArrayTypeSpec | SetTypeSpec | DictTypeSpec) -> TypeDefinition | None:
        """Resolve a type specification (string or structured TypeSpec) to a TypeDefinition.

        Returns None if the type cannot be resolved.
        """
        if isinstance(spec, str):
            if spec.endswith("[]"):
                base_name = spec[:-2]
                return self.registry.get_array_type(base_name)
            return self.registry.get(spec)
        elif isinstance(spec, ArrayTypeSpec):
            element_td = self._resolve_type_spec(spec.element_type)
            if element_td is None:
                return None
            return self.registry.get_array_type(element_td.name)
        elif isinstance(spec, SetTypeSpec):
            element_td = self._resolve_type_spec(spec.element_type)
            if element_td is None:
                return None
            return self.registry.get_or_create_set_type(element_td)
        elif isinstance(spec, DictTypeSpec):
            key_td = self._resolve_type_spec(spec.key_type)
            val_td = self._resolve_type_spec(spec.value_type)
            if key_td is None or val_td is None:
                return None
            return self.registry.get_or_create_dict_type(key_td, val_td)
        return None

    def _type_spec_to_string(self, spec: str | ArrayTypeSpec | SetTypeSpec | DictTypeSpec) -> str:
        """Convert a TypeSpec AST node back to a human-readable string."""
        if isinstance(spec, str):
            return spec
        elif isinstance(spec, ArrayTypeSpec):
            return "[" + self._type_spec_to_string(spec.element_type) + "]"
        elif isinstance(spec, SetTypeSpec):
            return "{" + self._type_spec_to_string(spec.element_type) + "}"
        elif isinstance(spec, DictTypeSpec):
            return "{" + self._type_spec_to_string(spec.key_type) + ": " + self._type_spec_to_string(spec.value_type) + "}"
        return str(spec)

    def _execute_create_alias(self, query: CreateAliasQuery) -> CreateResult:
        """Execute CREATE ALIAS query."""
        if query.name.startswith("_"):
            return CreateResult(
                columns=[], rows=[],
                message="Type names starting with '_' are reserved for system use",
                type_name=query.name,
            )
        # Check if alias name already exists
        if self.registry.get(query.name) is not None:
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Type '{query.name}' already exists",
                type_name=query.name,
            )

        # Get the base type
        base_type = self._resolve_type_spec(query.base_type)
        if base_type is None:
            type_str = self._type_spec_to_string(query.base_type)
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Unknown base type: {type_str}",
                type_name=query.name,
            )

        # Create and register the alias
        alias = AliasTypeDefinition(name=query.name, base_type=base_type)
        self.registry.register(alias)

        # Save updated metadata
        self.storage.save_metadata()

        type_str = self._type_spec_to_string(query.base_type)
        return CreateResult(
            columns=["alias", "base_type"],
            rows=[{"alias": query.name, "base_type": type_str}],
            message=f"Created alias '{query.name}' as '{type_str}'",
            type_name=query.name,
        )

    def _execute_create_interface(self, query: CreateInterfaceQuery) -> CreateResult:
        """Execute CREATE INTERFACE query."""
        if query.name.startswith("_"):
            return CreateResult(
                columns=[], rows=[],
                message="Type names starting with '_' are reserved for system use",
                type_name=query.name,
            )
        existing = self.registry.get(query.name)

        if existing is not None:
            if isinstance(existing, InterfaceTypeDefinition) and not existing.fields and (query.fields or query.parents):
                pass  # populate stub
            else:
                return CreateResult(
                    columns=[],
                    rows=[],
                    message=f"Type '{query.name}' already exists",
                    type_name=query.name,
                )

        # Register stub (idempotent if already exists)
        if existing is None:
            self.registry.register_interface_stub(query.name)

        # Handle interface inheritance
        parent_fields: list[FieldDefinition] = []
        interface_names: list[str] = []

        for parent_name in query.parents:
            if parent_name == query.name:
                return CreateResult(
                    columns=[], rows=[],
                    message=f"Circular inheritance: '{query.name}' cannot inherit from itself",
                    type_name=query.name,
                )

            parent_type = self.registry.get(parent_name)
            if parent_type is None:
                return CreateResult(
                    columns=[], rows=[],
                    message=f"Unknown parent type: {parent_name}",
                    type_name=query.name,
                )

            parent_base = parent_type.resolve_base_type()
            if not isinstance(parent_base, InterfaceTypeDefinition):
                return CreateResult(
                    columns=[], rows=[],
                    message=f"Interfaces can only inherit from other interfaces, not '{parent_name}'",
                    type_name=query.name,
                )

            # Merge fields with diamond-merge conflict detection
            interface_names.append(parent_name)
            for f in parent_base.fields:
                existing_field = next((pf for pf in parent_fields if pf.name == f.name), None)
                if existing_field is not None:
                    # Same name: must be same type (diamond merge)
                    if existing_field.type_def.name != f.type_def.name:
                        return CreateResult(
                            columns=[], rows=[],
                            message=f"Field conflict: '{f.name}' has type '{existing_field.type_def.name}' from one parent but '{f.type_def.name}' from '{parent_name}'",
                            type_name=query.name,
                        )
                    # Same name + same type → merge (skip duplicate)
                else:
                    parent_fields.append(f)

        # Build field definitions from query
        fields: list[FieldDefinition] = parent_fields.copy()
        for field_def in query.fields:
            field_type = self._resolve_type_spec(field_def.type_name)
            if field_type is None:
                type_str = self._type_spec_to_string(field_def.type_name)
                return CreateResult(
                    columns=[],
                    rows=[],
                    message=f"Unknown type: {type_str}",
                    type_name=query.name,
                )

            fd = FieldDefinition(name=field_def.name, type_def=field_type)
            if field_def.overflow is not None:
                err = self._validate_overflow_modifier(field_def.overflow, field_type, field_def.name)
                if err:
                    return CreateResult(columns=[], rows=[], message=err, type_name=query.name)
                fd.overflow = field_def.overflow
            if field_def.default_value is not None:
                try:
                    fd.default_value = self._resolve_default_value(field_def.default_value, field_type)
                except ValueError as e:
                    return CreateResult(
                        columns=[],
                        rows=[],
                        message=f"Invalid default for field '{field_def.name}': {e}",
                        type_name=query.name,
                    )
            fields.append(fd)

        # Populate the stub
        stub = self.registry.get(query.name)
        stub.fields = fields
        stub.interfaces = list(dict.fromkeys(interface_names))

        self.storage.save_metadata()

        return CreateResult(
            columns=["type", "fields"],
            rows=[{"type": query.name, "fields": len(fields)}],
            message=f"Created interface '{query.name}' with {len(fields)} field(s)",
            type_name=query.name,
        )

    def _execute_create_type(self, query: CreateTypeQuery) -> CreateResult:
        """Execute CREATE TYPE query.

        Supports self-referential types and populating forward-declared stubs:
        - `forward type B` then `create type B a:A` → populates the stub
        - `create type Node children:Node[]` → self-referential (stub registered first)
        """
        if query.name.startswith("_"):
            return CreateResult(
                columns=[], rows=[],
                message="Type names starting with '_' are reserved for system use",
                type_name=query.name,
            )
        existing = self.registry.get(query.name)

        if existing is not None:
            # Type already exists
            if isinstance(existing, CompositeTypeDefinition) and not existing.fields and query.fields:
                # Existing empty stub + query has fields → populate forward declaration
                pass  # fall through to field resolution below
            else:
                return CreateResult(
                    columns=[],
                    rows=[],
                    message=f"Type '{query.name}' already exists",
                    type_name=query.name,
                )

        # Register stub for self-reference support (idempotent if already exists)
        if existing is None:
            self.registry.register_stub(query.name)

        # Handle inheritance (supports multi-parent: at most 1 concrete parent + any number of interfaces)
        parents = query.parents
        parent_fields: list[FieldDefinition] = []
        interface_names: list[str] = []
        explicitly_declared: list[str] = []  # Only interfaces from the from clause
        concrete_parent: str | None = None

        for parent_name in parents:
            if parent_name == query.name:
                return CreateResult(
                    columns=[],
                    rows=[],
                    message=f"Circular inheritance: '{query.name}' cannot inherit from itself",
                    type_name=query.name,
                )

            parent_type = self.registry.get(parent_name)
            if parent_type is None:
                return CreateResult(
                    columns=[],
                    rows=[],
                    message=f"Unknown parent type: {parent_name}",
                    type_name=query.name,
                )

            parent_base = parent_type.resolve_base_type()

            if isinstance(parent_base, InterfaceTypeDefinition):
                # Interface parent: merge fields with conflict detection
                interface_names.append(parent_name)
                explicitly_declared.append(parent_name)
                for f in parent_base.fields:
                    existing_field = next((pf for pf in parent_fields if pf.name == f.name), None)
                    if existing_field is not None:
                        # Same name: must be same type (diamond merge)
                        if existing_field.type_def.name != f.type_def.name:
                            return CreateResult(
                                columns=[],
                                rows=[],
                                message=f"Field conflict: '{f.name}' has type '{existing_field.type_def.name}' from one parent but '{f.type_def.name}' from '{parent_name}'",
                                type_name=query.name,
                            )
                        # Same name + same type → merge (skip duplicate)
                    else:
                        parent_fields.append(f)
            elif isinstance(parent_base, CompositeTypeDefinition):
                concrete_parent = parent_name
                # Concrete parent: only one allowed
                if any(
                    isinstance(self.registry.get(p).resolve_base_type(), CompositeTypeDefinition)
                    for p in parents
                    if p != parent_name and self.registry.get(p) is not None
                    and not isinstance(self.registry.get(p).resolve_base_type(), InterfaceTypeDefinition)
                ):
                    return CreateResult(
                        columns=[],
                        rows=[],
                        message=f"At most one concrete parent allowed; '{parent_name}' is a concrete type",
                        type_name=query.name,
                    )
                # Copy concrete parent fields, with merge for interface fields already present
                for f in parent_base.fields:
                    existing_field = next((pf for pf in parent_fields if pf.name == f.name), None)
                    if existing_field is not None:
                        if existing_field.type_def.name != f.type_def.name:
                            return CreateResult(
                                columns=[],
                                rows=[],
                                message=f"Field conflict: '{f.name}' has type '{existing_field.type_def.name}' but concrete parent '{parent_name}' has '{f.type_def.name}'",
                                type_name=query.name,
                            )
                    else:
                        parent_fields.append(f)
                # Also inherit parent's interface list
                interface_names.extend(parent_base.interfaces)
            else:
                return CreateResult(
                    columns=[],
                    rows=[],
                    message=f"Cannot inherit from type: {parent_name}",
                    type_name=query.name,
                )

        # Build field definitions from query
        fields: list[FieldDefinition] = parent_fields.copy()
        for field_def in query.fields:
            field_type = self._resolve_type_spec(field_def.type_name)
            if field_type is None:
                type_str = self._type_spec_to_string(field_def.type_name)
                return CreateResult(
                    columns=[],
                    rows=[],
                    message=f"Unknown type: {type_str}",
                    type_name=query.name,
                )

            fd = FieldDefinition(name=field_def.name, type_def=field_type)
            if field_def.overflow is not None:
                err = self._validate_overflow_modifier(field_def.overflow, field_type, field_def.name)
                if err:
                    return CreateResult(columns=[], rows=[], message=err, type_name=query.name)
                fd.overflow = field_def.overflow
            if field_def.default_value is not None:
                try:
                    fd.default_value = self._resolve_default_value(field_def.default_value, field_type)
                except ValueError as e:
                    return CreateResult(
                        columns=[],
                        rows=[],
                        message=f"Invalid default for field '{field_def.name}': {e}",
                        type_name=query.name,
                    )
            fields.append(fd)

        # Mutate the stub in-place (Python object references propagate automatically)
        stub = self.registry.get(query.name)
        stub.fields = fields
        # Deduplicate interface names
        stub.interfaces = list(dict.fromkeys(interface_names))
        stub.declared_interfaces = list(dict.fromkeys(explicitly_declared))
        stub.parent = concrete_parent

        # Save updated metadata
        self.storage.save_metadata()

        return CreateResult(
            columns=["type", "fields"],
            rows=[{"type": query.name, "fields": len(fields)}],
            message=f"Created type '{query.name}' with {len(fields)} field(s)",
            type_name=query.name,
        )

    def _execute_forward_type(self, query: ForwardTypeQuery) -> CreateResult:
        """Execute FORWARD TYPE query - register a stub for forward references."""
        if query.name.startswith("_"):
            return CreateResult(
                columns=[], rows=[],
                message="Type names starting with '_' are reserved for system use",
                type_name=query.name,
            )
        existing = self.registry.get(query.name)

        if existing is not None:
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Type '{query.name}' already exists",
                type_name=query.name,
            )

        self.registry.register_stub(query.name)
        self.storage.save_metadata()

        return CreateResult(
            columns=["type", "fields"],
            rows=[{"type": query.name, "fields": 0}],
            message=f"Forward declared type '{query.name}'",
            type_name=query.name,
        )

    def _execute_create_enum(self, query: CreateEnumQuery) -> CreateResult:
        """Execute CREATE ENUM query."""
        if query.name.startswith("_"):
            return CreateResult(
                columns=[], rows=[],
                message="Type names starting with '_' are reserved for system use",
                type_name=query.name,
            )
        existing = self.registry.get(query.name)
        if existing is not None:
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Type '{query.name}' already exists",
                type_name=query.name,
            )

        # Validate: no mixing of explicit values and associated values
        has_explicit = any(v.explicit_value is not None for v in query.variants)
        has_fields = any(v.fields is not None and len(v.fields) > 0 for v in query.variants)
        if has_explicit and has_fields:
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Enum '{query.name}': explicit discriminant values and associated values cannot coexist",
                type_name=query.name,
            )

        # Build variants
        from typed_tables.types import EnumVariantDefinition as EVD

        variants: list[EVD] = []
        auto_disc = 0
        for vspec in query.variants:
            if vspec.explicit_value is not None:
                disc = vspec.explicit_value
                auto_disc = disc + 1
            else:
                disc = auto_disc
                auto_disc += 1

            fields: list[FieldDefinition] = []
            if vspec.fields:
                for fdef in vspec.fields:
                    field_type = self._resolve_type_spec(fdef.type_name)
                    if field_type is None:
                        type_str = self._type_spec_to_string(fdef.type_name)
                        return CreateResult(
                            columns=[],
                            rows=[],
                            message=f"Unknown type: {type_str}",
                            type_name=query.name,
                        )
                    fields.append(FieldDefinition(name=fdef.name, type_def=field_type))

            variants.append(EVD(name=vspec.name, discriminant=disc, fields=fields))

        # Validate and resolve backing type
        backing_prim = None
        if query.backing_type:
            prim = PRIMITIVE_TYPE_NAMES.get(query.backing_type)
            if prim is None or prim not in self._OVERFLOW_NUMERIC_TYPES:
                return CreateResult(
                    columns=[], rows=[],
                    message=f"Enum backing type must be an integer type, got '{query.backing_type}'",
                    type_name=query.name,
                )
            backing_prim = prim
            # Validate discriminants fit the backing type
            min_val, max_val = type_range(prim)
            for v in variants:
                if v.discriminant < min_val or v.discriminant > max_val:
                    return CreateResult(
                        columns=[], rows=[],
                        message=f"Discriminant {v.discriminant} for variant '{v.name}' out of range for {query.backing_type}",
                        type_name=query.name,
                    )

        # Create and register the enum type
        enum_def = EnumTypeDefinition(
            name=query.name,
            variants=variants,
            has_explicit_values=has_explicit,
            backing_type=backing_prim,
        )
        self.registry.register(enum_def)
        self.storage.save_metadata()

        return CreateResult(
            columns=["type", "variants"],
            rows=[{"type": query.name, "variants": len(variants)}],
            message=f"Created enum '{query.name}' with {len(variants)} variant(s)",
            type_name=query.name,
        )

    def _execute_create_instance(self, query: CreateInstanceQuery) -> CreateResult:
        """Execute CREATE instance query."""
        # Check if tag is used outside a scope
        if query.tag and not self._in_scope():
            return CreateResult(
                columns=[],
                rows=[],
                message="Tags can only be used within a scope block",
                type_name=query.type_name,
            )

        type_def = self.registry.get(query.type_name)
        if type_def is None:
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Unknown type: {query.type_name}",
                type_name=query.type_name,
            )

        base = type_def.resolve_base_type()
        if isinstance(base, InterfaceTypeDefinition):
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Cannot create instance of interface type: {query.type_name}",
                type_name=query.type_name,
            )
        if not isinstance(base, CompositeTypeDefinition):
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Cannot create instance of non-composite type: {query.type_name}",
                type_name=query.type_name,
            )

        # Build values dict from field values
        values: dict[str, Any] = {}
        for field_val in query.fields:
            value = self._resolve_instance_value(field_val.value)
            # For interface-typed fields, wrap the index with the concrete type_id
            field_info = base.get_field(field_val.name)
            if field_info is not None and isinstance(field_info.type_def.resolve_base_type(), InterfaceTypeDefinition):
                if value is not None and isinstance(value, int):
                    concrete_type_name = self._infer_concrete_type_name(field_val.value)
                    if concrete_type_name:
                        type_id = self.registry.get_type_id(concrete_type_name)
                        value = (type_id, value)
            values[field_val.name] = value

        # Default missing fields to their default value (None if no default specified)
        for field in base.fields:
            if field.name not in values:
                values[field.name] = field.default_value

        # Create the instance using storage manager
        try:
            index = self._create_instance(type_def, base, values)

            # Register tag if present (must be in a scope)
            if query.tag:
                self._define_tag(query.tag, query.type_name, index)

            return CreateResult(
                columns=["type", "index"],
                rows=[{"type": query.type_name, "index": index}],
                message=f"Created {query.type_name} at index {index}",
                type_name=query.type_name,
                index=index,
            )
        except Exception as e:
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Failed to create instance: {e}",
                type_name=query.type_name,
            )

    def _execute_variable_assignment(self, query: VariableAssignmentQuery) -> VariableAssignmentResult:
        """Execute a variable assignment: $var = create Type(...)."""
        if self._lookup_variable(query.var_name) is not None:
            return VariableAssignmentResult(
                columns=[],
                rows=[],
                message=f"Variable '${query.var_name}' is already bound (immutable)",
            )
        result = self._execute_create_instance(query.create_query)
        if result.index is not None:
            self._define_variable(query.var_name, query.create_query.type_name, result.index)
        return VariableAssignmentResult(
            columns=result.columns,
            rows=result.rows,
            message=result.message,
            var_name=query.var_name,
            type_name=query.create_query.type_name,
            index=result.index,
        )

    def _execute_collect(self, query: CollectQuery) -> CollectResult:
        """Execute a collect query: $var = collect source1 [where ...], source2 [where ...] [group by ...] [sort by ...] [offset N] [limit M]."""
        if self._lookup_variable(query.var_name) is not None:
            return CollectResult(
                columns=[],
                rows=[],
                message=f"Variable '${query.var_name}' is already bound (immutable)",
            )

        # Resolve all sources, enforce same-type constraint, union records
        resolved_type_name: str | None = None
        all_records: list[dict[str, Any]] = []
        seen_indices: set[int] = set()

        for source in query.sources:
            if source.variable:
                # Variable source
                var_binding = self._lookup_variable(source.variable)
                if var_binding is None:
                    return CollectResult(
                        columns=[], rows=[],
                        message=f"Undefined variable: ${source.variable}",
                    )
                src_type_name, ref = var_binding
                src_type_def = self.registry.get(src_type_name)
                if src_type_def is None:
                    return CollectResult(
                        columns=[], rows=[],
                        message=f"Unknown type: {src_type_name}",
                    )
                if isinstance(ref, list):
                    source_records = list(self._load_records_by_indices(src_type_name, src_type_def, ref))
                else:
                    source_records = list(self._load_records_by_indices(src_type_name, src_type_def, [ref]))
            else:
                # Table source
                src_type_name = source.table
                src_type_def = self.registry.get(src_type_name)
                if src_type_def is None:
                    return CollectResult(
                        columns=[], rows=[],
                        message=f"Unknown type: {src_type_name}",
                    )
                source_records = list(self._load_all_records(src_type_name, src_type_def))

            # Enforce same-type constraint
            if resolved_type_name is None:
                resolved_type_name = src_type_name
            elif src_type_name != resolved_type_name:
                return CollectResult(
                    columns=[], rows=[],
                    message=f"Type mismatch in collect: '{resolved_type_name}' vs '{src_type_name}'. All sources must be the same type.",
                )

            # Apply per-source WHERE filter
            if source.where:
                source_records = [r for r in source_records if self._evaluate_condition(r, source.where)]

            # Union with deduplication
            for r in source_records:
                idx = r["_index"]
                if idx not in seen_indices:
                    seen_indices.add(idx)
                    all_records.append(r)

        if resolved_type_name is None:
            resolved_type_name = "unknown"

        records = all_records

        # Apply post-union GROUP BY
        if query.group_by:
            records = self._apply_group_by(records, query.group_by)

        # Apply post-union SORT BY
        if query.sort_by:
            records = self._apply_sort_by(records, query.sort_by)

        # Apply OFFSET and LIMIT
        if query.offset:
            records = records[query.offset:]
        if query.limit is not None:
            records = records[:query.limit]

        # Extract indices
        indices = [r["_index"] for r in records]

        # Store in variables (scope-aware)
        self._define_variable(query.var_name, resolved_type_name, indices)

        return CollectResult(
            columns=["variable", "type", "count"],
            rows=[{"variable": f"${query.var_name}", "type": resolved_type_name, "count": len(indices)}],
            message=f"Collected {len(indices)} {resolved_type_name} record(s) into ${query.var_name}",
            var_name=query.var_name,
            type_name=resolved_type_name,
            count=len(indices),
        )

    def _execute_update(self, query: UpdateQuery) -> UpdateResult:
        """Execute UPDATE query — modify fields on an existing record or bulk update."""
        # Resolve target: variable, direct Type(index), or bulk Type
        if query.var_name:
            var_binding = self._lookup_variable(query.var_name)
            if var_binding is None:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"Undefined variable: ${query.var_name}",
                )
            type_name, ref = var_binding
            if isinstance(ref, list):
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"Cannot update a set variable ${query.var_name} (contains multiple records)",
                )
            index = ref
        elif query.index is not None:
            type_name = query.type_name
            index = query.index
        else:
            # Bulk update: UPDATE Type SET ... [WHERE ...]
            return self._execute_bulk_update(query)

        type_def = self.registry.get(type_name)
        if type_def is None:
            return UpdateResult(
                columns=[], rows=[],
                message=f"Unknown type: {type_name}",
            )

        base = type_def.resolve_base_type()
        if not isinstance(base, CompositeTypeDefinition):
            return UpdateResult(
                columns=[], rows=[],
                message=f"Cannot update non-composite type: {type_name}",
            )

        table = self.storage.get_table(type_name)
        if index < 0 or index >= table.count:
            return UpdateResult(
                columns=[], rows=[],
                message=f"Index {index} out of range for {type_name}",
            )
        if table.is_deleted(index):
            return UpdateResult(
                columns=[], rows=[],
                message=f"{type_name}[{index}] has been deleted",
            )

        error = self._apply_update_fields(type_name, base, table, index, query.fields)
        if error:
            return error

        return UpdateResult(
            columns=["type", "index"],
            rows=[{"type": type_name, "index": index}],
            message=f"Updated {type_name}[{index}]",
            type_name=type_name,
            index=index,
        )

    def _apply_update_fields(
        self,
        type_name: str,
        base: CompositeTypeDefinition,
        table: Any,
        index: int,
        fields: list,
    ) -> UpdateResult | None:
        """Apply SET field assignments to a single record. Returns an error UpdateResult or None on success."""
        raw_record = table.get(index)

        for fv in fields:
            field_def = base.get_field(fv.name)
            if field_def is None:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"Unknown field '{fv.name}' on type {type_name}",
                )

            # Chain mutation (no =): readings.sort().reverse()
            if fv.method_chain is not None and fv.value is None:
                error = self._apply_chain_mutation(type_name, base, fv, raw_record)
                if error:
                    return error
                continue

            # Chain/single-method assignment: readings = readings.sort().reverse()
            if fv.method_name is not None and fv.value is not None:
                error = self._apply_chain_assignment(type_name, base, fv, raw_record)
                if error:
                    return error
                continue

            if fv.method_chain is not None and fv.value is not None:
                error = self._apply_chain_assignment(type_name, base, fv, raw_record)
                if error:
                    return error
                continue

            # Single mutation (no =): readings.reverse()
            if fv.method_name is not None:
                error = self._apply_array_mutation(type_name, base, fv, raw_record)
                if error:
                    return error
                continue

            field_base = field_def.type_def.resolve_base_type()

            # Detect EnumValueExpr that's actually a method chain assignment
            # (for IDENTIFIER methods with no args: readings = readings.reverse())
            if (isinstance(fv.value, EnumValueExpr) and fv.value.enum_name is not None
                    and isinstance(field_base, (ArrayTypeDefinition, DictionaryTypeDefinition))):
                enum_type = self.registry.get(fv.value.enum_name)
                if enum_type is None or not isinstance(enum_type.resolve_base_type(), EnumTypeDefinition):
                    # Not an actual enum — reinterpret as method chain
                    source_field = fv.value.enum_name
                    method_name = fv.value.variant_name
                    method_args = fv.value.args
                    synthetic_fv = FieldValue(
                        name=fv.name, value=source_field,
                        method_name=method_name, method_args=method_args)
                    error = self._apply_chain_assignment(type_name, base, synthetic_fv, raw_record)
                    if error:
                        return error
                    continue

            resolved_value = self._resolve_instance_value(fv.value)

            # Resolve shorthand enum expressions for update context
            if isinstance(resolved_value, EnumValueExpr) and isinstance(field_base, EnumTypeDefinition):
                resolved_value = self._resolve_enum_value_expr(resolved_value, field_def.type_def.name)

            if resolved_value is None:
                raw_record[fv.name] = None
            elif isinstance(field_base, EnumTypeDefinition):
                raw_record[fv.name] = resolved_value
            elif isinstance(field_base, DictionaryTypeDefinition):
                if isinstance(resolved_value, (EmptyBraces, DictLiteral)):
                    # Re-use _create_instance logic by wrapping in a temp values dict
                    temp_values = {fv.name: resolved_value}
                    temp_refs: dict[str, Any] = {}
                    # Inline the dict creation logic
                    if isinstance(resolved_value, EmptyBraces) or (isinstance(resolved_value, DictLiteral) and not resolved_value.entries):
                        array_table = self.storage.get_array_table_for_type(field_def.type_def)
                        raw_record[fv.name] = array_table.insert([])
                    else:
                        entries = resolved_value.entries
                        keys_seen = []
                        entry_indices = []
                        entry_type = field_base.entry_type
                        entry_base = entry_type.resolve_base_type()
                        for entry in entries:
                            key = entry.key
                            key_check = key
                            if key_check in keys_seen:
                                return UpdateResult(
                                    columns=[], rows=[],
                                    message=f"Duplicate key in dictionary for field '{fv.name}': {key_check!r}",
                                )
                            keys_seen.append(key_check)
                            entry_values = {"key": key, "value": entry.value}
                            entry_index = self._create_instance(entry_type, entry_base, entry_values)
                            entry_indices.append(entry_index)
                        array_table = self.storage.get_array_table_for_type(field_def.type_def)
                        raw_record[fv.name] = array_table.insert(entry_indices)
                else:
                    return UpdateResult(
                        columns=[], rows=[],
                        message=f"Expected dict literal for field '{fv.name}'",
                    )
            elif isinstance(field_base, SetTypeDefinition):
                if isinstance(resolved_value, (EmptyBraces, SetLiteral)):
                    if isinstance(resolved_value, EmptyBraces) or (isinstance(resolved_value, SetLiteral) and not resolved_value.elements):
                        array_table = self.storage.get_array_table_for_type(field_def.type_def)
                        raw_record[fv.name] = array_table.insert([])
                    else:
                        elements = resolved_value.elements
                        seen = []
                        for elem in elements:
                            if elem in seen:
                                return UpdateResult(
                                    columns=[], rows=[],
                                    message=f"Duplicate element in set for field '{fv.name}': {elem!r}",
                                )
                            seen.append(elem)
                        if is_string_type(field_base.element_type):
                            char_table = self.storage.get_array_table_for_type(field_base.element_type)
                            elements = [char_table.insert(list(e) if isinstance(e, str) else e) for e in elements]
                        array_table = self.storage.get_array_table_for_type(field_def.type_def)
                        raw_record[fv.name] = array_table.insert(elements)
                elif isinstance(resolved_value, list):
                    # Plain list — treat as set elements
                    seen = []
                    for elem in resolved_value:
                        if elem in seen:
                            return UpdateResult(
                                columns=[], rows=[],
                                message=f"Duplicate element in set for field '{fv.name}': {elem!r}",
                            )
                        seen.append(elem)
                    if is_string_type(field_base.element_type):
                        char_table = self.storage.get_array_table_for_type(field_base.element_type)
                        resolved_value = [char_table.insert(list(e) if isinstance(e, str) else e) for e in resolved_value]
                    array_table = self.storage.get_array_table_for_type(field_def.type_def)
                    raw_record[fv.name] = array_table.insert(resolved_value)
                elif isinstance(resolved_value, str):
                    resolved_value = list(resolved_value)
                    array_table = self.storage.get_array_table_for_type(field_def.type_def)
                    raw_record[fv.name] = array_table.insert(resolved_value)
                else:
                    return UpdateResult(
                        columns=[], rows=[],
                        message=f"Expected set literal for field '{fv.name}'",
                    )
            elif isinstance(field_base, FractionTypeDefinition):
                if isinstance(resolved_value, Fraction):
                    raw_record[fv.name] = _fraction_encode(resolved_value, self.storage)
                elif isinstance(resolved_value, (int, BigInt, BigUInt)):
                    raw_record[fv.name] = _fraction_encode(Fraction(int(resolved_value)), self.storage)
                else:
                    return UpdateResult(
                        columns=[], rows=[],
                        message=f"Expected fraction value for field '{fv.name}'",
                    )
            elif isinstance(field_base, (BigIntTypeDefinition, BigUIntTypeDefinition)):
                val = int(resolved_value)
                signed = isinstance(field_base, BigIntTypeDefinition)
                if not signed and val < 0:
                    return UpdateResult(
                        columns=[], rows=[],
                        message=f"biguint field '{fv.name}' cannot store negative value: {val}",
                    )
                if val == 0:
                    byte_list = [0]
                elif signed:
                    byte_length = (val.bit_length() + 8) // 8
                    byte_list = list(val.to_bytes(byte_length, byteorder='little', signed=True))
                else:
                    byte_length = (val.bit_length() + 7) // 8
                    byte_list = list(val.to_bytes(byte_length, byteorder='little', signed=False))
                array_table = self.storage.get_array_table_for_type(field_def.type_def)
                raw_record[fv.name] = array_table.insert(byte_list)
            elif isinstance(field_base, ArrayTypeDefinition):
                if isinstance(resolved_value, str):
                    resolved_value = list(resolved_value)
                array_table = self.storage.get_array_table_for_type(field_def.type_def)
                raw_record[fv.name] = array_table.insert(resolved_value)
            elif isinstance(field_base, CompositeTypeDefinition):
                if isinstance(resolved_value, int):
                    raw_record[fv.name] = resolved_value
                else:
                    nested_index = self._create_instance(field_def.type_def, field_base, resolved_value)
                    raw_record[fv.name] = nested_index
            else:
                raw_record[fv.name] = resolved_value

        table.update(index, raw_record)
        return None

    def _apply_chain_mutation(
        self,
        type_name: str,
        base: CompositeTypeDefinition,
        fv: Any,
        raw_record: dict,
    ) -> UpdateResult | None:
        """Apply a chain of mutations (no =) like readings.sort().reverse(). Read once, apply all, write once."""
        field_def = base.get_field(fv.name)
        field_base = field_def.type_def.resolve_base_type()

        if not isinstance(field_base, (ArrayTypeDefinition, DictionaryTypeDefinition)):
            return UpdateResult(
                columns=[], rows=[],
                message=f"Method chaining can only be applied to array or collection fields",
            )

        # Read current array value
        value = self._read_array_for_chain(raw_record.get(fv.name), field_def)

        # Apply each method in the chain as an immutable projection
        for mc in fv.method_chain:
            value = self._apply_projection_method(value, mc.method_name, mc.method_args)

        # Write result back
        self._write_chain_result(raw_record, fv.name, field_def, value)
        return None

    def _apply_chain_assignment(
        self,
        type_name: str,
        base: CompositeTypeDefinition,
        fv: Any,
        raw_record: dict,
    ) -> UpdateResult | None:
        """Apply a chain assignment like readings = readings.sort().reverse(). Read source, apply chain, write to target."""
        field_def = base.get_field(fv.name)
        field_base = field_def.type_def.resolve_base_type()

        if not isinstance(field_base, (ArrayTypeDefinition, DictionaryTypeDefinition)):
            return UpdateResult(
                columns=[], rows=[],
                message=f"Method chain assignment can only target array or collection fields",
            )

        source_field = fv.value  # RHS field name string
        source_field_def = base.get_field(source_field)
        if source_field_def is None:
            return UpdateResult(
                columns=[], rows=[],
                message=f"Unknown source field '{source_field}' on type {type_name}",
            )

        source_field_base = source_field_def.type_def.resolve_base_type()
        if not isinstance(source_field_base, (ArrayTypeDefinition, DictionaryTypeDefinition)):
            return UpdateResult(
                columns=[], rows=[],
                message=f"Source field '{source_field}' is not an array or collection type",
            )

        # Read source array value
        value = self._read_array_for_chain(raw_record.get(source_field), source_field_def)

        # Apply chain
        if fv.method_chain:
            for mc in fv.method_chain:
                value = self._apply_projection_method(value, mc.method_name, mc.method_args)
        else:
            value = self._apply_projection_method(value, fv.method_name, fv.method_args)

        # Write result to target field
        self._write_chain_result(raw_record, fv.name, field_def, value)
        return None

    def _read_array_for_chain(self, ref: Any, field_def: Any) -> Any:
        """Read an array/set/dict field value from raw_record ref, returning resolved Python values."""
        if ref is None:
            return None
        field_base = field_def.type_def.resolve_base_type()

        # Dictionary: load entry indices → resolve to Python dict
        if isinstance(field_base, DictionaryTypeDefinition):
            start_index, length = ref
            if length == 0:
                return {}
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            entry_indices = [
                array_table.element_table.get(start_index + j)
                for j in range(length)
            ]
            entry_type = field_base.entry_type
            entry_table = self.storage.get_table(entry_type.name)
            result = {}
            for entry_idx in entry_indices:
                raw_entry = entry_table.get(entry_idx)
                resolved_entry = self._resolve_raw_composite(raw_entry, entry_type, entry_type)
                result[resolved_entry["key"]] = resolved_entry["value"]
            return result

        start_index, length = ref
        if length == 0:
            if isinstance(field_base, SetTypeDefinition):
                return SetValue()
            return []

        array_table = self.storage.get_array_table_for_type(field_def.type_def)
        elements = [
            array_table.element_table.get(start_index + j)
            for j in range(length)
        ]
        if is_string_type(field_def.type_def):
            return "".join(elements)

        # String array (string[]) — resolve each (start, length) to a string
        if is_string_type(field_base.element_type):
            char_table = self.storage.get_array_table_for_type(field_base.element_type)
            str_elements = []
            for elem in elements:
                if isinstance(elem, tuple):
                    cs, cl = elem
                    chars = char_table.get(cs, cl) if cl > 0 else []
                    str_elements.append("".join(chars))
                else:
                    str_elements.append(elem)
            return str_elements

        # Set: resolve string elements, wrap in SetValue
        if isinstance(field_base, SetTypeDefinition):
            elem_type = field_base.element_type
            if is_string_type(elem_type):
                # Each element is (start, length) tuple for a string
                char_table = self.storage.get_array_table_for_type(elem_type)
                resolved = []
                for elem in elements:
                    if isinstance(elem, tuple):
                        s_start, s_len = elem
                        chars = char_table.get(s_start, s_len) if s_len > 0 else []
                        resolved.append("".join(chars))
                    else:
                        resolved.append(elem)
                return SetValue(resolved)
            return SetValue(elements)

        # For composite arrays, resolve each element to a dict
        elem_type = field_base.element_type
        elem_base = elem_type.resolve_base_type()
        if isinstance(elem_base, CompositeTypeDefinition):
            resolved = []
            for elem in elements:
                if isinstance(elem, int):
                    ref_table = self.storage.get_table(elem_type.name)
                    raw = ref_table.get(elem)
                    resolved.append(self._resolve_raw_composite(raw, elem_base, elem_type))
                else:
                    resolved.append(elem)
            return resolved
        return elements

    def _write_chain_result(self, raw_record: dict, field_name: str, field_def: Any, value: Any) -> None:
        """Write the result of a chain back to the raw_record."""
        if value is None:
            raw_record[field_name] = None
            return
        field_base = field_def.type_def.resolve_base_type()

        # Dictionary: convert Python dict to entry composites and insert indices
        if isinstance(value, dict) and isinstance(field_base, DictionaryTypeDefinition):
            entry_type = field_base.entry_type
            entry_base = entry_type.resolve_base_type()
            entry_indices = []
            for k, v in value.items():
                entry_values = {"key": k, "value": v}
                entry_index = self._create_instance(entry_type, entry_base, entry_values)
                entry_indices.append(entry_index)
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            raw_record[field_name] = array_table.insert(entry_indices)
            return

        # SetValue: unwrap and write as array (handle string elements)
        if isinstance(value, SetValue):
            elements = list(value)
            if isinstance(field_base, SetTypeDefinition) and is_string_type(field_base.element_type):
                # String set: store each string in char table first
                char_table = self.storage.get_array_table_for_type(field_base.element_type)
                refs = []
                for s in elements:
                    if isinstance(s, str):
                        refs.append(char_table.insert(list(s)))
                    else:
                        refs.append(s)
                array_table = self.storage.get_array_table_for_type(field_def.type_def)
                raw_record[field_name] = array_table.insert(refs)
            else:
                array_table = self.storage.get_array_table_for_type(field_def.type_def)
                raw_record[field_name] = array_table.insert(elements)
            return

        if is_string_type(field_def.type_def):
            if isinstance(value, str):
                value = list(value)
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            raw_record[field_name] = array_table.insert(value)
        elif isinstance(value, list):
            # Check if elements are composite dicts that need to be written as records
            elem_type = field_base.element_type
            elem_base = elem_type.resolve_base_type()
            if isinstance(elem_base, CompositeTypeDefinition) and value and isinstance(value[0], dict):
                # Convert resolved dicts back to composite record indices
                ref_table = self.storage.get_table(elem_type.name)
                indices = []
                for elem in value:
                    if isinstance(elem, dict):
                        idx = self._create_instance(elem_type, elem_base, elem)
                        indices.append(idx)
                    else:
                        indices.append(elem)
                array_table = self.storage.get_array_table_for_type(field_def.type_def)
                raw_record[field_name] = array_table.insert(indices)
            else:
                array_table = self.storage.get_array_table_for_type(field_def.type_def)
                raw_record[field_name] = array_table.insert(value)
        else:
            # Shouldn't happen for array fields, but handle gracefully
            raw_record[field_name] = value

    def _apply_dict_mutation(
        self,
        type_name: str,
        base: CompositeTypeDefinition,
        fv: Any,
        raw_record: dict,
    ) -> UpdateResult | None:
        """Apply a dict mutation (e.g. remove()) to a field. Returns error UpdateResult or None."""
        field_def = base.get_field(fv.name)
        field_base = field_def.type_def.resolve_base_type()

        if fv.method_name == "remove":
            args = fv.method_args or []
            if len(args) != 1:
                return UpdateResult(
                    columns=[], rows=[],
                    message="remove() requires exactly 1 argument",
                )
            ref = raw_record.get(fv.name)
            if ref is None:
                return None  # no-op on null
            # Read as Python dict, remove key, write back
            value = self._read_array_for_chain(ref, field_def)
            key = self._resolve_instance_value(args[0]) if not isinstance(args[0], (int, float, str)) else args[0]
            new_dict = {k: v for k, v in value.items() if k != key}
            self._write_chain_result(raw_record, fv.name, field_def, new_dict)
            return None

        return UpdateResult(
            columns=[], rows=[],
            message=f"Unknown dict mutation method: {fv.method_name}()",
        )

    def _apply_set_mutation(
        self,
        type_name: str,
        base: CompositeTypeDefinition,
        fv: Any,
        raw_record: dict,
    ) -> UpdateResult | None:
        """Apply a set mutation (e.g. add(), union()) to a field. Returns error UpdateResult or None."""
        field_def = base.get_field(fv.name)
        field_base = field_def.type_def.resolve_base_type()
        set_methods = ("add", "union", "intersect", "difference", "symmetric_difference")

        if fv.method_name not in set_methods:
            return None  # Not a set-specific method, fall through

        ref = raw_record.get(fv.name)

        if fv.method_name == "add":
            args = fv.method_args or []
            if len(args) != 1:
                return UpdateResult(
                    columns=[], rows=[],
                    message="add() requires exactly 1 argument",
                )
            value = self._read_array_for_chain(ref, field_def)
            if value is None:
                value = SetValue()
            elem = self._resolve_instance_value(args[0]) if not isinstance(args[0], (int, float, str)) else args[0]
            if elem not in value:
                new_set = SetValue(list(value) + [elem])
            else:
                new_set = SetValue(list(value))
            self._write_chain_result(raw_record, fv.name, field_def, new_set)
            return None

        elif fv.method_name in ("union", "intersect", "difference", "symmetric_difference"):
            args = fv.method_args or []
            if len(args) != 1:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"{fv.method_name}() requires exactly 1 argument",
                )
            value = self._read_array_for_chain(ref, field_def)
            if value is None:
                value = SetValue()
            other = self._resolve_projection_arg(args[0])
            if not isinstance(other, (list, SetValue)):
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"{fv.method_name}() argument must be a set or list",
                )
            current = list(value)
            other_list = list(other)
            if fv.method_name == "union":
                result = list(current)
                for e in other_list:
                    if e not in result:
                        result.append(e)
            elif fv.method_name == "intersect":
                result = [e for e in current if e in other_list]
            elif fv.method_name == "difference":
                result = [e for e in current if e not in other_list]
            else:  # symmetric_difference
                result = [e for e in current if e not in other_list]
                result += [e for e in other_list if e not in current]
            self._write_chain_result(raw_record, fv.name, field_def, SetValue(result))
            return None

        return None

    def _apply_array_mutation(
        self,
        type_name: str,
        base: CompositeTypeDefinition,
        fv: Any,
        raw_record: dict,
    ) -> UpdateResult | None:
        """Apply an array mutation (e.g. reverse(), swap()) to a field. Returns error UpdateResult or None."""
        field_def = base.get_field(fv.name)
        field_base = field_def.type_def.resolve_base_type()

        if not isinstance(field_base, (ArrayTypeDefinition, DictionaryTypeDefinition)):
            return UpdateResult(
                columns=[], rows=[],
                message=f"Method '{fv.method_name}()' can only be applied to array fields",
            )

        # Dispatch to dict-specific mutations
        if isinstance(field_base, DictionaryTypeDefinition):
            return self._apply_dict_mutation(type_name, base, fv, raw_record)

        # Dispatch to set-specific mutations
        if isinstance(field_base, SetTypeDefinition):
            set_methods = ("add", "union", "intersect", "difference", "symmetric_difference")
            if fv.method_name in set_methods:
                return self._apply_set_mutation(type_name, base, fv, raw_record)

        # Dispatch to string-specific mutations
        if is_string_type(field_def.type_def) and fv.method_name in _STRING_MUTATION_METHODS:
            return self._apply_string_mutation(type_name, base, fv, raw_record)

        ref = raw_record.get(fv.name)

        if fv.method_name == "reverse":
            if ref is None:
                return None  # no-op on null
            start_index, length = ref
            if length <= 1:
                return None  # no-op on empty or single-element
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            elements = array_table.get(start_index, length)
            elements.reverse()
            array_table.update_in_place(start_index, length, elements)
            return None

        elif fv.method_name == "append":
            args = fv.method_args or []
            if not args:
                return UpdateResult(
                    columns=[], rows=[],
                    message="append() requires at least 1 argument",
                )
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            elements = self._resolve_append_elements(args, field_base)
            if ref is None:
                # Null array → create new array
                raw_record[fv.name] = array_table.insert(elements)
            else:
                start_index, length = ref
                raw_record[fv.name] = array_table.append(start_index, length, elements)
            return None

        elif fv.method_name == "prepend":
            args = fv.method_args or []
            if not args:
                return UpdateResult(
                    columns=[], rows=[],
                    message="prepend() requires at least 1 argument",
                )
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            elements = self._resolve_append_elements(args, field_base)
            if ref is None:
                # Null array → create new array
                raw_record[fv.name] = array_table.insert(elements)
            else:
                start_index, length = ref
                raw_record[fv.name] = array_table.prepend(start_index, length, elements)
            return None

        elif fv.method_name == "insert":
            args = fv.method_args or []
            if len(args) < 2:
                return UpdateResult(
                    columns=[], rows=[],
                    message="insert() requires at least 2 arguments: insert(index, value, ...)",
                )
            index_arg = self._resolve_instance_value(args[0])
            if not isinstance(index_arg, int):
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"insert() first argument must be an integer index, got {type(index_arg).__name__}",
                )
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            insert_elements = self._resolve_append_elements(args[1:], field_base)
            if ref is None:
                if index_arg == 0:
                    raw_record[fv.name] = array_table.insert(insert_elements)
                else:
                    return UpdateResult(
                        columns=[], rows=[],
                        message=f"Cannot insert at index {index_arg} into null array (only index 0 allowed)",
                    )
            else:
                start_index, length = ref
                if index_arg < 0 or index_arg > length:
                    return UpdateResult(
                        columns=[], rows=[],
                        message=f"insert() index {index_arg} out of range for array of length {length}",
                    )
                existing = array_table.get(start_index, length)
                spliced = existing[:index_arg] + insert_elements + existing[index_arg:]
                raw_record[fv.name] = array_table.insert(spliced)
            return None

        elif fv.method_name == "delete":
            args = fv.method_args or []
            if not args:
                return UpdateResult(
                    columns=[], rows=[],
                    message="delete() requires at least 1 argument",
                )
            if ref is None:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"Cannot apply delete() to null array field '{fv.name}'",
                )
            start_index, length = ref
            indices_to_delete = set()
            for arg in args:
                idx = int(self._resolve_instance_value(arg))
                if idx < 0 or idx >= length:
                    return UpdateResult(
                        columns=[], rows=[],
                        message=f"delete() index {idx} out of range for array of length {length}",
                    )
                indices_to_delete.add(idx)
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            raw_record[fv.name] = array_table.delete(start_index, length, indices_to_delete)
            return None

        elif fv.method_name == "remove":
            args = fv.method_args or []
            if len(args) != 1:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"remove() requires exactly 1 argument, got {len(args)}",
                )
            if ref is None:
                return None  # no-op on null
            start_index, length = ref
            if length == 0:
                return None  # no-op on empty
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            elements = array_table.get(start_index, length)
            search_value = self._resolve_remove_search_value(args[0], field_base)
            for i, elem in enumerate(elements):
                if self._compare_array_element(elem, search_value, field_base):
                    raw_record[fv.name] = array_table.delete(start_index, length, {i})
                    return None
            return None  # not found → no-op

        elif fv.method_name == "removeAll":
            args = fv.method_args or []
            if len(args) != 1:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"removeAll() requires exactly 1 argument, got {len(args)}",
                )
            if ref is None:
                return None  # no-op on null
            start_index, length = ref
            if length == 0:
                return None  # no-op on empty
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            elements = array_table.get(start_index, length)
            search_value = self._resolve_remove_search_value(args[0], field_base)
            indices_to_delete = set()
            for i, elem in enumerate(elements):
                if self._compare_array_element(elem, search_value, field_base):
                    indices_to_delete.add(i)
            if indices_to_delete:
                raw_record[fv.name] = array_table.delete(start_index, length, indices_to_delete)
            return None  # no matches → no-op

        elif fv.method_name == "replace":
            args = fv.method_args or []
            if len(args) != 2:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"replace() requires exactly 2 arguments, got {len(args)}",
                )
            if ref is None:
                return None  # no-op on null
            start_index, length = ref
            if length == 0:
                return None  # no-op on empty
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            elements = array_table.get(start_index, length)
            search_value = self._resolve_remove_search_value(args[0], field_base)
            replacement = self._resolve_append_elements([args[1]], field_base)
            for i, elem in enumerate(elements):
                if self._compare_array_element(elem, search_value, field_base):
                    if len(replacement) == 1:
                        # Length-preserving: update in place
                        elements[i] = replacement[0]
                        array_table.update_in_place(start_index, length, elements)
                    else:
                        # Different length: copy-on-write
                        new_elements = elements[:i] + replacement + elements[i + 1:]
                        raw_record[fv.name] = array_table.insert(new_elements)
                    return None
            return None  # not found → no-op

        elif fv.method_name == "replaceAll":
            args = fv.method_args or []
            if len(args) != 2:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"replaceAll() requires exactly 2 arguments, got {len(args)}",
                )
            if ref is None:
                return None  # no-op on null
            start_index, length = ref
            if length == 0:
                return None  # no-op on empty
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            elements = array_table.get(start_index, length)
            search_value = self._resolve_remove_search_value(args[0], field_base)
            replacement = self._resolve_append_elements([args[1]], field_base)
            found = False
            if len(replacement) == 1:
                # Length-preserving: replace in place
                for i, elem in enumerate(elements):
                    if self._compare_array_element(elem, search_value, field_base):
                        elements[i] = replacement[0]
                        found = True
                if found:
                    array_table.update_in_place(start_index, length, elements)
            else:
                # Different length: build new list
                new_elements = []
                for elem in elements:
                    if self._compare_array_element(elem, search_value, field_base):
                        new_elements.extend(replacement)
                        found = True
                    else:
                        new_elements.append(elem)
                if found:
                    raw_record[fv.name] = array_table.insert(new_elements)
            return None  # no matches → no-op

        elif fv.method_name == "sort":
            if ref is None:
                return None  # no-op on null
            start_index, length = ref
            if length <= 1:
                return None  # no-op on empty or single-element
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            elements = array_table.get(start_index, length)

            args = fv.method_args or []
            elem_base = field_base.element_type.resolve_base_type()
            is_composite = isinstance(elem_base, CompositeTypeDefinition)

            sort_keys = self._parse_sort_keys(args, is_composite)
            if isinstance(sort_keys, UpdateResult):
                return sort_keys  # error

            elements = self._sort_array_elements(elements, sort_keys, is_composite, elem_base)
            array_table.update_in_place(start_index, length, elements)
            return None

        elif fv.method_name == "swap":
            args = fv.method_args or []
            if len(args) != 2:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"swap() requires exactly 2 arguments, got {len(args)}",
                )
            if ref is None:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"Cannot apply swap() to null array field '{fv.name}'",
                )
            start_index, length = ref
            i, j = int(args[0]), int(args[1])
            if i < 0 or i >= length:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"swap() index {i} out of range for array of length {length}",
                )
            if j < 0 or j >= length:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"swap() index {j} out of range for array of length {length}",
                )
            if i == j:
                return None  # no-op
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            elements = array_table.get(start_index, length)
            elements[i], elements[j] = elements[j], elements[i]
            array_table.update_in_place(start_index, length, elements)
            return None

        else:
            return UpdateResult(
                columns=[], rows=[],
                message=f"Unknown array mutation method: {fv.method_name}()",
            )

    def _apply_string_mutation(
        self,
        type_name: str,
        base: CompositeTypeDefinition,
        fv: Any,
        raw_record: dict,
    ) -> UpdateResult | None:
        """Apply a string-specific mutation. Reads chars, transforms as string, writes back."""
        field_def = base.get_field(fv.name)
        ref = raw_record.get(fv.name)

        if ref is None:
            return None  # no-op on null

        start_index, length = ref
        array_table = self.storage.get_array_table_for_type(field_def.type_def)
        chars = array_table.get(start_index, length) if length > 0 else []
        value = "".join(chars)

        args = fv.method_args or []
        resolved_args = [self._resolve_instance_value(a) for a in args]

        if fv.method_name == "uppercase":
            result = value.upper()
        elif fv.method_name == "lowercase":
            result = value.lower()
        elif fv.method_name == "capitalize":
            result = value.capitalize()
        elif fv.method_name == "trim":
            result = value.strip()
        elif fv.method_name == "trimStart":
            result = value.lstrip()
        elif fv.method_name == "trimEnd":
            result = value.rstrip()
        elif fv.method_name == "padStart":
            if len(resolved_args) < 1 or len(resolved_args) > 2:
                return UpdateResult(columns=[], rows=[], message="padStart() requires 1 or 2 arguments")
            pad_len = int(resolved_args[0])
            pad_char = str(resolved_args[1]) if len(resolved_args) > 1 else " "
            if len(pad_char) != 1:
                return UpdateResult(columns=[], rows=[], message="padStart() pad character must be a single character")
            result = value.rjust(pad_len, pad_char)
        elif fv.method_name == "padEnd":
            if len(resolved_args) < 1 or len(resolved_args) > 2:
                return UpdateResult(columns=[], rows=[], message="padEnd() requires 1 or 2 arguments")
            pad_len = int(resolved_args[0])
            pad_char = str(resolved_args[1]) if len(resolved_args) > 1 else " "
            if len(pad_char) != 1:
                return UpdateResult(columns=[], rows=[], message="padEnd() pad character must be a single character")
            result = value.ljust(pad_len, pad_char)
        elif fv.method_name == "repeat":
            if len(resolved_args) != 1:
                return UpdateResult(columns=[], rows=[], message="repeat() requires exactly 1 argument")
            n = int(resolved_args[0])
            if n < 0:
                return UpdateResult(columns=[], rows=[], message="repeat() count must be non-negative")
            result = value * n
        else:
            return UpdateResult(columns=[], rows=[], message=f"Unknown string mutation method: {fv.method_name}()")

        # Write back as char array
        new_chars = list(result)
        if len(new_chars) == length:
            # Same length — update in place
            array_table.update_in_place(start_index, length, new_chars)
        else:
            # Different length — insert new
            raw_record[fv.name] = array_table.insert(new_chars)
        return None

    def _resolve_append_elements(
        self,
        args: list[Any],
        array_type: ArrayTypeDefinition,
    ) -> list[Any]:
        """Resolve append() arguments into a flat list of elements for the array element table."""
        elem_base = array_type.element_type.resolve_base_type()
        is_composite_elem = isinstance(elem_base, CompositeTypeDefinition)
        elements: list[Any] = []

        for arg in args:
            resolved = self._resolve_instance_value(arg)

            if isinstance(resolved, list):
                # Array literal — flatten into individual elements
                if is_composite_elem:
                    for elem in resolved:
                        elements.append(self._resolve_composite_element(elem, array_type))
                else:
                    elements.extend(resolved)
            elif is_composite_elem:
                elements.append(self._resolve_composite_element(resolved, array_type))
            elif isinstance(resolved, str) and not is_composite_elem:
                # String arg on character array → flatten to char list
                elements.extend(list(resolved))
            else:
                elements.append(resolved)

        return elements

    def _resolve_composite_element(
        self,
        value: Any,
        array_type: ArrayTypeDefinition,
    ) -> dict[str, Any]:
        """Resolve a single composite element for array storage.

        For composite arrays, element table stores full serialized records (dicts),
        not indices. InlineInstances are resolved to field reference dicts.
        Integer references are read from the main table.
        """
        if isinstance(value, InlineInstance):
            inline_type = self.registry.get(value.type_name)
            inline_base = inline_type.resolve_base_type()
            inline_values = {}
            for fv in value.fields:
                inline_values[fv.name] = self._resolve_instance_value(fv.value)
            return self._build_field_references(inline_base, inline_values)
        elif isinstance(value, int):
            elem_type_name = array_type.element_type.name
            ref_table = self.storage.get_table(elem_type_name)
            return ref_table.get(value)
        elif isinstance(value, dict):
            return value
        else:
            raise ValueError(f"Cannot append value of type {type(value).__name__} to composite array")

    def _resolve_remove_search_value(self, arg: Any, array_type: ArrayTypeDefinition) -> Any:
        """Resolve a remove()/removeAll() argument to a comparable value."""
        resolved = self._resolve_instance_value(arg)
        elem_base = array_type.element_type.resolve_base_type()
        if isinstance(elem_base, CompositeTypeDefinition):
            return self._resolve_composite_element(resolved, array_type)
        return resolved

    def _compare_array_element(self, element: Any, search_value: Any, array_type: ArrayTypeDefinition) -> bool:
        """Compare an array element against a search value for remove/removeAll."""
        return element == search_value

    def _parse_sort_keys(
        self, args: list[Any], is_composite: bool
    ) -> list[tuple[str | None, bool]] | UpdateResult:
        """Parse sort() arguments into list of (field_name|None, descending) tuples.

        Returns list of tuples or UpdateResult on error.
        """
        if not args:
            return [(None, False)]  # default: ascending, no key

        keys: list[tuple[str | None, bool]] = []
        for arg in args:
            if isinstance(arg, SortKeyExpr):
                if arg.field_name is not None and not is_composite:
                    return UpdateResult(
                        columns=[], rows=[],
                        message=f"sort() field key '.{arg.field_name}' can only be used on composite arrays",
                    )
                keys.append((arg.field_name, arg.descending))
            elif isinstance(arg, EnumValueExpr) and arg.enum_name is None:
                # .field shorthand (parsed as EnumValueExpr with no enum_name)
                if not is_composite:
                    return UpdateResult(
                        columns=[], rows=[],
                        message=f"sort() field key '.{arg.variant_name}' can only be used on composite arrays",
                    )
                keys.append((arg.variant_name, False))
            else:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"sort() arguments must be sort keys (.field, desc, asc), got {type(arg).__name__}",
                )
        return keys

    def _sort_array_elements(
        self,
        elements: list[Any],
        sort_keys: list[tuple[str | None, bool]],
        is_composite: bool,
        elem_base: Any,
    ) -> list[Any]:
        """Sort elements using parsed sort keys.

        Primitive: direct comparison. Composite: extract field values.
        Null values sort last. Uses reverse-stable-sort trick for multi-key mixed directions.
        """
        for field_name, descending in reversed(sort_keys):
            def make_key(fn: str | None) -> Any:
                def key_fn(elem: Any) -> tuple:
                    if is_composite and fn is not None:
                        val = elem.get(fn) if isinstance(elem, dict) else None
                        # Resolve string fields from raw (start_index, length) tuples
                        if val is not None and isinstance(val, tuple) and len(val) == 2:
                            field_def = elem_base.get_field(fn)
                            if field_def and is_string_type(field_def.type_def):
                                arr_table = self.storage.get_array_table_for_type(field_def.type_def)
                                start, length = val
                                chars = arr_table.get(start, length)
                                val = "".join(chars)
                    else:
                        val = elem
                    if val is None:
                        return (1, "")
                    elif isinstance(val, (int, float)):
                        return (0, val)
                    elif isinstance(val, list):
                        return (0, "".join(str(c) for c in val))
                    else:
                        return (0, str(val))
                return key_fn

            elements.sort(key=make_key(field_name), reverse=descending)
        return elements

    def _execute_bulk_update(self, query: UpdateQuery) -> UpdateResult:
        """Execute bulk UPDATE: UPDATE Type SET ... [WHERE ...]."""
        type_name = query.type_name
        type_def = self.registry.get(type_name)
        if type_def is None:
            return UpdateResult(
                columns=[], rows=[],
                message=f"Unknown type: {type_name}",
            )

        base = type_def.resolve_base_type()
        if not isinstance(base, CompositeTypeDefinition):
            return UpdateResult(
                columns=[], rows=[],
                message=f"Cannot update non-composite type: {type_name}",
            )

        # Load all records with resolved values for condition evaluation
        records = list(self._load_all_records(type_name, type_def))

        if query.where:
            # Resolve enum value expressions in the WHERE condition
            self._resolve_condition_enum_values(query.where, base)
            matching = [r for r in records if self._evaluate_condition(r, query.where)]
        else:
            matching = records

        if not matching:
            return UpdateResult(
                columns=[], rows=[],
                message="No matching records to update",
            )

        # Validate fields exist before applying any updates
        for fv in query.fields:
            if base.get_field(fv.name) is None:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"Unknown field '{fv.name}' on type {type_name}",
                )

        table = self.storage.get_table(type_name)
        count = 0
        for record in matching:
            index = record["_index"]
            error = self._apply_update_fields(type_name, base, table, index, query.fields)
            if error:
                return error
            count += 1

        return UpdateResult(
            columns=["updated"],
            rows=[{"updated": count}],
            message=f"Updated {count} record(s) in {type_name}",
        )

    def _resolve_condition_enum_values(
        self, condition: Any, base: CompositeTypeDefinition,
    ) -> None:
        """Resolve EnumValueExpr values in a condition tree to EnumValue objects."""
        if isinstance(condition, CompoundCondition):
            self._resolve_condition_enum_values(condition.left, base)
            self._resolve_condition_enum_values(condition.right, base)
            return

        if not isinstance(condition, Condition):
            return

        if isinstance(condition.value, EnumValueExpr):
            expr = condition.value
            # Determine the enum type from the field
            enum_base = None
            if expr.enum_name is None:
                field_def = base.get_field(condition.field)
                if field_def is not None:
                    field_base = field_def.type_def.resolve_base_type()
                    if isinstance(field_base, EnumTypeDefinition):
                        resolved = self._resolve_enum_value_expr(expr, field_def.type_def.name)
                        # Swift-style returns (disc, index) — resolve to EnumValue
                        if isinstance(resolved, tuple):
                            resolved = self._resolve_swift_enum_ref(resolved, field_base)
                        condition.value = resolved
                        enum_base = field_base
            else:
                resolved = self._resolve_enum_value_expr(expr)
                enum_type = self.registry.get(expr.enum_name)
                if enum_type is not None:
                    enum_base = enum_type.resolve_base_type()
                    if isinstance(enum_base, EnumTypeDefinition) and isinstance(resolved, tuple):
                        resolved = self._resolve_swift_enum_ref(resolved, enum_base)
                condition.value = resolved
            # Resolve array fields so condition values match loaded records
            if isinstance(condition.value, EnumValue) and isinstance(enum_base, EnumTypeDefinition):
                self._resolve_enum_associated_values(condition.value, enum_base)

    def _resolve_instance_value(self, value: Any) -> Any:
        """Resolve a value from CREATE instance, handling function calls, composite refs, inline instances, variable refs, tag refs, and null."""
        if isinstance(value, SetLiteral):
            return SetLiteral(elements=[self._resolve_instance_value(e) for e in value.elements])
        if isinstance(value, DictLiteral):
            return DictLiteral(entries=[
                DictEntry(key=self._resolve_instance_value(e.key), value=self._resolve_instance_value(e.value))
                for e in value.entries
            ])
        if isinstance(value, EmptyBraces):
            return value  # Pass through for context resolution in _create_instance
        if isinstance(value, TypedLiteral):
            return value.value  # Strip type info for storage
        if isinstance(value, list):
            # Resolve TypedLiteral elements; leave InlineInstance/CompositeRef/etc.
            # for downstream handling in _create_instance.
            if any(isinstance(elem, TypedLiteral) for elem in value):
                return [elem.value if isinstance(elem, TypedLiteral) else elem for elem in value]
            return value
        if isinstance(value, NullValue):
            return None
        elif isinstance(value, TagReference):
            # Tag references require a scope
            if not self._in_scope():
                raise ValueError("Tags can only be used within a scope block")
            # Tag reference - check if already bound, otherwise return sentinel for deferred patching
            tag_binding = self._lookup_tag(value.name)
            if tag_binding is not None:
                _, index = tag_binding
                return index
            # Return TagReference as-is - _create_instance will handle deferred patching
            return value
        elif isinstance(value, VariableReference):
            var_binding = self._lookup_variable(value.var_name)
            if var_binding is None:
                raise ValueError(f"Undefined variable: ${value.var_name}")
            type_name, ref = var_binding
            if isinstance(ref, list):
                raise ValueError(f"Cannot use set variable ${value.var_name} as a single field value")
            return ref
        elif isinstance(value, FunctionCall):
            name_lower = value.name.lower()
            if name_lower == "uuid":
                # Generate a random UUID as uint128
                return uuid_module.uuid4().int
            elif value.args:
                # Functions with positional args: range(), repeat(), type conversions, enum conversions
                evaluated_args = [self._resolve_instance_value(a) for a in value.args]
                return self._evaluate_math_func(value.name, evaluated_args)
            elif name_lower in PRIMITIVE_TYPE_NAMES or name_lower in ("bigint", "biguint", "fraction"):
                raise ValueError(f"{value.name}() requires at least 1 argument")
            else:
                raise ValueError(f"Unknown function: {value.name}()")
        elif isinstance(value, CompositeRef):
            # Check if this is actually a single-arg function call (e.g., range(5))
            if value.type_name.lower() in ("range", "repeat"):
                return self._evaluate_math_func(value.type_name, [value.index])
            # bigint/biguint/fraction conversion: bigint(42), biguint(200), fraction(3)
            if value.type_name.lower() in ("bigint", "biguint", "fraction"):
                return self._evaluate_math_func(value.type_name, [value.index])
            # Type conversion: int16(42), uint8(200), etc.
            if value.type_name.lower() in PRIMITIVE_TYPE_NAMES:
                return self._unwrap_typed(self._convert_to_type(value.index, value.type_name.lower()))
            # Enum conversion: Color(0)
            enum_td = self.registry.get(value.type_name)
            if enum_td is not None and isinstance(enum_td.resolve_base_type(), EnumTypeDefinition):
                return self._evaluate_math_func(value.type_name, [value.index])
            # Return the index directly - the type will be validated during instance creation
            return value.index
        elif isinstance(value, EnumValueExpr):
            # Shorthand form (.red, .circle(cx=50)) — defer to _create_instance
            if value.enum_name is None:
                return value
            # Fully-qualified form: Color.red or Shape.circle(cx=50, ...)
            return self._resolve_enum_value_expr(value)
        elif isinstance(value, InlineInstance):
            # Inline instance creation: resolve fields and create the instance
            type_def = self.registry.get(value.type_name)
            if type_def is None:
                raise ValueError(f"Unknown type: {value.type_name}")
            base = type_def.resolve_base_type()
            if not isinstance(base, CompositeTypeDefinition):
                raise ValueError(f"Not a composite type: {value.type_name}")
            # Check if tag is used outside a scope
            if value.tag and not self._in_scope():
                raise ValueError("Tags can only be used within a scope block")
            # Recursively resolve field values (handles nested InlineInstances)
            values: dict[str, Any] = {}
            for fv in value.fields:
                values[fv.name] = self._resolve_instance_value(fv.value)
            # Create the instance and return its index
            index = self._create_instance(type_def, base, values)
            # Register tag if present (must be in a scope, already checked)
            if value.tag:
                self._define_tag(value.tag, value.type_name, index)
            return index
        return value

    # Valid numeric primitive types for overflow modifiers (excludes float, bit, character)
    _OVERFLOW_NUMERIC_TYPES = {
        PrimitiveType.UINT8, PrimitiveType.INT8,
        PrimitiveType.UINT16, PrimitiveType.INT16,
        PrimitiveType.UINT32, PrimitiveType.INT32,
        PrimitiveType.UINT64, PrimitiveType.INT64,
        PrimitiveType.UINT128, PrimitiveType.INT128,
    }

    def _validate_overflow_modifier(self, overflow: str, field_type: TypeDefinition, field_name: str) -> str | None:
        """Validate that an overflow modifier is allowed on this field type. Returns error message or None."""
        base = field_type.resolve_base_type()
        if isinstance(base, PrimitiveTypeDefinition):
            if base.primitive in self._OVERFLOW_NUMERIC_TYPES:
                return None
            return f"Overflow modifier '{overflow}' not allowed on {base.primitive.value} field '{field_name}'"
        return f"Overflow modifier '{overflow}' only allowed on integer fields, not '{field_type.name}' field '{field_name}'"

    def _resolve_default_value(self, raw_value: Any, type_def: TypeDefinition) -> Any:
        """Resolve a default value from a type field definition.

        Only static values are allowed as defaults: literals, null, enum values, arrays of literals.
        Rejects function calls, inline instances, composite refs, variable/tag references.
        """
        if isinstance(raw_value, NullValue):
            return None
        if isinstance(raw_value, (FunctionCall, InlineInstance, CompositeRef, VariableReference, TagReference)):
            kind = type(raw_value).__name__
            raise ValueError(f"Default values cannot use {kind}")
        if isinstance(raw_value, EnumValueExpr):
            # Resolve enum default using the field's type
            base = type_def.resolve_base_type()
            if not isinstance(base, EnumTypeDefinition):
                raise ValueError(f"Enum default on non-enum field type: {type_def.name}")
            enum_name = raw_value.enum_name or type_def.name
            # Resolve without storage side-effects (no array inserts for defaults)
            enum_type = self.registry.get(enum_name)
            if enum_type is None:
                raise ValueError(f"Unknown enum type: {enum_name}")
            enum_base = enum_type.resolve_base_type()
            if not isinstance(enum_base, EnumTypeDefinition):
                raise ValueError(f"Not an enum type: {enum_name}")
            variant = enum_base.get_variant(raw_value.variant_name)
            if variant is None:
                raise ValueError(f"Unknown variant '{raw_value.variant_name}' on enum '{enum_name}'")
            fields_dict: dict[str, Any] = {}
            if raw_value.args:
                for fv in raw_value.args:
                    # Only allow literal values in enum default args
                    val = fv.value
                    if isinstance(val, (FunctionCall, InlineInstance, CompositeRef, VariableReference, TagReference)):
                        raise ValueError(f"Default enum associated values cannot use {type(val).__name__}")
                    if isinstance(val, NullValue):
                        val = None
                    fields_dict[fv.name] = val
            return EnumValue(
                variant_name=variant.name,
                discriminant=variant.discriminant,
                fields=fields_dict,
            )
        if isinstance(raw_value, list):
            # Array default: resolve elements (only literals allowed)
            resolved = []
            for elem in raw_value:
                if isinstance(elem, (FunctionCall, InlineInstance, CompositeRef, VariableReference, TagReference)):
                    raise ValueError(f"Default array elements cannot use {type(elem).__name__}")
                if isinstance(elem, NullValue):
                    resolved.append(None)
                else:
                    resolved.append(elem)
            return resolved
        # Literal (int, float, str)
        return raw_value

    def _infer_concrete_type_name(self, raw_value: Any) -> str | None:
        """Infer the concrete type name from a raw field value (before resolution).

        Used for interface-typed fields to determine the type_id for tagged references.
        """
        if isinstance(raw_value, InlineInstance):
            return raw_value.type_name
        elif isinstance(raw_value, CompositeRef):
            return raw_value.type_name
        elif isinstance(raw_value, VariableReference):
            var_binding = self._lookup_variable(raw_value.var_name)
            if var_binding is not None:
                return var_binding[0]  # type_name
        return None

    def _resolve_enum_value_expr(
        self, value: EnumValueExpr, enum_name: str | None = None,
    ) -> EnumValue | tuple[int, int]:
        """Resolve an EnumValueExpr to an EnumValue (C-style) or (disc, index) tuple (Swift-style).

        If enum_name is provided, it overrides value.enum_name (for shorthand resolution).
        """
        name = enum_name or value.enum_name
        enum_type = self.registry.get(name)
        if enum_type is None:
            raise ValueError(f"Unknown type: {name}")
        enum_base = enum_type.resolve_base_type()
        if not isinstance(enum_base, EnumTypeDefinition):
            raise ValueError(f"Not an enum type: {name}")
        variant = enum_base.get_variant(value.variant_name)
        if variant is None:
            raise ValueError(f"Unknown variant '{value.variant_name}' on enum '{name}'")
        # Resolve associated value fields
        fields_dict: dict[str, Any] = {}
        if value.args:
            for fv in value.args:
                fields_dict[fv.name] = self._resolve_instance_value(fv.value)
        # For fields with array values, store them and convert to (start, length)
        for vf in variant.fields:
            if vf.name in fields_dict:
                fval = fields_dict[vf.name]
                vf_base = vf.type_def.resolve_base_type()
                if isinstance(vf_base, ArrayTypeDefinition):
                    if isinstance(fval, str):
                        fval = list(fval)
                    array_table = self.storage.get_array_table_for_type(vf.type_def)
                    fields_dict[vf.name] = array_table.insert(fval)
                elif isinstance(vf_base, CompositeTypeDefinition):
                    if isinstance(fval, dict):
                        idx = self._create_instance(vf.type_def, vf_base, fval)
                        fields_dict[vf.name] = idx

        if enum_base.has_associated_values:
            # Swift-style: insert fields into variant table, return (disc, index)
            if variant.fields:
                variant_table = self.storage.get_variant_table(enum_base, variant.name)
                variant_record = {}
                for vf in variant.fields:
                    variant_record[vf.name] = fields_dict.get(vf.name)
                index = variant_table.insert(variant_record)
                return (variant.discriminant, index)
            else:
                # Bare variant in Swift-style enum
                return (variant.discriminant, NULL_REF)
        else:
            # C-style: return EnumValue
            return EnumValue(
                variant_name=variant.name,
                discriminant=variant.discriminant,
                fields=fields_dict,
            )

    def _resolve_enum_associated_values(
        self, enum_val: EnumValue, enum_base: EnumTypeDefinition,
    ) -> None:
        """Resolve array/string fields inside an EnumValue in-place."""
        if not enum_val.fields:
            return
        variant = enum_base.get_variant(enum_val.variant_name)
        if variant is None:
            return
        for vf in variant.fields:
            fval = enum_val.fields.get(vf.name)
            if fval is None:
                continue
            vf_base = vf.type_def.resolve_base_type()
            if isinstance(vf_base, ArrayTypeDefinition) and isinstance(fval, tuple):
                start_index, length = fval
                if length == 0:
                    enum_val.fields[vf.name] = [] if not is_string_type(vf.type_def) else ""
                else:
                    arr_table = self.storage.get_array_table_for_type(vf.type_def)
                    elements = [
                        arr_table.element_table.get(start_index + j)
                        for j in range(length)
                    ]
                    if is_string_type(vf.type_def):
                        enum_val.fields[vf.name] = "".join(elements)
                    else:
                        enum_val.fields[vf.name] = elements

    def _resolve_swift_enum_ref(
        self, ref: tuple, enum_def: EnumTypeDefinition,
    ) -> EnumValue:
        """Convert (discriminant, variant_table_index) to a fully resolved EnumValue."""
        disc, index = ref
        variant = enum_def.get_variant_by_discriminant(disc)
        if variant is None:
            return EnumValue(variant_name="?", discriminant=disc)
        if index == NULL_REF or not variant.fields:
            return EnumValue(variant_name=variant.name, discriminant=disc)

        variant_table = self.storage.get_variant_table(enum_def, variant.name)
        raw = variant_table.get(index)  # returns dict of field values
        return EnumValue(variant_name=variant.name, discriminant=disc, fields=raw)

    def _store_enum_value_to_variant_table(
        self, ev: EnumValue, enum_def: EnumTypeDefinition,
    ) -> tuple[int, int]:
        """Convert an EnumValue to (disc, index) by inserting its fields into a variant table.

        Used when an EnumValue (e.g. from a default) needs to be stored as a Swift-style
        variant table reference.
        """
        variant = enum_def.get_variant(ev.variant_name)
        if variant is None or not variant.fields:
            return (ev.discriminant, NULL_REF)

        # Store array fields in their tables first
        fields_dict = dict(ev.fields)
        for vf in variant.fields:
            if vf.name in fields_dict:
                fval = fields_dict[vf.name]
                if fval is None:
                    continue
                vf_base = vf.type_def.resolve_base_type()
                if isinstance(vf_base, ArrayTypeDefinition):
                    if isinstance(fval, str):
                        fval = list(fval)
                    if isinstance(fval, list):
                        array_table = self.storage.get_array_table_for_type(vf.type_def)
                        fields_dict[vf.name] = array_table.insert(fval)

        variant_table = self.storage.get_variant_table(enum_def, ev.variant_name)
        variant_record = {}
        for vf in variant.fields:
            variant_record[vf.name] = fields_dict.get(vf.name)
        index = variant_table.insert(variant_record)
        return (ev.discriminant, index)

    def _create_instance(
        self,
        type_def: TypeDefinition,
        composite_type: CompositeTypeDefinition,
        values: dict[str, Any],
    ) -> int:
        """Create a composite instance and return its index."""
        field_references: dict[str, Any] = {}

        for field in composite_type.fields:
            field_value = values.get(field.name)
            field_base = field.type_def.resolve_base_type()

            if field_value is None:
                field_references[field.name] = None
                continue

            if isinstance(field_base, DictionaryTypeDefinition):
                # Dictionary field
                if isinstance(field_value, EmptyBraces) or isinstance(field_value, DictLiteral) and not field_value.entries:
                    # Empty dict
                    array_table = self.storage.get_array_table_for_type(field.type_def)
                    field_references[field.name] = array_table.insert([])
                    continue
                if isinstance(field_value, DictLiteral):
                    entries = field_value.entries
                elif isinstance(field_value, list):
                    # Already resolved list of (key, value) tuples (e.g. from defaults)
                    entries = [DictEntry(key=k, value=v) for k, v in field_value]
                else:
                    raise ValueError(f"Expected dict literal for field '{field.name}', got {type(field_value).__name__}")
                # Enforce key uniqueness
                keys_seen = []
                entry_indices = []
                entry_type = field_base.entry_type
                entry_base = entry_type.resolve_base_type()
                for entry in entries:
                    key = entry.key
                    # Convert string key to char list for string keys
                    if isinstance(key, str) and is_string_type(field_base.key_type):
                        key_check = key
                    else:
                        key_check = key
                    if key_check in keys_seen:
                        raise ValueError(f"Duplicate key in dictionary for field '{field.name}': {key_check!r}")
                    keys_seen.append(key_check)
                    # Create entry composite instance
                    entry_values = {"key": key, "value": entry.value}
                    entry_index = self._create_instance(entry_type, entry_base, entry_values)
                    entry_indices.append(entry_index)
                array_table = self.storage.get_array_table_for_type(field.type_def)
                field_references[field.name] = array_table.insert(entry_indices)
                continue

            if isinstance(field_base, SetTypeDefinition):
                # Set field — like array but with uniqueness enforcement
                if isinstance(field_value, EmptyBraces) or isinstance(field_value, SetLiteral) and not field_value.elements:
                    array_table = self.storage.get_array_table_for_type(field.type_def)
                    field_references[field.name] = array_table.insert([])
                    continue
                if isinstance(field_value, SetLiteral):
                    elements = field_value.elements
                elif isinstance(field_value, list):
                    elements = field_value
                elif isinstance(field_value, str):
                    elements = list(field_value)
                else:
                    raise ValueError(f"Expected set literal for field '{field.name}', got {type(field_value).__name__}")
                # Enforce uniqueness
                seen = []
                for elem in elements:
                    if elem in seen:
                        raise ValueError(f"Duplicate element in set for field '{field.name}': {elem!r}")
                    seen.append(elem)
                # For {string} sets: each string → chars stored in char table → (start, length) tuple
                if is_string_type(field_base.element_type):
                    char_table = self.storage.get_array_table_for_type(field_base.element_type)
                    elements = [char_table.insert(list(e) if isinstance(e, str) else e) for e in elements]
                array_table = self.storage.get_array_table_for_type(field.type_def)
                field_references[field.name] = array_table.insert(elements)
                continue

            if isinstance(field_base, FractionTypeDefinition):
                if isinstance(field_value, Fraction):
                    field_references[field.name] = _fraction_encode(field_value, self.storage)
                elif isinstance(field_value, (int, BigInt, BigUInt)):
                    field_references[field.name] = _fraction_encode(Fraction(int(field_value)), self.storage)
                else:
                    raise ValueError(f"Expected fraction value for field '{field.name}', got {type(field_value).__name__}")
                continue

            if isinstance(field_base, (BigIntTypeDefinition, BigUIntTypeDefinition)):
                val = int(field_value)
                signed = isinstance(field_base, BigIntTypeDefinition)
                if not signed and val < 0:
                    raise ValueError(f"biguint field '{field.name}' cannot store negative value: {val}")
                if val == 0:
                    byte_list = [0]
                elif signed:
                    byte_length = (val.bit_length() + 8) // 8
                    byte_list = list(val.to_bytes(byte_length, byteorder='little', signed=True))
                else:
                    byte_length = (val.bit_length() + 7) // 8
                    byte_list = list(val.to_bytes(byte_length, byteorder='little', signed=False))
                array_table = self.storage.get_array_table_for_type(field.type_def)
                field_references[field.name] = array_table.insert(byte_list)
                continue

            if isinstance(field_base, ArrayTypeDefinition):
                # Convert string to character list if needed
                if isinstance(field_value, str):
                    field_value = list(field_value)
                # Handle EmptyBraces as empty array
                if isinstance(field_value, EmptyBraces):
                    field_value = []
                # Handle composite array elements (InlineInstance or int refs)
                elem_base = field_base.element_type.resolve_base_type()
                if isinstance(elem_base, CompositeTypeDefinition):
                    resolved = []
                    for elem in field_value:
                        if isinstance(elem, InlineInstance):
                            inline_type = self.registry.get(elem.type_name)
                            inline_base = inline_type.resolve_base_type()
                            inline_values = {}
                            for fv in elem.fields:
                                inline_values[fv.name] = self._resolve_instance_value(fv.value)
                            elem = self._build_field_references(inline_base, inline_values)
                        elif isinstance(elem, int):
                            elem_type_name = field_base.element_type.name
                            ref_table = self.storage.get_table(elem_type_name)
                            elem = ref_table.get(elem)
                        resolved.append(elem)
                    field_value = resolved
                # For string[] (array of strings): each string → chars stored in char table → (start, length) tuple
                if is_string_type(field_base.element_type):
                    char_table = self.storage.get_array_table_for_type(field_base.element_type)
                    field_value = [char_table.insert(list(e) if isinstance(e, str) else e) for e in field_value]
                # Store array elements and get (start_index, length) tuple
                array_table = self.storage.get_array_table_for_type(field.type_def)
                field_references[field.name] = array_table.insert(field_value)
                continue

            if isinstance(field_base, EnumTypeDefinition):
                # Resolve shorthand enum expressions (.red → Color.red)
                if isinstance(field_value, EnumValueExpr):
                    field_value = self._resolve_enum_value_expr(
                        field_value, enum_name=field.type_def.name,
                    )
                elif isinstance(field_value, EnumValue) and field_base.has_associated_values:
                    # EnumValue from default — convert to (disc, index) for variant table storage
                    field_value = self._store_enum_value_to_variant_table(field_value, field_base)
                # Enum field — value is EnumValue (C-style) or (disc, index) tuple (Swift-style)
                field_references[field.name] = field_value
            elif isinstance(field_base, InterfaceTypeDefinition):
                # Interface-typed field: stored as (type_id, index) tuple
                if isinstance(field_value, tuple) and len(field_value) == 2:
                    field_references[field.name] = field_value
                else:
                    raise ValueError(
                        f"Interface-typed field '{field.name}' requires a (type_id, index) tuple"
                    )
            elif isinstance(field_base, CompositeTypeDefinition):
                # Nested composite - either an index reference, dict for recursive create, or tag reference
                if isinstance(field_value, TagReference):
                    # Tag reference not yet bound - use None and defer patching
                    field_references[field.name] = None
                elif isinstance(field_value, int):
                    # Direct index reference: TypeName(index) syntax
                    field_references[field.name] = field_value
                else:
                    # Dict for recursive create
                    nested_index = self._create_instance(field.type_def, field_base, field_value)
                    field_references[field.name] = nested_index
            else:
                # Primitive value — store inline
                field_references[field.name] = field_value

        # Store the composite record
        table = self.storage.get_table(type_def.name)
        index = table.insert(field_references)

        # Collect deferred tag patches for tag references that weren't bound yet
        for field in composite_type.fields:
            fv = values.get(field.name)
            if isinstance(fv, TagReference):
                self._add_deferred_patch(type_def.name, index, field.name, fv.name)

        return index

    def _execute_eval(self, query: EvalQuery) -> QueryResult:
        """Execute SELECT without FROM - evaluate expressions."""
        columns = []
        row: dict[str, Any] = {}

        for i, expr_tuple in enumerate(query.expressions):
            # Expressions are now (expr, alias) tuples
            expr, alias = expr_tuple
            if isinstance(expr, (BinaryExpr, UnaryExpr, MethodCallExpr, TypedLiteral, list)) or (
                isinstance(expr, FunctionCall) and expr.args
            ):
                value = self._evaluate_expr(expr)
            else:
                value = self._resolve_instance_value(expr)

            # Use alias if provided, otherwise generate column name
            if alias:
                base_name = alias
            elif isinstance(expr, (BinaryExpr, UnaryExpr, FunctionCall, MethodCallExpr, TypedLiteral, list)):
                base_name = self._format_expr(expr)
            else:
                base_name = f"expr_{i}"

            # Make column name unique if needed
            col_name = base_name
            suffix = 1
            while col_name in row:
                suffix += 1
                col_name = f"{base_name}_{suffix}"

            columns.append(col_name)

            # Unwrap TypedValue for display
            if isinstance(value, TypedValue):
                value = value.value
            # Format the value for display
            if isinstance(value, list):
                # Unwrap TypedValues in list
                row[col_name] = [v.value if isinstance(v, TypedValue) else v for v in value]
            elif isinstance(value, Fraction):
                row[col_name] = value
            elif isinstance(value, (BigInt, BigUInt)):
                row[col_name] = value  # Keep as BigInt/BigUInt for decimal display in REPL
            elif isinstance(value, int) and value > 0xFFFFFFFF:
                # Format large integers as hex (likely UUIDs)
                row[col_name] = f"0x{value:032x}"
            else:
                row[col_name] = value

        return QueryResult(columns=columns, rows=[row])

    def _evaluate_expr(self, expr: Any) -> Any:
        """Recursively evaluate a BinaryExpr/UnaryExpr tree."""
        if isinstance(expr, BinaryExpr):
            left = self._evaluate_expr(expr.left)
            right = self._evaluate_expr(expr.right)
            op = expr.op
            # Element-wise / broadcast if either operand is a list
            if isinstance(left, list) or isinstance(right, list):
                return self._evaluate_array_binary(left, right, op)
            return self._apply_scalar_binary(left, right, op)
        if isinstance(expr, UnaryExpr):
            operand = self._evaluate_expr(expr.operand)
            if isinstance(operand, list):
                return [self._apply_scalar_unary(elem, expr.op) for elem in operand]
            return self._apply_scalar_unary(operand, expr.op)
        # Leaf: list literal (array of expressions)
        if isinstance(expr, list):
            return [self._evaluate_expr(item) for item in expr]
        # MethodCallExpr: expr.method(args) — e.g. [1,9,5,7,3].sort()
        if isinstance(expr, MethodCallExpr):
            target = self._evaluate_expr(expr.target)
            args = [self._evaluate_expr(a) for a in expr.method_args] if expr.method_args else None
            return self._apply_projection_method(target, expr.method_name, args)
        # TypedLiteral → TypedValue with range validation
        if isinstance(expr, TypedLiteral):
            prim = PRIMITIVE_TYPE_NAMES.get(expr.type_name)
            if prim is not None:
                min_val, max_val = type_range(prim)
                if isinstance(expr.value, (int, float)) and (expr.value < min_val or expr.value > max_val):
                    raise RuntimeError(f"Literal {expr.value} out of range for {expr.type_name} ({min_val}..{max_val})")
            return TypedValue(value=expr.value, type_name=expr.type_name)
        # Leaf: FunctionCall — with args → math function, no args → existing path
        if isinstance(expr, FunctionCall):
            if expr.args:
                evaluated_args = [self._evaluate_expr(a) for a in expr.args]
                return self._evaluate_math_func(expr.name, evaluated_args)
            return self._resolve_instance_value(expr)
        return expr

    def _enum_to_typed_value(self, value: Any) -> Any:
        """If value is an EnumValue with a backing type, convert to TypedValue. Otherwise return as-is."""
        if isinstance(value, EnumValue):
            # Find the enum type definition to check for backing type
            for type_name in self.registry.list_types():
                td = self.registry.get(type_name)
                if isinstance(td, EnumTypeDefinition) and td.backing_type:
                    variant = td.get_variant(value.variant_name)
                    if variant and variant.discriminant == value.discriminant:
                        return TypedValue(value=value.discriminant, type_name=td.backing_type.value)
        return value

    def _apply_scalar_binary(self, left: Any, right: Any, op: str) -> Any:
        """Apply a binary operator to two scalar values, with TypedValue propagation."""
        # Auto-convert enum values with backing types
        left = self._enum_to_typed_value(left)
        right = self._enum_to_typed_value(right)

        left_typed = isinstance(left, TypedValue)
        right_typed = isinstance(right, TypedValue)

        if left_typed or right_typed:
            # Determine the result type
            if left_typed and right_typed:
                if left.type_name != right.type_name:
                    raise RuntimeError(f"Type mismatch: {left.type_name} vs {right.type_name}")
                result_type = left.type_name
            elif left_typed:
                result_type = left.type_name
            else:
                result_type = right.type_name

            # Unwrap for raw arithmetic
            lv = left.value if left_typed else left
            rv = right.value if right_typed else right

            # Concat: unwrap to string, no TypedValue result
            if op == "++":
                return str(lv) + str(rv)

            # For typed integer context, / uses floor division
            effective_op = op
            prim = PRIMITIVE_TYPE_NAMES.get(result_type)
            if op == "/" and prim and prim not in (PrimitiveType.FLOAT16, PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
                effective_op = "//"

            result = self._apply_raw_binary(lv, rv, effective_op)
            # Enforce overflow on the typed result
            result = self._enforce_overflow(result, result_type, self._current_overflow_policy)
            return TypedValue(value=result, type_name=result_type)

        return self._apply_raw_binary(left, right, op)

    def _apply_raw_binary(self, left: Any, right: Any, op: str) -> Any:
        """Apply a binary operator to two raw (untyped) scalar values."""
        if op == "++":
            return str(left) + str(right)
        if op == "+":
            if not isinstance(left, (int, float, Fraction)) or not isinstance(right, (int, float, Fraction)):
                raise RuntimeError(f"Cannot add non-numeric values: {left!r} + {right!r} (use ++ for string concatenation)")
            return left + right
        if op == "-":
            if not isinstance(left, (int, float, Fraction)) or not isinstance(right, (int, float, Fraction)):
                raise RuntimeError(f"Cannot subtract non-numeric values: {left!r} - {right!r}")
            return left - right
        if op == "*":
            if not isinstance(left, (int, float, Fraction)) or not isinstance(right, (int, float, Fraction)):
                raise RuntimeError(f"Cannot multiply non-numeric values: {left!r} * {right!r}")
            return left * right
        if op == "/":
            if not isinstance(left, (int, float, Fraction)) or not isinstance(right, (int, float, Fraction)):
                raise RuntimeError(f"Cannot divide non-numeric values: {left!r} / {right!r}")
            if right == 0:
                raise RuntimeError("Division by zero")
            return left / right
        if op == "%":
            if not isinstance(left, (int, float, Fraction)) or not isinstance(right, (int, float, Fraction)):
                raise RuntimeError(f"Cannot modulo non-numeric values: {left!r} % {right!r}")
            if right == 0:
                raise RuntimeError("Division by zero")
            return left % right
        if op == "//":
            if not isinstance(left, (int, float, Fraction)) or not isinstance(right, (int, float, Fraction)):
                raise RuntimeError(f"Cannot integer-divide non-numeric values: {left!r} // {right!r}")
            if right == 0:
                raise RuntimeError("Division by zero")
            return left // right
        if op == "**":
            return left ** right
        raise RuntimeError(f"Unknown binary operator: {op}")

    def _apply_scalar_unary(self, operand: Any, op: str) -> Any:
        """Apply a unary operator to a scalar value."""
        if isinstance(operand, TypedValue):
            raw = operand.value
            if op == "-":
                return TypedValue(value=-raw, type_name=operand.type_name)
            if op == "+":
                return operand
            raise RuntimeError(f"Unknown unary operator: {op}")
        if op == "-":
            if not isinstance(operand, (int, float, Fraction)):
                raise RuntimeError(f"Cannot negate non-numeric value: {operand!r}")
            return -operand
        if op == "+":
            if not isinstance(operand, (int, float, Fraction)):
                raise RuntimeError(f"Cannot apply unary + to non-numeric value: {operand!r}")
            return operand
        raise RuntimeError(f"Unknown unary operator: {op}")

    def _evaluate_array_binary(self, left: Any, right: Any, op: str) -> list:
        """Apply a binary operator element-wise to arrays, with scalar broadcasting."""
        if isinstance(left, list) and isinstance(right, list):
            if len(left) != len(right):
                raise RuntimeError(
                    f"Array length mismatch: {len(left)} vs {len(right)}")
            return [self._apply_scalar_binary(l, r, op) for l, r in zip(left, right)]
        elif isinstance(left, list):
            return [self._apply_scalar_binary(l, right, op) for l in left]
        else:
            return [self._apply_scalar_binary(left, r, op) for r in right]

    @staticmethod
    def _enforce_overflow(value: int | float, type_name: str, policy: str | None) -> int | float:
        """Enforce overflow policy on a computed result.

        - policy=None or 'error': raise on overflow
        - policy='saturating': clamp to min/max
        - policy='wrapping': modular arithmetic
        """
        prim = PRIMITIVE_TYPE_NAMES.get(type_name)
        if prim is None:
            return value
        if prim in (PrimitiveType.FLOAT16, PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
            return value  # Float overflow not enforced
        min_val, max_val = type_range(prim)
        if min_val <= value <= max_val:
            return value

        if policy == "saturating":
            return max(min_val, min(value, max_val))
        elif policy == "wrapping":
            range_size = max_val - min_val + 1
            return ((value - min_val) % range_size) + min_val
        else:
            raise RuntimeError(
                f"Overflow: result {value} out of range for {type_name} ({min_val}..{max_val})")

    @staticmethod
    def _unwrap_typed(value: Any) -> Any:
        """Unwrap a TypedValue to its raw value. Recursively handles lists."""
        if isinstance(value, TypedValue):
            return value.value
        if isinstance(value, list):
            return [v.value if isinstance(v, TypedValue) else v for v in value]
        return value

    def _convert_to_type(self, value: Any, type_name: str) -> Any:
        """Convert a value to the given primitive type with range checking. Returns TypedValue."""
        prim = PRIMITIVE_TYPE_NAMES.get(type_name)
        if prim is None:
            raise RuntimeError(f"Unknown primitive type for conversion: {type_name}")
        if isinstance(value, list):
            return [self._convert_to_type(elem, type_name) for elem in value]
        # Unwrap TypedValue
        raw = value.value if isinstance(value, TypedValue) else value
        min_val, max_val = type_range(prim)
        if prim in (PrimitiveType.FLOAT16, PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
            return TypedValue(value=float(raw), type_name=type_name)
        # Integer conversion
        if isinstance(raw, float):
            if raw != int(raw):
                raise RuntimeError(f"Cannot convert {raw} to {type_name}: not an integer")
            raw = int(raw)
        if not isinstance(raw, int):
            raise RuntimeError(f"Cannot convert {type(raw).__name__} to {type_name}")
        if raw < min_val or raw > max_val:
            raise RuntimeError(f"Value {raw} out of range for {type_name} ({min_val}..{max_val})")
        return TypedValue(value=raw, type_name=type_name)

    def _convert_to_string(self, value: Any) -> str:
        """Convert a value to its string representation."""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, TypedValue):
            return str(value.value)
        if isinstance(value, EnumValue):
            return value.variant_name
        if isinstance(value, str):
            return value
        return str(value)

    _MATH_FUNCS_1 = {
        "sqrt": __import__("math").sqrt,
        "abs": abs,
        "ceil": __import__("math").ceil,
        "floor": __import__("math").floor,
        "round": round,
        "sin": __import__("math").sin,
        "cos": __import__("math").cos,
        "tan": __import__("math").tan,
        "log": __import__("math").log,
        "log2": __import__("math").log2,
        "log10": __import__("math").log10,
    }

    def _evaluate_math_func(self, name: str, args: list) -> Any:
        """Evaluate a math function call."""
        name_lower = name.lower()

        # string() cast — convert value to string representation
        if name_lower == "string":
            if len(args) != 1:
                raise RuntimeError("string() requires exactly 1 argument")
            arg = args[0]
            if isinstance(arg, list):
                return [self._convert_to_string(elem) for elem in arg]
            return self._convert_to_string(arg)

        # boolean() cast — convert 0/1 to boolean
        if name_lower == "boolean":
            if len(args) != 1:
                raise RuntimeError("boolean() requires exactly 1 argument")
            arg = self._unwrap_typed(args[0])
            if isinstance(arg, bool):
                return arg
            if isinstance(arg, int):
                if arg not in (0, 1):
                    raise RuntimeError(f"boolean() requires 0 or 1, got {arg}")
                return bool(arg)
            raise RuntimeError(f"boolean() requires an integer argument, got {type(arg).__name__}")

        # bigint() / biguint() conversion
        if name_lower == "bigint":
            if len(args) != 1:
                raise RuntimeError("bigint() requires exactly 1 argument")
            val = self._unwrap_typed(args[0])
            return BigInt(int(val))
        if name_lower == "biguint":
            if len(args) != 1:
                raise RuntimeError("biguint() requires exactly 1 argument")
            val = self._unwrap_typed(args[0])
            val = int(val)
            if val < 0:
                raise RuntimeError(f"biguint() cannot accept negative value: {val}")
            return BigUInt(val)

        # fraction() conversion
        if name_lower == "fraction":
            if len(args) == 1:
                val = self._unwrap_typed(args[0])
                if isinstance(val, Fraction):
                    return val
                if isinstance(val, (int, BigInt, BigUInt)):
                    return Fraction(int(val))
                if isinstance(val, float):
                    return Fraction(val).limit_denominator()
                raise RuntimeError(f"fraction() cannot convert {type(val).__name__}")
            elif len(args) == 2:
                num = self._unwrap_typed(args[0])
                den = self._unwrap_typed(args[1])
                num = int(num)
                den = int(den)
                if den == 0:
                    raise RuntimeError("fraction() denominator cannot be zero")
                return Fraction(num, den)
            else:
                raise RuntimeError("fraction() requires 1 or 2 arguments")

        # Type conversion functions: int8(), uint16(), float32(), etc.
        if name_lower in PRIMITIVE_TYPE_NAMES:
            if len(args) != 1:
                raise RuntimeError(f"{name}() requires exactly 1 argument")
            return self._convert_to_type(args[0], name_lower)

        # Enum conversion: Color(0) → Color.red, Color("red") → Color.red
        enum_def = self.registry.get(name)
        if enum_def is not None and isinstance(enum_def.resolve_base_type(), EnumTypeDefinition):
            enum_base = enum_def.resolve_base_type()
            if len(args) != 1:
                raise RuntimeError(f"{name}() requires exactly 1 argument")
            arg = self._unwrap_typed(args[0])
            if isinstance(arg, str):
                # Lookup by variant name
                variant = enum_base.get_variant(arg)
                if variant is None:
                    raise RuntimeError(f"No variant named '{arg}' on enum '{name}'")
                if variant.fields:
                    raise RuntimeError(f"Cannot convert string to variant '{arg}' with associated values")
                return EnumValue(variant_name=variant.name, discriminant=variant.discriminant, fields={})
            elif isinstance(arg, int):
                # Lookup by discriminant
                variant = enum_base.get_variant_by_discriminant(arg)
                if variant is None:
                    raise RuntimeError(f"No variant with discriminant {arg} on enum '{name}'")
                return EnumValue(variant_name=variant.name, discriminant=variant.discriminant, fields={})
            else:
                raise RuntimeError(f"{name}() requires an integer or string argument")

        if name_lower == "pow":
            if len(args) != 2:
                raise RuntimeError("pow() requires exactly 2 arguments")
            base, exp = args
            if isinstance(base, list) or isinstance(exp, list):
                return self._evaluate_array_binary(base, exp, "**")
            return base ** exp

        # Aggregate functions on arrays
        if name_lower == "sum":
            if len(args) != 1:
                raise RuntimeError("sum() requires exactly 1 argument")
            arg = args[0]
            if isinstance(arg, list):
                return sum(arg)
            return arg
        if name_lower == "average":
            if len(args) != 1:
                raise RuntimeError("average() requires exactly 1 argument")
            arg = args[0]
            if isinstance(arg, list):
                return sum(arg) / len(arg) if arg else None
            return float(arg)
        if name_lower == "product":
            if len(args) != 1:
                raise RuntimeError("product() requires exactly 1 argument")
            arg = args[0]
            if isinstance(arg, list):
                result = 1
                for v in arg:
                    result *= v
                return result
            return arg
        if name_lower == "count":
            if len(args) != 1:
                raise RuntimeError("count() requires exactly 1 argument")
            arg = args[0]
            if isinstance(arg, list):
                return len(arg)
            return 1
        if name_lower == "min":
            if len(args) == 1:
                arg = args[0]
                if isinstance(arg, list):
                    return min(arg) if arg else None
                return arg
            return min(args)
        if name_lower == "max":
            if len(args) == 1:
                arg = args[0]
                if isinstance(arg, list):
                    return max(arg) if arg else None
                return arg
            return max(args)

        if name_lower == "repeat":
            if len(args) != 2:
                raise RuntimeError("repeat() requires exactly 2 arguments: repeat(value, count)")
            value, count = args
            if not isinstance(count, int):
                raise RuntimeError(f"repeat() count must be an integer, got {type(count).__name__}")
            if count < 0:
                raise RuntimeError(f"repeat() count must be non-negative, got {count}")
            return [value] * count

        if name_lower == "range":
            if len(args) == 1:
                if not isinstance(args[0], (int, float)):
                    raise RuntimeError(f"range() arguments must be numeric, got {type(args[0]).__name__}")
                return list(range(int(args[0])))
            elif len(args) == 2:
                return list(range(int(args[0]), int(args[1])))
            elif len(args) == 3:
                return list(range(int(args[0]), int(args[1]), int(args[2])))
            else:
                raise RuntimeError("range() takes 1-3 arguments")

        func = self._MATH_FUNCS_1.get(name_lower)
        if func is None:
            raise RuntimeError(f"Unknown function: {name}()")
        if len(args) != 1:
            raise RuntimeError(f"{name}() requires exactly 1 argument")
        arg = args[0]
        if isinstance(arg, list):
            return [func(elem) for elem in arg]
        return func(arg)

    # Map primitive type names back to type suffixes for display
    _TYPE_NAME_TO_SUFFIX = {
        "int8": "i8", "uint8": "u8",
        "int16": "i16", "uint16": "u16",
        "int32": "i32", "uint32": "u32",
        "int64": "i64", "uint64": "u64",
        "int128": "i128", "uint128": "u128",
        "float16": "f16", "float32": "f32", "float64": "f64",
    }

    def _format_expr(self, expr: Any) -> str:
        """Format an expression tree as a human-readable column name."""
        if isinstance(expr, TypedLiteral):
            suffix = self._TYPE_NAME_TO_SUFFIX.get(expr.type_name, expr.type_name)
            if isinstance(expr.value, float):
                return f"{expr.value}{suffix}"
            return f"{expr.value}{suffix}"
        if isinstance(expr, MethodCallExpr):
            target = self._format_expr(expr.target)
            if expr.method_args:
                args_str = ", ".join(self._format_expr(a) for a in expr.method_args)
                return f"{target}.{expr.method_name}({args_str})"
            return f"{target}.{expr.method_name}()"
        if isinstance(expr, BinaryExpr):
            left = self._format_expr(expr.left)
            right = self._format_expr(expr.right)
            return f"{left} {expr.op} {right}"
        if isinstance(expr, UnaryExpr):
            operand = self._format_expr(expr.operand)
            if isinstance(expr.operand, BinaryExpr):
                return f"{expr.op}({operand})"
            return f"{expr.op}{operand}"
        if isinstance(expr, FunctionCall):
            if expr.args:
                args_str = ", ".join(self._format_expr(a) for a in expr.args)
                return f"{expr.name}({args_str})"
            return f"{expr.name}()"
        if isinstance(expr, list):
            elements = ", ".join(self._format_expr(e) for e in expr)
            return f"[{elements}]"
        if isinstance(expr, str):
            return f'"{expr}"'
        return str(expr)

    def _execute_delete(self, query: DeleteQuery) -> DeleteResult:
        """Execute DELETE query."""
        if query.table.startswith("_") and not query.force:
            return DeleteResult(
                columns=[], rows=[],
                message=f"Cannot delete system type records (use delete! to force)",
                deleted_count=0,
            )
        type_def = self.registry.get(query.table)
        if type_def is None:
            return DeleteResult(
                columns=[],
                rows=[],
                message=f"Unknown type: {query.table}",
                deleted_count=0,
            )

        base = type_def.resolve_base_type()
        if not isinstance(base, CompositeTypeDefinition):
            return DeleteResult(
                columns=[],
                rows=[],
                message=f"DELETE only supported for composite types: {query.table}",
                deleted_count=0,
            )

        # Get all records and filter by WHERE clause
        records = list(self._load_all_records(query.table, type_def))

        if query.where:
            records_to_delete = [r for r in records if self._evaluate_condition(r, query.where)]
        else:
            records_to_delete = records

        if not records_to_delete:
            return DeleteResult(
                columns=[],
                rows=[],
                message="No matching records to delete",
                deleted_count=0,
            )

        # Delete the matching records
        table = self.storage.get_table(query.table)
        for record in records_to_delete:
            index = record["_index"]
            table.delete(index)

        return DeleteResult(
            columns=["deleted"],
            rows=[{"deleted": len(records_to_delete)}],
            message=f"Deleted {len(records_to_delete)} record(s) from {query.table}",
            deleted_count=len(records_to_delete),
        )

    def _execute_select(self, query: SelectQuery) -> QueryResult:
        """Execute SELECT query."""
        # Resolve the source: either a variable or a table name
        if query.source_var:
            var_binding = self._lookup_variable(query.source_var)
            if var_binding is None:
                return QueryResult(
                    columns=[],
                    rows=[],
                    message=f"Undefined variable: ${query.source_var}",
                )
            type_name, ref = var_binding
            type_def = self.registry.get(type_name)
            if type_def is None:
                return QueryResult(
                    columns=[],
                    rows=[],
                    message=f"Unknown type: {type_name}",
                )
            if isinstance(ref, list):
                records = list(self._load_records_by_indices(type_name, type_def, ref))
            else:
                records = list(self._load_records_by_indices(type_name, type_def, [ref]))
        else:
            type_def = self.registry.get(query.table)
            if type_def is None:
                return QueryResult(
                    columns=[],
                    rows=[],
                    message=f"Unknown type: {query.table}",
                )

            # Handle variant-specific enum queries: from Shape.circle select *
            if query.variant:
                enum_base = type_def.resolve_base_type()
                if not isinstance(enum_base, EnumTypeDefinition):
                    return QueryResult(
                        columns=[], rows=[],
                        message=f"Cannot use variant syntax on non-enum type: {query.table}",
                    )
                variant = enum_base.get_variant(query.variant)
                if variant is None:
                    return QueryResult(
                        columns=[], rows=[],
                        message=f"Unknown variant '{query.variant}' on enum '{query.table}'",
                    )
                records = list(self._load_records_by_enum_type(query.table, type_def, variant_filter=query.variant))

                # Apply WHERE filter
                if query.where:
                    records = [r for r in records if self._evaluate_condition(r, query.where)]

                # Apply SORT BY
                if query.sort_by:
                    records = self._apply_sort_by(records, query.sort_by)

                # Apply OFFSET and LIMIT
                if query.offset:
                    records = records[query.offset:]
                if query.limit is not None:
                    records = records[: query.limit]

                # Build columns from variant fields
                if len(query.fields) == 1 and query.fields[0].name == "*" and query.fields[0].aggregate is None:
                    columns = ["_source", "_index", "_field"] + [f.name for f in variant.fields]
                    return QueryResult(columns=columns, rows=records)
                else:
                    columns, rows = self._select_fields(records, query, type_def)
                    return QueryResult(columns=columns, rows=rows)

            records = list(self._load_all_records(query.table, type_def))

        base = type_def.resolve_base_type()

        # For interface type queries, handle polymorphic fan-out
        if isinstance(base, InterfaceTypeDefinition):
            # Apply WHERE filter
            if query.where:
                records = [r for r in records if self._evaluate_condition(r, query.where)]

            # Apply SORT BY
            if query.sort_by:
                records = self._apply_sort_by(records, query.sort_by)

            # Apply OFFSET and LIMIT
            if query.offset:
                records = records[query.offset:]
            if query.limit is not None:
                records = records[: query.limit]

            if len(query.fields) == 1 and query.fields[0].name == "*" and query.fields[0].aggregate is None:
                interface_field_names = [f.name for f in base.fields]
                columns = ["_type", "_index"] + interface_field_names
                return QueryResult(columns=columns, rows=records)
            else:
                columns, rows = self._select_fields(records, query, type_def)
                return QueryResult(columns=columns, rows=rows)

        # For enum type overview queries, handle specially
        if isinstance(base, EnumTypeDefinition):
            if query.where:
                return QueryResult(
                    columns=[], rows=[],
                    message=f"WHERE not supported on enum overview query. Use 'from {query.table}.<variant> select *' for filtering.",
                )

            # Apply SORT BY
            if query.sort_by:
                records = self._apply_sort_by(records, query.sort_by)

            # Apply OFFSET and LIMIT
            if query.offset:
                records = records[query.offset:]
            if query.limit is not None:
                records = records[: query.limit]

            if len(query.fields) == 1 and query.fields[0].name == "*" and query.fields[0].aggregate is None:
                columns = ["_source", "_index", "_field", "_variant", "value"]
                return QueryResult(columns=columns, rows=records)
            else:
                # Allow selecting specific columns from overview records
                columns, rows = self._select_fields(records, query, type_def)
                return QueryResult(columns=columns, rows=rows)

        # Apply WHERE filter
        if query.where:
            records = [r for r in records if self._evaluate_condition(r, query.where)]

        # Apply GROUP BY
        if query.group_by:
            records = self._apply_group_by(records, query.group_by)

        # Apply SORT BY
        if query.sort_by:
            records = self._apply_sort_by(records, query.sort_by)

        # Apply OFFSET and LIMIT
        if query.offset:
            records = records[query.offset:]
        if query.limit is not None:
            records = records[: query.limit]

        # Select fields
        columns, rows = self._select_fields(records, query, type_def)

        return QueryResult(columns=columns, rows=rows)

    def _load_all_records(
        self, type_name: str, type_def: TypeDefinition
    ) -> Iterator[dict[str, Any]]:
        """Load all records from a table with resolved values."""
        base = type_def.resolve_base_type()

        if isinstance(base, CompositeTypeDefinition):
            table = self.storage.get_table(type_name)
            for i in range(table.count):
                # Skip deleted records
                if table.is_deleted(i):
                    continue

                record = table.get(i)
                resolved = {"_index": i}

                for field in base.fields:
                    ref = record[field.name]

                    if ref is None:
                        resolved[field.name] = None
                        continue

                    field_base = field.type_def.resolve_base_type()

                    if isinstance(field_base, EnumTypeDefinition):
                        if field_base.has_associated_values:
                            # Swift-style: ref is (disc, index) tuple — resolve from variant table
                            enum_val = self._resolve_swift_enum_ref(ref, field_base)
                            self._resolve_enum_associated_values(enum_val, field_base)
                            resolved[field.name] = enum_val
                        else:
                            # C-style: ref is already an EnumValue
                            self._resolve_enum_associated_values(ref, field_base)
                            resolved[field.name] = ref
                    elif isinstance(field_base, FractionTypeDefinition):
                        resolved[field.name] = _fraction_decode(self.storage, ref[0], ref[1], ref[2], ref[3])
                    elif isinstance(field_base, DictionaryTypeDefinition):
                        start_index, length = ref
                        if length == 0:
                            resolved[field.name] = {}
                        else:
                            arr_table = self.storage.get_array_table_for_type(field.type_def)
                            entry_type = field_base.entry_type
                            entry_base = entry_type.resolve_base_type()
                            result_dict = {}
                            for j in range(length):
                                entry_idx = arr_table.element_table.get(start_index + j)
                                entry_table = self.storage.get_table(entry_type.name)
                                entry_record = entry_table.get(entry_idx)
                                # Resolve key and value from entry record
                                key_ref = entry_record["key"]
                                val_ref = entry_record["value"]
                                key_val = self._resolve_entry_field(key_ref, entry_base.get_field("key"))
                                val_val = self._resolve_entry_field(val_ref, entry_base.get_field("value"))
                                result_dict[key_val] = val_val
                            resolved[field.name] = result_dict
                    elif isinstance(field_base, SetTypeDefinition):
                        start_index, length = ref
                        if length == 0:
                            resolved[field.name] = SetValue()
                        else:
                            arr_table = self.storage.get_array_table_for_type(field.type_def)
                            elements = [
                                arr_table.element_table.get(start_index + j)
                                for j in range(length)
                            ]
                            if is_string_type(field_base.element_type):
                                # String set elements: each element is (start, length) in char table
                                char_table = self.storage.get_array_table_for_type(field_base.element_type)
                                str_elements = []
                                for elem in elements:
                                    cs, cl = elem
                                    chars = [char_table.element_table.get(cs + k) for k in range(cl)]
                                    str_elements.append("".join(chars))
                                resolved[field.name] = SetValue(str_elements)
                            else:
                                resolved[field.name] = SetValue(elements)
                    elif isinstance(field_base, FractionTypeDefinition):
                        resolved[field.name] = _fraction_decode(self.storage, ref[0], ref[1], ref[2], ref[3])
                    elif isinstance(field_base, (BigIntTypeDefinition, BigUIntTypeDefinition)):
                        start_index, length = ref
                        if length == 0:
                            if isinstance(field_base, BigIntTypeDefinition):
                                resolved[field.name] = BigInt(0)
                            else:
                                resolved[field.name] = BigUInt(0)
                        else:
                            arr_table = self.storage.get_array_table_for_type(field.type_def)
                            elements = [arr_table.element_table.get(start_index + j) for j in range(length)]
                            signed = isinstance(field_base, BigIntTypeDefinition)
                            val = int.from_bytes(bytes(elements), 'little', signed=signed)
                            if signed:
                                resolved[field.name] = BigInt(val)
                            else:
                                resolved[field.name] = BigUInt(val)
                    elif isinstance(field_base, ArrayTypeDefinition):
                        start_index, length = ref
                        if length == 0:
                            resolved[field.name] = []
                        else:
                            arr_table = self.storage.get_array_table_for_type(field.type_def)
                            elements = [
                                arr_table.element_table.get(start_index + j)
                                for j in range(length)
                            ]
                            if is_string_type(field.type_def):
                                resolved[field.name] = "".join(elements)
                            elif is_string_type(field_base.element_type):
                                char_table = self.storage.get_array_table_for_type(field_base.element_type)
                                str_elements = []
                                for elem in elements:
                                    cs, cl = elem
                                    chars = [char_table.element_table.get(cs + k) for k in range(cl)]
                                    str_elements.append("".join(chars))
                                resolved[field.name] = str_elements
                            else:
                                resolved[field.name] = elements
                    elif isinstance(field_base, InterfaceTypeDefinition):
                        # ref is (type_id, index) tuple
                        type_id, idx = ref
                        concrete_name = self.registry.get_type_name_by_id(type_id)
                        if concrete_name:
                            resolved[field.name] = f"<{concrete_name}[{idx}]>"
                        else:
                            resolved[field.name] = f"<type_id={type_id}[{idx}]>"
                    elif isinstance(field_base, CompositeTypeDefinition):
                        resolved[field.name] = f"<{field.type_def.name}[{ref}]>"
                    else:
                        # Primitive — value is already inline
                        resolved[field.name] = bool(ref) if is_boolean_type(field.type_def) else ref

                yield resolved

        elif isinstance(base, InterfaceTypeDefinition):
            # Interface type — fan out across all implementing types
            yield from self._load_records_by_interface(type_name, base)

        elif isinstance(base, EnumTypeDefinition):
            # Enum type — scan all composites that contain this enum field type
            yield from self._load_records_by_enum_type(type_name, type_def)

        elif isinstance(base, StringTypeDefinition):
            # String type — scan all composites that contain string fields
            yield from self._load_records_by_field_type(type_name, type_def)

        elif isinstance(base, ArrayTypeDefinition):
            # Standalone array types no longer have header tables;
            # arrays are accessed through composites only
            return

        else:
            # Primitive/alias type — scan all composites that contain this field type
            yield from self._load_records_by_field_type(type_name, type_def)

    def _resolve_entry_field(self, ref: Any, field_def: FieldDefinition) -> Any:
        """Resolve a single field from a dict entry composite (key or value)."""
        if ref is None:
            return None
        field_base = field_def.type_def.resolve_base_type()
        if isinstance(field_base, ArrayTypeDefinition):
            start_index, length = ref
            if length == 0:
                return [] if not is_string_type(field_def.type_def) else ""
            arr_table = self.storage.get_array_table_for_type(field_def.type_def)
            elements = [arr_table.element_table.get(start_index + j) for j in range(length)]
            if is_string_type(field_def.type_def):
                return "".join(elements)
            if is_string_type(field_base.element_type):
                char_table = self.storage.get_array_table_for_type(field_base.element_type)
                str_elements = []
                for elem in elements:
                    cs, cl = elem
                    chars = [char_table.element_table.get(cs + k) for k in range(cl)]
                    str_elements.append("".join(chars))
                return str_elements
            return elements
        if isinstance(field_base, CompositeTypeDefinition):
            return f"<{field_def.type_def.name}[{ref}]>"
        if is_boolean_type(field_def.type_def):
            return bool(ref)
        return ref

    def _load_records_by_interface(
        self, interface_name: str, interface_def: InterfaceTypeDefinition
    ) -> Iterator[dict[str, Any]]:
        """Load records from all types implementing an interface.

        Returns records with interface fields + _type and _index columns.
        """
        interface_field_names = [f.name for f in interface_def.fields]
        impl_types = self.registry.find_implementing_types(interface_name)

        for impl_name, impl_def in impl_types:
            table_file = self.storage.data_dir / f"{impl_name}.bin"
            if not table_file.exists():
                continue

            table = self.storage.get_table(impl_name)
            for i in range(table.count):
                if table.is_deleted(i):
                    continue

                record = table.get(i)
                resolved: dict[str, Any] = {"_type": impl_name, "_index": i}

                for field in impl_def.fields:
                    if field.name not in interface_field_names:
                        continue

                    ref = record[field.name]
                    if ref is None:
                        resolved[field.name] = None
                        continue

                    field_base = field.type_def.resolve_base_type()

                    if isinstance(field_base, EnumTypeDefinition):
                        if field_base.has_associated_values:
                            enum_val = self._resolve_swift_enum_ref(ref, field_base)
                            self._resolve_enum_associated_values(enum_val, field_base)
                            resolved[field.name] = enum_val
                        else:
                            self._resolve_enum_associated_values(ref, field_base)
                            resolved[field.name] = ref
                    elif isinstance(field_base, FractionTypeDefinition):
                        resolved[field.name] = _fraction_decode(self.storage, ref[0], ref[1], ref[2], ref[3])
                    elif isinstance(field_base, (BigIntTypeDefinition, BigUIntTypeDefinition)):
                        start_index, length = ref
                        if length == 0:
                            if isinstance(field_base, BigIntTypeDefinition):
                                resolved[field.name] = BigInt(0)
                            else:
                                resolved[field.name] = BigUInt(0)
                        else:
                            arr_table = self.storage.get_array_table_for_type(field.type_def)
                            elements = [arr_table.element_table.get(start_index + j) for j in range(length)]
                            signed = isinstance(field_base, BigIntTypeDefinition)
                            val = int.from_bytes(bytes(elements), 'little', signed=signed)
                            if signed:
                                resolved[field.name] = BigInt(val)
                            else:
                                resolved[field.name] = BigUInt(val)
                    elif isinstance(field_base, ArrayTypeDefinition):
                        start_index, length = ref
                        if length == 0:
                            resolved[field.name] = []
                        else:
                            arr_table = self.storage.get_array_table_for_type(field.type_def)
                            elements = [
                                arr_table.element_table.get(start_index + j)
                                for j in range(length)
                            ]
                            if is_string_type(field.type_def):
                                resolved[field.name] = "".join(elements)
                            elif is_string_type(field_base.element_type):
                                char_table = self.storage.get_array_table_for_type(field_base.element_type)
                                str_elements = []
                                for elem in elements:
                                    cs, cl = elem
                                    chars = [char_table.element_table.get(cs + k) for k in range(cl)]
                                    str_elements.append("".join(chars))
                                resolved[field.name] = str_elements
                            else:
                                resolved[field.name] = elements
                    elif isinstance(field_base, CompositeTypeDefinition):
                        resolved[field.name] = f"<{field.type_def.name}[{ref}]>"
                    else:
                        resolved[field.name] = bool(ref) if is_boolean_type(field.type_def) else ref

                yield resolved

    def _load_records_by_field_type(
        self, type_name: str, type_def: TypeDefinition
    ) -> Iterator[dict[str, Any]]:
        """Load values of a non-composite type by scanning composites that use it.

        Returns records with _source, _index, _field, and the type_name value.
        """
        matches = self.registry.find_composites_with_field_type(type_name)
        if not matches:
            return

        is_string = isinstance(type_def.resolve_base_type(), StringTypeDefinition)
        is_frac = isinstance(type_def.resolve_base_type(), FractionTypeDefinition)
        is_bi = isinstance(type_def.resolve_base_type(), BigIntTypeDefinition)
        is_bu = isinstance(type_def.resolve_base_type(), BigUIntTypeDefinition)

        for comp_name, field_name, comp_def in matches:
            table_file = self.storage.data_dir / f"{comp_name}.bin"
            if not table_file.exists():
                continue
            field_def = next(f for f in comp_def.fields if f.name == field_name)
            table = self.storage.get_table(comp_name)
            for i in range(table.count):
                if table.is_deleted(i):
                    continue
                record = table.get(i)
                ref = record[field_name]
                if ref is None:
                    continue
                if is_frac:
                    value = _fraction_decode(self.storage, ref[0], ref[1], ref[2], ref[3])
                elif is_bi or is_bu:
                    start_index, length = ref
                    if length == 0:
                        value = BigInt(0) if is_bi else BigUInt(0)
                    else:
                        arr_table = self.storage.get_array_table_for_type(field_def.type_def)
                        elements = [arr_table.element_table.get(start_index + j) for j in range(length)]
                        val = int.from_bytes(bytes(elements), 'little', signed=is_bi)
                        value = BigInt(val) if is_bi else BigUInt(val)
                elif is_string:
                    start_index, length = ref
                    if length == 0:
                        value = ""
                    else:
                        arr_table = self.storage.get_array_table_for_type(field_def.type_def)
                        value = "".join(
                            arr_table.element_table.get(start_index + j)
                            for j in range(length)
                        )
                else:
                    value = bool(ref) if is_boolean_type(field_def.type_def) else ref
                yield {
                    "_source": comp_name,
                    "_index": i,
                    "_field": field_name,
                    type_name: value,
                    "_value": value,
                }

    def _load_records_by_enum_type(
        self, type_name: str, type_def: TypeDefinition,
        variant_filter: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Load records for an enum type by scanning composites that use it.

        Returns records with _source, _index, _field, _variant, and value columns.
        If variant_filter is set, only matching variants are included and
        associated value fields become columns.
        """
        enum_base = type_def.resolve_base_type()
        if not isinstance(enum_base, EnumTypeDefinition):
            return

        matches = self.registry.find_composites_with_field_type(type_name)
        if not matches:
            return

        for comp_name, field_name, comp_def in matches:
            table_file = self.storage.data_dir / f"{comp_name}.bin"
            if not table_file.exists():
                continue
            table = self.storage.get_table(comp_name)
            for i in range(table.count):
                if table.is_deleted(i):
                    continue
                record = table.get(i)
                ref = record[field_name]
                if ref is None:
                    continue

                # Resolve to EnumValue
                if enum_base.has_associated_values:
                    if not isinstance(ref, tuple):
                        continue
                    ev = self._resolve_swift_enum_ref(ref, enum_base)
                else:
                    if not isinstance(ref, EnumValue):
                        continue
                    ev = ref

                if variant_filter and ev.variant_name != variant_filter:
                    continue

                if variant_filter:
                    # Variant query: associated values as columns
                    row: dict[str, Any] = {
                        "_source": comp_name,
                        "_index": i,
                        "_field": field_name,
                    }
                    variant = enum_base.get_variant(ev.variant_name)
                    if variant:
                        for vf in variant.fields:
                            fval = ev.fields.get(vf.name)
                            vf_base = vf.type_def.resolve_base_type()
                            if isinstance(vf_base, ArrayTypeDefinition) and isinstance(fval, tuple):
                                start_index, length = fval
                                if length == 0:
                                    row[vf.name] = []
                                else:
                                    arr_table = self.storage.get_array_table_for_type(vf.type_def)
                                    elements = [
                                        arr_table.element_table.get(start_index + j)
                                        for j in range(length)
                                    ]
                                    if is_string_type(vf.type_def):
                                        row[vf.name] = "".join(elements)
                                    else:
                                        row[vf.name] = elements
                            else:
                                row[vf.name] = bool(fval) if is_boolean_type(vf.type_def) else fval
                    yield row
                else:
                    # Overview query: formatted value string
                    # Resolve associated values first
                    self._resolve_enum_associated_values(ev, enum_base)
                    if ev.fields:
                        field_strs = []
                        variant = enum_base.get_variant(ev.variant_name)
                        if variant:
                            for vf in variant.fields:
                                fval = ev.fields.get(vf.name)
                                if fval is not None:
                                    field_strs.append(f"{vf.name}={fval}")
                        value_str = f"{ev.variant_name}({', '.join(field_strs)})"
                    else:
                        value_str = ev.variant_name
                    yield {
                        "_source": comp_name,
                        "_index": i,
                        "_field": field_name,
                        "_variant": ev.variant_name,
                        "value": value_str,
                    }

    def _load_records_by_indices(
        self, type_name: str, type_def: TypeDefinition, indices: list[int]
    ) -> Iterator[dict[str, Any]]:
        """Load specific records by index from a composite table."""
        base = type_def.resolve_base_type()

        if not isinstance(base, CompositeTypeDefinition):
            return

        table = self.storage.get_table(type_name)
        for i in indices:
            if i >= table.count:
                continue
            if table.is_deleted(i):
                continue

            record = table.get(i)
            resolved = {"_index": i}

            for field in base.fields:
                ref = record[field.name]

                if ref is None:
                    resolved[field.name] = None
                    continue

                field_base = field.type_def.resolve_base_type()

                if isinstance(field_base, EnumTypeDefinition):
                    if field_base.has_associated_values:
                        enum_val = self._resolve_swift_enum_ref(ref, field_base)
                        self._resolve_enum_associated_values(enum_val, field_base)
                        resolved[field.name] = enum_val
                    else:
                        self._resolve_enum_associated_values(ref, field_base)
                        resolved[field.name] = ref
                elif isinstance(field_base, FractionTypeDefinition):
                    resolved[field.name] = _fraction_decode(self.storage, ref[0], ref[1], ref[2], ref[3])
                elif isinstance(field_base, (BigIntTypeDefinition, BigUIntTypeDefinition)):
                    start_index, length = ref
                    if length == 0:
                        if isinstance(field_base, BigIntTypeDefinition):
                            resolved[field.name] = BigInt(0)
                        else:
                            resolved[field.name] = BigUInt(0)
                    else:
                        arr_table = self.storage.get_array_table_for_type(field.type_def)
                        elements = [arr_table.element_table.get(start_index + j) for j in range(length)]
                        signed = isinstance(field_base, BigIntTypeDefinition)
                        val = int.from_bytes(bytes(elements), 'little', signed=signed)
                        if signed:
                            resolved[field.name] = BigInt(val)
                        else:
                            resolved[field.name] = BigUInt(val)
                elif isinstance(field_base, ArrayTypeDefinition):
                    start_index, length = ref
                    if length == 0:
                        resolved[field.name] = []
                    else:
                        arr_table = self.storage.get_array_table_for_type(field.type_def)
                        elements = [
                            arr_table.element_table.get(start_index + j)
                            for j in range(length)
                        ]
                        if is_string_type(field.type_def):
                            resolved[field.name] = "".join(elements)
                        elif is_string_type(field_base.element_type):
                            char_table = self.storage.get_array_table_for_type(field_base.element_type)
                            str_elements = []
                            for elem in elements:
                                cs, cl = elem
                                chars = [char_table.element_table.get(cs + k) for k in range(cl)]
                                str_elements.append("".join(chars))
                            resolved[field.name] = str_elements
                        else:
                            resolved[field.name] = elements
                elif isinstance(field_base, InterfaceTypeDefinition):
                    type_id, idx = ref
                    concrete_name = self.registry.get_type_name_by_id(type_id)
                    if concrete_name:
                        resolved[field.name] = f"<{concrete_name}[{idx}]>"
                    else:
                        resolved[field.name] = f"<type_id={type_id}[{idx}]>"
                elif isinstance(field_base, CompositeTypeDefinition):
                    resolved[field.name] = f"<{field.type_def.name}[{ref}]>"
                else:
                    # Primitive — value is already inline
                    resolved[field.name] = bool(ref) if is_boolean_type(field.type_def) else ref

            yield resolved

    def _evaluate_condition(
        self, record: dict[str, Any], condition: Condition | CompoundCondition
    ) -> bool:
        """Evaluate a condition against a record."""
        if isinstance(condition, CompoundCondition):
            left = self._evaluate_condition(record, condition.left)
            right = self._evaluate_condition(record, condition.right)
            if condition.operator == "and":
                return left and right
            else:  # or
                return left or right

        field_value = record.get(condition.field)
        if condition.method_chain is not None:
            for mc in condition.method_chain:
                field_value = self._apply_projection_method(field_value, mc.method_name, mc.method_args)
        elif condition.method_name is not None:
            field_value = self._apply_projection_method(field_value, condition.method_name, condition.method_args)
        if field_value is None:
            # Allow null comparisons: field = null, field != null
            if isinstance(condition.value, NullValue):
                result = condition.operator == "eq"
                return not result if condition.negate else result
            return condition.negate

        result = self._compare(field_value, condition.operator, condition.value)
        return not result if condition.negate else result

    def _compare(self, field_value: Any, operator: str, value: Any) -> bool:
        """Compare a field value against a condition value."""
        try:
            # Resolve EnumValueExpr against the actual field value's type
            if isinstance(field_value, EnumValue) and isinstance(value, EnumValueExpr):
                value = self._resolve_and_expand_enum_expr(value, field_value)
            # Enum-aware comparison
            if isinstance(field_value, EnumValue) and isinstance(value, EnumValue):
                eq = self._enum_values_equal(field_value, value)
                if operator == "eq":
                    return eq
                elif operator == "neq":
                    return not eq
                return False
            if isinstance(value, NullValue):
                if operator == "eq":
                    return field_value is None
                elif operator == "neq":
                    return field_value is not None
                return False
            if operator == "eq":
                return field_value == value
            elif operator == "neq":
                return field_value != value
            elif operator == "lt":
                return field_value < value
            elif operator == "lte":
                return field_value <= value
            elif operator == "gt":
                return field_value > value
            elif operator == "gte":
                return field_value >= value
            elif operator == "starts_with":
                if isinstance(field_value, str):
                    return field_value.startswith(value)
                elif isinstance(field_value, list):
                    str_val = "".join(str(v) for v in field_value)
                    return str_val.startswith(value)
                return False
            elif operator == "matches":
                if isinstance(field_value, str):
                    return bool(re.search(value, field_value))
                elif isinstance(field_value, list):
                    str_val = "".join(str(v) for v in field_value)
                    return bool(re.search(value, str_val))
                return False
        except (TypeError, ValueError):
            return False

        return False

    def _format_single_method_call(self, mc_name: str, mc_args: list[Any] | None) -> str:
        """Format a single method call with args."""
        if mc_args:
            arg_strs = []
            for a in mc_args:
                if isinstance(a, EnumValueExpr) and a.enum_name is None:
                    arg_strs.append(f".{a.variant_name}")
                elif isinstance(a, SortKeyExpr):
                    if a.field_name:
                        arg_strs.append(f".{a.field_name}" + (" desc" if a.descending else ""))
                    else:
                        arg_strs.append("desc" if a.descending else "asc")
                elif isinstance(a, SetLiteral):
                    if not a.elements:
                        arg_strs.append("{,}")
                    else:
                        elems = ", ".join(f'"{e}"' if isinstance(e, str) else str(e) for e in a.elements)
                        arg_strs.append(f"{{{elems}}}")
                elif isinstance(a, DictLiteral):
                    if not a.entries:
                        arg_strs.append("{:}")
                    else:
                        entries = ", ".join(
                            f'"{e.key}": {e.value}' if isinstance(e.key, str)
                            else f'{e.key}: {e.value}' for e in a.entries
                        )
                        arg_strs.append(f"{{{entries}}}")
                elif isinstance(a, EmptyBraces):
                    arg_strs.append("{}")
                elif isinstance(a, str):
                    arg_strs.append(f'"{a}"')
                else:
                    arg_strs.append(str(a))
            return f"{mc_name}({', '.join(arg_strs)})"
        return f"{mc_name}()"

    def _format_method_col_name(self, field: Any) -> str:
        """Format a column name for a method call field."""
        if field.method_chain is not None:
            parts = [self._format_single_method_call(mc.method_name, mc.method_args)
                     for mc in field.method_chain]
            return f"{field.name}.{'.'.join(parts)}"
        return f"{field.name}.{self._format_single_method_call(field.method_name, field.method_args)}"

    def _apply_array_method(self, value: Any, method_name: str, args: list[Any] | None) -> Any:
        """Apply an array method to a value."""
        if method_name == "length":
            if isinstance(value, (list, str, dict)):
                return len(value)
            return 0 if value is None else None
        elif method_name == "isEmpty":
            if isinstance(value, (list, str, dict)):
                return len(value) == 0
            return True if value is None else None
        elif method_name == "contains":
            if args is None or len(args) != 1:
                raise RuntimeError("contains() requires exactly 1 argument")
            if value is None:
                return False
            search = self._resolve_instance_value(args[0]) if not isinstance(args[0], (int, float, str)) else args[0]
            if isinstance(value, dict):
                return search in value
            if isinstance(value, str):
                return isinstance(search, str) and search in value
            if isinstance(value, list):
                for elem in value:
                    if elem == search:
                        return True
                return False
            return False
        elif method_name in ("min", "max"):
            if value is None or not isinstance(value, list) or len(value) == 0:
                return None
            builtin = min if method_name == "min" else max
            if not args:
                nums = [v for v in value if isinstance(v, (int, float))]
                return builtin(nums) if nums else None
            # With .field key: composite array
            key_field = None
            if isinstance(args[0], EnumValueExpr) and args[0].enum_name is None:
                key_field = args[0].variant_name
            elif isinstance(args[0], SortKeyExpr) and args[0].field_name:
                key_field = args[0].field_name
            if key_field:
                nums = [(elem.get(key_field), elem) for elem in value
                        if isinstance(elem, dict) and isinstance(elem.get(key_field), (int, float))]
                return builtin(nums, key=lambda x: x[0])[0] if nums else None
            return None
        else:
            raise RuntimeError(f"Unknown array method: {method_name}()")

    def _sort_resolved_elements(
        self, elements: list[Any], sort_keys: list[tuple[str | None, bool]]
    ) -> list[Any]:
        """Sort already-resolved Python values (strings are strings, not raw tuples)."""
        result = list(elements)
        for field_name, descending in reversed(sort_keys):
            def make_key(fn: str | None) -> Any:
                def key_fn(elem: Any) -> tuple:
                    if fn is not None:
                        val = elem.get(fn) if isinstance(elem, dict) else None
                    else:
                        val = elem
                    if val is None:
                        return (1, "")
                    elif isinstance(val, (int, float)):
                        return (0, val)
                    elif isinstance(val, list):
                        return (0, "".join(str(c) for c in val))
                    else:
                        return (0, str(val))
                return key_fn
            result.sort(key=make_key(field_name), reverse=descending)
        return result

    def _resolve_projection_arg(self, arg: Any) -> Any:
        """Resolve a method argument for use in projection context (no storage writes)."""
        if isinstance(arg, (int, float, str)):
            return arg
        if isinstance(arg, NullValue):
            return None
        if isinstance(arg, list):
            return [self._resolve_projection_arg(a) for a in arg]
        if isinstance(arg, SetLiteral):
            return SetValue([self._resolve_projection_arg(e) for e in arg.elements])
        if isinstance(arg, DictLiteral):
            return {self._resolve_projection_arg(e.key): self._resolve_projection_arg(e.value) for e in arg.entries}
        if isinstance(arg, EmptyBraces):
            return SetValue()
        raise RuntimeError(f"Projection methods only support literal arguments, got {type(arg).__name__}")

    def _wrap_set_if_needed(self, value: Any, was_set: bool) -> Any:
        """Wrap a list result back into SetValue if the input was a set."""
        if was_set and isinstance(value, list) and not isinstance(value, SetValue):
            return SetValue(value)
        return value

    def _apply_string_method(self, value: str, method_name: str, args: list[Any] | None) -> Any:
        """Apply a string-only method. Assumes value is already a str."""
        resolved_args = [self._resolve_projection_arg(a) for a in args] if args else []

        if method_name == "uppercase":
            return value.upper()
        elif method_name == "lowercase":
            return value.lower()
        elif method_name == "capitalize":
            return value.capitalize()
        elif method_name == "trim":
            return value.strip()
        elif method_name == "trimStart":
            return value.lstrip()
        elif method_name == "trimEnd":
            return value.rstrip()
        elif method_name == "startsWith":
            if len(resolved_args) != 1:
                raise RuntimeError("startsWith() requires exactly 1 argument")
            return value.startswith(str(resolved_args[0]))
        elif method_name == "endsWith":
            if len(resolved_args) != 1:
                raise RuntimeError("endsWith() requires exactly 1 argument")
            return value.endswith(str(resolved_args[0]))
        elif method_name == "indexOf":
            if len(resolved_args) != 1:
                raise RuntimeError("indexOf() requires exactly 1 argument")
            return value.find(str(resolved_args[0]))
        elif method_name == "lastIndexOf":
            if len(resolved_args) != 1:
                raise RuntimeError("lastIndexOf() requires exactly 1 argument")
            return value.rfind(str(resolved_args[0]))
        elif method_name == "padStart":
            if len(resolved_args) < 1 or len(resolved_args) > 2:
                raise RuntimeError("padStart() requires 1 or 2 arguments: length[, char]")
            pad_len = int(resolved_args[0])
            pad_char = str(resolved_args[1]) if len(resolved_args) > 1 else " "
            if len(pad_char) != 1:
                raise RuntimeError("padStart() pad character must be a single character")
            return value.rjust(pad_len, pad_char)
        elif method_name == "padEnd":
            if len(resolved_args) < 1 or len(resolved_args) > 2:
                raise RuntimeError("padEnd() requires 1 or 2 arguments: length[, char]")
            pad_len = int(resolved_args[0])
            pad_char = str(resolved_args[1]) if len(resolved_args) > 1 else " "
            if len(pad_char) != 1:
                raise RuntimeError("padEnd() pad character must be a single character")
            return value.ljust(pad_len, pad_char)
        elif method_name == "repeat":
            if len(resolved_args) != 1:
                raise RuntimeError("repeat() requires exactly 1 argument")
            n = int(resolved_args[0])
            if n < 0:
                raise RuntimeError("repeat() count must be non-negative")
            return value * n
        elif method_name == "split":
            if len(resolved_args) != 1:
                raise RuntimeError("split() requires exactly 1 argument: delimiter")
            return value.split(str(resolved_args[0]))
        elif method_name == "match":
            if len(resolved_args) != 1:
                raise RuntimeError("match() requires exactly 1 argument: pattern")
            pattern = str(resolved_args[0])
            m = re.search(pattern, value)
            if m is None:
                return None
            return [m.group(0)] + list(m.groups())
        else:
            raise RuntimeError(f"Unknown string method: {method_name}()")

    def _apply_projection_method(self, value: Any, method_name: str, args: list[Any] | None) -> Any:
        """Apply a method as an immutable projection (returns copy, no storage writes)."""
        # Read-only methods → delegate
        if method_name in ("length", "isEmpty", "contains", "min", "max"):
            return self._apply_array_method(value, method_name, args)

        # Set-specific methods
        if isinstance(value, SetValue):
            if method_name == "add":
                if not args or len(args) != 1:
                    raise RuntimeError("add() requires exactly 1 argument")
                elem = self._resolve_projection_arg(args[0])
                if elem not in value:
                    return SetValue(list(value) + [elem])
                return SetValue(list(value))
            elif method_name == "union":
                if not args or len(args) != 1:
                    raise RuntimeError("union() requires exactly 1 argument")
                other = self._resolve_projection_arg(args[0])
                if not isinstance(other, (list, SetValue)):
                    raise RuntimeError("union() argument must be a set or list")
                result = list(value)
                for elem in other:
                    if elem not in result:
                        result.append(elem)
                return SetValue(result)
            elif method_name == "intersect":
                if not args or len(args) != 1:
                    raise RuntimeError("intersect() requires exactly 1 argument")
                other = self._resolve_projection_arg(args[0])
                if not isinstance(other, (list, SetValue)):
                    raise RuntimeError("intersect() argument must be a set or list")
                other_list = list(other)
                return SetValue([e for e in value if e in other_list])
            elif method_name == "difference":
                if not args or len(args) != 1:
                    raise RuntimeError("difference() requires exactly 1 argument")
                other = self._resolve_projection_arg(args[0])
                if not isinstance(other, (list, SetValue)):
                    raise RuntimeError("difference() argument must be a set or list")
                other_list = list(other)
                return SetValue([e for e in value if e not in other_list])
            elif method_name == "symmetric_difference":
                if not args or len(args) != 1:
                    raise RuntimeError("symmetric_difference() requires exactly 1 argument")
                other = self._resolve_projection_arg(args[0])
                if not isinstance(other, (list, SetValue)):
                    raise RuntimeError("symmetric_difference() argument must be a set or list")
                other_list = list(other)
                result = [e for e in value if e not in other_list]
                result += [e for e in other_list if e not in list(value)]
                return SetValue(result)
            # Fall through for methods shared with list (sort, reverse, etc.)

        # Dict-specific methods
        if isinstance(value, dict):
            if method_name == "hasKey":
                if not args or len(args) != 1:
                    raise RuntimeError("hasKey() requires exactly 1 argument")
                key = self._resolve_projection_arg(args[0])
                return key in value
            elif method_name == "keys":
                return SetValue(list(value.keys()))
            elif method_name == "values":
                return list(value.values())
            elif method_name == "entries":
                return [{"key": k, "value": v} for k, v in value.items()]
            elif method_name == "remove":
                if not args or len(args) != 1:
                    raise RuntimeError("remove() requires exactly 1 argument")
                key = self._resolve_projection_arg(args[0])
                return {k: v for k, v in value.items() if k != key}
            else:
                raise RuntimeError(f"Unknown dict method: {method_name}()")

        # String-only methods
        if method_name in _STRING_ONLY_METHODS:
            if isinstance(value, str):
                return self._apply_string_method(value, method_name, args)
            if value is None:
                return None
            raise RuntimeError(f"{method_name}() can only be applied to string values")

        # Track whether input was a SetValue for wrapping results
        was_set = isinstance(value, SetValue)

        # Null handling: most projections on None return None
        if value is None:
            if method_name in ("append", "prepend"):
                # append/prepend on null → create new list from args
                if not args:
                    raise RuntimeError(f"{method_name}() requires at least 1 argument")
                return [self._resolve_projection_arg(a) for a in args]
            return None

        if method_name == "sort":
            if isinstance(value, str):
                return "".join(sorted(value))
            if not isinstance(value, list):
                return value
            is_composite = any(isinstance(e, dict) for e in value)
            sort_keys = self._parse_sort_keys(args or [], is_composite)
            if isinstance(sort_keys, UpdateResult):
                raise RuntimeError(sort_keys.message)
            return self._wrap_set_if_needed(self._sort_resolved_elements(value, sort_keys), was_set)

        elif method_name == "reverse":
            if isinstance(value, list):
                return self._wrap_set_if_needed(list(reversed(value)), was_set)
            elif isinstance(value, str):
                return value[::-1]
            return value

        elif method_name == "append":
            if not args:
                raise RuntimeError("append() requires at least 1 argument")
            resolved = [self._resolve_projection_arg(a) for a in args]
            # Flatten: if arg is a list, extend rather than nest
            new_elements = []
            for r in resolved:
                if isinstance(r, list):
                    new_elements.extend(r)
                else:
                    new_elements.append(r)
            if isinstance(value, list):
                return self._wrap_set_if_needed(list(value) + new_elements, was_set)
            elif isinstance(value, str):
                return value + "".join(str(e) for e in new_elements)
            return value

        elif method_name == "prepend":
            if not args:
                raise RuntimeError("prepend() requires at least 1 argument")
            resolved = [self._resolve_projection_arg(a) for a in args]
            new_elements = []
            for r in resolved:
                if isinstance(r, list):
                    new_elements.extend(r)
                else:
                    new_elements.append(r)
            if isinstance(value, list):
                return self._wrap_set_if_needed(new_elements + list(value), was_set)
            elif isinstance(value, str):
                return "".join(str(e) for e in new_elements) + value
            return value

        elif method_name == "insert":
            if not args or len(args) < 2:
                raise RuntimeError("insert() requires at least 2 arguments: index, value(s)")
            idx = self._resolve_projection_arg(args[0])
            if not isinstance(idx, int):
                raise RuntimeError(f"insert() index must be an integer, got {type(idx).__name__}")
            new_elements = [self._resolve_projection_arg(a) for a in args[1:]]
            if isinstance(value, str):
                insert_str = "".join(str(e) for e in new_elements)
                return value[:idx] + insert_str + value[idx:]
            if isinstance(value, list):
                return self._wrap_set_if_needed(list(value[:idx]) + new_elements + list(value[idx:]), was_set)
            return value

        elif method_name == "delete":
            if not args or len(args) != 1:
                raise RuntimeError("delete() requires exactly 1 argument: index")
            idx = self._resolve_projection_arg(args[0])
            if not isinstance(idx, int):
                raise RuntimeError(f"delete() index must be an integer, got {type(idx).__name__}")
            if isinstance(value, str):
                if idx < 0 or idx >= len(value):
                    raise RuntimeError(f"delete() index {idx} out of range for string of length {len(value)}")
                return value[:idx] + value[idx + 1:]
            if isinstance(value, list):
                if idx < 0 or idx >= len(value):
                    raise RuntimeError(f"delete() index {idx} out of range for array of length {len(value)}")
                return self._wrap_set_if_needed(list(value[:idx]) + list(value[idx + 1:]), was_set)
            return value

        elif method_name == "remove":
            if not args or len(args) != 1:
                raise RuntimeError("remove() requires exactly 1 argument")
            search = self._resolve_projection_arg(args[0])
            if isinstance(value, str):
                idx = value.find(str(search))
                if idx >= 0:
                    return value[:idx] + value[idx + len(str(search)):]
                return value
            if isinstance(value, list):
                result = list(value)
                for i, elem in enumerate(result):
                    if elem == search:
                        return self._wrap_set_if_needed(result[:i] + result[i + 1:], was_set)
                return self._wrap_set_if_needed(result, was_set)
            return value

        elif method_name == "removeAll":
            if not args or len(args) != 1:
                raise RuntimeError("removeAll() requires exactly 1 argument")
            search = self._resolve_projection_arg(args[0])
            if isinstance(value, str):
                return value.replace(str(search), "")
            if isinstance(value, list):
                return self._wrap_set_if_needed([e for e in value if e != search], was_set)
            return value

        elif method_name == "replace":
            if not args or len(args) != 2:
                raise RuntimeError("replace() requires exactly 2 arguments: old, new")
            old = self._resolve_projection_arg(args[0])
            new = self._resolve_projection_arg(args[1])
            if isinstance(value, str):
                return value.replace(str(old), str(new), 1)
            if isinstance(value, list):
                result = list(value)
                for i, elem in enumerate(result):
                    if elem == old:
                        result[i] = new
                        return self._wrap_set_if_needed(result, was_set)
                return self._wrap_set_if_needed(result, was_set)
            return value

        elif method_name == "replaceAll":
            if not args or len(args) != 2:
                raise RuntimeError("replaceAll() requires exactly 2 arguments: old, new")
            old = self._resolve_projection_arg(args[0])
            new = self._resolve_projection_arg(args[1])
            if isinstance(value, str):
                return value.replace(str(old), str(new))
            if isinstance(value, list):
                return self._wrap_set_if_needed([new if e == old else e for e in value], was_set)
            return value

        elif method_name == "swap":
            if not args or len(args) != 2:
                raise RuntimeError("swap() requires exactly 2 arguments: index_a, index_b")
            a = self._resolve_projection_arg(args[0])
            b = self._resolve_projection_arg(args[1])
            if not isinstance(a, int) or not isinstance(b, int):
                raise RuntimeError("swap() indices must be integers")
            if isinstance(value, str):
                if a < 0 or a >= len(value) or b < 0 or b >= len(value):
                    raise RuntimeError(f"swap() index out of range for string of length {len(value)}")
                chars = list(value)
                chars[a], chars[b] = chars[b], chars[a]
                return "".join(chars)
            if isinstance(value, list):
                if a < 0 or a >= len(value) or b < 0 or b >= len(value):
                    raise RuntimeError(f"swap() index out of range for array of length {len(value)}")
                result = list(value)
                result[a], result[b] = result[b], result[a]
                return result
            return value

        else:
            raise RuntimeError(f"Unknown array method: {method_name}()")

    def _resolve_and_expand_enum_expr(self, expr: EnumValueExpr, field_value: EnumValue) -> EnumValue:
        """Resolve an EnumValueExpr by inferring the enum type from a field's EnumValue."""
        # Find the enum type from the registry using the field_value's variant
        enum_name = expr.enum_name
        if enum_name is None:
            # Shorthand: search registry for an enum type that has this variant
            for name in self.registry.list_types():
                td = self.registry.get(name)
                base = td.resolve_base_type()
                if isinstance(base, EnumTypeDefinition) and base.get_variant(expr.variant_name):
                    # Verify this enum also has the field_value's variant (same enum)
                    if base.get_variant(field_value.variant_name):
                        enum_name = name
                        break
        if enum_name is None:
            return EnumValue(variant_name=expr.variant_name, discriminant=-1)
        resolved = self._resolve_enum_value_expr(expr, enum_name)
        # For Swift-style, _resolve_enum_value_expr returns (disc, index) tuple —
        # resolve to EnumValue for comparison
        enum_type = self.registry.get(enum_name)
        if enum_type is not None:
            enum_base = enum_type.resolve_base_type()
            if isinstance(enum_base, EnumTypeDefinition):
                if isinstance(resolved, tuple):
                    resolved = self._resolve_swift_enum_ref(resolved, enum_base)
                self._resolve_enum_associated_values(resolved, enum_base)
        return resolved

    def _enum_values_equal(self, a: EnumValue, b: EnumValue) -> bool:
        """Compare two EnumValue objects by variant name and resolved fields."""
        if a.variant_name != b.variant_name:
            return False
        if not a.fields and not b.fields:
            return True
        return a.fields == b.fields

    def _apply_group_by(
        self, records: list[dict[str, Any]], group_by: list[str]
    ) -> list[dict[str, Any]]:
        """Apply GROUP BY clause."""
        groups: dict[tuple, list[dict[str, Any]]] = {}

        for record in records:
            key = tuple(record.get(f) for f in group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(record)

        # Return one record per group (the one with lowest _index)
        result = []
        for key, group_records in groups.items():
            # Sort by _index and take first
            group_records.sort(key=lambda r: r.get("_index", 0))
            representative = group_records[0].copy()
            representative["_group_count"] = len(group_records)
            representative["_group_records"] = group_records
            result.append(representative)

        return result

    def _apply_sort_by(
        self, records: list[dict[str, Any]], sort_fields: list[str]
    ) -> list[dict[str, Any]]:
        """Apply SORT BY clause."""

        def sort_key(record: dict[str, Any]) -> tuple:
            values = []
            for field in sort_fields:
                val = record.get(field)
                # Handle None and make sortable
                if val is None:
                    values.append((0, ""))
                elif isinstance(val, (int, float)):
                    values.append((1, val))
                else:
                    values.append((2, str(val)))
            return tuple(values)

        return sorted(records, key=sort_key)

    def _select_fields(
        self,
        records: list[dict[str, Any]],
        query: SelectQuery,
        type_def: TypeDefinition,
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """Select specific fields from records."""
        base = type_def.resolve_base_type()

        # Determine columns
        if len(query.fields) == 1 and query.fields[0].name == "*" and query.fields[0].aggregate is None:
            # SELECT * - get all fields
            if isinstance(base, CompositeTypeDefinition):
                columns = ["_index"] + [f.name for f in base.fields]
            elif records and "_source" in records[0]:
                # Type-based query: include referent columns
                columns = ["_source", "_index", "_field", "_value"]
            else:
                columns = ["_index", "_value"]
        else:
            columns = []
            for field in query.fields:
                if field.aggregate:
                    columns.append(f"{field.aggregate}({field.name})")
                elif field.array_index is not None and field.post_path is not None:
                    post = ".".join(field.post_path)
                    columns.append(f"{field.name}[{self._format_array_index(field.array_index)}].{post}")
                elif field.array_index is not None:
                    columns.append(f"{field.name}[{self._format_array_index(field.array_index)}]")
                elif field.method_chain is not None or field.method_name is not None:
                    columns.append(self._format_method_col_name(field))
                else:
                    columns.append(field.name)

        # Check for aggregates without GROUP BY
        has_aggregate = any(f.aggregate for f in query.fields)
        if has_aggregate and not query.group_by:
            # Aggregate over all records
            return self._compute_global_aggregates(records, query.fields)

        # Project records to selected columns
        rows = []
        for record in records:
            row = {}
            for field in query.fields:
                if field.aggregate:
                    col_name = f"{field.aggregate}({field.name})"
                    if field.aggregate == "count":
                        row[col_name] = record.get("_group_count", 1)
                    else:
                        group_records = record.get("_group_records", [record])
                        row[col_name] = self._compute_aggregate(
                            group_records, field.name, field.aggregate
                        )
                elif field.name == "*":
                    row.update(record)
                else:
                    # Handle dotted paths like "address.state"
                    value = self._resolve_field_path(record, field.path, type_def)
                    # Apply array indexing if specified
                    if field.array_index is not None and isinstance(value, (list, str, dict)):
                        value = self._apply_array_index(value, field.array_index)
                    # Apply post-index path (e.g., employees[0].name)
                    if field.post_path is not None and value is not None:
                        value = self._resolve_post_index_path(value, field.post_path, field, type_def)
                    # Resolve raw composite array elements before projection
                    if (field.method_chain is not None or field.method_name is not None):
                        value = self._maybe_resolve_composite_array(value, field.path, type_def)
                    # Apply method call (e.g., readings.length())
                    if field.method_chain is not None:
                        for mc in field.method_chain:
                            value = self._apply_projection_method(value, mc.method_name, mc.method_args)
                    elif field.method_name is not None:
                        value = self._apply_projection_method(value, field.method_name, field.method_args)
                    # Build column name with index notation if applicable
                    col_name = field.name
                    if field.method_chain is not None or field.method_name is not None:
                        col_name = self._format_method_col_name(field)
                    elif field.array_index is not None and field.post_path is not None:
                        post = ".".join(field.post_path)
                        col_name = f"{field.name}[{self._format_array_index(field.array_index)}].{post}"
                    elif field.array_index is not None:
                        col_name = f"{field.name}[{self._format_array_index(field.array_index)}]"
                    row[col_name] = value
            rows.append(row)

        return columns, rows

    def _resolve_field_path(
        self,
        record: dict[str, Any],
        path: list[str],
        type_def: TypeDefinition,
    ) -> Any:
        """Resolve a dotted field path like ['address', 'state']."""
        if not path:
            return None

        base = type_def.resolve_base_type()
        if not isinstance(base, CompositeTypeDefinition):
            return record.get(path[0]) if len(path) == 1 else None

        first_field = path[0]
        value = record.get(first_field)

        # If this is the only part of the path, return directly
        if len(path) == 1:
            return value

        # Need to resolve nested fields
        # Find the field definition to get its type
        field_def = base.get_field(first_field)
        if field_def is None:
            return None

        field_base = field_def.type_def.resolve_base_type()

        # Array projection: employees.name projects 'name' over each element
        if isinstance(field_base, ArrayTypeDefinition):
            elem_base = field_base.element_type.resolve_base_type()
            if isinstance(elem_base, CompositeTypeDefinition) and isinstance(value, list):
                # Elements are dicts of raw field references from element_table.get()
                # Resolve each element and project the remaining path
                projected = []
                for elem in value:
                    if isinstance(elem, dict):
                        # Raw field reference dict - resolve it into a full record
                        resolved_elem = self._resolve_raw_composite(elem, elem_base, field_base.element_type)
                        result = self._resolve_field_path(resolved_elem, path[1:], field_base.element_type)
                        projected.append(result)
                    elif isinstance(elem, str) and elem.startswith("<") and elem.endswith(">"):
                        # Already a composite reference string
                        projected.append(self._resolve_composite_ref_path(elem, path[1:]))
                    else:
                        projected.append(None)
                return projected
            return None

        if not isinstance(field_base, CompositeTypeDefinition):
            # Can't traverse into non-composite types
            return None

        # The value should be a string like "<Address[0]>" - resolve remaining path
        return self._resolve_composite_ref_path(value, path[1:])

    def _resolve_raw_composite(
        self,
        raw_record: dict[str, Any],
        composite_base: CompositeTypeDefinition,
        type_def: TypeDefinition,
    ) -> dict[str, Any]:
        """Resolve raw field references in a composite record to actual values."""
        resolved: dict[str, Any] = {}
        for f in composite_base.fields:
            ref = raw_record[f.name]

            if ref is None:
                resolved[f.name] = None
                continue

            field_type_base = f.type_def.resolve_base_type()
            if isinstance(field_type_base, FractionTypeDefinition):
                resolved[f.name] = _fraction_decode(self.storage, ref[0], ref[1], ref[2], ref[3])
            elif isinstance(field_type_base, ArrayTypeDefinition):
                start_index, length = ref
                if is_bigint_type(f.type_def):
                    if length == 0:
                        resolved[f.name] = BigInt(0)
                    else:
                        arr_table = self.storage.get_array_table_for_type(f.type_def)
                        elements = [arr_table.element_table.get(start_index + j) for j in range(length)]
                        resolved[f.name] = BigInt(int.from_bytes(bytes(elements), 'little', signed=True))
                elif is_biguint_type(f.type_def):
                    if length == 0:
                        resolved[f.name] = BigUInt(0)
                    else:
                        arr_table = self.storage.get_array_table_for_type(f.type_def)
                        elements = [arr_table.element_table.get(start_index + j) for j in range(length)]
                        resolved[f.name] = BigUInt(int.from_bytes(bytes(elements), 'little', signed=False))
                elif length == 0:
                    resolved[f.name] = []
                else:
                    arr_table = self.storage.get_array_table_for_type(f.type_def)
                    elements = [
                        arr_table.element_table.get(start_index + j)
                        for j in range(length)
                    ]
                    if is_string_type(f.type_def):
                        resolved[f.name] = "".join(elements)
                    else:
                        resolved[f.name] = elements
            elif isinstance(field_type_base, CompositeTypeDefinition):
                resolved[f.name] = f"<{f.type_def.name}[{ref}]>"
            else:
                # Primitive — value is already inline
                resolved[f.name] = bool(ref) if is_boolean_type(f.type_def) else ref
        return resolved

    def _maybe_resolve_composite_array(
        self, value: Any, path: list[str], type_def: TypeDefinition
    ) -> Any:
        """Resolve raw composite array elements (with unresolved string fields) to fully resolved dicts."""
        if not isinstance(value, list) or not value or not isinstance(value[0], dict):
            return value
        base = type_def.resolve_base_type()
        if not isinstance(base, CompositeTypeDefinition):
            return value
        field_def = base.get_field(path[0])
        if field_def is None:
            return value
        field_base = field_def.type_def.resolve_base_type()
        if not isinstance(field_base, ArrayTypeDefinition):
            return value
        elem_type = field_base.element_type
        elem_base = elem_type.resolve_base_type()
        if not isinstance(elem_base, CompositeTypeDefinition):
            return value
        return [
            self._resolve_raw_composite(elem, elem_base, elem_type)
            if isinstance(elem, dict) else elem
            for elem in value
        ]

    def _load_composite_record(
        self,
        type_name: str,
        index: int,
    ) -> tuple[dict[str, Any], TypeDefinition] | None:
        """Load and resolve a composite record by type name and index.

        Returns (resolved_record, type_def) or None if not found.
        """
        nested_type_def = self.registry.get(type_name)
        if nested_type_def is None:
            return None

        nested_base = nested_type_def.resolve_base_type()
        if not isinstance(nested_base, CompositeTypeDefinition):
            return None

        nested_table = self.storage.get_table(type_name)
        raw_record = nested_table.get(index)

        nested_resolved = self._resolve_raw_composite(raw_record, nested_base, nested_type_def)
        nested_resolved["_index"] = index

        return nested_resolved, nested_type_def

    def _resolve_composite_ref_path(
        self,
        value: Any,
        path: list[str],
    ) -> Any:
        """Resolve a dotted path starting from a composite reference string like '<Address[0]>'."""
        if isinstance(value, str) and value.startswith("<") and value.endswith(">"):
            match = re.match(r"<(\w+)\[(\d+)\]>", value)
            if match:
                type_name = match.group(1)
                index = int(match.group(2))
                result = self._load_composite_record(type_name, index)
                if result is None:
                    return None
                nested_resolved, nested_type_def = result
                return self._resolve_field_path(nested_resolved, path, nested_type_def)
        return None

    def _resolve_post_index_path(
        self,
        value: Any,
        post_path: list[str],
        field: SelectField,
        type_def: TypeDefinition,
    ) -> Any:
        """Resolve a dotted path after array indexing, e.g., employees[0].name."""
        # If value is a list (from slice/multi-index), map the post-path over each element
        if isinstance(value, list):
            return [self._resolve_post_index_path(elem, post_path, field, type_def) for elem in value]

        return self._resolve_composite_ref_path(value, post_path)

    def _compute_global_aggregates(
        self, records: list[dict[str, Any]], fields: list[SelectField]
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """Compute aggregates over all records."""
        columns = []
        row = {}

        for field in fields:
            if field.aggregate:
                col_name = f"{field.aggregate}({field.name})"
                columns.append(col_name)

                if field.aggregate == "count":
                    row[col_name] = len(records)
                else:
                    row[col_name] = self._compute_aggregate(
                        records, field.name, field.aggregate
                    )
            else:
                columns.append(field.name)
                if records:
                    row[field.name] = records[0].get(field.name)
                else:
                    row[field.name] = None

        return columns, [row] if row else []

    def _compute_aggregate(
        self, records: list[dict[str, Any]], field_name: str, aggregate: str
    ) -> Any:
        """Compute an aggregate function over records."""
        values = []
        for record in records:
            val = record.get(field_name)
            if val is not None and isinstance(val, (int, float)):
                values.append(val)

        if not values:
            return None

        if aggregate == "sum":
            return sum(values)
        elif aggregate == "average":
            return sum(values) / len(values)
        elif aggregate == "product":
            result = 1
            for v in values:
                result *= v
            return result
        elif aggregate == "count":
            return len(values)
        elif aggregate == "min":
            return min(values)
        elif aggregate == "max":
            return max(values)

        return None

    def _apply_array_index(self, value: list | str | dict, array_index: ArrayIndex) -> Any:
        """Apply array indexing to a list, string, or dict value."""
        if isinstance(value, dict):
            idx = array_index.index
            if isinstance(idx, str):
                return value.get(idx)
            return None

        if not isinstance(value, (list, str)):
            return value

        idx = array_index.index
        if isinstance(idx, int):
            if -len(value) <= idx < len(value):
                return value[idx]
            return None
        elif isinstance(idx, ArraySlice):
            return value[idx.start:idx.end]

        return value

    def _format_array_index(self, array_index: ArrayIndex) -> str:
        """Format an ArrayIndex for display."""
        idx = array_index.index
        if isinstance(idx, str):
            return f'"{idx}"'
        if isinstance(idx, int):
            return str(idx)
        elif isinstance(idx, ArraySlice):
            start = str(idx.start) if idx.start is not None else ""
            end = str(idx.end) if idx.end is not None else ""
            return f"{start}:{end}"
        return ""

    def _format_default_for_dump(self, value: Any, type_def: TypeDefinition) -> str:
        """Format a default value for dump output."""
        if value is None:
            return "null"
        if is_boolean_type(type_def):
            return "true" if value else "false"
        if isinstance(value, EnumValue):
            base = type_def.resolve_base_type()
            enum_name = base.name if isinstance(base, EnumTypeDefinition) else type_def.name
            if value.fields:
                field_strs = []
                for k, v in value.fields.items():
                    if isinstance(v, str):
                        field_strs.append(f'{k}="{v}"')
                    elif isinstance(v, float):
                        field_strs.append(f"{k}={v}")
                    else:
                        field_strs.append(f"{k}={v}")
                return f"{enum_name}.{value.variant_name}({', '.join(field_strs)})"
            return f"{enum_name}.{value.variant_name}"
        if isinstance(value, str):
            return f'"{value}"'
        if isinstance(value, list):
            elem_strs = []
            for elem in value:
                if isinstance(elem, str):
                    elem_strs.append(f'"{elem}"')
                else:
                    elem_strs.append(str(elem))
            return f"[{', '.join(elem_strs)}]"
        if isinstance(value, Fraction):
            if value.denominator == 1:
                return f"fraction({value.numerator})"
            return f"fraction({value.numerator}, {value.denominator})"
        if isinstance(value, (BigInt, BigUInt)):
            return str(int(value))
        base = type_def.resolve_base_type()
        if isinstance(base, PrimitiveTypeDefinition):
            if base.primitive in (PrimitiveType.UINT128, PrimitiveType.INT128):
                return f"0x{value:032x}"
            if base.primitive in (PrimitiveType.FLOAT16, PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
                # Use repr to preserve the decimal point
                return repr(float(value))
        return str(value)

    def _execute_dump(self, query: DumpQuery) -> DumpResult:
        """Execute DUMP query — serialize database as TTQ script."""
        from collections import defaultdict

        # Resolve into dump_targets: dict[str, set[int] | None] | None
        # None means full dump; {name: None} means all records of that type;
        # {name: {1,3}} means specific indices
        dump_targets: dict[str, set[int] | None] | None = None

        if query.items is not None:
            # Dump list: merge all items
            dump_targets = {}
            for item in query.items:
                if item.variable:
                    var_binding = self._lookup_variable(item.variable)
                    if var_binding is None:
                        return DumpResult(
                            columns=[], rows=[],
                            message=f"Undefined variable: ${item.variable}",
                        )
                    type_name, ref = var_binding
                    idx_set = set(ref) if isinstance(ref, list) else {ref}
                    if type_name in dump_targets:
                        existing = dump_targets[type_name]
                        if existing is not None:
                            dump_targets[type_name] = existing | idx_set
                        # else already None (all), stays None
                    else:
                        dump_targets[type_name] = idx_set
                elif item.table:
                    if item.table in dump_targets:
                        # Already present; if specific indices, upgrade to all
                        dump_targets[item.table] = None
                    else:
                        dump_targets[item.table] = None
        elif query.variable:
            var_binding = self._lookup_variable(query.variable)
            if var_binding is None:
                return DumpResult(
                    columns=[], rows=[],
                    message=f"Undefined variable: ${query.variable}",
                )
            type_name, ref = var_binding
            if isinstance(ref, list):
                dump_targets = {type_name: set(ref)}
            else:
                dump_targets = {type_name: {ref}}
        elif query.table:
            dump_targets = {query.table: None}
        # else: dump_targets stays None → full dump

        pretty = query.pretty

        # Auto-append extension if output_file has none
        if query.output_file and not Path(query.output_file).suffix:
            ext_map = {"yaml": ".yaml", "json": ".json", "xml": ".xml", "ttq": ".ttq"}
            query.output_file += ext_map.get(query.format, ".ttq")

        lines: list[str] = []
        lines.append("-- TTQ dump")

        # Collect user-defined types (skip primitives and auto-generated array types)
        aliases: list[tuple[str, AliasTypeDefinition]] = []
        composites: list[tuple[str, CompositeTypeDefinition]] = []
        enums: list[tuple[str, EnumTypeDefinition]] = []
        interfaces: list[tuple[str, InterfaceTypeDefinition]] = []

        for type_name in self.registry.list_types():
            if type_name.startswith("_") and not query.include_system:
                continue
            type_def = self.registry.get(type_name)
            if type_def is None:
                continue
            if isinstance(type_def, PrimitiveTypeDefinition):
                continue
            if isinstance(type_def, DictionaryTypeDefinition):
                continue  # auto-generated dict types
            if isinstance(type_def, ArrayTypeDefinition):
                continue  # auto-generated array/set types
            if type_name.startswith("Dict_"):
                continue  # synthetic dict entry composites
            if isinstance(type_def, InterfaceTypeDefinition):
                interfaces.append((type_name, type_def))
            elif isinstance(type_def, EnumTypeDefinition):
                enums.append((type_name, type_def))
            elif isinstance(type_def, AliasTypeDefinition):
                aliases.append((type_name, type_def))
            elif isinstance(type_def, CompositeTypeDefinition):
                composites.append((type_name, type_def))

        # Sort composites in dependency order
        sorted_composites, cycle_composites = self._sort_composites_by_dependency(composites)

        # If dumping specific targets, compute transitive closure of dependencies
        if dump_targets is not None:
            needed: set[str] = set()
            for tname in dump_targets:
                needed |= self._transitive_type_closure(tname)
            aliases = [(n, t) for n, t in aliases if n in needed]
            enums = [(n, t) for n, t in enums if n in needed]
            interfaces = [(n, t) for n, t in interfaces if n in needed]
            sorted_composites = [(n, t) for n, t in sorted_composites if n in needed]
            cycle_composites = [(n, t) for n, t in cycle_composites if n in needed]

        # Recombine for record dumping (sorted first, then cycle types)
        composites = sorted_composites + cycle_composites

        # Branch based on output format
        if query.format == "yaml":
            return self._execute_dump_yaml(
                query, dump_targets, aliases, composites, sorted_composites, cycle_composites,
                interfaces=interfaces,
            )
        elif query.format == "json":
            return self._execute_dump_json(
                query, dump_targets, aliases, composites, sorted_composites, cycle_composites,
                interfaces=interfaces,
            )
        elif query.format == "xml":
            return self._execute_dump_xml(
                query, dump_targets, aliases, composites, sorted_composites, cycle_composites,
                interfaces=interfaces,
            )

        # TTQ format output
        # Emit aliases
        for name, alias_def in aliases:
            base_name = alias_def.base_type.name
            lines.append(f"alias {name} = {base_name}")

        # Emit enum type definitions
        for name, enum_def in enums:
            variant_strs = []
            for v in enum_def.variants:
                if v.fields:
                    field_strs = [f"{f.name}: {f.type_def.name}" for f in v.fields]
                    variant_strs.append(f"{v.name}({', '.join(field_strs)})")
                elif enum_def.has_explicit_values:
                    variant_strs.append(f"{v.name} = {v.discriminant}")
                else:
                    variant_strs.append(v.name)
            backing_clause = f" : {enum_def.backing_type.value}" if enum_def.backing_type else ""
            if pretty:
                lines.append(f"enum {name}{backing_clause} {{")
                for i, vs in enumerate(variant_strs):
                    comma = "," if i < len(variant_strs) - 1 else ""
                    lines.append(f"    {vs}{comma}")
                lines.append("}")
                lines.append("")
            else:
                lines.append(f"enum {name}{backing_clause} {{ {', '.join(variant_strs)} }}")

        # Sort interfaces in dependency order (parents before children)
        interfaces = self._sort_interfaces_by_dependency(interfaces)

        # Emit interface definitions
        for name, iface_def in interfaces:
            # Compute inherited field names to skip
            inherited_fields: set[str] = set()
            for parent_name in iface_def.interfaces:
                parent_td = self.registry.get(parent_name)
                if parent_td and isinstance(parent_td, InterfaceTypeDefinition):
                    for f in parent_td.fields:
                        inherited_fields.add(f.name)

            field_strs = []
            for f in iface_def.fields:
                if f.name in inherited_fields:
                    continue
                field_type_name = f.type_def.name
                if field_type_name.endswith("[]"):
                    base_elem = field_type_name[:-2]
                    field_type_name = f"{base_elem}[]"
                overflow_prefix = f"{f.overflow} " if f.overflow else ""
                fs = f"{f.name}: {overflow_prefix}{field_type_name}"
                if f.default_value is not None:
                    fs += f" = {self._format_default_for_dump(f.default_value, f.type_def)}"
                field_strs.append(fs)

            from_clause = f" from {', '.join(iface_def.interfaces)}" if iface_def.interfaces else ""

            if pretty:
                lines.append(f"interface {name}{from_clause} {{")
                for i, fs in enumerate(field_strs):
                    comma = "," if i < len(field_strs) - 1 else ""
                    lines.append(f"    {fs}{comma}")
                lines.append("}")
                lines.append("")
            else:
                fields_part = ", ".join(field_strs)
                lines.append(f"interface {name}{from_clause} {{ {fields_part} }}")

        # Determine which cycle types need forward declarations
        # Only types referenced before they're defined need forwarding
        cycle_type_names = {name for name, _ in cycle_composites}
        needs_forward: set[str] = set()
        defined_so_far: set[str] = set()

        for name, comp_def in cycle_composites:
            # Check what this type references
            for f in comp_def.fields:
                fb = f.type_def.resolve_base_type()
                ref_name = None
                if isinstance(fb, CompositeTypeDefinition) and fb.name != name:
                    ref_name = fb.name
                elif isinstance(fb, ArrayTypeDefinition):
                    elem_base = fb.element_type.resolve_base_type()
                    if isinstance(elem_base, CompositeTypeDefinition) and elem_base.name != name:
                        ref_name = elem_base.name
                # If referencing a cycle type not yet defined, it needs forwarding
                if ref_name and ref_name in cycle_type_names and ref_name not in defined_so_far:
                    needs_forward.add(ref_name)
            defined_so_far.add(name)

        # Emit only the necessary forward declarations
        for name in sorted(needs_forward):
            lines.append(f"forward {name}")

        # Helper to emit a type definition
        def emit_type_def(name: str, comp_def: CompositeTypeDefinition) -> None:
            # Collect inherited field names from interfaces and concrete parent
            inherited_fields: set[str] = set()
            if comp_def.parent:
                parent_def = self.registry.get(comp_def.parent)
                if parent_def and isinstance(parent_def, CompositeTypeDefinition):
                    for f in parent_def.fields:
                        inherited_fields.add(f.name)
            if comp_def.interfaces:
                for iface_name in comp_def.interfaces:
                    iface = self.registry.get(iface_name)
                    if iface and isinstance(iface, InterfaceTypeDefinition):
                        for f in iface.fields:
                            inherited_fields.add(f.name)

            # Build own fields (excluding inherited)
            field_strs = []
            for f in comp_def.fields:
                if f.name in inherited_fields:
                    continue
                field_type_name = f.type_def.name
                if field_type_name.endswith("[]"):
                    base_elem = field_type_name[:-2]
                    field_type_name = f"{base_elem}[]"
                overflow_prefix = f"{f.overflow} " if f.overflow else ""
                fs = f"{f.name}: {overflow_prefix}{field_type_name}"
                if f.default_value is not None:
                    fs += f" = {self._format_default_for_dump(f.default_value, f.type_def)}"
                field_strs.append(fs)

            # Build the from clause (concrete parent first, then interfaces)
            from_clause = ""
            parents_list = []
            if comp_def.parent:
                parents_list.append(comp_def.parent)
            parents_list.extend(comp_def.interfaces)
            if parents_list:
                from_clause = f" from {', '.join(parents_list)}"

            if pretty:
                if field_strs:
                    lines.append(f"type {name}{from_clause} {{")
                    for i, fs in enumerate(field_strs):
                        comma = "," if i < len(field_strs) - 1 else ""
                        lines.append(f"    {fs}{comma}")
                    lines.append("}")
                else:
                    lines.append(f"type {name}{from_clause}")
                lines.append("")  # blank line between type blocks
            else:
                if field_strs:
                    fields_part = ", ".join(field_strs)
                    lines.append(f"type {name}{from_clause} {{ {fields_part} }}")
                else:
                    lines.append(f"type {name}{from_clause}")

        # Emit non-cycle composite type definitions
        for name, comp_def in sorted_composites:
            emit_type_def(name, comp_def)

        # Emit cycle type definitions
        for name, comp_def in cycle_composites:
            emit_type_def(name, comp_def)

        # Determine which composites to dump records for
        if dump_targets is not None:
            types_to_dump = [(n, t) for n, t in composites if n in dump_targets]
        else:
            types_to_dump = composites

        # Helper: should this record be included in the dump?
        def _include_record(name: str, index: int) -> bool:
            if dump_targets is None:
                return True
            if name not in dump_targets:
                return False
            indices = dump_targets[name]
            return indices is None or index in indices

        # Pass 1: Count composite references across all records
        ref_counts: dict[tuple[str, int], int] = defaultdict(int)
        for name, comp_def in types_to_dump:
            table_file = self.storage.data_dir / f"{name}.bin"
            if not table_file.exists():
                continue
            table = self.storage.get_table(name)
            for i in range(table.count):
                if table.is_deleted(i):
                    continue
                if not _include_record(name, i):
                    continue
                raw_record = table.get(i)
                for f in comp_def.fields:
                    ref = raw_record[f.name]
                    if ref is None:
                        continue
                    field_base = f.type_def.resolve_base_type()
                    if isinstance(field_base, ArrayTypeDefinition):
                        start_index, length = ref
                        elem_base = field_base.element_type.resolve_base_type()
                        if isinstance(elem_base, CompositeTypeDefinition):
                            arr_table = self.storage.get_array_table_for_type(f.type_def)
                            for j in range(length):
                                elem = arr_table.element_table.get(start_index + j)
                                if isinstance(elem, dict):
                                    # Array element composites are stored inline,
                                    # not as index references — skip for ref counting
                                    pass
                    elif isinstance(field_base, CompositeTypeDefinition):
                        ref_counts[(f.type_def.name, ref)] += 1

        # Pass 2: Assign variable names to multiply-referenced composites
        dump_vars: dict[tuple[str, int], str] = {}
        for (type_name, index), count in ref_counts.items():
            if count > 1:
                dump_vars[(type_name, index)] = f"{type_name}_{index}"

        # Pass 2.5: Detect data cycles and build tag mappings for cycle handling
        back_edges = self._detect_back_edges(composites, _include_record)

        # Build tag mappings for all back-edges (scope-scoped tags handle all cycles)
        def _generate_tag_name(counter: int) -> str:
            """Generate sequential tag names: A, B, ..., Z, AA, AB, ..."""
            result = ""
            n = counter
            while True:
                result = chr(ord('A') + n % 26) + result
                n = n // 26 - 1
                if n < 0:
                    break
            return result

        record_tags: dict[tuple[str, int], str] = {}  # target → tag_name
        back_edge_tags: dict[tuple[str, int, str], str] = {}  # (src_type, src_idx, field) → tag_name
        tag_counter = 0

        for src_type, src_idx, field_name, tgt_type, tgt_idx in back_edges:
            tgt_key = (tgt_type, tgt_idx)
            if tgt_key not in record_tags:
                record_tags[tgt_key] = _generate_tag_name(tag_counter)
                tag_counter += 1
            back_edge_tags[(src_type, src_idx, field_name)] = record_tags[tgt_key]

        # Determine if we need a scope block (when there are tags for cycles)
        needs_scope = bool(record_tags)

        # Pass 3: Emit variable assignments for shared refs
        # Build a dependency-ordered emit list for dump_vars. After removing
        # back-edges, the remaining forward references form a DAG.
        emitted_vars: set[tuple[str, int]] = set()
        # Track all records consumed (inlined) during Pass 3 so Pass 4 skips them
        inlined_records: set[tuple[str, int]] = set()
        comp_map = dict(composites)

        # Collect statements for scope block if needed
        scope_statements: list[str] = []
        statement_target = scope_statements if needs_scope else lines

        def _collect_dump_var_deps(name: str, idx: int, visited: set[tuple[str, int]]) -> None:
            """Recursively find all dump_var keys that a record depends on
            by following inline composite chains (non-back-edge, non-dump-var)."""
            key = (name, idx)
            if key in visited:
                return
            visited.add(key)
            comp_def = comp_map.get(name)
            if comp_def is None:
                return
            table_file = self.storage.data_dir / f"{name}.bin"
            if not table_file.exists():
                return
            table = self.storage.get_table(name)
            if idx >= table.count or table.is_deleted(idx):
                return
            raw_record = table.get(idx)
            for f in comp_def.fields:
                ref = raw_record[f.name]
                if ref is None:
                    continue
                if (name, idx, f.name) in back_edge_tags:
                    continue
                field_base = f.type_def.resolve_base_type()
                if isinstance(field_base, ArrayTypeDefinition):
                    continue
                if isinstance(field_base, CompositeTypeDefinition):
                    dep_key = (f.type_def.name, ref)
                    if dep_key in dump_vars:
                        # Direct dependency on a dump_var — just record it
                        pass
                    else:
                        # Inlined record — follow transitively
                        _collect_dump_var_deps(f.type_def.name, ref, visited)

        def _emit_var(key: tuple[str, int]) -> None:
            if key in emitted_vars:
                return
            name, idx = key
            # Find all transitive dump_var dependencies
            visited: set[tuple[str, int]] = set()
            _collect_dump_var_deps(name, idx, visited)
            # Emit all dump_var deps found in the transitive closure
            for v_key in visited:
                if v_key == key:
                    continue
                v_name, v_idx = v_key
                comp_def_v = comp_map.get(v_name)
                if comp_def_v is None:
                    continue
                table_file_v = self.storage.data_dir / f"{v_name}.bin"
                if not table_file_v.exists():
                    continue
                table_v = self.storage.get_table(v_name)
                if v_idx >= table_v.count or table_v.is_deleted(v_idx):
                    continue
                raw_record_v = table_v.get(v_idx)
                for f in comp_def_v.fields:
                    ref = raw_record_v[f.name]
                    if ref is None:
                        continue
                    if (v_name, v_idx, f.name) in back_edge_tags:
                        continue
                    field_base = f.type_def.resolve_base_type()
                    if isinstance(field_base, ArrayTypeDefinition):
                        continue
                    if isinstance(field_base, CompositeTypeDefinition):
                        dep_key = (f.type_def.name, ref)
                        if dep_key in dump_vars and dep_key not in emitted_vars:
                            _emit_var(dep_key)
            # Also check direct composite fields of this record
            comp_def = comp_map.get(name)
            if comp_def:
                table_file = self.storage.data_dir / f"{name}.bin"
                if table_file.exists():
                    table = self.storage.get_table(name)
                    if idx < table.count and not table.is_deleted(idx):
                        raw_record = table.get(idx)
                        for f in comp_def.fields:
                            ref = raw_record[f.name]
                            if ref is None:
                                continue
                            if (name, idx, f.name) in back_edge_tags:
                                continue
                            field_base = f.type_def.resolve_base_type()
                            if isinstance(field_base, ArrayTypeDefinition):
                                continue
                            if isinstance(field_base, CompositeTypeDefinition):
                                dep_key = (f.type_def.name, ref)
                                if dep_key in dump_vars and dep_key not in emitted_vars:
                                    _emit_var(dep_key)
            # Emit this record
            comp_def = comp_map.get(name)
            if comp_def is None:
                return
            table_file = self.storage.data_dir / f"{name}.bin"
            if not table_file.exists():
                return
            table = self.storage.get_table(name)
            if idx >= table.count or table.is_deleted(idx):
                return
            raw_record = table.get(idx)
            create_str = self._format_record_as_create(
                name, comp_def, raw_record, dump_vars,
                record_index=idx,
                pretty=pretty,
                record_tags=record_tags, back_edge_tags=back_edge_tags,
            )
            var_name = dump_vars[key]
            statement_target.append(f"${var_name} = {create_str}")
            emitted_vars.add(key)
            # Mark inlined records so Pass 4 skips them
            for v_key in visited:
                if v_key != key and v_key not in dump_vars:
                    inlined_records.add(v_key)

        for name, comp_def in composites:
            table_file = self.storage.data_dir / f"{name}.bin"
            if not table_file.exists():
                continue
            table = self.storage.get_table(name)
            for i in range(table.count):
                if table.is_deleted(i):
                    continue
                key = (name, i)
                if key in dump_vars:
                    _emit_var(key)

        # Pass 3.5: Collect all records that would be inlined by top-level creates
        # This ensures records reachable via inline paths (including tag-based cycles)
        # are not emitted as separate create statements.
        def _collect_inlined(name: str, idx: int, visited: set[tuple[str, int]]) -> None:
            """Recursively collect all records reachable via inline composite fields."""
            key = (name, idx)
            if key in visited:
                return
            visited.add(key)
            comp_def = comp_map.get(name)
            if comp_def is None:
                return
            table_file = self.storage.data_dir / f"{name}.bin"
            if not table_file.exists():
                return
            table = self.storage.get_table(name)
            if idx >= table.count or table.is_deleted(idx):
                return
            raw_record = table.get(idx)
            for f in comp_def.fields:
                ref = raw_record[f.name]
                if ref is None:
                    continue
                # Skip back-edges (they're emitted as tag refs, not inlined)
                if (name, idx, f.name) in back_edge_tags:
                    continue
                field_base = f.type_def.resolve_base_type()
                if isinstance(field_base, ArrayTypeDefinition):
                    continue
                if isinstance(field_base, CompositeTypeDefinition):
                    dep_key = (f.type_def.name, ref)
                    if dep_key in dump_vars:
                        # This is a $var reference, not inlined
                        continue
                    _collect_inlined(f.type_def.name, ref, visited)

        # Identify top-level records and collect their inlined children
        for name, comp_def in types_to_dump:
            table_file = self.storage.data_dir / f"{name}.bin"
            if not table_file.exists():
                continue
            table = self.storage.get_table(name)
            for i in range(table.count):
                if table.is_deleted(i):
                    continue
                if not _include_record(name, i):
                    continue
                key = (name, i)
                if key in dump_vars or key in inlined_records:
                    continue
                # This will be a top-level create — collect its inlined children
                visited: set[tuple[str, int]] = set()
                _collect_inlined(name, i, visited)
                # Mark all visited records except the root as inlined
                for v_key in visited:
                    if v_key != key:
                        inlined_records.add(v_key)

        # Pass 4: Emit remaining create statements using $var references
        for name, comp_def in types_to_dump:
            table_file = self.storage.data_dir / f"{name}.bin"
            if not table_file.exists():
                continue
            table = self.storage.get_table(name)
            for i in range(table.count):
                if table.is_deleted(i):
                    continue
                if not _include_record(name, i):
                    continue
                # Skip records that were emitted as variable assignments
                # or inlined into another record's create statement
                if (name, i) in dump_vars or (name, i) in inlined_records:
                    continue
                raw_record = table.get(i)
                create_str = self._format_record_as_create(
                    name, comp_def, raw_record, dump_vars,
                    record_index=i,
                    pretty=pretty,
                    record_tags=record_tags, back_edge_tags=back_edge_tags,
                )
                statement_target.append(f"{create_str}")

        # If we collected statements for a scope block, wrap them
        if needs_scope and scope_statements:
            if pretty:
                # Pretty-print scope with indented statements
                lines.append("scope {")
                for stmt in scope_statements:
                    # Indent each line of the statement
                    for line in stmt.split('\n'):
                        lines.append(f"    {line}")
                lines.append("}")
            else:
                lines.append("scope { " + " ".join(scope_statements) + " }")

        return DumpResult(columns=[], rows=[], script="\n".join(lines) + "\n", output_file=query.output_file)

    def _execute_dump_yaml(
        self,
        query: DumpQuery,
        dump_targets: dict[str, set[int] | None] | None,
        aliases: list[tuple[str, AliasTypeDefinition]],
        composites: list[tuple[str, CompositeTypeDefinition]],
        sorted_composites: list[tuple[str, CompositeTypeDefinition]],
        cycle_composites: list[tuple[str, CompositeTypeDefinition]],
        interfaces: list[tuple[str, InterfaceTypeDefinition]] | None = None,
    ) -> DumpResult:
        """Execute DUMP query with YAML output format.

        All records are emitted at the top level under their type name.
        Each record gets an anchor (&type_idx) and composite field references
        use aliases (*type_idx). This handles cycles naturally.
        """
        pretty = query.pretty
        indent = "  " if pretty else ""

        lines: list[str] = []
        lines.append("# YAML dump")

        # Determine which composites to dump records for
        if dump_targets is not None:
            types_to_dump = [(n, t) for n, t in composites if n in dump_targets]
        else:
            types_to_dump = composites

        # Helper: filter records by dump_targets
        def _include_record(name: str, idx: int) -> bool:
            if dump_targets is None:
                return True
            if name not in dump_targets:
                return False
            allowed = dump_targets[name]
            return allowed is None or idx in allowed

        # Generate anchor names for ALL records (simple approach: type_idx)
        def anchor_name(type_name: str, idx: int) -> str:
            return f"{type_name}_{idx}"

        # Helper to format a primitive value
        def fmt_primitive(val: Any, prim_type: PrimitiveTypeDefinition) -> str:
            if isinstance(prim_type, BooleanTypeDefinition):
                return "true" if val else "false"
            elif prim_type.primitive == PrimitiveType.CHARACTER:
                return repr(chr(val)) if isinstance(val, int) else repr(val)
            elif prim_type.primitive in (PrimitiveType.FLOAT16, PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
                return str(val)
            else:
                return str(val)

        # Helper to format a field value
        def fmt_value(val: Any, field_type: TypeDefinition, depth: int = 0) -> str:
            ind_inner = indent * (depth + 1) if pretty else ""

            base = field_type.resolve_base_type()

            if val is None:
                return "null"

            if is_fraction_type(field_type):
                frac = _fraction_decode(self.storage, val[0], val[1], val[2], val[3])
                if frac.denominator == 1:
                    return str(frac.numerator)
                return str(frac)

            if is_bigint_type(field_type) or is_biguint_type(field_type):
                start_idx, length = val
                if length == 0:
                    return "0"
                arr_table = self.storage.get_array_table_for_type(field_type)
                byte_list = [arr_table.element_table.get(start_idx + j) for j in range(length)]
                signed = is_bigint_type(field_type)
                return str(int.from_bytes(bytes(byte_list), byteorder='little', signed=signed))

            if isinstance(base, ArrayTypeDefinition):
                start_idx, length = val

                if length == 0:
                    return "[]"

                arr_table = self.storage.get_array_table_for_type(field_type)
                elem_base = base.element_type.resolve_base_type()

                # String type → joined string
                if is_string_type(field_type):
                    chars = [arr_table.element_table.get(start_idx + j) for j in range(length)]
                    s = "".join(chars)
                    # YAML string - use double quotes
                    escaped = s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
                    return f'"{escaped}"'

                elements = [arr_table.element_table.get(start_idx + j) for j in range(length)]

                # String array (string[]) — resolve each (start, length) to a string
                if is_string_type(base.element_type):
                    char_table = self.storage.get_array_table_for_type(base.element_type)
                    elem_strs = []
                    for elem in elements:
                        cs, cl = elem
                        chars = [char_table.element_table.get(cs + k) for k in range(cl)]
                        s = "".join(chars)
                        escaped = s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
                        elem_strs.append(f'"{escaped}"')
                    if pretty and len(elem_strs) > 5:
                        elem_lines = "\n".join(f"{ind_inner}- {e}" for e in elem_strs)
                        return f"\n{elem_lines}"
                    else:
                        return "[" + ", ".join(elem_strs) + "]"

                if isinstance(elem_base, CompositeTypeDefinition):
                    # Composite array - always use alias references
                    elem_strs = []
                    for elem_ref in elements:
                        if elem_ref is None:
                            elem_strs.append("null")
                        else:
                            elem_strs.append(f"*{anchor_name(elem_base.name, elem_ref)}")
                    if pretty:
                        elem_lines = "\n".join(f"{ind_inner}- {e}" for e in elem_strs)
                        return f"\n{elem_lines}"
                    else:
                        return "[" + ", ".join(elem_strs) + "]"
                else:
                    # Primitive array
                    elem_strs = [fmt_primitive(e, elem_base) for e in elements]
                    if pretty and len(elem_strs) > 5:
                        elem_lines = "\n".join(f"{ind_inner}- {e}" for e in elem_strs)
                        return f"\n{elem_lines}"
                    else:
                        return "[" + ", ".join(elem_strs) + "]"
            elif isinstance(base, PrimitiveTypeDefinition):
                return fmt_primitive(val, base)
            elif isinstance(base, CompositeTypeDefinition):
                # Always use alias reference - record is at top level
                return f"*{anchor_name(field_type.name, val)}"
            elif isinstance(base, AliasTypeDefinition):
                return fmt_value(val, base.base_type, depth)
            else:
                return str(val)

        def fmt_record_fields(comp_def: CompositeTypeDefinition, raw: dict[str, Any], depth: int) -> str:
            """Format record fields as YAML."""
            ind_inner = indent * (depth + 1) if pretty else ""

            field_strs = []
            for f in comp_def.fields:
                val = raw[f.name]
                val_str = fmt_value(val, f.type_def, depth + 1)
                field_strs.append((f.name, val_str))

            if pretty:
                result_lines = []
                for fname, fval in field_strs:
                    if "\n" in fval:
                        result_lines.append(f"{ind_inner}{fname}:{fval}")
                    else:
                        result_lines.append(f"{ind_inner}{fname}: {fval}")
                return "\n" + "\n".join(result_lines)
            else:
                return "{" + ", ".join(f"{fn}: {fv}" for fn, fv in field_strs) + "}"

        # Emit records grouped by type
        for name, comp_def in types_to_dump:
            table_file = self.storage.data_dir / f"{name}.bin"
            if not table_file.exists():
                continue
            table = self.storage.get_table(name)

            records_output = []
            for i in range(table.count):
                if table.is_deleted(i) or not _include_record(name, i):
                    continue

                raw = table.get(i)
                anchor = anchor_name(name, i)
                record_yaml = fmt_record_fields(comp_def, raw, 1)

                if pretty:
                    records_output.append(f"  - &{anchor}{record_yaml}")
                else:
                    records_output.append(f"- &{anchor} {record_yaml}")

            if records_output:
                lines.append(f"{name}:")
                lines.extend(records_output)
                if pretty:
                    lines.append("")

        return DumpResult(columns=[], rows=[], script="\n".join(lines) + "\n", output_file=query.output_file)

    def _execute_dump_json(
        self,
        query: DumpQuery,
        dump_targets: dict[str, set[int] | None] | None,
        aliases: list[tuple[str, AliasTypeDefinition]],
        composites: list[tuple[str, CompositeTypeDefinition]],
        sorted_composites: list[tuple[str, CompositeTypeDefinition]],
        cycle_composites: list[tuple[str, CompositeTypeDefinition]],
        interfaces: list[tuple[str, InterfaceTypeDefinition]] | None = None,
    ) -> DumpResult:
        """Execute DUMP query with JSON output format.

        Uses $id/$ref convention for references:
        - Each record has "$id": "Type_idx" for identification
        - References use {"$ref": "Type_idx"}
        """
        import json

        pretty = query.pretty

        # Determine which composites to dump records for
        if dump_targets is not None:
            types_to_dump = [(n, t) for n, t in composites if n in dump_targets]
        else:
            types_to_dump = composites

        # Helper: filter records by dump_targets
        def _include_record(name: str, idx: int) -> bool:
            if dump_targets is None:
                return True
            if name not in dump_targets:
                return False
            allowed = dump_targets[name]
            return allowed is None or idx in allowed

        # Generate ID for a record
        def record_id(type_name: str, idx: int) -> str:
            return f"{type_name}_{idx}"

        # Helper to format a primitive value for JSON
        def fmt_primitive(val: Any, prim_type: PrimitiveTypeDefinition) -> Any:
            if isinstance(prim_type, BooleanTypeDefinition):
                return bool(val)
            elif prim_type.primitive == PrimitiveType.CHARACTER:
                return chr(val) if isinstance(val, int) else val
            elif prim_type.primitive in (PrimitiveType.FLOAT16, PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
                return float(val)
            elif prim_type.primitive in (
                PrimitiveType.INT8, PrimitiveType.INT16, PrimitiveType.INT32, PrimitiveType.INT64,
                PrimitiveType.UINT8, PrimitiveType.UINT16, PrimitiveType.UINT32, PrimitiveType.UINT64,
                PrimitiveType.INT128, PrimitiveType.UINT128,
            ):
                return int(val)
            else:
                return val

        # Helper to format a field value for JSON
        def fmt_value(val: Any, field_type: TypeDefinition) -> Any:
            base = field_type.resolve_base_type()

            if val is None:
                return None

            if is_fraction_type(field_type):
                frac = _fraction_decode(self.storage, val[0], val[1], val[2], val[3])
                return {"numerator": frac.numerator, "denominator": frac.denominator}

            if is_bigint_type(field_type) or is_biguint_type(field_type):
                start_idx, length = val
                if length == 0:
                    return 0
                arr_table = self.storage.get_array_table_for_type(field_type)
                byte_list = [arr_table.element_table.get(start_idx + j) for j in range(length)]
                signed = is_bigint_type(field_type)
                return int.from_bytes(bytes(byte_list), byteorder='little', signed=signed)

            if isinstance(base, ArrayTypeDefinition):
                start_idx, length = val

                if length == 0:
                    return []

                arr_table = self.storage.get_array_table_for_type(field_type)
                elem_base = base.element_type.resolve_base_type()

                # String type → joined string
                if is_string_type(field_type):
                    chars = [arr_table.element_table.get(start_idx + j) for j in range(length)]
                    return "".join(chars)

                elements = [arr_table.element_table.get(start_idx + j) for j in range(length)]

                # String array (string[]) — resolve each (start, length) to a string
                if is_string_type(base.element_type):
                    char_table = self.storage.get_array_table_for_type(base.element_type)
                    str_elements = []
                    for elem in elements:
                        cs, cl = elem
                        chars = [char_table.element_table.get(cs + k) for k in range(cl)]
                        str_elements.append("".join(chars))
                    return str_elements

                if isinstance(elem_base, CompositeTypeDefinition):
                    # Composite array - return references
                    result = []
                    for elem_ref in elements:
                        if elem_ref is None:
                            result.append(None)
                        else:
                            result.append({"$ref": record_id(elem_base.name, elem_ref)})
                    return result
                else:
                    # Primitive array
                    return [fmt_primitive(e, elem_base) for e in elements]
            elif isinstance(base, PrimitiveTypeDefinition):
                return fmt_primitive(val, base)
            elif isinstance(base, CompositeTypeDefinition):
                # Reference to another record
                return {"$ref": record_id(field_type.name, val)}
            elif isinstance(base, AliasTypeDefinition):
                return fmt_value(val, base.base_type)
            else:
                return val

        # Build the JSON structure
        output: dict[str, list[dict[str, Any]]] = {}

        for name, comp_def in types_to_dump:
            table_file = self.storage.data_dir / f"{name}.bin"
            if not table_file.exists():
                continue
            table = self.storage.get_table(name)

            records = []
            for i in range(table.count):
                if table.is_deleted(i) or not _include_record(name, i):
                    continue

                raw = table.get(i)
                record: dict[str, Any] = {"$id": record_id(name, i)}

                for f in comp_def.fields:
                    val = raw[f.name]
                    record[f.name] = fmt_value(val, f.type_def)

                records.append(record)

            if records:
                output[name] = records

        # Serialize to JSON
        if pretty:
            script = json.dumps(output, indent=2) + "\n"
        else:
            script = json.dumps(output) + "\n"

        return DumpResult(columns=[], rows=[], script=script, output_file=query.output_file)

    def _execute_dump_xml(
        self,
        query: DumpQuery,
        dump_targets: dict[str, set[int] | None] | None,
        aliases: list[tuple[str, AliasTypeDefinition]],
        composites: list[tuple[str, CompositeTypeDefinition]],
        sorted_composites: list[tuple[str, CompositeTypeDefinition]],
        cycle_composites: list[tuple[str, CompositeTypeDefinition]],
        interfaces: list[tuple[str, InterfaceTypeDefinition]] | None = None,
    ) -> DumpResult:
        """Execute DUMP query with XML output format.

        Uses id/ref convention for references:
        - Each record has id="Type_idx" attribute for identification
        - References use ref="#Type_idx" attribute
        """
        from xml.sax.saxutils import escape

        pretty = query.pretty
        indent = "  " if pretty else ""
        newline = "\n" if pretty else ""

        # Determine which composites to dump records for
        if dump_targets is not None:
            types_to_dump = [(n, t) for n, t in composites if n in dump_targets]
        else:
            types_to_dump = composites

        # Helper: filter records by dump_targets
        def _include_record(name: str, idx: int) -> bool:
            if dump_targets is None:
                return True
            if name not in dump_targets:
                return False
            allowed = dump_targets[name]
            return allowed is None or idx in allowed

        # Generate ID for a record
        def record_id(type_name: str, idx: int) -> str:
            return f"{type_name}_{idx}"

        # Helper to format a primitive value for XML
        def fmt_primitive(val: Any, prim_type: PrimitiveTypeDefinition) -> str:
            if isinstance(prim_type, BooleanTypeDefinition):
                return "true" if val else "false"
            elif prim_type.primitive == PrimitiveType.CHARACTER:
                ch = chr(val) if isinstance(val, int) else val
                return escape(ch)
            elif prim_type.primitive in (PrimitiveType.FLOAT16, PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
                return str(float(val))
            elif prim_type.primitive in (
                PrimitiveType.INT8, PrimitiveType.INT16, PrimitiveType.INT32, PrimitiveType.INT64,
                PrimitiveType.UINT8, PrimitiveType.UINT16, PrimitiveType.UINT32, PrimitiveType.UINT64,
                PrimitiveType.INT128, PrimitiveType.UINT128,
            ):
                return str(int(val))
            else:
                return escape(str(val))

        # Helper to format a field value for XML
        def fmt_value(field_name: str, val: Any, field_type: TypeDefinition, depth: int) -> str:
            base = field_type.resolve_base_type()
            ind = indent * depth if pretty else ""
            ind_inner = indent * (depth + 1) if pretty else ""

            if val is None:
                return f"{ind}<{field_name} null=\"true\"/>"

            if is_fraction_type(field_type):
                frac = _fraction_decode(self.storage, val[0], val[1], val[2], val[3])
                return f"{ind}<{field_name}>{frac}</{field_name}>"

            if is_bigint_type(field_type) or is_biguint_type(field_type):
                start_idx, length = val
                if length == 0:
                    return f"{ind}<{field_name}>0</{field_name}>"
                arr_table = self.storage.get_array_table_for_type(field_type)
                byte_list = [arr_table.element_table.get(start_idx + j) for j in range(length)]
                signed = is_bigint_type(field_type)
                int_val = int.from_bytes(bytes(byte_list), byteorder='little', signed=signed)
                return f"{ind}<{field_name}>{int_val}</{field_name}>"

            if isinstance(base, ArrayTypeDefinition):
                start_idx, length = val

                if length == 0:
                    return f"{ind}<{field_name}/>"

                arr_table = self.storage.get_array_table_for_type(field_type)
                elem_base = base.element_type.resolve_base_type()

                # String type → joined string
                if is_string_type(field_type):
                    chars = [arr_table.element_table.get(start_idx + j) for j in range(length)]
                    text = escape("".join(chars))
                    return f"{ind}<{field_name}>{text}</{field_name}>"

                # String array (string[]) — resolve each (start, length) to a string
                if is_string_type(base.element_type):
                    char_table = self.storage.get_array_table_for_type(base.element_type)
                    elements_xml = []
                    for j in range(length):
                        cs, cl = arr_table.element_table.get(start_idx + j)
                        chars = [char_table.element_table.get(cs + k) for k in range(cl)]
                        text = escape("".join(chars))
                        elements_xml.append(f"{ind_inner}<item>{text}</item>")
                    if pretty:
                        inner = newline + newline.join(elements_xml) + newline + ind
                    else:
                        inner = "".join(elements_xml)
                    return f"{ind}<{field_name}>{inner}</{field_name}>"

                # Build array elements
                elements_xml = []
                for j in range(length):
                    elem_val = arr_table.element_table.get(start_idx + j)
                    if isinstance(elem_base, CompositeTypeDefinition):
                        if elem_val is None:
                            elements_xml.append(f"{ind_inner}<item null=\"true\"/>")
                        else:
                            elements_xml.append(f"{ind_inner}<item ref=\"#{record_id(elem_base.name, elem_val)}\"/>")
                    elif isinstance(elem_base, PrimitiveTypeDefinition):
                        prim_val = fmt_primitive(elem_val, elem_base)
                        elements_xml.append(f"{ind_inner}<item>{prim_val}</item>")
                    else:
                        elements_xml.append(f"{ind_inner}<item>{escape(str(elem_val))}</item>")

                if pretty:
                    inner = newline + newline.join(elements_xml) + newline + ind
                else:
                    inner = "".join(elements_xml)
                return f"{ind}<{field_name}>{inner}</{field_name}>"
            elif isinstance(base, PrimitiveTypeDefinition):
                prim_val = fmt_primitive(val, base)
                return f"{ind}<{field_name}>{prim_val}</{field_name}>"
            elif isinstance(base, CompositeTypeDefinition):
                # Reference to another record using ref
                return f"{ind}<{field_name} ref=\"#{record_id(field_type.name, val)}\"/>"
            elif isinstance(base, AliasTypeDefinition):
                return fmt_value(field_name, val, base.base_type, depth)
            else:
                return f"{ind}<{field_name}>{escape(str(val))}</{field_name}>"

        # Build the XML output
        db_name = escape(self.storage.data_dir.name)
        lines = ['<?xml version="1.0" encoding="UTF-8"?>\n']
        lines.append(f"<database name=\"{db_name}\">{newline}")

        for name, comp_def in types_to_dump:
            table_file = self.storage.data_dir / f"{name}.bin"
            if not table_file.exists():
                continue
            table = self.storage.get_table(name)

            has_records = False
            for i in range(table.count):
                if table.is_deleted(i) or not _include_record(name, i):
                    continue

                if not has_records:
                    lines.append(f"{indent}<{name}s>{newline}")
                    has_records = True

                raw = table.get(i)
                rec_id = record_id(name, i)

                # Build field elements
                field_lines = []
                for f in comp_def.fields:
                    val = raw[f.name]
                    field_lines.append(fmt_value(f.name, val, f.type_def, 3))

                if pretty:
                    fields_xml = newline.join(field_lines)
                    lines.append(f"{indent}{indent}<{name} id=\"{rec_id}\">{newline}{fields_xml}{newline}{indent}{indent}</{name}>{newline}")
                else:
                    fields_xml = "".join(field_lines)
                    lines.append(f"<{name} id=\"{rec_id}\">{fields_xml}</{name}>")

            if has_records:
                lines.append(f"{indent}</{name}s>{newline}")

        lines.append("</database>")

        script = "".join(lines) + "\n"
        return DumpResult(columns=[], rows=[], script=script, output_file=query.output_file)

    def _sort_composites_by_dependency(
        self, composites: list[tuple[str, CompositeTypeDefinition]]
    ) -> tuple[list[tuple[str, CompositeTypeDefinition]], list[tuple[str, CompositeTypeDefinition]]]:
        """Sort composite types so dependencies come first.

        Returns (sorted_types, cycle_types) where cycle_types are types
        involved in mutual references that couldn't be topologically sorted.
        Self-references are not considered cycles (they're handled naturally).
        """
        remaining = dict(composites)
        result: list[tuple[str, CompositeTypeDefinition]] = []
        cycle_types: list[tuple[str, CompositeTypeDefinition]] = []
        emitted: set[str] = set()

        max_iterations = len(remaining) + 1
        for _ in range(max_iterations):
            if not remaining:
                break
            emitted_this_pass = []
            for name, comp_def in remaining.items():
                deps_met = True
                for f in comp_def.fields:
                    fb = f.type_def.resolve_base_type()
                    if isinstance(fb, CompositeTypeDefinition) and fb.name != name:
                        if fb.name not in emitted:
                            deps_met = False
                            break
                    if isinstance(fb, ArrayTypeDefinition):
                        elem_base = fb.element_type.resolve_base_type()
                        if isinstance(elem_base, CompositeTypeDefinition) and elem_base.name != name:
                            if elem_base.name not in emitted:
                                deps_met = False
                                break
                if deps_met:
                    result.append((name, comp_def))
                    emitted.add(name)
                    emitted_this_pass.append(name)
            for name in emitted_this_pass:
                del remaining[name]
            if not emitted_this_pass:
                # Remaining types form mutual reference cycles
                cycle_types = list(remaining.items())
                break

        return result, cycle_types

    def _sort_interfaces_by_dependency(
        self, interfaces: list[tuple[str, InterfaceTypeDefinition]]
    ) -> list[tuple[str, InterfaceTypeDefinition]]:
        """Sort interfaces so parent interfaces come before children."""
        remaining = dict(interfaces)
        result: list[tuple[str, InterfaceTypeDefinition]] = []
        emitted: set[str] = set()

        max_iterations = len(remaining) + 1
        for _ in range(max_iterations):
            if not remaining:
                break
            emitted_this_pass = []
            for name, iface_def in remaining.items():
                # All parent interfaces must be emitted first
                if all(p in emitted or p not in remaining for p in iface_def.interfaces):
                    result.append((name, iface_def))
                    emitted.add(name)
                    emitted_this_pass.append(name)
            for name in emitted_this_pass:
                del remaining[name]
            if not emitted_this_pass:
                # Shouldn't happen for interfaces (no cycles), but append remainder
                result.extend(remaining.items())
                break

        return result

    def _transitive_type_closure(self, table_name: str) -> set[str]:
        """Compute transitive closure of type dependencies for a table."""
        needed: set[str] = set()
        stack = [table_name]
        while stack:
            name = stack.pop()
            if name in needed:
                continue
            needed.add(name)
            type_def = self.registry.get(name)
            if type_def is None:
                continue
            if isinstance(type_def, AliasTypeDefinition):
                stack.append(type_def.base_type.name)
            elif isinstance(type_def, EnumTypeDefinition):
                for v in type_def.variants:
                    for f in v.fields:
                        stack.append(f.type_def.name)
                        fb = f.type_def.resolve_base_type()
                        if isinstance(fb, ArrayTypeDefinition):
                            stack.append(fb.element_type.name)
            elif isinstance(type_def, InterfaceTypeDefinition):
                for f in type_def.fields:
                    stack.append(f.type_def.name)
                    fb = f.type_def.resolve_base_type()
                    if isinstance(fb, ArrayTypeDefinition):
                        stack.append(fb.element_type.name)
                # Include parent interfaces in the closure
                for iface_name in type_def.interfaces:
                    stack.append(iface_name)
            elif isinstance(type_def, CompositeTypeDefinition):
                for f in type_def.fields:
                    stack.append(f.type_def.name)
                    fb = f.type_def.resolve_base_type()
                    if isinstance(fb, ArrayTypeDefinition):
                        stack.append(fb.element_type.name)
                # Include interfaces in the closure
                for iface_name in type_def.interfaces:
                    stack.append(iface_name)
            elif isinstance(type_def, ArrayTypeDefinition):
                stack.append(type_def.element_type.name)
        return needed

    def _detect_back_edges(
        self,
        composites: list[tuple[str, CompositeTypeDefinition]],
        include_record: Any,
    ) -> set[tuple[str, int, str, str, int]]:
        """DFS to find back-edges in composite reference graph.

        Returns set of (src_type, src_idx, field_name, tgt_type, tgt_idx).
        Only follows direct composite fields (not array elements).
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[tuple[str, int], int] = {}
        back_edges: set[tuple[str, int, str, str, int]] = set()

        comp_map = dict(composites)

        def dfs(type_name: str, idx: int) -> None:
            key = (type_name, idx)
            color[key] = GRAY
            comp_def = comp_map.get(type_name)
            if comp_def is None:
                color[key] = BLACK
                return
            table_file = self.storage.data_dir / f"{type_name}.bin"
            if not table_file.exists():
                color[key] = BLACK
                return
            table = self.storage.get_table(type_name)
            if idx >= table.count or table.is_deleted(idx):
                color[key] = BLACK
                return
            raw_record = table.get(idx)
            for f in comp_def.fields:
                ref = raw_record[f.name]
                if ref is None:
                    continue
                field_base = f.type_def.resolve_base_type()
                if isinstance(field_base, ArrayTypeDefinition):
                    continue
                if not isinstance(field_base, CompositeTypeDefinition):
                    continue
                tgt_key = (f.type_def.name, ref)
                c = color.get(tgt_key, WHITE)
                if c == GRAY:
                    # Back-edge found
                    back_edges.add((type_name, idx, f.name, f.type_def.name, ref))
                elif c == WHITE:
                    dfs(f.type_def.name, ref)
            color[key] = BLACK

        for name, comp_def in composites:
            table_file = self.storage.data_dir / f"{name}.bin"
            if not table_file.exists():
                continue
            table = self.storage.get_table(name)
            for i in range(table.count):
                if table.is_deleted(i):
                    continue
                if not include_record(name, i):
                    continue
                key = (name, i)
                if color.get(key, WHITE) == WHITE:
                    dfs(name, i)

        return back_edges

    def _format_record_as_create(
        self,
        type_name: str,
        composite_def: CompositeTypeDefinition,
        raw_record: dict[str, Any],
        dump_vars: dict[tuple[str, int], str] | None = None,
        record_index: int | None = None,
        pretty: bool = False,
        indent: int = 0,
        record_tags: dict[tuple[str, int], str] | None = None,
        back_edge_tags: dict[tuple[str, int, str], str] | None = None,
    ) -> str:
        """Format a raw composite record as a TTQ create statement with inline instances."""
        formatting: set[tuple[str, int]] = set()
        field_strs = []

        # Check if this record needs a tag declaration
        tag_name = None
        if record_tags and record_index is not None:
            key = (type_name, record_index)
            if key in record_tags:
                tag_name = record_tags[key]

        for f in composite_def.fields:
            ref = raw_record[f.name]
            # If this field is a back-edge, emit tag reference
            if back_edge_tags and record_index is not None and (type_name, record_index, f.name) in back_edge_tags:
                value_str = back_edge_tags[(type_name, record_index, f.name)]
            else:
                value_str = self._format_field_value(
                    f, ref, dump_vars, formatting, pretty=pretty, indent=indent + 4,
                    record_tags=record_tags, back_edge_tags=back_edge_tags,
                )
            field_strs.append(f"{f.name}={value_str}")

        # Prepend tag declaration if needed
        if tag_name:
            field_strs.insert(0, f"tag({tag_name})")

        if pretty:
            inner_indent = " " * (indent + 4)
            close_indent = " " * indent
            fields_joined = (",\n" + inner_indent).join(field_strs)
            return f"create {type_name}(\n{inner_indent}{fields_joined}\n{close_indent})"
        return f"create {type_name}({', '.join(field_strs)})"

    def _format_field_value(
        self,
        field: FieldDefinition,
        ref: Any,
        dump_vars: dict[tuple[str, int], str] | None = None,
        formatting: set[tuple[str, int]] | None = None,
        pretty: bool = False,
        indent: int = 0,
        record_tags: dict[tuple[str, int], str] | None = None,
        back_edge_tags: dict[tuple[str, int, str], str] | None = None,
    ) -> str:
        """Format a single field's value for dump output.

        The `formatting` set tracks the current recursion path to detect
        data cycles in self/mutually-referential types.
        """
        if formatting is None:
            formatting = set()

        if ref is None:
            return "null"

        field_base = field.type_def.resolve_base_type()

        if isinstance(field_base, EnumTypeDefinition):
            # For Swift-style enums, ref may be (disc, index) tuple — resolve first
            if field_base.has_associated_values and isinstance(ref, tuple):
                ref = self._resolve_swift_enum_ref(ref, field_base)
            if isinstance(ref, EnumValue):
                return self._format_enum_value_ttq(ref, field_base)
            return str(ref)

        elif isinstance(field_base, DictionaryTypeDefinition):
            start_index, length = ref
            if length == 0:
                return "{:}"
            arr_table = self.storage.get_array_table_for_type(field.type_def)
            entry_type = field_base.entry_type
            entry_base = entry_type.resolve_base_type()
            key_field = entry_base.get_field("key")
            val_field = entry_base.get_field("value")
            key_base = key_field.type_def.resolve_base_type()
            val_base = val_field.type_def.resolve_base_type()
            entry_strs = []
            for j in range(length):
                entry_idx = arr_table.element_table.get(start_index + j)
                entry_table = self.storage.get_table(entry_type.name)
                entry_record = entry_table.get(entry_idx)
                k_str = self._format_field_value(
                    key_field, entry_record["key"], dump_vars, formatting,
                    pretty=pretty, indent=indent,
                    record_tags=record_tags, back_edge_tags=back_edge_tags,
                )
                v_str = self._format_field_value(
                    val_field, entry_record["value"], dump_vars, formatting,
                    pretty=pretty, indent=indent,
                    record_tags=record_tags, back_edge_tags=back_edge_tags,
                )
                entry_strs.append(f"{k_str}: {v_str}")
            return "{" + ", ".join(entry_strs) + "}"

        elif isinstance(field_base, SetTypeDefinition):
            start_index, length = ref
            if length == 0:
                return "{,}"
            arr_table = self.storage.get_array_table_for_type(field.type_def)
            elem_base = field_base.element_type.resolve_base_type()
            elements = [arr_table.element_table.get(start_index + j) for j in range(length)]
            if is_string_type(field_base.element_type):
                # String set elements: each is (start, length) in char table
                char_table = self.storage.get_array_table_for_type(field_base.element_type)
                elem_strs = []
                for elem in elements:
                    cs, cl = elem
                    chars = [char_table.element_table.get(cs + k) for k in range(cl)]
                    elem_strs.append(self._format_ttq_string("".join(chars)))
            else:
                elem_strs = [self._format_ttq_value(e, elem_base) for e in elements]
            return "{" + ", ".join(elem_strs) + "}"

        elif is_fraction_type(field.type_def):
            frac = _fraction_decode(self.storage, ref[0], ref[1], ref[2], ref[3])
            if frac.denominator == 1:
                return f"fraction({frac.numerator})"
            return f"fraction({frac.numerator}, {frac.denominator})"

        elif is_bigint_type(field.type_def) or is_biguint_type(field.type_def):
            start_index, length = ref
            if length == 0:
                return "0"
            arr_table = self.storage.get_array_table_for_type(field.type_def)
            byte_list = [arr_table.element_table.get(start_index + j) for j in range(length)]
            signed = is_bigint_type(field.type_def)
            value = int.from_bytes(bytes(byte_list), byteorder='little', signed=signed)
            return str(value)

        elif isinstance(field_base, ArrayTypeDefinition):
            start_index, length = ref
            if length == 0:
                return "[]"
            arr_table = self.storage.get_array_table_for_type(field.type_def)

            elem_base = field_base.element_type.resolve_base_type()

            # String type → joined string
            if is_string_type(field.type_def):
                chars = [arr_table.element_table.get(start_index + j) for j in range(length)]
                s = "".join(chars)
                return self._format_ttq_string(s)

            elements = [arr_table.element_table.get(start_index + j) for j in range(length)]

            # String array (string[]) — resolve each (start, length) to a string
            if is_string_type(field_base.element_type):
                char_table = self.storage.get_array_table_for_type(field_base.element_type)
                elem_strs = []
                for elem in elements:
                    cs, cl = elem
                    chars = [char_table.element_table.get(cs + k) for k in range(cl)]
                    s = "".join(chars)
                    elem_strs.append(self._format_ttq_string(s))
                return f"[{', '.join(elem_strs)}]"

            if isinstance(elem_base, CompositeTypeDefinition):
                # Array of composites — format each as inline instance
                elem_strs = []
                for elem in elements:
                    if isinstance(elem, dict):
                        inline = self._format_inline_composite(
                            field_base.element_type, elem_base, elem, dump_vars, formatting,
                            pretty=pretty, indent=indent + 4,
                            record_tags=record_tags, back_edge_tags=back_edge_tags,
                        )
                        elem_strs.append(inline)
                    else:
                        elem_strs.append(str(elem))
                if pretty:
                    inner_indent = " " * (indent + 4)
                    close_indent = " " * indent
                    elems_joined = (",\n" + inner_indent).join(elem_strs)
                    return f"[\n{inner_indent}{elems_joined}\n{close_indent}]"
                return f"[{', '.join(elem_strs)}]"
            else:
                # Primitive array
                elem_strs = [self._format_ttq_value(e, elem_base) for e in elements]
                return f"[{', '.join(elem_strs)}]"

        elif isinstance(field_base, CompositeTypeDefinition):
            # Check if this reference has a variable binding
            if dump_vars:
                key = (field.type_def.name, ref)
                if key in dump_vars:
                    return f"${dump_vars[key]}"
            # Check for data cycle
            cycle_key = (field.type_def.name, ref)
            if cycle_key in formatting:
                # Data cycle detected — emit CompositeRef syntax instead of recursing
                return f"{field.type_def.name}({ref})"
            # Mark this node as being formatted
            formatting.add(cycle_key)
            # Composite field — load and format as inline instance
            comp_table = self.storage.get_table(field.type_def.name)
            nested_record = comp_table.get(ref)
            result = self._format_inline_composite(
                field.type_def, field_base, nested_record, dump_vars, formatting,
                pretty=pretty, indent=indent,
                record_index=ref, record_tags=record_tags, back_edge_tags=back_edge_tags,
            )
            # Remove from path so sibling branches don't get false positives
            formatting.discard(cycle_key)
            return result

        else:
            # Primitive/alias field — value is already inline
            return self._format_ttq_value(ref, field_base)

    def _format_inline_composite(
        self,
        type_def: TypeDefinition,
        composite_base: CompositeTypeDefinition,
        raw_record: dict[str, Any],
        dump_vars: dict[tuple[str, int], str] | None = None,
        formatting: set[tuple[str, int]] | None = None,
        pretty: bool = False,
        indent: int = 0,
        record_index: int | None = None,
        record_tags: dict[tuple[str, int], str] | None = None,
        back_edge_tags: dict[tuple[str, int, str], str] | None = None,
    ) -> str:
        """Format a composite record as an inline instance string."""
        if formatting is None:
            formatting = set()

        # Check if this record needs a tag declaration
        tag_name = None
        if record_tags and record_index is not None:
            key = (type_def.name, record_index)
            if key in record_tags:
                tag_name = record_tags[key]

        field_strs = []
        for f in composite_base.fields:
            ref = raw_record[f.name]
            # If this field is a taggable back-edge, emit tag reference
            if back_edge_tags and record_index is not None and (type_def.name, record_index, f.name) in back_edge_tags:
                value_str = back_edge_tags[(type_def.name, record_index, f.name)]
            else:
                value_str = self._format_field_value(
                    f, ref, dump_vars, formatting, pretty=pretty, indent=indent + 4,
                    record_tags=record_tags, back_edge_tags=back_edge_tags,
                )
            field_strs.append(f"{f.name}={value_str}")

        # Prepend tag declaration if needed
        if tag_name:
            field_strs.insert(0, f"tag({tag_name})")

        if pretty:
            inner_indent = " " * (indent + 4)
            close_indent = " " * indent
            fields_joined = (",\n" + inner_indent).join(field_strs)
            return f"{type_def.name}(\n{inner_indent}{fields_joined}\n{close_indent})"
        return f"{type_def.name}({', '.join(field_strs)})"

    def _format_enum_value_ttq(self, ev: EnumValue, enum_def: EnumTypeDefinition) -> str:
        """Format an EnumValue as TTQ syntax: Color.red or Shape.circle(cx=50.0, ...)."""
        if not ev.fields:
            return f"{enum_def.name}.{ev.variant_name}"
        variant = enum_def.get_variant(ev.variant_name)
        if not variant or not variant.fields:
            return f"{enum_def.name}.{ev.variant_name}"
        field_strs = []
        for vf in variant.fields:
            fval = ev.fields.get(vf.name)
            if fval is None:
                field_strs.append(f"{vf.name}=null")
            else:
                vf_base = vf.type_def.resolve_base_type()
                if isinstance(vf_base, ArrayTypeDefinition):
                    # Array stored as (start, length) — resolve it
                    if isinstance(fval, tuple):
                        start_index, length = fval
                        if length == 0:
                            field_strs.append(f"{vf.name}=[]")
                        else:
                            arr_table = self.storage.get_array_table_for_type(vf.type_def)
                            elements = [arr_table.element_table.get(start_index + j) for j in range(length)]
                            if is_string_type(vf.type_def):
                                field_strs.append(f"{vf.name}={self._format_ttq_string(''.join(elements))}")
                            else:
                                elem_strs = [self._format_ttq_value(e, vf_base.element_type.resolve_base_type()) for e in elements]
                                field_strs.append(f"{vf.name}=[{', '.join(elem_strs)}]")
                    else:
                        field_strs.append(f"{vf.name}={fval}")
                elif isinstance(vf_base, EnumTypeDefinition) and isinstance(fval, EnumValue):
                    field_strs.append(f"{vf.name}={self._format_enum_value_ttq(fval, vf_base)}")
                else:
                    field_strs.append(f"{vf.name}={self._format_ttq_value(fval, vf_base)}")
        return f"{enum_def.name}.{ev.variant_name}({', '.join(field_strs)})"

    def _format_ttq_value(self, value: Any, type_base: TypeDefinition) -> str:
        """Format a primitive value as a TTQ literal."""
        if isinstance(type_base, BooleanTypeDefinition):
            return "true" if value else "false"
        if isinstance(type_base, PrimitiveTypeDefinition):
            if type_base.primitive == PrimitiveType.BIT:
                return "1" if value else "0"
            elif type_base.primitive == PrimitiveType.CHARACTER:
                return self._format_ttq_string(value)
            elif type_base.primitive in (PrimitiveType.FLOAT16, PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
                return repr(value)
            else:
                return str(value)
        return str(value)

    def _format_ttq_string(self, value: str) -> str:
        """Format a string as a TTQ string literal with proper escaping."""
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'

    def _build_field_references(
        self,
        composite_type: CompositeTypeDefinition,
        values: dict[str, Any],
    ) -> dict[str, Any]:
        """Build field references for a composite, storing sub-field values in their tables.

        Same logic as _create_instance but returns the field_references dict
        instead of inserting into the composite's own table.
        """
        field_references: dict[str, Any] = {}

        for field in composite_type.fields:
            field_value = values.get(field.name)
            field_base = field.type_def.resolve_base_type()

            if field_value is None:
                field_references[field.name] = None
                continue

            if isinstance(field_base, FractionTypeDefinition):
                if isinstance(field_value, Fraction):
                    field_references[field.name] = _fraction_encode(field_value, self.storage)
                elif isinstance(field_value, (int, BigInt, BigUInt)):
                    field_references[field.name] = _fraction_encode(Fraction(int(field_value)), self.storage)
                else:
                    raise ValueError(f"Expected fraction value for field '{field.name}', got {type(field_value).__name__}")
                continue

            if isinstance(field_base, (BigIntTypeDefinition, BigUIntTypeDefinition)):
                val = int(field_value)
                signed = isinstance(field_base, BigIntTypeDefinition)
                if not signed and val < 0:
                    raise ValueError(f"biguint field '{field.name}' cannot store negative value: {val}")
                if val == 0:
                    byte_list = [0]
                elif signed:
                    byte_length = (val.bit_length() + 8) // 8
                    byte_list = list(val.to_bytes(byte_length, byteorder='little', signed=True))
                else:
                    byte_length = (val.bit_length() + 7) // 8
                    byte_list = list(val.to_bytes(byte_length, byteorder='little', signed=False))
                array_table = self.storage.get_array_table_for_type(field.type_def)
                field_references[field.name] = array_table.insert(byte_list)
                continue

            if isinstance(field_base, ArrayTypeDefinition):
                if isinstance(field_value, str):
                    field_value = list(field_value)
                if isinstance(field_base.element_type.resolve_base_type(), CompositeTypeDefinition):
                    resolved = []
                    for elem in field_value:
                        if isinstance(elem, InlineInstance):
                            inline_type = self.registry.get(elem.type_name)
                            inline_base = inline_type.resolve_base_type()
                            inline_values = {}
                            for fv in elem.fields:
                                inline_values[fv.name] = self._resolve_instance_value(fv.value)
                            elem = self._build_field_references(inline_base, inline_values)
                        elif isinstance(elem, int):
                            elem_type_name = field_base.element_type.name
                            ref_table = self.storage.get_table(elem_type_name)
                            elem = ref_table.get(elem)
                        resolved.append(elem)
                    field_value = resolved
                array_table = self.storage.get_array_table_for_type(field.type_def)
                field_references[field.name] = array_table.insert(field_value)
                continue

            if isinstance(field_base, CompositeTypeDefinition):
                if isinstance(field_value, int):
                    field_references[field.name] = field_value
                else:
                    nested_index = self._create_instance(field.type_def, field_base, field_value)
                    field_references[field.name] = nested_index
            else:
                # Primitive — store value inline
                field_references[field.name] = field_value

        return field_references

    # --- Execute script ---

    def _execute_execute(self, query: ExecuteQuery) -> ExecuteResult:
        """Execute a TTQ script file."""
        # Resolve file path relative to the current script directory (if any)
        raw_path = Path(query.file_path)
        if not raw_path.is_absolute() and self._script_stack:
            raw_path = self._script_stack[-1] / raw_path

        # Auto-append extension if not a file (directories are skipped)
        script_path = raw_path
        if not script_path.is_file() and not script_path.suffix:
            for ext in (".ttq", ".ttq.gz"):
                candidate = Path(str(script_path) + ext)
                if candidate.is_file():
                    script_path = candidate
                    break

        if not script_path.is_file():
            raise FileNotFoundError(f"Script file not found: {script_path}")

        # Resolve to absolute for dedup tracking
        abs_path = str(script_path.resolve())

        # Check for re-execution (cycle detection)
        if abs_path in self._loaded_scripts:
            raise RuntimeError(f"Script already loaded: {script_path} (circular execute detected)")

        self._loaded_scripts.add(abs_path)

        # Read file content
        if script_path.suffix == ".gz":
            with gzip.open(script_path, "rt", encoding="utf-8") as f:
                content = f.read()
        else:
            content = script_path.read_text()

        # Parse all statements
        parser = QueryParser()
        content = content.strip()
        if not content:
            return ExecuteResult(
                columns=[],
                rows=[],
                message=f"Executed {script_path} (0 statements)",
                file_path=str(script_path),
                statements_executed=0,
            )
        queries = parser.parse_program(content)

        # Reject lifecycle commands inside executed scripts
        for q in queries:
            if isinstance(q, (UseQuery, DropDatabaseQuery, RestoreQuery)):
                raise RuntimeError(
                    f"{type(q).__name__} is not allowed inside executed scripts"
                )

        # Push script directory onto stack for relative path resolution
        script_dir = script_path.resolve().parent
        self._script_stack.append(script_dir)

        try:
            count = 0
            for q in queries:
                self.execute(q)
                count += 1
        finally:
            self._script_stack.pop()

        return ExecuteResult(
            columns=[],
            rows=[],
            message=f"Executed {script_path} ({count} statement{'s' if count != 1 else ''})",
            file_path=str(script_path),
            statements_executed=count,
        )

    # --- Import ---

    def _ensure_import_record_type(self) -> None:
        """Lazily create the _ImportRecord system type if it doesn't exist."""
        if self.registry.get("_ImportRecord") is not None:
            return
        path_type = self.registry.get_or_raise("path")
        import_type = CompositeTypeDefinition(
            name="_ImportRecord",
            fields=[FieldDefinition(name="script", type_def=path_type)],
        )
        self.registry.register(import_type)
        self.storage.save_metadata()

    def _is_imported(self, import_key: str) -> bool:
        """Check if a script has already been imported into the database."""
        table_file = self.storage.data_dir / "_ImportRecord.bin"
        if not table_file.exists():
            return False
        type_def = self.registry.get_or_raise("_ImportRecord")
        base = type_def.resolve_base_type()
        table = self.storage.get_table("_ImportRecord")
        for i in range(table.count):
            raw = table.get(i)
            if raw is None:
                continue
            resolved = self._resolve_raw_composite(raw, base, type_def)
            if resolved.get("script") == import_key:
                return True
        return False

    def _record_import(self, import_key: str) -> None:
        """Record that a script has been imported."""
        type_def = self.registry.get_or_raise("_ImportRecord")
        base = type_def.resolve_base_type()
        values = {"script": import_key}
        self._create_instance(type_def, base, values)

    def _execute_import(self, query: ImportQuery) -> ImportResult:
        """Execute an IMPORT query — run a script once per database."""
        user_file = query.file_path

        # Resolve file path for execution (same logic as _execute_execute)
        raw_path = Path(user_file)
        if not raw_path.is_absolute() and self._script_stack:
            raw_path = self._script_stack[-1] / raw_path

        script_path = raw_path
        auto_ext = ""
        if not script_path.is_file() and not script_path.suffix:
            for ext in (".ttq", ".ttq.gz"):
                candidate = Path(str(script_path) + ext)
                if candidate.is_file():
                    script_path = candidate
                    auto_ext = ext
                    break

        if not script_path.is_file():
            raise FileNotFoundError(f"Script file not found: {script_path}")

        # Build normalized import key from the user's original path
        # Relative paths stay relative, absolute stay absolute.
        # normpath collapses "./" so "setup.ttq" == "./setup.ttq".
        import_key = os.path.normpath(user_file + auto_ext)

        # Ensure tracking type exists
        self._ensure_import_record_type()

        # Check if already imported
        if self._is_imported(import_key):
            return ImportResult(
                columns=[], rows=[],
                message=f"Already imported: {import_key}",
                file_path=import_key,
                skipped=True,
            )

        # Delegate to execute
        exec_result = self._execute_execute(ExecuteQuery(file_path=query.file_path))

        # Record the import
        self._record_import(import_key)

        return ImportResult(
            columns=[], rows=[],
            message=f"Imported {import_key} ({exec_result.statements_executed} statement{'s' if exec_result.statements_executed != 1 else ''})",
            file_path=import_key,
            skipped=False,
        )

    # --- Compact ---

    def _execute_compact(self, query: CompactQuery) -> CompactResult:
        """Execute a COMPACT TO query: create a compacted copy of the database."""
        output_path = Path(query.output_path)

        # Error if output path already exists
        if output_path.exists():
            return CompactResult(
                columns=[], rows=[],
                message=f"Output path already exists: {output_path}",
            )

        # Collect all composite type names that have .bin files
        composite_types: list[tuple[str, CompositeTypeDefinition]] = []
        for type_name in self.registry.list_types():
            type_def = self.registry.get(type_name)
            if isinstance(type_def, CompositeTypeDefinition):
                bin_file = self.storage.data_dir / f"{type_name}.bin"
                if bin_file.exists():
                    composite_types.append((type_name, type_def))

        # Phase 1: Build composite index mappings
        comp_index_map: dict[str, dict[int, int]] = {}  # type_name → {old_idx: new_idx}
        total_before = 0
        total_after = 0

        for type_name, type_def in composite_types:
            table = self.storage.get_table(type_name)
            old_to_new: dict[int, int] = {}
            new_idx = 0
            for old_idx in range(table.count):
                if not table.is_deleted(old_idx):
                    old_to_new[old_idx] = new_idx
                    new_idx += 1
            comp_index_map[type_name] = old_to_new
            total_before += table.count
            total_after += new_idx

        # Phase 2: Collect referenced array ranges and variant indices
        # array_refs[type_name] = set of (start_index, length) tuples
        array_refs: dict[str, set[tuple[int, int]]] = {}
        # variant_refs[enum_name][variant_name] = set of variant_table indices
        variant_refs: dict[str, dict[str, set[int]]] = {}

        def _collect_refs_from_record(
            record: dict[str, Any],
            fields: list[FieldDefinition],
        ) -> None:
            """Collect array and variant references from a record's fields."""
            for fld in fields:
                val = record.get(fld.name)
                if val is None:
                    continue
                fld_base = fld.type_def.resolve_base_type()
                if isinstance(fld_base, FractionTypeDefinition):
                    num_start, num_len, den_start, den_len = val
                    if num_len > 0:
                        if "_frac_num" not in array_refs:
                            array_refs["_frac_num"] = set()
                        array_refs["_frac_num"].add((num_start, num_len))
                    if den_len > 0:
                        if "_frac_den" not in array_refs:
                            array_refs["_frac_den"] = set()
                        array_refs["_frac_den"].add((den_start, den_len))
                elif isinstance(fld_base, ArrayTypeDefinition):
                    start_idx, length = val
                    if length > 0:
                        arr_type_name = fld.type_def.name
                        if arr_type_name not in array_refs:
                            array_refs[arr_type_name] = set()
                        array_refs[arr_type_name].add((start_idx, length))
                elif isinstance(fld_base, EnumTypeDefinition) and fld_base.has_associated_values:
                    disc, variant_idx = val
                    if variant_idx != NULL_REF:
                        enum_name = fld_base.name
                        variant = fld_base.get_variant_by_discriminant(disc)
                        if variant is not None:
                            if enum_name not in variant_refs:
                                variant_refs[enum_name] = {}
                            if variant.name not in variant_refs[enum_name]:
                                variant_refs[enum_name][variant.name] = set()
                            variant_refs[enum_name][variant.name].add(variant_idx)

        # Walk all live composite records
        for type_name, type_def in composite_types:
            table = self.storage.get_table(type_name)
            for old_idx in comp_index_map[type_name]:
                record = table.get(old_idx)
                _collect_refs_from_record(record, type_def.fields)

        # Walk all live variant records to collect nested array refs
        for enum_name, variants_dict in variant_refs.items():
            enum_def = self.registry.get(enum_name)
            if not isinstance(enum_def, EnumTypeDefinition):
                continue
            for variant_name, indices in variants_dict.items():
                variant = enum_def.get_variant(variant_name)
                if variant is None or not variant.fields:
                    continue
                variant_table = self.storage.get_variant_table(enum_def, variant_name)
                for vidx in indices:
                    vrecord = variant_table.get(vidx)
                    _collect_refs_from_record(vrecord, variant.fields)

        # Phase 3: Build array start_index mapping
        array_start_map: dict[str, dict[int, int]] = {}  # type_name → {old_start: new_start}
        for arr_type_name, ranges in array_refs.items():
            sorted_ranges = sorted(ranges, key=lambda r: r[0])
            mapping: dict[int, int] = {}
            new_start = 0
            for old_start, length in sorted_ranges:
                mapping[old_start] = new_start
                new_start += length
            array_start_map[arr_type_name] = mapping

        # Phase 4: Build variant index mapping
        variant_index_map: dict[str, dict[str, dict[int, int]]] = {}  # enum → variant → {old: new}
        for enum_name, variants_dict in variant_refs.items():
            variant_index_map[enum_name] = {}
            for variant_name, indices in variants_dict.items():
                sorted_indices = sorted(indices)
                mapping = {}
                for new_idx, old_idx in enumerate(sorted_indices):
                    mapping[old_idx] = new_idx
                variant_index_map[enum_name][variant_name] = mapping

        # Phase 5: Create output database
        output_path.mkdir(parents=True, exist_ok=True)
        # Copy metadata
        src_metadata = self.storage.data_dir / "_metadata.json"
        if src_metadata.exists():
            shutil.copy2(src_metadata, output_path / "_metadata.json")

        # Create output storage
        out_registry = self.registry  # Share registry (read-only)
        out_storage = StorageManager(output_path, out_registry)

        try:
            # Phase 6: Write compacted array element tables
            for arr_type_name, ranges in array_refs.items():
                sorted_ranges = sorted(ranges, key=lambda r: r[0])
                # Special case: fraction byte tables (not in registry)
                if arr_type_name in ("_frac_num", "_frac_den"):
                    if arr_type_name == "_frac_num":
                        src_array_table = self.storage.get_fraction_num_table()
                        dst_array_table = out_storage.get_fraction_num_table()
                    else:
                        src_array_table = self.storage.get_fraction_den_table()
                        dst_array_table = out_storage.get_fraction_den_table()
                    for old_start, length in sorted_ranges:
                        elements = src_array_table.get(old_start, length)
                        dst_array_table.insert(elements)
                    continue
                type_def = self.registry.get(arr_type_name)
                if type_def is None:
                    continue
                base = type_def.resolve_base_type()
                element_base = base.element_type.resolve_base_type() if isinstance(base, ArrayTypeDefinition) else None
                src_array_table = self.storage.get_array_table_for_type(type_def)
                dst_array_table = out_storage.get_array_table_for_type(type_def)
                for old_start, length in sorted_ranges:
                    elements = src_array_table.get(old_start, length)
                    # Remap composite ref elements (e.g., Person[] stores uint32 indices)
                    if isinstance(element_base, CompositeTypeDefinition):
                        ref_type_name = element_base.name
                        remapped_elements = []
                        for elem in elements:
                            if ref_type_name in comp_index_map:
                                new_idx = comp_index_map[ref_type_name].get(elem)
                                if new_idx is not None:
                                    remapped_elements.append(new_idx)
                                else:
                                    # Dangling ref — skip element (can't store null in element table)
                                    # Use NULL_REF as sentinel
                                    remapped_elements.append(NULL_REF)
                            else:
                                remapped_elements.append(elem)
                        elements = remapped_elements
                    dst_array_table.insert(elements)

            # Phase 7: Write compacted variant tables
            for enum_name, variants_dict in variant_index_map.items():
                enum_def = self.registry.get(enum_name)
                if not isinstance(enum_def, EnumTypeDefinition):
                    continue
                for variant_name, idx_mapping in variants_dict.items():
                    variant = enum_def.get_variant(variant_name)
                    if variant is None or not variant.fields:
                        continue
                    src_vtable = self.storage.get_variant_table(enum_def, variant_name)
                    dst_vtable = out_storage.get_variant_table(enum_def, variant_name)
                    for old_vidx in sorted(idx_mapping.keys()):
                        vrecord = src_vtable.get(old_vidx)
                        remapped = self._remap_record(
                            vrecord, variant.fields,
                            comp_index_map, array_start_map, variant_index_map,
                        )
                        dst_vtable.insert(remapped)

            # Phase 8: Write compacted composite tables
            for type_name, type_def in composite_types:
                if not comp_index_map[type_name]:
                    continue  # No live records
                src_table = self.storage.get_table(type_name)
                dst_table = out_storage.get_table(type_name)
                for old_idx in sorted(comp_index_map[type_name].keys()):
                    record = src_table.get(old_idx)
                    remapped = self._remap_record(
                        record, type_def.fields,
                        comp_index_map, array_start_map, variant_index_map,
                    )
                    dst_table.insert(remapped)

        finally:
            out_storage.close()

        return CompactResult(
            columns=[], rows=[],
            output_path=str(output_path),
            records_before=total_before,
            records_after=total_after,
            message=f"Compacted to {output_path} ({total_before} -> {total_after} records)",
        )

    def _remap_record(
        self,
        record: dict[str, Any],
        fields: list[FieldDefinition],
        comp_index_map: dict[str, dict[int, int]],
        array_start_map: dict[str, dict[int, int]],
        variant_index_map: dict[str, dict[str, dict[int, int]]],
    ) -> dict[str, Any]:
        """Remap references in a record for compaction."""
        remapped: dict[str, Any] = {}
        for fld in fields:
            val = record.get(fld.name)
            if val is None:
                remapped[fld.name] = None
                continue

            fld_base = fld.type_def.resolve_base_type()

            if isinstance(fld_base, InterfaceTypeDefinition):
                type_id, old_idx = val
                ref_type_name = self.registry.get_type_name_by_id(type_id)
                if ref_type_name and ref_type_name in comp_index_map:
                    new_idx = comp_index_map[ref_type_name].get(old_idx)
                    if new_idx is not None:
                        remapped[fld.name] = (type_id, new_idx)
                    else:
                        remapped[fld.name] = None  # Dangling ref
                else:
                    remapped[fld.name] = None  # Unknown type
            elif isinstance(fld_base, CompositeTypeDefinition):
                old_idx = val
                ref_type_name = fld_base.name
                if ref_type_name in comp_index_map:
                    new_idx = comp_index_map[ref_type_name].get(old_idx)
                    if new_idx is not None:
                        remapped[fld.name] = new_idx
                    else:
                        remapped[fld.name] = None  # Dangling ref
                else:
                    remapped[fld.name] = val
            elif isinstance(fld_base, FractionTypeDefinition):
                num_start, num_len, den_start, den_len = val
                new_num_start = num_start
                new_den_start = den_start
                if num_len > 0 and "_frac_num" in array_start_map and num_start in array_start_map["_frac_num"]:
                    new_num_start = array_start_map["_frac_num"][num_start]
                if den_len > 0 and "_frac_den" in array_start_map and den_start in array_start_map["_frac_den"]:
                    new_den_start = array_start_map["_frac_den"][den_start]
                remapped[fld.name] = (new_num_start, num_len, new_den_start, den_len)
            elif isinstance(fld_base, ArrayTypeDefinition):
                old_start, length = val
                if length == 0:
                    remapped[fld.name] = (0, 0)
                else:
                    arr_type_name = fld.type_def.name
                    if arr_type_name in array_start_map and old_start in array_start_map[arr_type_name]:
                        remapped[fld.name] = (array_start_map[arr_type_name][old_start], length)
                    else:
                        remapped[fld.name] = val
            elif isinstance(fld_base, EnumTypeDefinition) and fld_base.has_associated_values:
                disc, variant_idx = val
                if variant_idx == NULL_REF:
                    remapped[fld.name] = (disc, NULL_REF)
                else:
                    enum_name = fld_base.name
                    variant = fld_base.get_variant_by_discriminant(disc)
                    if (variant is not None
                            and enum_name in variant_index_map
                            and variant.name in variant_index_map[enum_name]
                            and variant_idx in variant_index_map[enum_name][variant.name]):
                        new_vidx = variant_index_map[enum_name][variant.name][variant_idx]
                        remapped[fld.name] = (disc, new_vidx)
                    else:
                        remapped[fld.name] = val
            else:
                # Primitive, C-style enum — pass through unchanged
                remapped[fld.name] = val

        return remapped

    def _execute_archive(self, query: ArchiveQuery) -> ArchiveResult:
        """Execute an ARCHIVE TO query: compact then bundle into a .ttar file."""
        if query.output_file is None:
            # Derive from database name
            output_file = self.storage.data_dir.name + ".ttar"
        else:
            output_file = query.output_file
            # Append .ttar if no extension
            if not Path(output_file).suffix:
                output_file += ".ttar"

        output_path = Path(output_file)
        if output_path.exists() and not query.overwrite:
            return ArchiveResult(
                columns=[], rows=[],
                message=f"Output file already exists: {output_path}",
                output_file=str(output_path),
                exists=True,
            )
        if output_path.exists() and query.overwrite:
            output_path.unlink()

        tmp_dir = None
        try:
            # Compact into a temp directory
            tmp_dir = Path(tempfile.mkdtemp(prefix="ttar_"))
            compact_dir = tmp_dir / "db"
            compact_result = self._execute_compact(CompactQuery(output_path=str(compact_dir)))
            if compact_result.message and "already exists" in compact_result.message:
                return ArchiveResult(columns=[], rows=[], message=compact_result.message)

            # Read metadata
            metadata_path = compact_dir / "_metadata.json"
            metadata_bytes = metadata_path.read_bytes() if metadata_path.exists() else b"{}"

            # Enumerate .bin files sorted by relative path
            bin_files: list[tuple[str, Path]] = []
            for f in sorted(compact_dir.rglob("*.bin")):
                rel = f.relative_to(compact_dir)
                bin_files.append((str(rel), f))

            # Compute trimmed sizes and build file index
            file_entries: list[tuple[str, int, int]] = []  # (rel_path, data_offset, data_length)
            data_offset = 0
            for rel_path, abs_path in bin_files:
                trimmed = self._calc_trimmed_bin_size(abs_path, rel_path)
                file_entries.append((rel_path, data_offset, trimmed))
                data_offset += trimmed

            # Write the archive
            total_bytes = _write_ttar(output_path, metadata_bytes, file_entries, bin_files)

            return ArchiveResult(
                columns=[], rows=[],
                output_file=str(output_path),
                file_count=len(bin_files),
                total_bytes=total_bytes,
                message=f"Archived to {output_path} ({len(bin_files)} files, {total_bytes} bytes)",
            )
        finally:
            if tmp_dir and tmp_dir.exists():
                shutil.rmtree(tmp_dir)

    def _calc_trimmed_bin_size(self, abs_path: Path, rel_path: str) -> int:
        """Calculate the trimmed size of a .bin file (header + actual records only)."""
        file_size = abs_path.stat().st_size
        if file_size < 8:
            return file_size

        with open(abs_path, "rb") as f:
            count = struct.unpack("<Q", f.read(8))[0]

        record_size = self._get_record_size_for_bin(rel_path)
        if record_size is not None:
            return 8 + count * record_size
        # Fallback: use actual file size (compacted db has minimal padding)
        return file_size

    def _get_record_size_for_bin(self, rel_path: str) -> int | None:
        """Look up the record size for a .bin file by its relative path."""
        parts = Path(rel_path).parts
        if len(parts) == 1:
            # Root file: Person.bin → type name "Person"
            type_name = Path(parts[0]).stem
            type_def = self.registry.get(type_name)
            if type_def is not None:
                return type_def.size_bytes
        elif len(parts) == 2:
            # Subdirectory: Shape/circle.bin → enum "Shape", variant "circle"
            enum_name = parts[0]
            variant_name = Path(parts[1]).stem
            enum_def = self.registry.get(enum_name)
            if isinstance(enum_def, EnumTypeDefinition):
                variant = enum_def.get_variant(variant_name)
                if variant is not None and variant.fields:
                    synth = CompositeTypeDefinition(
                        name=f"_{enum_name}_{variant_name}",
                        fields=list(variant.fields),
                    )
                    return synth.size_bytes
        return None


def _write_ttar(
    output_path: Path,
    metadata_bytes: bytes,
    file_entries: list[tuple[str, int, int]],
    bin_files: list[tuple[str, Path]],
) -> int:
    """Write a .ttar archive file. Returns total bytes written."""
    MAGIC = b"TTAR"
    VERSION = 1

    opener = gzip.open if output_path.suffix == ".gz" else open
    with opener(output_path, "wb") as f:
        # Header
        f.write(MAGIC)
        f.write(struct.pack("<H", VERSION))
        f.write(struct.pack("<I", len(metadata_bytes)))
        f.write(metadata_bytes)
        f.write(struct.pack("<I", len(file_entries)))

        # File index
        for rel_path, data_offset, data_length in file_entries:
            path_bytes = rel_path.encode("utf-8")
            f.write(struct.pack("<H", len(path_bytes)))
            f.write(path_bytes)
            f.write(struct.pack("<I", data_offset))
            f.write(struct.pack("<I", data_length))

        # Data section
        for (rel_path, abs_path), (_, _, data_length) in zip(bin_files, file_entries):
            with open(abs_path, "rb") as src:
                data = src.read(data_length)
                f.write(data)

        total = f.tell()
    return total


def execute_restore(query: RestoreQuery) -> RestoreResult:
    """Execute a RESTORE query: extract a .ttar archive into a new database directory.

    This is a module-level function so it can be called without an executor instance.
    """
    archive_path = Path(query.archive_file)

    # Auto-add .ttar or .ttar.gz extension if file not found
    if not archive_path.exists() and not archive_path.suffix:
        for ext in (".ttar", ".ttar.gz"):
            candidate = Path(str(archive_path) + ext)
            if candidate.exists():
                archive_path = candidate
                break

    if query.output_path is not None:
        output_path = Path(query.output_path)
    else:
        # Derive from archive filename: strip .gz then .ttar
        name = archive_path.name
        for ext in (".gz", ".ttar"):
            if name.endswith(ext):
                name = name[: -len(ext)]
        output_path = archive_path.parent / name

    if not archive_path.exists():
        return RestoreResult(
            columns=[], rows=[],
            message=f"Archive file not found: {archive_path}",
        )

    if output_path.exists():
        return RestoreResult(
            columns=[], rows=[],
            message=f"Output path already exists: {output_path}",
        )

    MAGIC = b"TTAR"

    try:
        opener = gzip.open if archive_path.suffix == ".gz" else open
        with opener(archive_path, "rb") as f:
            # Validate magic
            magic = f.read(4)
            if magic != MAGIC:
                return RestoreResult(
                    columns=[], rows=[],
                    message=f"Invalid archive file (bad magic bytes): {archive_path}",
                )

            # Read version
            version = struct.unpack("<H", f.read(2))[0]
            if version != 1:
                return RestoreResult(
                    columns=[], rows=[],
                    message=f"Unsupported archive version: {version}",
                )

            # Read metadata
            metadata_len = struct.unpack("<I", f.read(4))[0]
            metadata_bytes = f.read(metadata_len)

            # Read file count
            file_count = struct.unpack("<I", f.read(4))[0]

            # Read file index
            file_index: list[tuple[str, int, int]] = []
            for _ in range(file_count):
                path_len = struct.unpack("<H", f.read(2))[0]
                rel_path = f.read(path_len).decode("utf-8")
                data_offset = struct.unpack("<I", f.read(4))[0]
                data_length = struct.unpack("<I", f.read(4))[0]
                file_index.append((rel_path, data_offset, data_length))

            # Record where data section starts
            data_section_start = f.tell()

            # Create output directory and write metadata
            output_path.mkdir(parents=True, exist_ok=True)
            (output_path / "_metadata.json").write_bytes(metadata_bytes)

            # Write each .bin file
            for rel_path, data_offset, data_length in file_index:
                bin_path = output_path / rel_path
                bin_path.parent.mkdir(parents=True, exist_ok=True)
                f.seek(data_section_start + data_offset)
                data = f.read(data_length)
                with open(bin_path, "wb") as out:
                    out.write(data)

    except Exception as e:
        # Clean up partial output on error
        if output_path.exists():
            shutil.rmtree(output_path)
        return RestoreResult(
            columns=[], rows=[],
            message=f"Error restoring archive: {e}",
        )

    return RestoreResult(
        columns=[], rows=[],
        output_path=str(output_path),
        file_count=file_count,
        message=f"Restored to {output_path} ({file_count} files)",
    )
