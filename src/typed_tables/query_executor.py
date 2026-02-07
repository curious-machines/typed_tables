"""Query executor for TTQ queries."""

from __future__ import annotations

import re
import uuid as uuid_module
from dataclasses import dataclass
from typing import Any, Iterator

from typed_tables.parsing.query_parser import (
    ArrayIndex,
    ArraySlice,
    CollectQuery,
    CollectSource,
    CompoundCondition,
    CompositeRef,
    Condition,
    CreateAliasQuery,
    CreateInstanceQuery,
    CreateTypeQuery,
    DeleteQuery,
    ForwardTypeQuery,
    DescribeQuery,
    DropDatabaseQuery,
    DumpItem,
    DumpQuery,
    EvalQuery,
    FieldValue,
    FunctionCall,
    InlineInstance,
    NullValue,
    Query,
    ScopeBlock,
    SelectField,
    SelectQuery,
    ShowTablesQuery,
    TagReference,
    UpdateQuery,
    UseQuery,
    VariableAssignmentQuery,
    VariableReference,
)
from typed_tables.storage import StorageManager
from typed_tables.types import (
    AliasTypeDefinition,
    ArrayTypeDefinition,
    CompositeTypeDefinition,
    FieldDefinition,
    PrimitiveType,
    PrimitiveTypeDefinition,
    TypeDefinition,
    TypeRegistry,
)


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

    path: str = ""


@dataclass
class DumpResult(QueryResult):
    """Result of a DUMP query."""

    script: str = ""
    output_file: str | None = None


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

    def execute(self, query: Query) -> QueryResult:
        """Execute a query and return results."""
        if isinstance(query, ShowTablesQuery):
            return self._execute_show_tables()
        elif isinstance(query, DescribeQuery):
            return self._execute_describe(query)
        elif isinstance(query, SelectQuery):
            return self._execute_select(query)
        elif isinstance(query, UseQuery):
            return self._execute_use(query)
        elif isinstance(query, CreateTypeQuery):
            return self._execute_create_type(query)
        elif isinstance(query, ForwardTypeQuery):
            return self._execute_forward_type(query)
        elif isinstance(query, CreateAliasQuery):
            return self._execute_create_alias(query)
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

    def _execute_show_tables(self) -> QueryResult:
        """Execute SHOW TABLES query."""
        rows = []
        for type_name in sorted(self.registry.list_types()):
            type_def = self.registry.get(type_name)
            if type_def is None:
                continue

            # Check if table file exists
            table_file = self.storage.data_dir / f"{type_name}.bin"
            if not table_file.exists():
                continue

            base = type_def.resolve_base_type()
            kind = type_def.__class__.__name__.replace("TypeDefinition", "")

            # Skip standalone array types (no header table file anymore)
            if isinstance(base, ArrayTypeDefinition):
                continue

            # Get record count
            try:
                count = self.storage.get_table(type_name).count
            except Exception:
                count = 0

            rows.append({
                "table": type_name,
                "kind": kind,
                "count": count,
            })

        return QueryResult(
            columns=["table", "kind", "count"],
            rows=rows,
        )

    def _execute_describe(self, query: DescribeQuery) -> QueryResult:
        """Execute DESCRIBE query."""
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

        if isinstance(base, CompositeTypeDefinition):
            for field in base.fields:
                field_base = field.type_def.resolve_base_type()
                rows.append({
                    "property": field.name,
                    "type": field.type_def.name,
                    "size": field.type_def.reference_size,
                })
        elif isinstance(base, ArrayTypeDefinition):
            rows.append({
                "property": "(element_type)",
                "type": base.element_type.name,
                "size": base.element_type.size_bytes,
            })

        return QueryResult(
            columns=["property", "type", "size"],
            rows=rows,
        )

    def _execute_use(self, query: UseQuery) -> UseResult:
        """Execute USE query - returns path for REPL to switch databases."""
        return UseResult(
            columns=[],
            rows=[],
            path=query.path,
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

    def _execute_create_alias(self, query: CreateAliasQuery) -> CreateResult:
        """Execute CREATE ALIAS query."""
        # Check if alias name already exists
        if self.registry.get(query.name) is not None:
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Type '{query.name}' already exists",
                type_name=query.name,
            )

        # Get the base type
        base_type = self.registry.get(query.base_type)
        if base_type is None:
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Unknown base type: {query.base_type}",
                type_name=query.name,
            )

        # Create and register the alias
        alias = AliasTypeDefinition(name=query.name, base_type=base_type)
        self.registry.register(alias)

        # Save updated metadata
        self.storage.save_metadata()

        return CreateResult(
            columns=["alias", "base_type"],
            rows=[{"alias": query.name, "base_type": query.base_type}],
            message=f"Created alias '{query.name}' as '{query.base_type}'",
            type_name=query.name,
        )

    def _execute_create_type(self, query: CreateTypeQuery) -> CreateResult:
        """Execute CREATE TYPE query.

        Supports self-referential types and populating forward-declared stubs:
        - `forward type B` then `create type B a:A` → populates the stub
        - `create type Node children:Node[]` → self-referential (stub registered first)
        """
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

        # Handle inheritance
        parent_fields: list[FieldDefinition] = []
        if query.parent:
            # Check for circular inheritance
            if query.parent == query.name:
                return CreateResult(
                    columns=[],
                    rows=[],
                    message=f"Circular inheritance: '{query.name}' cannot inherit from itself",
                    type_name=query.name,
                )

            parent_type = self.registry.get(query.parent)
            if parent_type is None:
                return CreateResult(
                    columns=[],
                    rows=[],
                    message=f"Unknown parent type: {query.parent}",
                    type_name=query.name,
                )

            parent_base = parent_type.resolve_base_type()
            if not isinstance(parent_base, CompositeTypeDefinition):
                return CreateResult(
                    columns=[],
                    rows=[],
                    message=f"Cannot inherit from non-composite type: {query.parent}",
                    type_name=query.name,
                )

            # Copy parent fields
            parent_fields = list(parent_base.fields)

        # Build field definitions from query
        fields: list[FieldDefinition] = parent_fields.copy()
        for field_def in query.fields:
            # Handle 'string' as alias for 'character[]'
            type_name = field_def.type_name
            if type_name == "string":
                field_type = self.registry.get_array_type("character")
            else:
                # Check if it's an array type (ends with [])
                if type_name.endswith("[]"):
                    base_name = type_name[:-2]
                    field_type = self.registry.get_array_type(base_name)
                else:
                    field_type = self.registry.get(type_name)
                    if field_type is None:
                        return CreateResult(
                            columns=[],
                            rows=[],
                            message=f"Unknown type: {type_name}",
                            type_name=query.name,
                        )

            fields.append(FieldDefinition(name=field_def.name, type_def=field_type))

        # Mutate the stub in-place (Python object references propagate automatically)
        stub = self.registry.get(query.name)
        stub.fields = fields

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
            values[field_val.name] = value

        # Default missing fields to null
        for field in base.fields:
            if field.name not in values:
                values[field.name] = None

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
        """Execute UPDATE query — modify fields on an existing record."""
        # Resolve target: variable or direct Type(index)
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
        else:
            type_name = query.type_name
            index = query.index

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

        # Read current raw record
        raw_record = table.get(index)

        # Apply SET fields
        for fv in query.fields:
            field_def = base.get_field(fv.name)
            if field_def is None:
                return UpdateResult(
                    columns=[], rows=[],
                    message=f"Unknown field '{fv.name}' on type {type_name}",
                )

            resolved_value = self._resolve_instance_value(fv.value)
            field_base = field_def.type_def.resolve_base_type()

            if resolved_value is None:
                raw_record[fv.name] = None
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
                # Primitive — store value inline
                raw_record[fv.name] = resolved_value

        # Write back modified record
        table.update(index, raw_record)

        return UpdateResult(
            columns=["type", "index"],
            rows=[{"type": type_name, "index": index}],
            message=f"Updated {type_name}[{index}]",
            type_name=type_name,
            index=index,
        )

    def _resolve_instance_value(self, value: Any) -> Any:
        """Resolve a value from CREATE instance, handling function calls, composite refs, inline instances, variable refs, tag refs, and null."""
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
            if value.name == "uuid":
                # Generate a random UUID as uint128
                return uuid_module.uuid4().int
            else:
                raise ValueError(f"Unknown function: {value.name}()")
        elif isinstance(value, CompositeRef):
            # Return the index directly - the type will be validated during instance creation
            return value.index
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

            if isinstance(field_base, ArrayTypeDefinition):
                # Convert string to character list if needed
                if isinstance(field_value, str):
                    field_value = list(field_value)
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
                # Store array elements and get (start_index, length) tuple
                array_table = self.storage.get_array_table_for_type(field.type_def)
                field_references[field.name] = array_table.insert(field_value)
                continue

            if isinstance(field_base, CompositeTypeDefinition):
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
            value = self._resolve_instance_value(expr)

            # Use alias if provided, otherwise generate column name
            if alias:
                base_name = alias
            elif isinstance(expr, FunctionCall):
                base_name = f"{expr.name}()"
            else:
                base_name = f"expr_{i}"

            # Make column name unique if needed
            col_name = base_name
            suffix = 1
            while col_name in row:
                suffix += 1
                col_name = f"{base_name}_{suffix}"

            columns.append(col_name)

            # Format the value for display
            if isinstance(value, int) and value > 0xFFFFFFFF:
                # Format large integers as hex (likely UUIDs)
                row[col_name] = f"0x{value:032x}"
            else:
                row[col_name] = value

        return QueryResult(columns=columns, rows=[row])

    def _execute_delete(self, query: DeleteQuery) -> DeleteResult:
        """Execute DELETE query."""
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
            records = list(self._load_all_records(query.table, type_def))

        base = type_def.resolve_base_type()

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

                    if isinstance(field_base, ArrayTypeDefinition):
                        start_index, length = ref
                        if length == 0:
                            resolved[field.name] = []
                        else:
                            arr_table = self.storage.get_array_table_for_type(field.type_def)
                            elements = [
                                arr_table.element_table.get(start_index + j)
                                for j in range(length)
                            ]
                            # Convert character arrays to strings for easier querying
                            if all(isinstance(e, str) and len(e) == 1 for e in elements):
                                resolved[field.name] = "".join(elements)
                            else:
                                resolved[field.name] = elements
                    elif isinstance(field_base, CompositeTypeDefinition):
                        resolved[field.name] = f"<{field.type_def.name}[{ref}]>"
                    else:
                        # Primitive — value is already inline
                        resolved[field.name] = ref

                yield resolved

        elif isinstance(base, ArrayTypeDefinition):
            # Standalone array types no longer have header tables;
            # arrays are accessed through composites only
            return

        else:
            # Primitive/alias type — scan all composites that contain this field type
            yield from self._load_records_by_field_type(type_name, type_def)

    def _load_records_by_field_type(
        self, type_name: str, type_def: TypeDefinition
    ) -> Iterator[dict[str, Any]]:
        """Load values of a non-composite type by scanning composites that use it.

        Returns records with _source, _index, _field, and the type_name value.
        """
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
                yield {
                    "_source": comp_name,
                    "_index": i,
                    "_field": field_name,
                    type_name: ref,
                    "_value": ref,
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

                if isinstance(field_base, ArrayTypeDefinition):
                    start_index, length = ref
                    if length == 0:
                        resolved[field.name] = []
                    else:
                        arr_table = self.storage.get_array_table_for_type(field.type_def)
                        elements = [
                            arr_table.element_table.get(start_index + j)
                            for j in range(length)
                        ]
                        if all(isinstance(e, str) and len(e) == 1 for e in elements):
                            resolved[field.name] = "".join(elements)
                        else:
                            resolved[field.name] = elements
                elif isinstance(field_base, CompositeTypeDefinition):
                    resolved[field.name] = f"<{field.type_def.name}[{ref}]>"
                else:
                    # Primitive — value is already inline
                    resolved[field.name] = ref

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
        if field_value is None:
            return condition.negate

        result = self._compare(field_value, condition.operator, condition.value)
        return not result if condition.negate else result

    def _compare(self, field_value: Any, operator: str, value: Any) -> bool:
        """Compare a field value against a condition value."""
        try:
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
                    if field.array_index is not None and isinstance(value, (list, str)):
                        value = self._apply_array_index(value, field.array_index)
                    # Apply post-index path (e.g., employees[0].name)
                    if field.post_path is not None and value is not None:
                        value = self._resolve_post_index_path(value, field.post_path, field, type_def)
                    # Build column name with index notation if applicable
                    col_name = field.name
                    if field.array_index is not None and field.post_path is not None:
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
            if isinstance(field_type_base, ArrayTypeDefinition):
                start_index, length = ref
                if length == 0:
                    resolved[f.name] = []
                else:
                    arr_table = self.storage.get_array_table_for_type(f.type_def)
                    elements = [
                        arr_table.element_table.get(start_index + j)
                        for j in range(length)
                    ]
                    if all(isinstance(e, str) and len(e) == 1 for e in elements):
                        resolved[f.name] = "".join(elements)
                    else:
                        resolved[f.name] = elements
            elif isinstance(field_type_base, CompositeTypeDefinition):
                resolved[f.name] = f"<{f.type_def.name}[{ref}]>"
            else:
                # Primitive — value is already inline
                resolved[f.name] = ref
        return resolved

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

        return None

    def _apply_array_index(self, value: list | str, array_index: ArrayIndex) -> Any:
        """Apply array indexing to a list or string value."""
        if not isinstance(value, (list, str)):
            return value

        result = []
        for idx in array_index.indices:
            if isinstance(idx, int):
                # Single index
                if 0 <= idx < len(value):
                    result.append(value[idx])
            elif isinstance(idx, ArraySlice):
                # Slice
                start = idx.start if idx.start is not None else 0
                end = idx.end if idx.end is not None else len(value)
                result.extend(value[start:end])

        # If only one index was requested, return scalar
        if len(array_index.indices) == 1 and isinstance(array_index.indices[0], int):
            return result[0] if result else None

        # For strings, join back together
        if isinstance(value, str):
            return "".join(result)

        return result

    def _format_array_index(self, array_index: ArrayIndex) -> str:
        """Format an ArrayIndex for display."""
        parts = []
        for idx in array_index.indices:
            if isinstance(idx, int):
                parts.append(str(idx))
            elif isinstance(idx, ArraySlice):
                start = str(idx.start) if idx.start is not None else ""
                end = str(idx.end) if idx.end is not None else ""
                parts.append(f"{start}:{end}")
        return ", ".join(parts)

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

        lines: list[str] = []
        lines.append("-- TTQ dump")

        # Collect user-defined types (skip primitives and auto-generated array types)
        aliases: list[tuple[str, AliasTypeDefinition]] = []
        composites: list[tuple[str, CompositeTypeDefinition]] = []

        for type_name in self.registry.list_types():
            type_def = self.registry.get(type_name)
            if type_def is None:
                continue
            if isinstance(type_def, PrimitiveTypeDefinition):
                continue
            if isinstance(type_def, ArrayTypeDefinition):
                continue  # auto-generated array types
            if isinstance(type_def, AliasTypeDefinition):
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
            sorted_composites = [(n, t) for n, t in sorted_composites if n in needed]
            cycle_composites = [(n, t) for n, t in cycle_composites if n in needed]

        # Recombine for record dumping (sorted first, then cycle types)
        composites = sorted_composites + cycle_composites

        # Branch based on output format
        if query.format == "yaml":
            return self._execute_dump_yaml(
                query, dump_targets, aliases, composites, sorted_composites, cycle_composites
            )
        elif query.format == "json":
            return self._execute_dump_json(
                query, dump_targets, aliases, composites, sorted_composites, cycle_composites
            )
        elif query.format == "xml":
            return self._execute_dump_xml(
                query, dump_targets, aliases, composites, sorted_composites, cycle_composites
            )

        # TTQ format output
        # Emit aliases
        for name, alias_def in aliases:
            base_name = alias_def.base_type.name
            lines.append(f"create alias {name} as {base_name};")

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
            lines.append(f"forward type {name};")

        # Helper to emit a type definition
        def emit_type_def(name: str, comp_def: CompositeTypeDefinition) -> None:
            field_strs = []
            for f in comp_def.fields:
                field_type_name = f.type_def.name
                if field_type_name == "character[]":
                    field_type_name = "string"
                elif field_type_name.endswith("[]"):
                    base_elem = field_type_name[:-2]
                    field_type_name = f"{base_elem}[]"
                field_strs.append(f"{f.name}:{field_type_name}")
            if pretty:
                lines.append(f"create type {name}")
                for i, fs in enumerate(field_strs):
                    fs_with_space = fs.replace(":", ": ", 1)
                    suffix = ";" if i == len(field_strs) - 1 else ""
                    lines.append(f"    {fs_with_space}{suffix}")
                lines.append("")  # blank line between type blocks
            else:
                fields_part = " ".join(field_strs)
                lines.append(f"create type {name} {fields_part};")

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
            statement_target.append(f"${var_name} = {create_str};")
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
                statement_target.append(f"{create_str};")

        # If we collected statements for a scope block, wrap them
        if needs_scope and scope_statements:
            if pretty:
                # Pretty-print scope with indented statements
                lines.append("scope {")
                for stmt in scope_statements:
                    # Indent each line of the statement
                    for line in stmt.split('\n'):
                        lines.append(f"    {line}")
                lines.append("};")
            else:
                lines.append("scope { " + " ".join(scope_statements) + " };")

        return DumpResult(columns=[], rows=[], script="\n".join(lines) + "\n", output_file=query.output_file)

    def _execute_dump_yaml(
        self,
        query: DumpQuery,
        dump_targets: dict[str, set[int] | None] | None,
        aliases: list[tuple[str, AliasTypeDefinition]],
        composites: list[tuple[str, CompositeTypeDefinition]],
        sorted_composites: list[tuple[str, CompositeTypeDefinition]],
        cycle_composites: list[tuple[str, CompositeTypeDefinition]],
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
            if prim_type.primitive == PrimitiveType.CHARACTER:
                return repr(chr(val)) if isinstance(val, int) else repr(val)
            elif prim_type.primitive in (PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
                return str(val)
            else:
                return str(val)

        # Helper to format a field value
        def fmt_value(val: Any, field_type: TypeDefinition, depth: int = 0) -> str:
            ind_inner = indent * (depth + 1) if pretty else ""

            base = field_type.resolve_base_type()

            if val is None:
                return "null"

            if isinstance(base, ArrayTypeDefinition):
                start_idx, length = val

                if length == 0:
                    return "[]"

                arr_table = self.storage.get_array_table_for_type(field_type)
                elem_base = base.element_type.resolve_base_type()

                # Character array → string
                if isinstance(elem_base, PrimitiveTypeDefinition) and elem_base.primitive == PrimitiveType.CHARACTER:
                    chars = [arr_table.element_table.get(start_idx + j) for j in range(length)]
                    s = "".join(chars)
                    # YAML string - use double quotes
                    escaped = s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
                    return f'"{escaped}"'

                elements = [arr_table.element_table.get(start_idx + j) for j in range(length)]

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
            if prim_type.primitive == PrimitiveType.CHARACTER:
                return chr(val) if isinstance(val, int) else val
            elif prim_type.primitive in (PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
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

            if isinstance(base, ArrayTypeDefinition):
                start_idx, length = val

                if length == 0:
                    return []

                arr_table = self.storage.get_array_table_for_type(field_type)
                elem_base = base.element_type.resolve_base_type()

                # Character array → string
                if isinstance(elem_base, PrimitiveTypeDefinition) and elem_base.primitive == PrimitiveType.CHARACTER:
                    chars = [arr_table.element_table.get(start_idx + j) for j in range(length)]
                    return "".join(chars)

                elements = [arr_table.element_table.get(start_idx + j) for j in range(length)]

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
            if prim_type.primitive == PrimitiveType.CHARACTER:
                ch = chr(val) if isinstance(val, int) else val
                return escape(ch)
            elif prim_type.primitive in (PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
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

            if isinstance(base, ArrayTypeDefinition):
                start_idx, length = val

                if length == 0:
                    return f"{ind}<{field_name}/>"

                arr_table = self.storage.get_array_table_for_type(field_type)
                elem_base = base.element_type.resolve_base_type()

                # Character array → string
                if isinstance(elem_base, PrimitiveTypeDefinition) and elem_base.primitive == PrimitiveType.CHARACTER:
                    chars = [arr_table.element_table.get(start_idx + j) for j in range(length)]
                    text = escape("".join(chars))
                    return f"{ind}<{field_name}>{text}</{field_name}>"

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
            elif isinstance(type_def, CompositeTypeDefinition):
                for f in type_def.fields:
                    stack.append(f.type_def.name)
                    fb = f.type_def.resolve_base_type()
                    if isinstance(fb, ArrayTypeDefinition):
                        stack.append(fb.element_type.name)
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

        if isinstance(field_base, ArrayTypeDefinition):
            start_index, length = ref
            if length == 0:
                return "[]"
            arr_table = self.storage.get_array_table_for_type(field.type_def)

            elem_base = field_base.element_type.resolve_base_type()

            # Character array → string
            if isinstance(elem_base, PrimitiveTypeDefinition) and elem_base.primitive == PrimitiveType.CHARACTER:
                chars = [arr_table.element_table.get(start_index + j) for j in range(length)]
                s = "".join(chars)
                return self._format_ttq_string(s)

            elements = [arr_table.element_table.get(start_index + j) for j in range(length)]

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

    def _format_ttq_value(self, value: Any, type_base: TypeDefinition) -> str:
        """Format a primitive value as a TTQ literal."""
        if isinstance(type_base, PrimitiveTypeDefinition):
            if type_base.primitive == PrimitiveType.BIT:
                return "1" if value else "0"
            elif type_base.primitive == PrimitiveType.CHARACTER:
                return self._format_ttq_string(value)
            elif type_base.primitive in (PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
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
