"""Query executor for TTQ queries."""

from __future__ import annotations

import re
import uuid as uuid_module
from dataclasses import dataclass
from typing import Any, Iterator

from typed_tables.parsing.query_parser import (
    CompoundCondition,
    Condition,
    CreateInstanceQuery,
    CreateTypeQuery,
    DeleteQuery,
    DescribeQuery,
    EvalQuery,
    FieldValue,
    FunctionCall,
    Query,
    SelectField,
    SelectQuery,
    ShowTablesQuery,
    UseQuery,
)
from typed_tables.storage import StorageManager
from typed_tables.types import (
    AliasTypeDefinition,
    ArrayTypeDefinition,
    CompositeTypeDefinition,
    FieldDefinition,
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


class QueryExecutor:
    """Executes TTQ queries against storage."""

    def __init__(self, storage: StorageManager, registry: TypeRegistry) -> None:
        self.storage = storage
        self.registry = registry

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
        elif isinstance(query, CreateInstanceQuery):
            return self._execute_create_instance(query)
        elif isinstance(query, EvalQuery):
            return self._execute_eval(query)
        elif isinstance(query, DeleteQuery):
            return self._execute_delete(query)
        else:
            raise ValueError(f"Unknown query type: {type(query)}")

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

            # Get record count
            try:
                if isinstance(base, ArrayTypeDefinition):
                    count = self.storage.get_array_table(type_name).count
                else:
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

    def _execute_create_type(self, query: CreateTypeQuery) -> CreateResult:
        """Execute CREATE TYPE query."""
        # Check if type already exists
        if self.registry.get(query.name) is not None:
            return CreateResult(
                columns=[],
                rows=[],
                message=f"Type '{query.name}' already exists",
                type_name=query.name,
            )

        # Build field definitions
        fields: list[FieldDefinition] = []
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

        # Create and register the composite type
        composite = CompositeTypeDefinition(name=query.name, fields=fields)
        self.registry.register(composite)

        # Save updated metadata
        self.storage.save_metadata()

        return CreateResult(
            columns=["type", "fields"],
            rows=[{"type": query.name, "fields": len(fields)}],
            message=f"Created type '{query.name}' with {len(fields)} field(s)",
            type_name=query.name,
        )

    def _execute_create_instance(self, query: CreateInstanceQuery) -> CreateResult:
        """Execute CREATE instance query."""
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

        # Check all required fields are present
        for field in base.fields:
            if field.name not in values:
                return CreateResult(
                    columns=[],
                    rows=[],
                    message=f"Missing required field: {field.name}",
                    type_name=query.type_name,
                )

        # Create the instance using storage manager
        try:
            index = self._create_instance(type_def, base, values)
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

    def _resolve_instance_value(self, value: Any) -> Any:
        """Resolve a value from CREATE instance, handling function calls."""
        if isinstance(value, FunctionCall):
            if value.name == "uuid":
                # Generate a random UUID as uint128
                return uuid_module.uuid4().int
            else:
                raise ValueError(f"Unknown function: {value.name}()")
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
            field_value = values[field.name]
            field_base = field.type_def.resolve_base_type()

            if isinstance(field_base, ArrayTypeDefinition):
                # Convert string to character list if needed
                if isinstance(field_value, str):
                    field_value = list(field_value)
                # Store array elements
                array_table = self.storage.get_array_table_for_type(field.type_def)
                array_index = array_table.insert(field_value)
                field_references[field.name] = array_table.get_header(array_index)
            elif isinstance(field_base, CompositeTypeDefinition):
                # Nested composite - recursive create
                nested_index = self._create_instance(field.type_def, field_base, field_value)
                field_references[field.name] = nested_index
            else:
                # Primitive value - store in field type's table
                field_table = self.storage.get_table(field.type_def.name)
                ref_index = field_table.insert(field_value)
                field_references[field.name] = ref_index

        # Store the composite record
        table = self.storage.get_table(type_def.name)
        return table.insert(field_references)

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
        type_def = self.registry.get(query.table)
        if type_def is None:
            return QueryResult(
                columns=[],
                rows=[],
                message=f"Unknown type: {query.table}",
            )

        base = type_def.resolve_base_type()

        # Get all records
        records = list(self._load_all_records(query.table, type_def))

        # Apply WHERE filter
        if query.where:
            records = [r for r in records if self._evaluate_condition(r, query.where)]

        # Apply GROUP BY
        if query.group_by:
            records = self._apply_group_by(records, query)

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

        if isinstance(base, ArrayTypeDefinition):
            array_table = self.storage.get_array_table(type_name)
            for i in range(array_table.count):
                elements = array_table.get(i)
                yield {"_index": i, "_value": elements}

        elif isinstance(base, CompositeTypeDefinition):
            table = self.storage.get_table(type_name)
            for i in range(table.count):
                # Skip deleted records
                if table.is_deleted(i):
                    continue

                record = table.get(i)
                resolved = {"_index": i}

                for field in base.fields:
                    ref = record[field.name]
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
                        field_table = self.storage.get_table(field.type_def.name)
                        resolved[field.name] = field_table.get(ref)

                yield resolved

        else:
            # Primitive type
            table = self.storage.get_table(type_name)
            for i in range(table.count):
                value = table.get(i)
                yield {"_index": i, "_value": value}

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
        self, records: list[dict[str, Any]], query: SelectQuery
    ) -> list[dict[str, Any]]:
        """Apply GROUP BY clause."""
        groups: dict[tuple, list[dict[str, Any]]] = {}

        for record in records:
            key = tuple(record.get(f) for f in query.group_by)
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
                    row[field.name] = record.get(field.name)
            rows.append(row)

        return columns, rows

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
