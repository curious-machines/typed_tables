"""Tool for dumping table contents to the console."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from typed_tables.storage import StorageManager
from typed_tables.types import (
    AliasTypeDefinition,
    ArrayTypeDefinition,
    BooleanTypeDefinition,
    CompositeTypeDefinition,
    EnumTypeDefinition,
    EnumValue,
    EnumVariantDefinition,
    FieldDefinition,
    InterfaceTypeDefinition,
    PrimitiveType,
    PrimitiveTypeDefinition,
    StringTypeDefinition,
    TypeDefinition,
    TypeRegistry,
    is_boolean_type,
    is_string_type,
)


def load_registry_from_metadata(data_dir: Path) -> TypeRegistry:
    """Load type registry from metadata file.

    Uses two-phase resolution to support cyclical type definitions:
    Phase 1: Pre-register stubs for all composite types.
    Phase 2: Iteratively resolve, populating composite stubs' fields.
    """
    metadata_path = data_dir / "_metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    with open(metadata_path) as f:
        metadata = json.load(f)

    registry = TypeRegistry()
    types_data = metadata.get("types", {})

    # Phase 1: Pre-register stubs for all composite, enum, and interface types
    for name, spec in types_data.items():
        if name not in registry:
            if spec.get("kind") == "composite":
                registry.register_stub(name)
            elif spec.get("kind") == "enum":
                registry.register_enum_stub(name)
            elif spec.get("kind") == "interface":
                registry.register_interface_stub(name)

    # Phase 2: Collect non-primitive specs to resolve
    to_resolve = {}
    for name, spec in types_data.items():
        if name not in registry:
            # Not yet registered (non-composite, non-primitive, non-enum)
            to_resolve[name] = spec
        elif registry.is_stub(name) or registry.is_enum_stub(name) or registry.is_interface_stub(name):
            # Composite/enum/interface stub that needs field population
            to_resolve[name] = spec

    # Iteratively resolve
    max_iterations = len(to_resolve) + 1
    for _ in range(max_iterations):
        if not to_resolve:
            break

        resolved_this_pass = []
        for name, spec in to_resolve.items():
            try:
                if spec.get("kind") == "composite":
                    _populate_composite_from_spec(name, spec, registry)
                    resolved_this_pass.append(name)
                elif spec.get("kind") == "enum":
                    _populate_enum_from_spec(name, spec, registry)
                    resolved_this_pass.append(name)
                elif spec.get("kind") == "interface":
                    _populate_interface_from_spec(name, spec, registry)
                    resolved_this_pass.append(name)
                else:
                    type_def = _create_type_from_spec(name, spec, registry)
                    if type_def:
                        registry.register(type_def)
                        resolved_this_pass.append(name)
            except KeyError:
                # Dependency not yet resolved
                pass

        for name in resolved_this_pass:
            del to_resolve[name]

        if not resolved_this_pass and to_resolve:
            raise ValueError(f"Cannot resolve types: {list(to_resolve.keys())}")

    # Restore type_ids for tagged interface references
    type_ids_data = metadata.get("type_ids", {})
    for name, tid in type_ids_data.items():
        registry._type_ids[name] = tid
    if type_ids_data:
        registry._next_type_id = max(type_ids_data.values()) + 1

    return registry


def _create_type_from_spec(
    name: str, spec: dict[str, Any], registry: TypeRegistry
) -> TypeDefinition | None:
    """Create a type definition from a metadata spec (non-composite types)."""
    kind = spec.get("kind")

    if kind == "primitive":
        # Already registered
        return None
    elif kind == "alias":
        base_type = registry.get_or_raise(spec["base_type"])
        return AliasTypeDefinition(name=name, base_type=base_type)
    elif kind == "string":
        element_type = registry.get_or_raise(spec["element_type"])
        return StringTypeDefinition(name=name, element_type=element_type)
    elif kind == "boolean":
        return BooleanTypeDefinition(name=name, primitive=PrimitiveType.BIT)
    elif kind == "array":
        element_type = registry.get_or_raise(spec["element_type"])
        return ArrayTypeDefinition(name=name, element_type=element_type)
    elif kind == "composite":
        # Composites are handled by _populate_composite_from_spec
        return None

    return None


def _deserialize_default_value(json_val: Any, type_def: TypeDefinition) -> Any:
    """Deserialize a default value from JSON metadata."""
    if json_val is None:
        return None
    base = type_def.resolve_base_type()
    if isinstance(base, EnumTypeDefinition):
        if isinstance(json_val, str):
            # C-style enum: variant name string
            variant = base.get_variant(json_val)
            if variant is None:
                return None
            return EnumValue(variant_name=variant.name, discriminant=variant.discriminant, fields={})
        elif isinstance(json_val, dict) and "_variant" in json_val:
            # Swift-style enum: {"_variant": name, field: val, ...}
            variant_name = json_val["_variant"]
            variant = base.get_variant(variant_name)
            if variant is None:
                return None
            fields = {k: v for k, v in json_val.items() if k != "_variant"}
            return EnumValue(variant_name=variant.name, discriminant=variant.discriminant, fields=fields)
    if isinstance(base, PrimitiveTypeDefinition):
        from typed_tables.types import PrimitiveType as PT
        if base.primitive in (PT.UINT128, PT.INT128) and isinstance(json_val, str):
            return int(json_val, 16)
    return json_val


def _populate_interface_from_spec(
    name: str, spec: dict[str, Any], registry: TypeRegistry
) -> None:
    """Populate a pre-registered interface stub with its fields.

    Gets the existing stub via registry.get() and sets stub.fields.
    Skips if already populated (idempotent).
    """
    stub = registry.get(name)
    if not isinstance(stub, InterfaceTypeDefinition):
        return
    # Skip if already populated
    if stub.fields:
        return

    fields = []
    for field_spec in spec.get("fields", []):
        field_type = registry.get_or_raise(field_spec["type"])
        default = _deserialize_default_value(field_spec.get("default"), field_type)
        overflow = field_spec.get("overflow")
        fields.append(FieldDefinition(name=field_spec["name"], type_def=field_type, default_value=default, overflow=overflow))
    stub.fields = fields


def _populate_composite_from_spec(
    name: str, spec: dict[str, Any], registry: TypeRegistry
) -> None:
    """Populate a pre-registered composite stub with its fields.

    Gets the existing stub via registry.get() and sets stub.fields.
    Skips if already populated (idempotent).
    """
    from typed_tables.types import FieldDefinition as FD

    stub = registry.get(name)
    if not isinstance(stub, CompositeTypeDefinition):
        return
    # Skip if already populated
    if stub.fields:
        return

    fields = []
    for field_spec in spec["fields"]:
        field_type = registry.get_or_raise(field_spec["type"])
        default = _deserialize_default_value(field_spec.get("default"), field_type)
        overflow = field_spec.get("overflow")
        fields.append(FD(name=field_spec["name"], type_def=field_type, default_value=default, overflow=overflow))
    stub.fields = fields
    stub.interfaces = spec.get("interfaces", [])


def _populate_enum_from_spec(
    name: str, spec: dict[str, Any], registry: TypeRegistry
) -> None:
    """Populate a pre-registered enum stub with its variants.

    Gets the existing stub via registry.get() and sets stub.variants.
    Skips if already populated (idempotent).
    """
    stub = registry.get(name)
    if not isinstance(stub, EnumTypeDefinition):
        return
    # Skip if already populated
    if stub.variants:
        return

    variants = []
    for vspec in spec["variants"]:
        vfields = []
        for fspec in vspec.get("fields", []):
            ftype = registry.get_or_raise(fspec["type"])
            vfields.append(FieldDefinition(name=fspec["name"], type_def=ftype))
        variants.append(EnumVariantDefinition(
            name=vspec["name"],
            discriminant=vspec["discriminant"],
            fields=vfields,
        ))
    stub.variants = variants
    stub.has_explicit_values = spec.get("has_explicit_values", False)
    backing = spec.get("backing_type")
    if backing:
        from typed_tables.types import PrimitiveType as PT, PRIMITIVE_TYPE_NAMES as PTN
        stub.backing_type = PTN.get(backing)


def format_value(value: Any, type_def: TypeDefinition) -> str:
    """Format a value for display."""
    base = type_def.resolve_base_type()

    if is_boolean_type(type_def):
        return "true" if value else "false"
    elif isinstance(base, PrimitiveTypeDefinition):
        if base.primitive == PrimitiveType.CHARACTER:
            if isinstance(value, str):
                return repr(value)
            return repr(chr(value)) if value else repr("\x00")
        elif base.primitive in (PrimitiveType.UINT128, PrimitiveType.INT128):
            return f"0x{value:032x}"
        elif base.primitive in (PrimitiveType.FLOAT32, PrimitiveType.FLOAT64):
            return f"{value:.6g}"
        else:
            return str(value)
    elif isinstance(base, ArrayTypeDefinition):
        if isinstance(value, tuple):
            start, length = value
            return f"(start={start}, len={length})"
        elif isinstance(value, list):
            # Resolved array
            if is_string_type(type_def):
                return repr("".join(value))
            return str(value)
    elif isinstance(base, EnumTypeDefinition):
        if isinstance(value, EnumValue):
            if value.fields:
                field_strs = [f"{k}={v}" for k, v in value.fields.items()]
                return f"{base.name}.{value.variant_name}({', '.join(field_strs)})"
            return f"{base.name}.{value.variant_name}"
        if isinstance(value, tuple) and len(value) == 2 and base.has_associated_values:
            disc, index = value
            variant = base.get_variant_by_discriminant(disc)
            vname = variant.name if variant else "?"
            return f"{base.name}.{vname}(variant_index={index})"
        return str(value)
    elif isinstance(base, InterfaceTypeDefinition):
        if isinstance(value, tuple) and len(value) == 2:
            type_id, index = value
            return f"<interface_ref(type_id={type_id}, index={index})>"
        return str(value)
    elif isinstance(base, CompositeTypeDefinition):
        return str(value)

    return str(value)


def dump_table_raw(
    storage: StorageManager,
    type_name: str,
    type_def: TypeDefinition,
    limit: int | None = None,
) -> None:
    """Dump table contents in raw format (showing indices/references)."""
    base = type_def.resolve_base_type()

    print(f"Table: {type_name}")
    print(f"Type: {type_def.__class__.__name__}")
    print("-" * 60)

    if isinstance(base, CompositeTypeDefinition):
        table = storage.get_table(type_name)
        count = table.count
        print(f"Records: {count}")
        print(f"Record size: {type_def.size_bytes} bytes")
        print()

        # Print header
        field_headers = []
        for field in base.fields:
            field_base = field.type_def.resolve_base_type()
            if isinstance(field_base, ArrayTypeDefinition):
                field_headers.append(f"{field.name} (start, len)")
            else:
                field_headers.append(f"{field.name} (idx)")
        print(f"{'#':>4}  " + "  ".join(f"{h:>16}" for h in field_headers))
        print("-" * (6 + 18 * len(field_headers)))

        display_count = min(count, limit) if limit else count
        for i in range(display_count):
            record = table.get(i)
            values = []
            for field in base.fields:
                ref = record[field.name]
                if ref is None:
                    values.append("NULL")
                else:
                    field_base = field.type_def.resolve_base_type()
                    if isinstance(field_base, ArrayTypeDefinition):
                        values.append(f"({ref[0]}, {ref[1]})")
                    else:
                        values.append(str(ref))
            print(f"{i:>4}  " + "  ".join(f"{v:>16}" for v in values))

        if limit and count > limit:
            print(f"... ({count - limit} more records)")

    else:
        # Primitive or alias to primitive
        table = storage.get_table(type_name)
        count = table.count
        print(f"Records: {count}")
        print(f"Record size: {type_def.size_bytes} bytes")
        print()

        display_count = min(count, limit) if limit else count
        for i in range(display_count):
            value = table.get(i)
            formatted = format_value(value, type_def)
            print(f"[{i}] {formatted}")

        if limit and count > limit:
            print(f"... ({count - limit} more records)")


def dump_table_resolved(
    storage: StorageManager,
    type_name: str,
    type_def: TypeDefinition,
    registry: TypeRegistry,
    limit: int | None = None,
) -> None:
    """Dump table contents with resolved values."""
    base = type_def.resolve_base_type()

    print(f"Table: {type_name}")
    print(f"Type: {type_def.__class__.__name__}")
    print("-" * 60)

    if isinstance(base, CompositeTypeDefinition):
        table = storage.get_table(type_name)
        count = table.count
        print(f"Records: {count}")
        print()

        display_count = min(count, limit) if limit else count
        for i in range(display_count):
            record = table.get(i)
            print(f"[{i}]")
            for field in base.fields:
                ref = record[field.name]

                if ref is None:
                    print(f"    {field.name}: NULL")
                    continue

                field_base = field.type_def.resolve_base_type()

                if isinstance(field_base, ArrayTypeDefinition):
                    # Resolve array
                    start_index, length = ref
                    if length == 0:
                        resolved = []
                    else:
                        arr_table = storage.get_array_table_for_type(field.type_def)
                        resolved = [
                            arr_table.element_table.get(start_index + j)
                            for j in range(length)
                        ]
                    formatted = format_value(resolved, field.type_def)
                elif isinstance(field_base, CompositeTypeDefinition):
                    # Resolve nested composite (just show index for now)
                    formatted = f"<{field.type_def.name}[{ref}]>"
                else:
                    # Primitive — value is already inline
                    formatted = format_value(ref, field.type_def)

                print(f"    {field.name}: {formatted}")
            print()

        if limit and count > limit:
            print(f"... ({count - limit} more records)")

    else:
        # Primitive or alias - same as raw
        dump_table_raw(storage, type_name, type_def, limit)


def dump_table_json(
    storage: StorageManager,
    type_name: str,
    type_def: TypeDefinition,
    registry: TypeRegistry,
    limit: int | None = None,
    raw: bool = False,
) -> None:
    """Dump table contents as JSON."""
    base = type_def.resolve_base_type()
    records = []

    if isinstance(base, CompositeTypeDefinition):
        table = storage.get_table(type_name)
        count = table.count
        display_count = min(count, limit) if limit else count

        for i in range(display_count):
            record = table.get(i)
            if raw:
                rec = {"_index": i}
                for field in base.fields:
                    ref = record[field.name]
                    if ref is None:
                        rec[field.name] = None
                    else:
                        field_base = field.type_def.resolve_base_type()
                        if isinstance(field_base, ArrayTypeDefinition):
                            rec[field.name] = {"start_index": ref[0], "length": ref[1]}
                        elif isinstance(field_base, CompositeTypeDefinition):
                            rec[field.name] = {"index": ref}
                        else:
                            rec[field.name] = {"value": ref}
                records.append(rec)
            else:
                rec = {"_index": i}
                for field in base.fields:
                    ref = record[field.name]

                    if ref is None:
                        rec[field.name] = None
                        continue

                    field_base = field.type_def.resolve_base_type()

                    if isinstance(field_base, ArrayTypeDefinition):
                        start_index, length = ref
                        if length == 0:
                            rec[field.name] = []
                        else:
                            arr_table = storage.get_array_table_for_type(field.type_def)
                            rec[field.name] = [
                                arr_table.element_table.get(start_index + j)
                                for j in range(length)
                            ]
                    elif isinstance(field_base, CompositeTypeDefinition):
                        rec[field.name] = f"<{field.type_def.name}[{ref}]>"
                    else:
                        # Primitive — value is already inline
                        value = ref
                        # Handle large integers for JSON
                        if isinstance(value, int) and (value > 2**53 or value < -(2**53)):
                            value = hex(value)
                        rec[field.name] = value
                records.append(rec)

    else:
        table = storage.get_table(type_name)
        count = table.count
        display_count = min(count, limit) if limit else count

        for i in range(display_count):
            value = table.get(i)
            if isinstance(value, int) and (value > 2**53 or value < -(2**53)):
                value = hex(value)
            records.append({"_index": i, "value": value})

    output = {
        "table": type_name,
        "count": len(records),
        "records": records,
    }
    print(json.dumps(output, indent=2))


def list_tables(storage: StorageManager, registry: TypeRegistry) -> None:
    """List all available tables."""
    print("Available tables:")
    print("-" * 40)

    for type_name in sorted(registry.list_types()):
        if type_name.startswith("_"):
            continue
        type_def = registry.get(type_name)
        if type_def is None:
            continue

        base = type_def.resolve_base_type()
        table_file = storage.data_dir / f"{type_name}.bin"

        if not table_file.exists():
            continue

        # Skip standalone array types (no header table file anymore)
        if isinstance(base, ArrayTypeDefinition):
            continue

        kind = type_def.__class__.__name__.replace("TypeDefinition", "")
        try:
            table = storage.get_table(type_name)
            count = table.count
        except Exception:
            count = "?"

        print(f"  {type_name:<20} {kind:<12} {count:>6} records")


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump typed table contents to the console"
    )
    parser.add_argument(
        "data_dir",
        type=Path,
        help="Path to the data directory containing table files",
    )
    parser.add_argument(
        "table",
        nargs="?",
        help="Name of the table/type to dump (omit to list tables)",
    )
    parser.add_argument(
        "-r", "--raw",
        action="store_true",
        help="Show raw values (indices/references) instead of resolved values",
    )
    parser.add_argument(
        "-j", "--json",
        action="store_true",
        help="Output as JSON",
    )
    parser.add_argument(
        "-n", "--limit",
        type=int,
        default=None,
        help="Limit number of records to display",
    )

    args = parser.parse_args(argv)

    if not args.data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}", file=sys.stderr)
        return 1

    try:
        registry = load_registry_from_metadata(args.data_dir)
        storage = StorageManager(args.data_dir, registry)
    except Exception as e:
        print(f"Error loading data: {e}", file=sys.stderr)
        return 1

    try:
        if args.table is None:
            list_tables(storage, registry)
        else:
            type_def = registry.get(args.table)
            if type_def is None:
                print(f"Error: Unknown table/type: {args.table}", file=sys.stderr)
                print("\nAvailable tables:")
                list_tables(storage, registry)
                return 1

            if args.json:
                dump_table_json(
                    storage, args.table, type_def, registry, args.limit, args.raw
                )
            elif args.raw:
                dump_table_raw(storage, args.table, type_def, args.limit)
            else:
                dump_table_resolved(
                    storage, args.table, type_def, registry, args.limit
                )
    finally:
        storage.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
