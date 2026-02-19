"""MetaDatabaseBuilder — builds _meta/ database from user registry.

Walks the user's TypeRegistry and creates meta-schema records
(CompositeDef, FieldDef, etc.) in a _meta/ database directory.
Uses SHA-256 hash of the source _metadata.json to avoid rebuilding
when the schema hasn't changed.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

from typed_tables.types import (
    AliasTypeDefinition,
    ArrayTypeDefinition,
    BigIntTypeDefinition,
    BigUIntTypeDefinition,
    BooleanTypeDefinition,
    CompositeTypeDefinition,
    DictionaryTypeDefinition,
    EnumTypeDefinition,
    FractionTypeDefinition,
    InterfaceTypeDefinition,
    OverflowTypeDefinition,
    PrimitiveType,
    PrimitiveTypeDefinition,
    SetTypeDefinition,
    StringTypeDefinition,
    TypeDefinition,
    TypeRegistry,
)


# Mapping from PrimitiveType enum values to meta-schema type names
_PRIMITIVE_META_TYPES: dict[str, str] = {
    "bit": "BitDef",
    "character": "CharacterDef",
    "uint8": "UInt8Def",
    "int8": "Int8Def",
    "uint16": "UInt16Def",
    "int16": "Int16Def",
    "uint32": "UInt32Def",
    "int32": "Int32Def",
    "uint64": "UInt64Def",
    "int64": "Int64Def",
    "uint128": "UInt128Def",
    "int128": "Int128Def",
    "float16": "Float16Def",
    "float32": "Float32Def",
    "float64": "Float64Def",
}

_BUILTIN_META_TYPES: dict[str, str] = {
    "boolean": "BooleanDef",
    "string": "StringDef",
    "bigint": "BigIntDef",
    "biguint": "BigUIntDef",
    "fraction": "FractionDef",
}

_HASH_FILE = "_source_hash"


def _escape_ttq_string(s: str) -> str:
    """Escape a string for TTQ literal use."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


class MetaDatabaseBuilder:
    """Builds a _meta/ database containing meta-schema records for a user schema."""

    def __init__(self, user_registry: TypeRegistry, user_db_path: Path) -> None:
        self._user_registry = user_registry
        self._user_db_path = user_db_path
        self._meta_path = user_db_path / "_meta"

    def build(self) -> tuple[TypeRegistry, Any]:
        """Build or load the _meta/ database.

        Returns:
            Tuple of (meta_registry, meta_storage).
        """
        from typed_tables.storage import StorageManager

        if not self._is_stale():
            return self._load_existing()

        # Rebuild from scratch
        if self._meta_path.exists():
            shutil.rmtree(self._meta_path)

        meta_registry, meta_storage = self._create_meta_schema()
        self._populate(meta_storage, meta_registry)
        self._write_hash()
        return meta_registry, meta_storage

    def is_stale(self) -> bool:
        """Check if the _meta/ database needs rebuilding."""
        return self._is_stale()

    def _is_stale(self) -> bool:
        """Check if rebuild is needed by comparing metadata hashes."""
        hash_path = self._meta_path / _HASH_FILE
        if not hash_path.exists():
            return True
        try:
            stored_hash = hash_path.read_text().strip()
            current_hash = self._compute_source_hash()
            return stored_hash != current_hash
        except Exception:
            return True

    def _compute_source_hash(self) -> str:
        """Compute SHA-256 hash of the user's _metadata.json."""
        metadata_path = self._user_db_path / "_metadata.json"
        if not metadata_path.exists():
            return ""
        content = metadata_path.read_bytes()
        return hashlib.sha256(content).hexdigest()

    def _write_hash(self) -> None:
        """Write the current source hash to _meta/_source_hash."""
        hash_path = self._meta_path / _HASH_FILE
        hash_path.write_text(self._compute_source_hash())

    def _load_existing(self) -> tuple[TypeRegistry, Any]:
        """Load existing _meta/ database."""
        from typed_tables.dump import load_registry_from_metadata
        from typed_tables.storage import StorageManager

        meta_registry = load_registry_from_metadata(self._meta_path)
        meta_storage = StorageManager(self._meta_path, meta_registry)
        return meta_registry, meta_storage

    def _create_meta_schema(self) -> tuple[TypeRegistry, Any]:
        """Create the meta-schema database by parsing meta_schema.ttq."""
        from typed_tables.schema import Schema

        schema_path = Path(__file__).parent / "meta_schema.ttq"
        schema_text = schema_path.read_text()
        schema = Schema.parse(schema_text, self._meta_path)
        return schema.registry, schema.storage

    def _populate(self, storage: Any, registry: TypeRegistry) -> None:
        """Populate the _meta/ database with records from the user registry."""
        from typed_tables.parsing.query_parser import QueryParser
        from typed_tables.query_executor import QueryExecutor

        parser = QueryParser()
        parser.build(debug=False, write_tables=False)
        executor = QueryExecutor(storage, registry)

        def run(ttq: str) -> Any:
            stmts = parser.parse_program(ttq)
            result = None
            for stmt in stmts:
                result = executor.execute(stmt)
            return result

        # Track which meta-records have been created, keyed by user type name
        # Maps user type name → meta variable name
        type_vars: dict[str, str] = {}
        # Counter for unique variable names
        var_counter = [0]

        def next_var(prefix: str = "v") -> str:
            var_counter[0] += 1
            return f"${prefix}{var_counter[0]}"

        # ---- Pass 1: Leaf types (primitives, builtins) ----
        # Only create records for types actually referenced by the user schema
        referenced = self._collect_referenced_types()

        for prim_name, meta_type in _PRIMITIVE_META_TYPES.items():
            if prim_name in referenced:
                var = next_var("p")
                run(f'{var} = create {meta_type}(name="{prim_name}")')
                type_vars[prim_name] = var

        for builtin_name, meta_type in _BUILTIN_META_TYPES.items():
            if builtin_name in referenced:
                var = next_var("b")
                run(f'{var} = create {meta_type}(name="{builtin_name}")')
                type_vars[builtin_name] = var

        # ---- Pass 2: Wrapping types (alias, array, set, overflow, dict) ----
        # Process in dependency order - these reference leaf types or each other
        # We may need multiple iterations for chains (alias of alias, etc.)
        wrapping_types = self._collect_wrapping_types()
        remaining = list(wrapping_types)
        max_iterations = len(remaining) + 1
        iteration = 0

        while remaining and iteration < max_iterations:
            iteration += 1
            still_remaining = []
            for type_name, type_def in remaining:
                if self._try_create_wrapping(type_name, type_def, type_vars, run, next_var):
                    pass  # Created successfully
                else:
                    still_remaining.append((type_name, type_def))
            remaining = still_remaining

        # ---- Pass 3: Stub container types (Enum, Interface, Composite) ----
        # Create with just name so they can be referenced by FieldDef.type
        for type_name in self._user_registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self._user_registry.get(type_name)
            if type_def is None:
                continue
            base = type_def.resolve_base_type()
            if type_name in type_vars:
                continue  # Already created (e.g., dict entry composites)
            escaped = _escape_ttq_string(type_name)
            if isinstance(base, EnumTypeDefinition):
                var = next_var("e")
                run(f'{var} = create EnumDef(name="{escaped}")')
                type_vars[type_name] = var
            elif isinstance(base, InterfaceTypeDefinition):
                var = next_var("i")
                run(f'{var} = create InterfaceDef(name="{escaped}")')
                type_vars[type_name] = var
            elif isinstance(base, CompositeTypeDefinition):
                var = next_var("c")
                run(f'{var} = create CompositeDef(name="{escaped}")')
                type_vars[type_name] = var

        # ---- Pass 3b: Retry remaining wrapping types ----
        # Some wrapping types (e.g., Node[]) reference container types
        if remaining:
            max_iterations2 = len(remaining) + 1
            iteration2 = 0
            while remaining and iteration2 < max_iterations2:
                iteration2 += 1
                still_remaining = []
                for type_name, type_def in remaining:
                    if self._try_create_wrapping(type_name, type_def, type_vars, run, next_var):
                        pass
                    else:
                        still_remaining.append((type_name, type_def))
                remaining = still_remaining

        # ---- Pass 4: FieldDef records ----
        # Now all type refs (primitives, builtins, wrapping, containers) are available
        field_vars: dict[str, str] = {}  # "TypeName.fieldName" → var

        for type_name in self._user_registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self._user_registry.get(type_name)
            if type_def is None:
                continue
            base = type_def.resolve_base_type()
            if isinstance(base, (CompositeTypeDefinition, InterfaceTypeDefinition)):
                for f in base.fields:
                    fkey = f"{type_name}.{f.name}"
                    if fkey not in field_vars:
                        var = self._create_field_record(f, type_vars, run, next_var)
                        if var:
                            field_vars[fkey] = var

        # ---- Pass 5: VariantDef records ----
        variant_vars: dict[str, str] = {}  # "EnumName.variantName" → var

        for type_name in self._user_registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self._user_registry.get(type_name)
            if type_def is None:
                continue
            base = type_def.resolve_base_type()
            if isinstance(base, EnumTypeDefinition):
                for variant in base.variants:
                    vkey = f"{type_name}.{variant.name}"
                    variant_field_vars = []
                    for f in variant.fields:
                        ffkey = f"{vkey}.{f.name}"
                        if ffkey not in field_vars:
                            var = self._create_field_record(f, type_vars, run, next_var)
                            if var:
                                field_vars[ffkey] = var
                        if ffkey in field_vars:
                            variant_field_vars.append(field_vars[ffkey])

                    var = next_var("vr")
                    fields_arr_fixed = "[" + ", ".join(variant_field_vars) + "]" if variant_field_vars else "[]"
                    run(f'{var} = create VariantDef(name="{_escape_ttq_string(variant.name)}", discriminant={variant.discriminant}, fields={fields_arr_fixed})')
                    variant_vars[vkey] = var

        # ---- Pass 6: Update container types with fields/variants/extends/parent ----
        # Update EnumDef records
        for type_name in self._user_registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self._user_registry.get(type_name)
            if type_def is None:
                continue
            base = type_def.resolve_base_type()
            if isinstance(base, EnumTypeDefinition):
                var = type_vars[type_name]
                enum_variant_vars = []
                for variant in base.variants:
                    vkey = f"{type_name}.{variant.name}"
                    if vkey in variant_vars:
                        enum_variant_vars.append(variant_vars[vkey])

                variants_arr = "[" + ", ".join(enum_variant_vars) + "]" if enum_variant_vars else "[]"
                has_explicit = "true" if base.has_explicit_values else "false"

                backing_ref = "null"
                if base.backing_type is not None:
                    bt_name = base.backing_type.value
                    if bt_name in type_vars:
                        backing_ref = type_vars[bt_name]

                run(f'update {var} set variants={variants_arr}, has_explicit_values={has_explicit}, backing_type={backing_ref}')

        # Update InterfaceDef records
        for type_name in self._user_registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self._user_registry.get(type_name)
            if type_def is None:
                continue
            base = type_def.resolve_base_type()
            if isinstance(base, InterfaceTypeDefinition):
                var = type_vars[type_name]
                iface_field_vars = []
                for f in base.fields:
                    fkey = f"{type_name}.{f.name}"
                    if fkey in field_vars:
                        iface_field_vars.append(field_vars[fkey])

                fields_arr = "[" + ", ".join(iface_field_vars) + "]" if iface_field_vars else "[]"

                extends_vars = []
                for parent_name in getattr(base, "interfaces", []):
                    if parent_name in type_vars:
                        extends_vars.append(type_vars[parent_name])
                extends_arr = "[" + ", ".join(extends_vars) + "]" if extends_vars else "[]"

                run(f'update {var} set fields={fields_arr}, extends={extends_arr}')

        # Update CompositeDef records
        for type_name in self._user_registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self._user_registry.get(type_name)
            if type_def is None:
                continue
            base = type_def.resolve_base_type()
            if isinstance(base, CompositeTypeDefinition):
                var = type_vars[type_name]
                comp_field_vars = []
                for f in base.fields:
                    fkey = f"{type_name}.{f.name}"
                    if fkey in field_vars:
                        comp_field_vars.append(field_vars[fkey])

                fields_arr = "[" + ", ".join(comp_field_vars) + "]" if comp_field_vars else "[]"

                iface_refs = []
                for iface_name in getattr(base, "declared_interfaces", base.interfaces):
                    if iface_name in type_vars:
                        iface_refs.append(type_vars[iface_name])
                ifaces_arr = "[" + ", ".join(iface_refs) + "]" if iface_refs else "[]"

                parent_ref = "null"
                if base.parent and base.parent in type_vars:
                    parent_ref = type_vars[base.parent]

                run(f'update {var} set fields={fields_arr}, `interfaces`={ifaces_arr}, parent={parent_ref}')

        # Save metadata
        storage.save_metadata()

    def _collect_referenced_types(self) -> set[str]:
        """Collect all type names referenced by user-defined types."""
        referenced: set[str] = set()

        def walk(td: TypeDefinition) -> None:
            if isinstance(td, OverflowTypeDefinition):
                referenced.add(td.name)
                walk(td.base_type)
            elif isinstance(td, AliasTypeDefinition):
                referenced.add(td.name)
                walk(td.base_type)
            elif isinstance(td, FractionTypeDefinition):
                referenced.add(td.name)
            elif isinstance(td, (BigIntTypeDefinition, BigUIntTypeDefinition)):
                referenced.add(td.name)
            elif isinstance(td, StringTypeDefinition):
                referenced.add(td.name)
            elif isinstance(td, BooleanTypeDefinition):
                referenced.add(td.name)
            elif isinstance(td, SetTypeDefinition):
                referenced.add(td.name)
                walk(td.element_type)
            elif isinstance(td, DictionaryTypeDefinition):
                referenced.add(td.name)
                walk(td.key_type)
                walk(td.value_type)
                # entry_type is a synthetic composite — add it too
                if td.entry_type:
                    referenced.add(td.entry_type.name)
            elif isinstance(td, PrimitiveTypeDefinition):
                referenced.add(td.name)
            elif isinstance(td, ArrayTypeDefinition):
                referenced.add(td.name)
                walk(td.element_type)
            elif isinstance(td, CompositeTypeDefinition):
                referenced.add(td.name)
            elif isinstance(td, InterfaceTypeDefinition):
                referenced.add(td.name)
            elif isinstance(td, EnumTypeDefinition):
                referenced.add(td.name)

        for type_name in self._user_registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self._user_registry.get(type_name)
            if type_def is None:
                continue
            walk(type_def)
            base = type_def.resolve_base_type()
            if isinstance(base, (CompositeTypeDefinition, InterfaceTypeDefinition)):
                for f in base.fields:
                    walk(f.type_def)
            if isinstance(base, EnumTypeDefinition):
                if base.backing_type is not None:
                    referenced.add(base.backing_type.value)
                for v in base.variants:
                    for f in v.fields:
                        walk(f.type_def)

        return referenced

    def _collect_wrapping_types(self) -> list[tuple[str, TypeDefinition]]:
        """Collect all wrapping type definitions (alias, array, set, overflow, dict)."""
        result = []
        for type_name in self._user_registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self._user_registry.get(type_name)
            if type_def is None:
                continue
            if isinstance(type_def, (AliasTypeDefinition, OverflowTypeDefinition)):
                result.append((type_name, type_def))
            elif isinstance(type_def, SetTypeDefinition):
                result.append((type_name, type_def))
            elif isinstance(type_def, DictionaryTypeDefinition):
                result.append((type_name, type_def))
            elif isinstance(type_def, ArrayTypeDefinition) and not isinstance(type_def, (StringTypeDefinition, SetTypeDefinition)):
                result.append((type_name, type_def))
        return result

    def _try_create_wrapping(
        self,
        type_name: str,
        type_def: TypeDefinition,
        type_vars: dict[str, str],
        run: Any,
        next_var: Any,
    ) -> bool:
        """Try to create a wrapping type record. Returns True if successful."""
        escaped_name = _escape_ttq_string(type_name)

        if isinstance(type_def, AliasTypeDefinition):
            base_name = type_def.base_type.name
            if base_name not in type_vars:
                return False
            var = next_var("a")
            run(f'{var} = create AliasDef(name="{escaped_name}", base_type={type_vars[base_name]})')
            type_vars[type_name] = var
            return True

        elif isinstance(type_def, OverflowTypeDefinition):
            base_name = type_def.base_type.name
            if base_name not in type_vars:
                return False
            var = next_var("o")
            run(f'{var} = create OverflowDef(name="{escaped_name}", base_type={type_vars[base_name]}, policy=.{type_def.overflow})')
            type_vars[type_name] = var
            return True

        elif isinstance(type_def, SetTypeDefinition):
            elem_name = type_def.element_type.name
            if elem_name not in type_vars:
                return False
            var = next_var("s")
            run(f'{var} = create SetDef(name="{escaped_name}", element_type={type_vars[elem_name]})')
            type_vars[type_name] = var
            return True

        elif isinstance(type_def, DictionaryTypeDefinition):
            key_name = type_def.key_type.name
            val_name = type_def.value_type.name
            entry_name = type_def.entry_type.name
            if key_name not in type_vars or val_name not in type_vars:
                return False
            # Entry type might be a composite - check if it's already tracked
            if entry_name not in type_vars:
                # The entry type is a synthetic composite, create it as a CompositeDef
                # but only if its dependencies (key/val types) are available
                entry_var = next_var("de")
                run(f'{entry_var} = create CompositeDef(name="{_escape_ttq_string(entry_name)}")')
                type_vars[entry_name] = entry_var
            var = next_var("d")
            run(f'{var} = create DictDef(name="{escaped_name}", key_type={type_vars[key_name]}, value_type={type_vars[val_name]}, entry_type={type_vars[entry_name]})')
            type_vars[type_name] = var
            return True

        elif isinstance(type_def, ArrayTypeDefinition):
            elem_name = type_def.element_type.name
            if elem_name not in type_vars:
                return False
            var = next_var("ar")
            run(f'{var} = create ArrayDef(name="{escaped_name}", element_type={type_vars[elem_name]})')
            type_vars[type_name] = var
            return True

        return False

    def _create_field_record(
        self,
        field_def: Any,
        type_vars: dict[str, str],
        run: Any,
        next_var: Any,
    ) -> str | None:
        """Create a FieldDef record. Returns variable name or None if deps not ready."""
        field_type_name = field_def.type_def.name
        if field_type_name not in type_vars:
            return None

        var = next_var("f")
        escaped_name = _escape_ttq_string(field_def.name)
        type_ref = type_vars[field_type_name]

        # Handle default value
        default_part = ""
        if field_def.default_value is not None:
            default_str = _escape_ttq_string(str(field_def.default_value))
            default_part = f', default_value="{default_str}"'

        run(f'{var} = create FieldDef(name="{escaped_name}", `type`={type_ref}{default_part})')
        return var
