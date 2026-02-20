"""Data provider for TTG — resolves selectors and axes against a database.

The DatabaseProvider reads meta-schema records from a database (via the
storage layer) and builds an in-memory graph model. It can work with any
database that conforms to a given GraphConfig — both the _meta/ database
(meta-schema context) and user databases (data context).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from typed_tables.ttg.types import GraphConfig


@dataclass
class NodeInfo:
    """A node in the graph with its selector kind and properties."""
    identity: str        # Display name (usually the type name)
    selector: str        # Which selector this node belongs to
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class EdgeInfo:
    """A traversable relationship from source to target."""
    source_id: str       # Source node identity
    target_id: str       # Target node identity
    axis_name: str       # Which axis this edge belongs to
    label: str = ""      # Default edge label (axis name)


class DatabaseProvider:
    """Builds an in-memory graph model from database records for TTG evaluation.

    Reads records from a StorageManager and TypeRegistry, using a GraphConfig
    to map selector names to type names and axis names to field paths.
    """

    def __init__(self, storage: Any, registry: Any, config: GraphConfig) -> None:
        self.storage = storage
        self.registry = registry
        self.config = config
        # node_identity → NodeInfo
        self._nodes: dict[str, NodeInfo] = {}
        # selector_name → set of node identities
        self._selector_nodes: dict[str, set[str]] = {}
        # axis_name → list of EdgeInfo
        self._axis_edges: dict[str, list[EdgeInfo]] = {}
        # Build the model
        self._build()

    def get_nodes_for_selector(self, selector: str) -> set[str]:
        """Get all node identities matching a selector name."""
        return self._selector_nodes.get(selector, set())

    def get_node(self, identity: str) -> NodeInfo | None:
        """Get node info by identity."""
        return self._nodes.get(identity)

    def get_edges_for_axis(self, axis_name: str, source_ids: set[str]) -> list[EdgeInfo]:
        """Get edges for an axis, filtered to only those from source_ids."""
        edges = self._axis_edges.get(axis_name, [])
        return [e for e in edges if e.source_id in source_ids]

    def get_all_edges_for_axis(self, axis_name: str) -> list[EdgeInfo]:
        """Get all edges for an axis (used for reverse edge computation)."""
        return self._axis_edges.get(axis_name, [])

    def get_node_property(self, identity: str, prop: str) -> Any:
        """Get a property value from a node."""
        node = self._nodes.get(identity)
        if node is None:
            return None
        return node.properties.get(prop)

    # ---- Build model from database ----

    def _build(self) -> None:
        """Build the in-memory model by reading records from the database."""
        from typed_tables.types import (
            CompositeTypeDefinition,
        )

        config = self.config
        identity_field = config.identity.get("default", "name")

        # Record index tracking: (type_name, index) → identity
        record_identities: dict[tuple[str, int], str] = {}

        # Cache: selector_name → (type_name, type_def_base, table)
        selector_info: dict[str, tuple[str, Any, Any]] = {}

        # First pass: read all selectors, detect which have duplicate identities
        # Selectors with duplicates need qualified identities (created in pass 2)
        qualified_selectors: set[str] = set()

        for sel_name, type_name in config.selectors.items():
            type_def = self.registry.get(type_name)
            if type_def is None:
                continue
            base = type_def.resolve_base_type()
            if not isinstance(base, CompositeTypeDefinition):
                continue
            try:
                table = self.storage.get_table(type_name)
            except (ValueError, FileNotFoundError):
                continue

            selector_info[sel_name] = (type_name, base, table)

            # Scan for duplicate identities
            id_field = config.identity.get(sel_name, identity_field)
            seen_ids: set[str] = set()
            has_duplicates = False
            for idx in range(table.count):
                try:
                    record = table.get(idx)
                except (IndexError, Exception):
                    continue
                if record is None or isinstance(record, (bytes, bytearray)):
                    continue
                identity = self._resolve_string_field(record, id_field, base)
                if identity is None:
                    continue
                if identity in seen_ids:
                    has_duplicates = True
                    break
                seen_ids.add(identity)

            if has_duplicates:
                qualified_selectors.add(sel_name)
                continue

            # No duplicates — create nodes directly
            for idx in range(table.count):
                try:
                    record = table.get(idx)
                except (IndexError, Exception):
                    continue
                if record is None or isinstance(record, (bytes, bytearray)):
                    continue
                identity = self._resolve_string_field(record, id_field, base)
                if identity is None:
                    continue
                props = self._read_properties(record, base, id_field)
                props["name"] = identity
                node = NodeInfo(identity=identity, selector=sel_name, properties=props)
                self._nodes[identity] = node
                self._selector_nodes.setdefault(sel_name, set()).add(identity)
                record_identities[(type_name, idx)] = identity

        # Second pass: build edges from axis definitions
        for axis_name, paths in config.axes.items():
            for path in paths:
                segments = path.split(".")
                if len(segments) < 2:
                    continue
                source_sel = segments[0]
                remaining = segments[1:]

                source_type_name = config.selectors.get(source_sel)
                if source_type_name is None:
                    continue

                source_def = self.registry.get(source_type_name)
                if source_def is None:
                    continue

                source_base = source_def.resolve_base_type()
                if not isinstance(source_base, CompositeTypeDefinition):
                    continue

                try:
                    source_table = self.storage.get_table(source_type_name)
                except (ValueError, FileNotFoundError):
                    continue

                for idx in range(source_table.count):
                    source_key = (source_type_name, idx)
                    source_id = record_identities.get(source_key)
                    if source_id is None:
                        continue

                    try:
                        record = source_table.get(idx)
                    except (IndexError, Exception):
                        continue

                    if record is None:
                        continue

                    target_ids = self._walk_path(
                        record, source_base, remaining,
                        record_identities, identity_field,
                        selector_info, qualified_selectors,
                        source_id,
                    )

                    for target_id in target_ids:
                        self._add_edge(axis_name, source_id, target_id)

    def _walk_path(
        self,
        record: dict,
        composite_base: Any,
        segments: list[str],
        record_identities: dict[tuple[str, int], str],
        identity_field: str,
        selector_info: dict[str, tuple[str, Any, Any]],
        qualified_selectors: set[str],
        source_id: str,
    ) -> list[str]:
        """Walk a dotted path from a composite record, resolving each segment.

        Returns a list of target node identities reached at the end of the path.
        """
        from typed_tables.types import (
            ArrayTypeDefinition,
            CompositeTypeDefinition,
            DictionaryTypeDefinition,
            EnumTypeDefinition,
            InterfaceTypeDefinition,
            NULL_REF,
        )

        if not segments:
            return []

        field_name = segments[0]
        remaining = segments[1:]

        # Find field definition on the composite
        field_def = None
        for f in composite_base.fields:
            if f.name == field_name:
                field_def = f
                break
        if field_def is None:
            return []

        ref_value = record.get(field_name)
        if ref_value is None:
            return []

        field_base = field_def.type_def.resolve_base_type()

        # --- Enum field ---
        if isinstance(field_base, EnumTypeDefinition):
            if not field_base.has_associated_values:
                return []  # C-style enum — no traversal
            # Swift-style: ref_value is (disc, variant_table_index)
            if not (isinstance(ref_value, tuple) and len(ref_value) == 2):
                return []
            disc, variant_idx = ref_value
            if variant_idx == NULL_REF:
                return []  # Bare variant

            if remaining:
                # Next segment is a variant name filter or a variant field
                next_seg = remaining[0]
                variant = field_base.get_variant(next_seg)
                if variant is not None:
                    # It's a variant name — filter: only process if disc matches
                    if variant.discriminant != disc:
                        return []
                    if not variant.fields:
                        return []
                    # Read variant record
                    variant_record = self._read_variant_record(field_base, variant.name, variant_idx)
                    if variant_record is None:
                        return []
                    variant_base = self._get_variant_composite(field_base, variant)
                    if variant_base is None:
                        return []
                    after_variant = remaining[1:]
                    if not after_variant:
                        # Path ends at variant — follow all variant fields to find selector nodes
                        return self._resolve_variant_to_nodes(
                            variant_record, variant_base,
                            record_identities, identity_field,
                            selector_info, qualified_selectors,
                            source_id,
                        )
                    # Continue walking from variant record
                    return self._walk_path(
                        variant_record, variant_base, after_variant,
                        record_identities, identity_field,
                        selector_info, qualified_selectors,
                        source_id,
                    )
                # Not a variant name — might be a variant field name
                # Resolve variant from disc and walk into it
                return self._walk_enum_variant_field(
                    field_base, disc, variant_idx, remaining,
                    record_identities, identity_field,
                    selector_info, qualified_selectors,
                    source_id,
                )
            else:
                # No more segments — follow all variants, find selector nodes
                return self._follow_all_variants(
                    field_base, disc, variant_idx,
                    record_identities, identity_field,
                    selector_info, qualified_selectors,
                    source_id,
                )

        # --- Dict field ---
        if isinstance(field_base, DictionaryTypeDefinition):
            entries = self._read_dict_entries(field_def, ref_value, field_base)
            if not entries:
                return []
            entry_base = field_base.entry_type
            if remaining:
                # Next segment should be 'key' or 'value' (entry fields)
                results = []
                for entry_record in entries:
                    results.extend(self._walk_path(
                        entry_record, entry_base, remaining,
                        record_identities, identity_field,
                        selector_info, qualified_selectors,
                        source_id,
                    ))
                return results
            else:
                # No more segments — check if entry_type is a selector node
                return self._resolve_composites_to_nodes(
                    entries, entry_base,
                    record_identities, identity_field,
                    selector_info, qualified_selectors,
                    source_id,
                )

        # --- Array field ---
        if isinstance(field_base, ArrayTypeDefinition):
            if not (isinstance(ref_value, tuple) and len(ref_value) == 2):
                return []
            start, length = ref_value
            if length == 0:
                return []
            try:
                array_table = self.storage.get_array_table_for_type(field_def.type_def)
                elements = array_table.get(start, length)
            except Exception:
                return []

            elem_base = field_base.element_type.resolve_base_type()
            elem_type_name = field_base.element_type.name

            if remaining:
                # Walk into each element
                results = []
                if isinstance(elem_base, CompositeTypeDefinition):
                    for elem in elements:
                        elem_record = self._read_element_record(elem, elem_type_name, elem_base)
                        if elem_record is not None:
                            results.extend(self._walk_path(
                                elem_record, elem_base, remaining,
                                record_identities, identity_field,
                                selector_info, qualified_selectors,
                                source_id,
                            ))
                return results
            else:
                # Terminal — resolve elements to selector nodes
                return self._resolve_array_elements_to_nodes(
                    elements, elem_base, elem_type_name,
                    record_identities, identity_field,
                    selector_info, qualified_selectors,
                    source_id,
                )

        # --- Composite ref field ---
        if isinstance(field_base, CompositeTypeDefinition):
            if isinstance(ref_value, int):
                ref_type_name = field_def.type_def.name
                if remaining:
                    # Continue walking into the referenced composite
                    try:
                        ref_table = self.storage.get_table(ref_type_name)
                        ref_record = ref_table.get(ref_value)
                    except Exception:
                        return []
                    if ref_record is None or isinstance(ref_record, (bytes, bytearray)):
                        return []
                    return self._walk_path(
                        ref_record, field_base, remaining,
                        record_identities, identity_field,
                        selector_info, qualified_selectors,
                        source_id,
                    )
                else:
                    key = (ref_type_name, ref_value)
                    target = record_identities.get(key)
                    if target:
                        return [target]
            return []

        # --- Interface ref field ---
        if isinstance(field_base, InterfaceTypeDefinition):
            if isinstance(ref_value, tuple) and len(ref_value) == 2:
                type_id, index = ref_value
                concrete_name = self.registry.get_type_name_by_id(type_id)
                if concrete_name:
                    key = (concrete_name, index)
                    target = record_identities.get(key)
                    if target:
                        return [target]
            return []

        return []

    # ---- Enum traversal helpers ----

    def _read_variant_record(self, enum_def: Any, variant_name: str, variant_idx: int) -> dict | None:
        """Read a variant record from its table."""
        try:
            variant_table = self.storage.get_variant_table(enum_def, variant_name)
            record = variant_table.get(variant_idx)
        except Exception:
            return None
        if record is None or isinstance(record, (bytes, bytearray)):
            return None
        return record

    def _get_variant_composite(self, enum_def: Any, variant: Any) -> Any:
        """Get a synthetic CompositeTypeDefinition for a variant's fields."""
        from typed_tables.types import CompositeTypeDefinition, FieldDefinition
        return CompositeTypeDefinition(
            name=f"_{enum_def.name}_{variant.name}",
            fields=list(variant.fields),
        )

    def _follow_all_variants(
        self,
        enum_def: Any,
        disc: int,
        variant_idx: int,
        record_identities: dict[tuple[str, int], str],
        identity_field: str,
        selector_info: dict[str, tuple[str, Any, Any]],
        qualified_selectors: set[str],
        source_id: str,
    ) -> list[str]:
        """When path ends at an enum field, follow the actual variant's fields to find nodes."""
        from typed_tables.types import NULL_REF
        variant = enum_def.get_variant_by_discriminant(disc)
        if variant is None or not variant.fields:
            return []
        if variant_idx == NULL_REF:
            return []
        variant_record = self._read_variant_record(enum_def, variant.name, variant_idx)
        if variant_record is None:
            return []
        variant_base = self._get_variant_composite(enum_def, variant)
        return self._resolve_variant_to_nodes(
            variant_record, variant_base,
            record_identities, identity_field,
            selector_info, qualified_selectors,
            source_id,
        )

    def _resolve_variant_to_nodes(
        self,
        variant_record: dict,
        variant_base: Any,
        record_identities: dict[tuple[str, int], str],
        identity_field: str,
        selector_info: dict[str, tuple[str, Any, Any]],
        qualified_selectors: set[str],
        source_id: str,
    ) -> list[str]:
        """From a variant record, follow all fields to find selector nodes."""
        results = []
        for f in variant_base.fields:
            val = variant_record.get(f.name)
            if val is None:
                continue
            targets = self._walk_path(
                variant_record, variant_base, [f.name],
                record_identities, identity_field,
                selector_info, qualified_selectors,
                source_id,
            )
            results.extend(targets)
        return results

    def _walk_enum_variant_field(
        self,
        enum_def: Any,
        disc: int,
        variant_idx: int,
        segments: list[str],
        record_identities: dict[tuple[str, int], str],
        identity_field: str,
        selector_info: dict[str, tuple[str, Any, Any]],
        qualified_selectors: set[str],
        source_id: str,
    ) -> list[str]:
        """Walk remaining segments starting from an enum's actual variant record."""
        from typed_tables.types import NULL_REF
        variant = enum_def.get_variant_by_discriminant(disc)
        if variant is None or not variant.fields:
            return []
        if variant_idx == NULL_REF:
            return []
        variant_record = self._read_variant_record(enum_def, variant.name, variant_idx)
        if variant_record is None:
            return []
        variant_base = self._get_variant_composite(enum_def, variant)
        return self._walk_path(
            variant_record, variant_base, segments,
            record_identities, identity_field,
            selector_info, qualified_selectors,
            source_id,
        )

    # ---- Dict traversal helpers ----

    def _read_dict_entries(self, field_def: Any, ref_value: Any, dict_base: Any) -> list[dict]:
        """Read all entry records from a dict field."""
        if not (isinstance(ref_value, tuple) and len(ref_value) == 2):
            return []
        start, length = ref_value
        if length == 0:
            return []
        try:
            array_table = self.storage.get_array_table_for_type(field_def.type_def)
            entry_indices = array_table.get(start, length)
        except Exception:
            return []

        entry_type_name = dict_base.entry_type.name
        entries = []
        try:
            entry_table = self.storage.get_table(entry_type_name)
        except (ValueError, FileNotFoundError):
            return []
        for entry_idx in entry_indices:
            if not isinstance(entry_idx, int):
                continue
            try:
                entry_record = entry_table.get(entry_idx)
            except Exception:
                continue
            if entry_record is not None and not isinstance(entry_record, (bytes, bytearray)):
                entries.append(entry_record)
        return entries

    # ---- Array traversal helpers ----

    def _read_element_record(self, elem: Any, elem_type_name: str, elem_base: Any) -> dict | None:
        """Read an element record — either inline dict or by-index lookup."""
        if isinstance(elem, dict):
            return elem
        if isinstance(elem, int):
            try:
                table = self.storage.get_table(elem_type_name)
                record = table.get(elem)
            except Exception:
                return None
            if record is None or isinstance(record, (bytes, bytearray)):
                return None
            return record
        return None

    def _resolve_array_elements_to_nodes(
        self,
        elements: list,
        elem_base: Any,
        elem_type_name: str,
        record_identities: dict[tuple[str, int], str],
        identity_field: str,
        selector_info: dict[str, tuple[str, Any, Any]],
        qualified_selectors: set[str],
        source_id: str,
    ) -> list[str]:
        """Resolve array elements to selector node identities (terminal case)."""
        from typed_tables.types import (
            CompositeTypeDefinition,
            InterfaceTypeDefinition,
        )

        target_ids: list[str] = []

        # Check if target elements need qualified identities
        target_is_qualified = False
        target_sel = None
        for s, t in self.config.selectors.items():
            if t == elem_type_name:
                if s in qualified_selectors:
                    target_is_qualified = True
                    target_sel = s
                else:
                    target_sel = s
                break

        if isinstance(elem_base, CompositeTypeDefinition):
            for elem in elements:
                if isinstance(elem, int):
                    key = (elem_type_name, elem)
                    existing = record_identities.get(key)
                    if existing:
                        target_ids.append(existing)
                    elif target_is_qualified and selector_info:
                        tid = self._create_qualified_node(
                            elem, elem_type_name, elem_base,
                            source_id, target_sel or "unknown",
                            identity_field, record_identities,
                            selector_info,
                            qualified_selectors,
                        )
                        if tid:
                            target_ids.append(tid)
                elif isinstance(elem, dict):
                    id_field = self.config.identity.get(target_sel, identity_field) if target_sel else identity_field
                    elem_name = self._resolve_string_field(
                        elem, id_field, elem_base
                    )
                    if elem_name is None:
                        continue

                    if target_is_qualified:
                        qualified_id = f"{source_id}.{elem_name}"
                        if qualified_id not in self._nodes:
                            sel = target_sel or "unknown"
                            props = self._read_properties(
                                elem, elem_base, id_field
                            )
                            props["name"] = elem_name
                            props["owner"] = source_id
                            node = NodeInfo(
                                identity=qualified_id, selector=sel,
                                properties=props
                            )
                            self._nodes[qualified_id] = node
                            self._selector_nodes.setdefault(sel, set()).add(qualified_id)
                            self._create_element_edges(
                                qualified_id, elem, elem_base,
                                record_identities, identity_field
                            )
                        target_ids.append(qualified_id)
                    else:
                        if elem_name in self._nodes:
                            target_ids.append(elem_name)
                        else:
                            sel = target_sel or "unknown"
                            props = self._read_properties(
                                elem, elem_base, id_field
                            )
                            props["name"] = elem_name
                            node = NodeInfo(
                                identity=elem_name, selector=sel,
                                properties=props
                            )
                            self._nodes[elem_name] = node
                            self._selector_nodes.setdefault(sel, set()).add(elem_name)
                            target_ids.append(elem_name)

        elif isinstance(elem_base, InterfaceTypeDefinition):
            for elem in elements:
                if isinstance(elem, tuple) and len(elem) == 2:
                    type_id, index = elem
                    concrete_name = self.registry.get_type_name_by_id(type_id)
                    if concrete_name:
                        key = (concrete_name, index)
                        target = record_identities.get(key)
                        if target:
                            target_ids.append(target)

        return target_ids

    # ---- Composite-to-node resolution (for dict entries at terminal) ----

    def _resolve_composites_to_nodes(
        self,
        records: list[dict],
        composite_base: Any,
        record_identities: dict[tuple[str, int], str],
        identity_field: str,
        selector_info: dict[str, tuple[str, Any, Any]],
        qualified_selectors: set[str],
        source_id: str,
    ) -> list[str]:
        """Check if composite records are selector nodes; if not, follow their fields."""
        type_name = composite_base.name
        target_sel = None
        for s, t in self.config.selectors.items():
            if t == type_name:
                target_sel = s
                break

        if target_sel is not None:
            # These records are a known selector type
            id_field = self.config.identity.get(target_sel, identity_field)
            target_ids = []
            for rec in records:
                identity = self._resolve_string_field(rec, id_field, composite_base)
                if identity is None:
                    continue
                if identity not in self._nodes:
                    props = self._read_properties(rec, composite_base, id_field)
                    props["name"] = identity
                    node = NodeInfo(identity=identity, selector=target_sel, properties=props)
                    self._nodes[identity] = node
                    self._selector_nodes.setdefault(target_sel, set()).add(identity)
                target_ids.append(identity)
            return target_ids

        # Not a direct selector — follow fields recursively to find selector nodes
        results = []
        for rec in records:
            for f in composite_base.fields:
                val = rec.get(f.name)
                if val is None:
                    continue
                targets = self._walk_path(
                    rec, composite_base, [f.name],
                    record_identities, identity_field,
                    selector_info, qualified_selectors,
                    source_id,
                )
                results.extend(targets)
        return results

    # ---- Shared helpers ----

    def _resolve_string_field(self, record: dict, field_name: str, composite_base: Any) -> str | None:
        """Resolve a string field from a record to its actual string value."""
        from typed_tables.types import StringTypeDefinition

        value = record.get(field_name)
        if value is None:
            return None

        if isinstance(value, str):
            return value

        # Find the field definition to check if it's a string type
        field_def = None
        for f in composite_base.fields:
            if f.name == field_name:
                field_def = f
                break
        if field_def is None:
            return str(value)

        field_base = field_def.type_def.resolve_base_type()

        if isinstance(field_base, StringTypeDefinition):
            if isinstance(value, tuple) and len(value) == 2:
                start, length = value
                if length == 0:
                    return ""
                try:
                    array_table = self.storage.get_array_table_for_type(field_def.type_def)
                    chars = array_table.get(start, length)
                    return "".join(chars)
                except Exception:
                    return None

        return str(value)

    def _read_properties(self, record: dict, composite_base: Any, identity_field: str) -> dict[str, Any]:
        """Read scalar properties from a record."""
        from typed_tables.types import (
            BooleanTypeDefinition,
            PrimitiveTypeDefinition,
            StringTypeDefinition,
        )

        props: dict[str, Any] = {}
        for field_def in composite_base.fields:
            fname = field_def.name
            if fname == identity_field:
                continue
            field_base = field_def.type_def.resolve_base_type()
            if isinstance(field_base, BooleanTypeDefinition):
                val = record.get(fname)
                if val is not None:
                    props[fname] = bool(val)
            elif isinstance(field_base, PrimitiveTypeDefinition):
                val = record.get(fname)
                if val is not None:
                    props[fname] = val
            elif isinstance(field_base, StringTypeDefinition):
                val = self._resolve_string_field(record, fname, composite_base)
                if val is not None:
                    props[fname] = val
        return props

    def _create_qualified_node(
        self,
        elem_index: int,
        elem_type_name: str,
        elem_base: Any,
        source_id: str,
        selector: str,
        identity_field: str,
        record_identities: dict[tuple[str, int], str],
        selector_info: dict[str, tuple[str, Any, Any]],
        qualified_selectors: set[str],
    ) -> str | None:
        """Create a qualified node for an element from a qualified selector."""
        try:
            elem_table = self.storage.get_table(elem_type_name)
            record = elem_table.get(elem_index)
        except (ValueError, FileNotFoundError, IndexError, Exception):
            return None

        if record is None or isinstance(record, (bytes, bytearray)):
            return None

        elem_name = self._resolve_string_field(record, identity_field, elem_base)
        if elem_name is None:
            return None

        qualified_id = f"{source_id}.{elem_name}"
        record_identities[(elem_type_name, elem_index)] = qualified_id

        if qualified_id not in self._nodes:
            props = self._read_properties(record, elem_base, identity_field)
            props["name"] = elem_name
            props["owner"] = source_id
            node = NodeInfo(
                identity=qualified_id, selector=selector,
                properties=props
            )
            self._nodes[qualified_id] = node
            self._selector_nodes.setdefault(selector, set()).add(qualified_id)

            self._create_element_edges(
                qualified_id, record, elem_base,
                record_identities, identity_field
            )

        return qualified_id

    def _create_element_edges(
        self,
        element_identity: str,
        elem_record: dict,
        elem_base: Any,
        record_identities: dict[tuple[str, int], str],
        identity_field: str,
    ) -> None:
        """Create edges from inline element nodes to their referenced types."""
        from typed_tables.types import (
            CompositeTypeDefinition,
            InterfaceTypeDefinition,
        )

        elem_selector = None
        node = self._nodes.get(element_identity)
        if node:
            elem_selector = node.selector

        if elem_selector is None:
            return

        for axis_name, paths in self.config.axes.items():
            for path in paths:
                segments = path.split(".")
                if len(segments) < 2:
                    continue
                sel_name = segments[0]
                if sel_name != elem_selector:
                    continue
                field_name = segments[1]

                ref_value = elem_record.get(field_name)
                if ref_value is None:
                    continue

                field_def = None
                for f in elem_base.fields:
                    if f.name == field_name:
                        field_def = f
                        break
                if field_def is None:
                    continue

                field_base = field_def.type_def.resolve_base_type()

                if isinstance(field_base, InterfaceTypeDefinition):
                    if isinstance(ref_value, tuple) and len(ref_value) == 2:
                        type_id, index = ref_value
                        concrete_name = self.registry.get_type_name_by_id(type_id)
                        if concrete_name:
                            key = (concrete_name, index)
                            target = record_identities.get(key)
                            if target:
                                self._add_edge(axis_name, element_identity, target)

                elif isinstance(field_base, CompositeTypeDefinition):
                    if isinstance(ref_value, int):
                        ref_type_name = field_def.type_def.name
                        key = (ref_type_name, ref_value)
                        target = record_identities.get(key)
                        if target:
                            self._add_edge(axis_name, element_identity, target)

    def _add_edge(self, axis_name: str, source_id: str, target_id: str) -> None:
        """Add an edge to the model."""
        edge = EdgeInfo(
            source_id=source_id,
            target_id=target_id,
            axis_name=axis_name,
            label=axis_name,
        )
        self._axis_edges.setdefault(axis_name, []).append(edge)
