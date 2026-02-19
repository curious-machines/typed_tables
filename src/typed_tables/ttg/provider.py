"""Data provider for TTG — resolves selectors and axes against a schema.

The MetaSchemaProvider builds an in-memory model from a TypeRegistry,
mapping selector names to node sets and axis names to traversable relationships.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


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


class MetaSchemaProvider:
    """Builds an in-memory graph model from a TypeRegistry for TTG evaluation."""

    def __init__(self, registry: Any) -> None:
        self.registry = registry
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

    def get_node_property(self, identity: str, prop: str) -> Any:
        """Get a property value from a node."""
        node = self._nodes.get(identity)
        if node is None:
            return None
        return node.properties.get(prop)

    def _build(self) -> None:
        """Build the in-memory model from the registry."""
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
            PrimitiveTypeDefinition,
            SetTypeDefinition,
            StringTypeDefinition,
        )

        # First pass: create nodes for all types
        for type_name in self.registry.list_types():
            if type_name.startswith("_"):
                continue
            type_def = self.registry.get(type_name)
            if type_def is None:
                continue

            base = type_def.resolve_base_type()

            # Classify into selector
            selector = self._classify_selector(type_def, base)
            if selector is None:
                continue

            props = {"name": type_name}
            node = NodeInfo(identity=type_name, selector=selector, properties=props)
            self._nodes[type_name] = node
            self._selector_nodes.setdefault(selector, set()).add(type_name)

        # Second pass: create edges (axes)
        for type_name, node in list(self._nodes.items()):
            type_def = self.registry.get(type_name)
            if type_def is None:
                continue

            base = type_def.resolve_base_type()

            if isinstance(base, CompositeTypeDefinition):
                self._build_composite_edges(type_name, base)
            elif isinstance(base, InterfaceTypeDefinition):
                self._build_interface_edges(type_name, base)
            elif isinstance(base, EnumTypeDefinition):
                self._build_enum_edges(type_name, base)
            elif isinstance(base, AliasTypeDefinition):
                self._add_edge("alias", type_name, base.base_type.name)
            elif isinstance(base, OverflowTypeDefinition):
                self._add_edge("base", type_name, base.base_type.name)
            elif isinstance(base, SetTypeDefinition):
                self._add_edge("element", type_name, base.element_type.name)
            elif isinstance(base, DictionaryTypeDefinition):
                self._add_edge("key", type_name, base.key_type.name)
                self._add_edge("value", type_name, base.value_type.name)
            elif isinstance(base, ArrayTypeDefinition):
                self._add_edge("element", type_name, base.element_type.name)

    def _classify_selector(self, type_def: Any, base: Any) -> str | None:
        """Map a type definition to its selector name."""
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
            PrimitiveTypeDefinition,
            SetTypeDefinition,
            StringTypeDefinition,
        )

        if isinstance(type_def, CompositeTypeDefinition):
            return "composites"
        elif isinstance(type_def, InterfaceTypeDefinition):
            return "interfaces"
        elif isinstance(type_def, EnumTypeDefinition):
            return "enums"
        elif isinstance(type_def, AliasTypeDefinition):
            return "aliases"
        elif isinstance(type_def, OverflowTypeDefinition):
            return "overflows"
        elif isinstance(type_def, BooleanTypeDefinition):
            return "boolean"
        elif isinstance(type_def, StringTypeDefinition):
            return "string"
        elif isinstance(type_def, FractionTypeDefinition):
            return "fraction"
        elif isinstance(type_def, BigIntTypeDefinition):
            return "bigint"
        elif isinstance(type_def, BigUIntTypeDefinition):
            return "biguint"
        elif isinstance(type_def, SetTypeDefinition):
            return "sets"
        elif isinstance(type_def, DictionaryTypeDefinition):
            return "dictionaries"
        elif isinstance(type_def, ArrayTypeDefinition):
            return "arrays"
        elif isinstance(type_def, PrimitiveTypeDefinition):
            # Map to specific primitive selector
            return type_def.name
        return None

    def _build_composite_edges(self, type_name: str, comp: Any) -> None:
        """Build edges for a composite type."""
        # Fields axis
        for f in comp.fields:
            field_id = self._ensure_field_node(type_name, f)
            self._add_edge("fields", type_name, field_id)
            # Type axis (from field to its type)
            self._add_edge("type", field_id, f.type_def.name)

        # Extends axis (parent)
        if comp.parent:
            self._add_edge("extends", type_name, comp.parent)

        # Interfaces axis
        for iface_name in getattr(comp, "declared_interfaces", comp.interfaces):
            self._add_edge("interfaces", type_name, iface_name)

    def _build_interface_edges(self, type_name: str, iface: Any) -> None:
        """Build edges for an interface type."""
        # Fields axis
        for f in iface.fields:
            field_id = self._ensure_field_node(type_name, f)
            self._add_edge("fields", type_name, field_id)
            self._add_edge("type", field_id, f.type_def.name)

        # Extends axis (parent interfaces)
        for parent_name in iface.interfaces:
            self._add_edge("extends", type_name, parent_name)

    def _build_enum_edges(self, type_name: str, enum_def: Any) -> None:
        """Build edges for an enum type."""
        for variant in enum_def.variants:
            variant_id = f"{type_name}.{variant.name}"
            # Create variant node
            props = {"name": variant.name}
            self._nodes[variant_id] = NodeInfo(
                identity=variant_id, selector="variants", properties=props
            )
            self._selector_nodes.setdefault("variants", set()).add(variant_id)
            self._add_edge("variants", type_name, variant_id)

            # Variant fields
            for f in variant.fields:
                field_id = self._ensure_field_node(variant_id, f)
                self._add_edge("fields", variant_id, field_id)
                self._add_edge("type", field_id, f.type_def.name)

        # Backing type axis
        if enum_def.backing_type:
            self._add_edge("backing", type_name, enum_def.backing_type.value)

    def _ensure_field_node(self, owner: str, field_def: Any) -> str:
        """Ensure a field node exists, returning its identity."""
        field_id = f"{owner}.{field_def.name}"
        if field_id not in self._nodes:
            props = {"name": field_def.name, "owner": owner}
            self._nodes[field_id] = NodeInfo(
                identity=field_id, selector="fields", properties=props
            )
            self._selector_nodes.setdefault("fields", set()).add(field_id)
        return field_id

    def _add_edge(self, axis_name: str, source_id: str, target_id: str) -> None:
        """Add an edge to the model."""
        # Ensure target node exists (may be a primitive or other built-in)
        if target_id not in self._nodes:
            # Try to find it in the registry
            target_def = self.registry.get(target_id)
            if target_def is not None:
                base = target_def.resolve_base_type()
                selector = self._classify_selector(target_def, base)
                if selector:
                    props = {"name": target_id}
                    self._nodes[target_id] = NodeInfo(
                        identity=target_id, selector=selector, properties=props
                    )
                    self._selector_nodes.setdefault(selector, set()).add(target_id)

        edge = EdgeInfo(
            source_id=source_id,
            target_id=target_id,
            axis_name=axis_name,
            label=axis_name,
        )
        self._axis_edges.setdefault(axis_name, []).append(edge)
