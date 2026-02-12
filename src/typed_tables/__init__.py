"""Typed Tables - A typed, file-based database for structured data."""

from typed_tables.instance import InstanceRef
from typed_tables.schema import Schema
from typed_tables.storage import StorageManager
from typed_tables.table import Table
from typed_tables.types import (
    AliasTypeDefinition,
    ArrayTypeDefinition,
    BooleanTypeDefinition,
    CompositeTypeDefinition,
    EnumTypeDefinition,
    EnumValue,
    EnumVariantDefinition,
    FieldDefinition,
    FractionTypeDefinition,
    InterfaceTypeDefinition,
    PrimitiveType,
    PrimitiveTypeDefinition,
    StringTypeDefinition,
    TypeDefinition,
    TypeRegistry,
    is_boolean_type,
    is_fraction_type,
    is_string_type,
)

__all__ = [
    # Main API
    "Schema",
    "InstanceRef",
    # Storage
    "Table",
    "StorageManager",
    # Type definitions
    "TypeDefinition",
    "PrimitiveType",
    "PrimitiveTypeDefinition",
    "AliasTypeDefinition",
    "ArrayTypeDefinition",
    "StringTypeDefinition",
    "BooleanTypeDefinition",
    "FractionTypeDefinition",
    "CompositeTypeDefinition",
    "InterfaceTypeDefinition",
    "EnumTypeDefinition",
    "EnumValue",
    "EnumVariantDefinition",
    "FieldDefinition",
    "TypeRegistry",
    "is_string_type",
    "is_boolean_type",
    "is_fraction_type",
]

__version__ = "0.1.0"
