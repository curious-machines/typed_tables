"""Typed Tables - A typed, file-based database for structured data."""

from typed_tables.instance import InstanceRef
from typed_tables.parsing import TypeParser
from typed_tables.schema import Schema
from typed_tables.storage import StorageManager
from typed_tables.table import Table
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

__all__ = [
    # Main API
    "Schema",
    "TypeParser",
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
    "CompositeTypeDefinition",
    "FieldDefinition",
    "TypeRegistry",
]

__version__ = "0.1.0"
