"""TTG (Typed Tables Graph) — composable graph expression language."""

from typed_tables.ttg.types import (
    FileResult,
    GraphConfig,
    GraphEdge,
    GraphResult,
)
from typed_tables.ttg.engine import TTGEngine

__all__ = [
    "GraphEdge",
    "GraphResult",
    "FileResult",
    "GraphConfig",
    "TTGEngine",
]
