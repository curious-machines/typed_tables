"""TTGE (Typed Tables Graph Expression) — composable graph expression language."""

from typed_tables.ttge.types import (
    FileResult,
    GraphConfig,
    GraphEdge,
    GraphResult,
)
from typed_tables.ttge.engine import TTGEngine

__all__ = [
    "GraphEdge",
    "GraphResult",
    "FileResult",
    "GraphConfig",
    "TTGEngine",
]
