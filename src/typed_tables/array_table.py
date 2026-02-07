"""Array table storage for typed data."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from typed_tables.table import Table
from typed_tables.types import ArrayTypeDefinition

if TYPE_CHECKING:
    pass


class ArrayTable:
    """Manages storage for array types.

    An array table stores elements in an element table. The (start_index, length)
    pair is stored inline in the composite record that owns the array, not in a
    separate header table.
    """

    def __init__(
        self,
        array_type: ArrayTypeDefinition,
        element_table: Table,
    ) -> None:
        """Initialize an array table.

        Args:
            array_type: The array type definition.
            element_table: Table storing the actual array elements.
        """
        self.array_type = array_type
        self.element_table = element_table

    @property
    def count(self) -> int:
        """Return the number of elements stored."""
        return self.element_table.count

    def insert(self, elements: list[Any]) -> tuple[int, int]:
        """Insert an array and return (start_index, length).

        Args:
            elements: List of elements to store.

        Returns:
            Tuple of (start_index, length) for inline storage in composite records.
        """
        if not elements:
            return (0, 0)

        start_index = self.element_table.count
        for element in elements:
            self.element_table.insert(element)
        length = len(elements)

        return (start_index, length)

    def get(self, start_index: int, length: int) -> list[Any]:
        """Get an array by its start_index and length.

        Args:
            start_index: Starting index in the element table.
            length: Number of elements.

        Returns:
            List of array elements.
        """
        if length == 0:
            return []

        return [self.element_table.get(start_index + i) for i in range(length)]

    def close(self) -> None:
        """Close underlying tables."""
        self.element_table.close()


def create_array_table(
    array_type: ArrayTypeDefinition,
    data_dir: Path,
    table_name: str | None = None,
) -> ArrayTable:
    """Create an ArrayTable with its element table.

    Args:
        array_type: The array type definition.
        data_dir: Directory to store table files.
        table_name: Optional name for the table files. If not provided,
            uses the array_type's name. This allows aliases to have
            their own named tables.

    Returns:
        An ArrayTable instance.
    """
    if table_name is None:
        table_name = array_type.name

    # Create element table (stores actual elements)
    element_type = array_type.element_type.resolve_base_type()
    element_table = Table(
        element_type,
        data_dir / f"{table_name}_elements.bin",
    )

    return ArrayTable(array_type, element_table)
