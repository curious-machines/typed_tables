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

    An array table stores (start_index, length) pairs that reference
    elements in an element table. This allows arrays of variable length
    while maintaining fixed-size records in both tables.
    """

    def __init__(
        self,
        array_type: ArrayTypeDefinition,
        header_table: Table,
        element_table: Table,
    ) -> None:
        """Initialize an array table.

        Args:
            array_type: The array type definition.
            header_table: Table storing (start_index, length) pairs.
            element_table: Table storing the actual array elements.
        """
        self.array_type = array_type
        self.header_table = header_table
        self.element_table = element_table

    @property
    def count(self) -> int:
        """Return the number of arrays stored."""
        return self.header_table.count

    def insert(self, elements: list[Any]) -> int:
        """Insert an array and return its index.

        Args:
            elements: List of elements to store.

        Returns:
            Index of the array in the header table.
        """
        # Insert all elements into the element table
        if not elements:
            start_index = 0
            length = 0
        else:
            start_index = self.element_table.count
            for element in elements:
                self.element_table.insert(element)
            length = len(elements)

        # Insert the header (start_index, length)
        return self.header_table.insert((start_index, length))

    def get(self, index: int) -> list[Any]:
        """Get an array by its index.

        Args:
            index: Index of the array in the header table.

        Returns:
            List of array elements.
        """
        start_index, length = self.header_table.get(index)

        if length == 0:
            return []

        return [self.element_table.get(start_index + i) for i in range(length)]

    def get_header(self, index: int) -> tuple[int, int]:
        """Get the header (start_index, length) for an array.

        Args:
            index: Index of the array in the header table.

        Returns:
            Tuple of (start_index, length).
        """
        return self.header_table.get(index)

    def close(self) -> None:
        """Close underlying tables."""
        self.header_table.close()
        self.element_table.close()


def create_array_table(
    array_type: ArrayTypeDefinition,
    data_dir: Path,
    table_name: str | None = None,
) -> ArrayTable:
    """Create an ArrayTable with its header and element tables.

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

    # Create header table (stores start_index, length pairs)
    header_table = Table(
        array_type,
        data_dir / f"{table_name}.bin",
    )

    # Create element table (stores actual elements)
    element_type = array_type.element_type.resolve_base_type()
    element_table = Table(
        element_type,
        data_dir / f"{table_name}_elements.bin",
    )

    return ArrayTable(array_type, header_table, element_table)
