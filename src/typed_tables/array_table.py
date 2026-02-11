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

    def append(self, start_index: int, length: int, new_elements: list[Any]) -> tuple[int, int]:
        """Append elements. Returns new (start_index, length).

        Tail fast path: if array is at end of element table, extend in place.
        Otherwise: copy-on-write (read existing + new, write all to end).
        """
        if not new_elements:
            return (start_index, length)

        if start_index + length == self.element_table.count:
            # Tail fast path — just extend
            for elem in new_elements:
                self.element_table.insert(elem)
            return (start_index, length + len(new_elements))

        # Copy-on-write
        existing = self.get(start_index, length)
        return self.insert(existing + new_elements)

    def prepend(self, start_index: int, length: int, new_elements: list[Any]) -> tuple[int, int]:
        """Prepend elements. Copy-on-write: new_elements + existing → write to end."""
        if not new_elements:
            return (start_index, length)
        existing = self.get(start_index, length)
        return self.insert(new_elements + existing)

    def delete(self, start_index: int, length: int, indices_to_delete: set[int]) -> tuple[int, int]:
        """Delete elements at given indices. Returns new (start_index, length).

        Fast paths:
        - All deleted from end → just decrement length
        - All deleted from start → bump start_index
        - General → copy-on-write with only surviving elements
        """
        if not indices_to_delete:
            return (start_index, length)

        new_length = length - len(indices_to_delete)
        if new_length == 0:
            return (0, 0)

        sorted_indices = sorted(indices_to_delete)

        # Fast path: all deleted from end
        if sorted_indices == list(range(length - len(indices_to_delete), length)):
            return (start_index, new_length)

        # Fast path: all deleted from start
        if sorted_indices == list(range(len(indices_to_delete))):
            return (start_index + len(indices_to_delete), new_length)

        # General case: copy-on-write
        existing = self.get(start_index, length)
        surviving = [elem for i, elem in enumerate(existing) if i not in indices_to_delete]
        return self.insert(surviving)

    def update_in_place(self, start_index: int, length: int, elements: list[Any]) -> None:
        """Overwrite elements at existing positions. Length must match original."""
        assert len(elements) == length
        for i, element in enumerate(elements):
            self.element_table.update(start_index + i, element)

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
        data_dir / f"{table_name}.bin",
    )

    return ArrayTable(array_type, element_table)
