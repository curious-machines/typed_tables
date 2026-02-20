"""Tests for lazy table creation — .bin files should only be created on first insert."""

import tempfile
from pathlib import Path

import pytest

from typed_tables.array_table import create_array_table
from typed_tables.table import Table
from typed_tables.types import (
    ArrayTypeDefinition,
    CompositeTypeDefinition,
    FieldDefinition,
    PrimitiveType,
    PrimitiveTypeDefinition,
)


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def uint8_type():
    return PrimitiveTypeDefinition(name="uint8", primitive=PrimitiveType.UINT8)


@pytest.fixture
def uint32_type():
    return PrimitiveTypeDefinition(name="uint32", primitive=PrimitiveType.UINT32)


class TestLazyTableCreation:
    """Table .bin files should not be created until first insert."""

    def test_no_bin_file_on_construction(self, tmp_dir, uint32_type):
        """Constructing a Table should NOT create a .bin file."""
        path = tmp_dir / "test.bin"
        table = Table(uint32_type, path)
        assert not path.exists()
        table.close()

    def test_bin_file_created_on_first_insert(self, tmp_dir, uint32_type):
        """First insert should create the .bin file."""
        path = tmp_dir / "test.bin"
        table = Table(uint32_type, path)
        assert not path.exists()
        table.insert(42)
        assert path.exists()
        table.close()

    def test_count_is_zero_before_insert(self, tmp_dir, uint32_type):
        """Count should be 0 before any insert, even without a file."""
        path = tmp_dir / "test.bin"
        table = Table(uint32_type, path)
        assert table.count == 0
        table.close()

    def test_get_raises_on_empty_lazy_table(self, tmp_dir, uint32_type):
        """get() on a lazy table should raise IndexError (count is 0)."""
        path = tmp_dir / "test.bin"
        table = Table(uint32_type, path)
        with pytest.raises(IndexError):
            table.get(0)
        table.close()

    def test_insert_then_get(self, tmp_dir, uint32_type):
        """Insert and get should work normally after lazy init."""
        path = tmp_dir / "test.bin"
        table = Table(uint32_type, path)
        idx = table.insert(99)
        assert idx == 0
        assert table.get(0) == 99
        assert table.count == 1
        table.close()

    def test_multiple_inserts(self, tmp_dir, uint8_type):
        """Multiple inserts should work correctly after lazy init."""
        path = tmp_dir / "test.bin"
        table = Table(uint8_type, path)
        for i in range(10):
            table.insert(i)
        assert table.count == 10
        for i in range(10):
            assert table.get(i) == i
        table.close()

    def test_reopen_existing_file(self, tmp_dir, uint32_type):
        """Opening an existing .bin file should load it eagerly."""
        path = tmp_dir / "test.bin"
        table1 = Table(uint32_type, path)
        table1.insert(42)
        table1.insert(99)
        table1.close()

        # Reopen — should open eagerly since file exists
        table2 = Table(uint32_type, path)
        assert table2.count == 2
        assert table2.get(0) == 42
        assert table2.get(1) == 99
        table2.close()

    def test_close_without_insert_is_safe(self, tmp_dir, uint32_type):
        """Closing a lazy table that was never written should not error."""
        path = tmp_dir / "test.bin"
        table = Table(uint32_type, path)
        table.close()  # Should not raise
        assert not path.exists()

    def test_parent_dirs_created_on_insert(self, tmp_dir, uint32_type):
        """Parent directories should be created when the file is created on insert."""
        path = tmp_dir / "sub" / "dir" / "test.bin"
        table = Table(uint32_type, path)
        assert not path.parent.exists()
        table.insert(1)
        assert path.parent.exists()
        assert path.exists()
        table.close()

    def test_no_parent_dirs_without_insert(self, tmp_dir, uint32_type):
        """Parent directories should NOT be created if no insert happens."""
        path = tmp_dir / "sub" / "dir" / "test.bin"
        table = Table(uint32_type, path)
        assert not path.parent.exists()
        table.close()


class TestLazyArrayTable:
    """ArrayTable element tables should also be lazy."""

    def test_no_bin_file_for_array_table(self, tmp_dir, uint8_type):
        """Creating an ArrayTable should not create any .bin files."""
        array_type = ArrayTypeDefinition(name="uint8[]", element_type=uint8_type)
        array_table = create_array_table(array_type, tmp_dir)
        assert not (tmp_dir / "uint8[].bin").exists()
        array_table.close()

    def test_array_table_insert_creates_file(self, tmp_dir, uint8_type):
        """Inserting into an ArrayTable should create the element .bin file."""
        array_type = ArrayTypeDefinition(name="uint8[]", element_type=uint8_type)
        array_table = create_array_table(array_type, tmp_dir)
        array_table.insert([1, 2, 3])
        assert (tmp_dir / "uint8[].bin").exists()
        array_table.close()

    def test_empty_array_insert_no_file(self, tmp_dir, uint8_type):
        """Inserting an empty array should not create any file."""
        array_type = ArrayTypeDefinition(name="uint8[]", element_type=uint8_type)
        array_table = create_array_table(array_type, tmp_dir)
        start, length = array_table.insert([])
        assert start == 0
        assert length == 0
        assert not (tmp_dir / "uint8[].bin").exists()
        array_table.close()


class TestLazyCompositeTable:
    """Composite tables should also be lazy."""

    def test_composite_table_no_file_on_construction(self, tmp_dir, uint8_type, uint32_type):
        """Constructing a composite Table should not create a .bin file."""
        comp_type = CompositeTypeDefinition(
            name="TestComp",
            fields=[
                FieldDefinition(name="a", type_def=uint8_type),
                FieldDefinition(name="b", type_def=uint32_type),
            ],
        )
        path = tmp_dir / "TestComp.bin"
        table = Table(comp_type, path)
        assert not path.exists()
        table.close()

    def test_composite_table_created_on_insert(self, tmp_dir, uint8_type, uint32_type):
        """Inserting into a composite Table should create the .bin file."""
        comp_type = CompositeTypeDefinition(
            name="TestComp",
            fields=[
                FieldDefinition(name="a", type_def=uint8_type),
                FieldDefinition(name="b", type_def=uint32_type),
            ],
        )
        path = tmp_dir / "TestComp.bin"
        table = Table(comp_type, path)
        table.insert({"a": 1, "b": 2})
        assert path.exists()
        assert table.count == 1
        table.close()


class TestLazyWithStorageManager:
    """Integration: StorageManager should not create .bin files for unused types."""

    def test_meta_database_no_unused_bin_files(self, tmp_dir):
        """Types that are never inserted into should have no .bin files."""
        from typed_tables.types import TypeRegistry

        registry = TypeRegistry()
        # Register a few composite types
        uint8 = registry.get("uint8")
        comp_a = CompositeTypeDefinition(
            name="TypeA",
            fields=[FieldDefinition(name="x", type_def=uint8)],
        )
        comp_b = CompositeTypeDefinition(
            name="TypeB",
            fields=[FieldDefinition(name="y", type_def=uint8)],
        )
        registry.register(comp_a)
        registry.register(comp_b)

        from typed_tables.storage import StorageManager

        storage = StorageManager(tmp_dir, registry)

        # Only insert into TypeA
        table_a = storage.get_table("TypeA")
        table_a.insert({"x": 42})

        # Get TypeB table but don't insert
        storage.get_table("TypeB")

        storage.close()

        # TypeA.bin should exist (we inserted)
        assert (tmp_dir / "TypeA.bin").exists()
        # TypeB.bin should NOT exist (never inserted)
        assert not (tmp_dir / "TypeB.bin").exists()
