"""Tests for storage and schema functionality."""

import tempfile
from pathlib import Path

import pytest

from typed_tables import Schema
from typed_tables.table import Table
from typed_tables.types import PrimitiveType, PrimitiveTypeDefinition


class TestTable:
    """Tests for the Table class."""

    def test_insert_and_get_uint32(self, tmp_path):
        """Test inserting and retrieving uint32 values."""
        type_def = PrimitiveTypeDefinition(name="uint32", primitive=PrimitiveType.UINT32)

        with Table(type_def, tmp_path / "uint32.bin") as table:
            idx0 = table.insert(42)
            idx1 = table.insert(100)
            idx2 = table.insert(0)

            assert idx0 == 0
            assert idx1 == 1
            assert idx2 == 2

            assert table.get(0) == 42
            assert table.get(1) == 100
            assert table.get(2) == 0
            assert table.count == 3

    def test_insert_and_get_float64(self, tmp_path):
        """Test inserting and retrieving float64 values."""
        type_def = PrimitiveTypeDefinition(name="float64", primitive=PrimitiveType.FLOAT64)

        with Table(type_def, tmp_path / "float64.bin") as table:
            table.insert(3.14159)
            table.insert(-2.71828)

            assert abs(table.get(0) - 3.14159) < 1e-10
            assert abs(table.get(1) - (-2.71828)) < 1e-10

    def test_insert_and_get_uint128(self, tmp_path):
        """Test inserting and retrieving uint128 values."""
        type_def = PrimitiveTypeDefinition(name="uint128", primitive=PrimitiveType.UINT128)

        with Table(type_def, tmp_path / "uint128.bin") as table:
            # Test with a large value
            large_value = (1 << 100) + 12345
            table.insert(large_value)

            assert table.get(0) == large_value

    def test_insert_and_get_character(self, tmp_path):
        """Test inserting and retrieving character values."""
        type_def = PrimitiveTypeDefinition(name="character", primitive=PrimitiveType.CHARACTER)

        with Table(type_def, tmp_path / "character.bin") as table:
            table.insert("A")
            table.insert("\u00E9")  # é
            table.insert("\U0001F600")  # emoji

            assert table.get(0) == "A"
            assert table.get(1) == "\u00E9"
            assert table.get(2) == "\U0001F600"

    def test_index_out_of_range(self, tmp_path):
        """Test that out of range index raises."""
        type_def = PrimitiveTypeDefinition(name="uint32", primitive=PrimitiveType.UINT32)

        with Table(type_def, tmp_path / "uint32.bin") as table:
            table.insert(42)

            with pytest.raises(IndexError):
                table.get(1)

            with pytest.raises(IndexError):
                table.get(-1)

    def test_persistence(self, tmp_path):
        """Test that data persists across table instances."""
        type_def = PrimitiveTypeDefinition(name="uint32", primitive=PrimitiveType.UINT32)
        file_path = tmp_path / "uint32.bin"

        # Write data
        with Table(type_def, file_path) as table:
            table.insert(100)
            table.insert(200)
            table.insert(300)

        # Read data in new instance
        with Table(type_def, file_path) as table:
            assert table.count == 3
            assert table.get(0) == 100
            assert table.get(1) == 200
            assert table.get(2) == 300

    def test_file_growth(self, tmp_path):
        """Test that file grows when needed."""
        type_def = PrimitiveTypeDefinition(name="uint32", primitive=PrimitiveType.UINT32)

        with Table(type_def, tmp_path / "uint32.bin") as table:
            # Insert many values to trigger growth
            for i in range(2000):
                table.insert(i)

            assert table.count == 2000
            assert table.get(1999) == 1999


class TestSchema:
    """Tests for the Schema class."""

    def test_parse_and_create_primitive(self, tmp_path):
        """Test creating a primitive instance."""
        with Schema.parse("define counter as uint32", tmp_path) as schema:
            instance = schema.create_instance("counter", 42)

            assert instance.type_name == "counter"
            assert instance.load() == 42

    def test_parse_and_create_array(self, tmp_path):
        """Test creating an array instance."""
        with Schema.parse("define numbers as uint32[]", tmp_path) as schema:
            instance = schema.create_instance("numbers", [1, 2, 3, 4, 5])

            assert instance.type_name == "numbers"
            assert instance.load() == [1, 2, 3, 4, 5]

    def test_parse_and_create_string_like(self, tmp_path):
        """Test creating a string-like array of characters."""
        with Schema.parse("define name as character[]", tmp_path) as schema:
            chars = ["H", "e", "l", "l", "o"]
            instance = schema.create_instance("name", chars)

            assert instance.load() == chars

    def test_example_from_readme(self, tmp_path):
        """Test the example from CLAUDE.md."""
        types = """
        define uuid as uint128
        define name as character[]

        Person {
            id: uuid,
            name
        }
        """

        with Schema.parse(types, tmp_path) as schema:
            # Create a Person instance
            person = schema.create_instance(
                "Person",
                {
                    "id": 0x12345678_12345678_12345678_12345678,
                    "name": ["B", "i", "l", "l"],
                },
            )

            # Verify data persisted
            data = person.load()
            assert data["id"] == 0x12345678_12345678_12345678_12345678

    def test_list_types(self, tmp_path):
        """Test listing types in schema."""
        types = """
        define uuid as uint128
        Point { x: float64, y: float64 }
        """

        with Schema.parse(types, tmp_path) as schema:
            type_names = schema.list_types()

            assert "uuid" in type_names
            assert "Point" in type_names
            assert "uint128" in type_names
            assert "float64" in type_names

    def test_get_type(self, tmp_path):
        """Test getting a type definition."""
        with Schema.parse("define counter as uint32", tmp_path) as schema:
            type_def = schema.get_type("counter")
            assert type_def.name == "counter"

            with pytest.raises(KeyError):
                schema.get_type("nonexistent")

    def test_persistence_full_workflow(self, tmp_path):
        """Test full workflow with persistence."""
        types = """
        define uuid as uint128
        define name as character[]

        Person {
            id: uuid,
            name
        }
        """

        # Create and store data
        with Schema.parse(types, tmp_path) as schema:
            person1 = schema.create_instance(
                "Person",
                {"id": 1, "name": ["A", "l", "i", "c", "e"]},
            )
            person2 = schema.create_instance(
                "Person",
                {"id": 2, "name": ["B", "o", "b"]},
            )

            assert person1.index == 0
            assert person2.index == 1

        # Verify files were created
        assert (tmp_path / "Person.bin").exists()
        assert (tmp_path / "_metadata.json").exists()

    def test_tuple_values_for_composite(self, tmp_path):
        """Test creating composite with tuple values."""
        types = """
        Point {
            x: float64,
            y: float64
        }
        """

        with Schema.parse(types, tmp_path) as schema:
            point = schema.create_instance("Point", (3.0, 4.0))

            data = point.load()
            assert abs(data["x"] - 3.0) < 1e-10
            assert abs(data["y"] - 4.0) < 1e-10


class TestInstanceRef:
    """Tests for InstanceRef."""

    def test_equality(self, tmp_path):
        """Test InstanceRef equality."""
        with Schema.parse("define num as uint32", tmp_path) as schema:
            instance1 = schema.create_instance("num", 42)
            instance2 = schema.get_instance("num", 0)

            assert instance1 == instance2

    def test_hash(self, tmp_path):
        """Test InstanceRef hashing."""
        with Schema.parse("define num as uint32", tmp_path) as schema:
            instance1 = schema.create_instance("num", 42)
            instance2 = schema.get_instance("num", 0)

            # Should be usable in sets
            instances = {instance1, instance2}
            assert len(instances) == 1

    def test_repr(self, tmp_path):
        """Test InstanceRef string representation."""
        with Schema.parse("define num as uint32", tmp_path) as schema:
            instance = schema.create_instance("num", 42)

            repr_str = repr(instance)
            assert "num" in repr_str
            assert "0" in repr_str
