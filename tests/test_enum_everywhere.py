"""Tests for enum values in all contexts: arrays, sets, dicts, display resolution.

Covers the 8 gaps identified in enum value handling:
- Display: _resolve_entry_field, _resolve_enum_associated_values (arrays, dicts)
- Storage: _resolve_instance_value, _create_instance (arrays, sets),
           _apply_update_fields (arrays, sets)
"""

from __future__ import annotations

import pytest

from typed_tables.query_executor import (
    CreateResult,
    DumpResult,
    QueryExecutor,
    QueryResult,
    UpdateResult,
)
from typed_tables.storage import StorageManager
from typed_tables.types import EnumTypeDefinition, EnumValue, TypeRegistry


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary database directory and executor."""
    db_dir = tmp_path / "test_db"
    db_dir.mkdir()
    registry = TypeRegistry()
    storage = StorageManager(db_dir, registry)
    executor = QueryExecutor(storage, registry)
    return executor, db_dir, registry, storage


def _exec(executor, query: str):
    """Parse and execute a TTQ query, return the result."""
    from typed_tables.parsing.query_parser import QueryParser
    parser = QueryParser()
    try:
        stmts = parser.parse(query)
        if not isinstance(stmts, list):
            stmts = [stmts]
    except SyntaxError:
        stmts = parser.parse_program(query)
    results = []
    for stmt in stmts:
        results.append(executor.execute(stmt))
    return results[-1] if len(results) == 1 else results


# ──────────────────────────────────────────────────
# Enum arrays — CREATE
# ──────────────────────────────────────────────────

class TestEnumArrayCreate:
    """Test creating instances with enum-typed array fields."""

    def test_create_c_style_enum_array(self, tmp_db):
        """Create with a C-style enum array field using shorthand."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            type Palette { name: string, colors: Color[] }
        ''')
        result = _exec(executor, 'create Palette(name="warm", colors=[.red, .green])')
        assert isinstance(result, CreateResult)
        assert "Created" in result.message

    def test_create_c_style_enum_array_qualified(self, tmp_db):
        """Create with a C-style enum array field using fully-qualified names."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            type Palette { name: string, colors: Color[] }
        ''')
        result = _exec(executor, 'create Palette(name="cool", colors=[Color.blue, Color.green])')
        assert isinstance(result, CreateResult)
        assert "Created" in result.message

    def test_create_swift_style_enum_array(self, tmp_db):
        """Create with a Swift-style enum array field."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Shape { none, circle(r: float32), rect(w: float32, h: float32) }
            type Drawing { shapes: Shape[] }
        ''')
        result = _exec(executor, 'create Drawing(shapes=[.circle(r=5.0), .rect(w=10.0, h=20.0), .none])')
        assert isinstance(result, CreateResult)
        assert "Created" in result.message

    def test_select_c_style_enum_array(self, tmp_db):
        """Select resolves C-style enum array elements to EnumValues."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            type Palette { name: string, colors: Color[] }
            create Palette(name="test", colors=[.red, .blue])
        ''')
        result = _exec(executor, 'from Palette select *')
        assert len(result.rows) == 1
        colors = result.rows[0]["colors"]
        assert len(colors) == 2
        assert all(isinstance(c, EnumValue) for c in colors)
        assert colors[0].variant_name == "red"
        assert colors[1].variant_name == "blue"

    def test_select_swift_style_enum_array(self, tmp_db):
        """Select resolves Swift-style enum array elements."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Shape { none, circle(r: float32) }
            type Drawing { shapes: Shape[] }
            create Drawing(shapes=[.circle(r=5.0), .none])
        ''')
        result = _exec(executor, 'from Drawing select *')
        shapes = result.rows[0]["shapes"]
        assert len(shapes) == 2
        assert isinstance(shapes[0], EnumValue)
        assert shapes[0].variant_name == "circle"
        assert shapes[0].fields["r"] == pytest.approx(5.0)
        assert shapes[1].variant_name == "none"


# ──────────────────────────────────────────────────
# Enum sets — CREATE
# ──────────────────────────────────────────────────

class TestEnumSetCreate:
    """Test creating instances with enum-typed set fields."""

    def test_create_c_style_enum_set(self, tmp_db):
        """Create with a C-style enum set field."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            type Favorites { colors: {Color} }
        ''')
        result = _exec(executor, 'create Favorites(colors={.red, .blue})')
        assert isinstance(result, CreateResult)
        assert "Created" in result.message

    def test_create_c_style_enum_set_qualified(self, tmp_db):
        """Create with fully-qualified enum set values."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            type Favorites { colors: {Color} }
        ''')
        result = _exec(executor, 'create Favorites(colors={Color.red, Color.green})')
        assert isinstance(result, CreateResult)
        assert "Created" in result.message

    def test_select_c_style_enum_set(self, tmp_db):
        """Select resolves C-style enum set elements."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            type Favorites { colors: {Color} }
            create Favorites(colors={.red, .blue})
        ''')
        result = _exec(executor, 'from Favorites select *')
        colors = result.rows[0]["colors"]
        assert len(colors) == 2
        assert all(isinstance(c, EnumValue) for c in colors)
        names = {c.variant_name for c in colors}
        assert names == {"red", "blue"}


# ──────────────────────────────────────────────────
# Enum arrays — UPDATE
# ──────────────────────────────────────────────────

class TestEnumArrayUpdate:
    """Test updating enum-typed array fields."""

    def test_update_c_style_enum_array(self, tmp_db):
        """Update replaces a C-style enum array."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            type Palette { name: string, colors: Color[] }
        ''')
        result = _exec(executor, '$p = create Palette(name="test", colors=[.red])')
        result = _exec(executor, 'update $p set colors=[.blue, .green]')
        assert isinstance(result, UpdateResult)

        result = _exec(executor, 'from Palette select *')
        colors = result.rows[0]["colors"]
        assert len(colors) == 2
        assert colors[0].variant_name == "blue"
        assert colors[1].variant_name == "green"

    def test_update_swift_style_enum_array(self, tmp_db):
        """Update replaces a Swift-style enum array."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Shape { none, circle(r: float32) }
            type Drawing { shapes: Shape[] }
        ''')
        result = _exec(executor, '$d = create Drawing(shapes=[.none])')
        result = _exec(executor, 'update $d set shapes=[.circle(r=10.0), .none]')
        assert isinstance(result, UpdateResult)

        result = _exec(executor, 'from Drawing select *')
        shapes = result.rows[0]["shapes"]
        assert len(shapes) == 2
        assert shapes[0].variant_name == "circle"
        assert shapes[0].fields["r"] == pytest.approx(10.0)


# ──────────────────────────────────────────────────
# Enum sets — UPDATE
# ──────────────────────────────────────────────────

class TestEnumSetUpdate:
    """Test updating enum-typed set fields."""

    def test_update_c_style_enum_set(self, tmp_db):
        """Update replaces a C-style enum set."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            type Favorites { colors: {Color} }
        ''')
        result = _exec(executor, '$f = create Favorites(colors={.red})')
        result = _exec(executor, 'update $f set colors={.blue, .green}')
        assert isinstance(result, UpdateResult)

        result = _exec(executor, 'from Favorites select *')
        colors = result.rows[0]["colors"]
        assert len(colors) == 2
        names = {c.variant_name for c in colors}
        assert names == {"blue", "green"}


# ──────────────────────────────────────────────────
# Enum values in dict values — display resolution
# ──────────────────────────────────────────────────

class TestEnumDictValueDisplay:
    """Test that enum values in dictionary value positions display correctly."""

    def test_dict_with_c_style_enum_values(self, tmp_db):
        """Dict with enum values resolves correctly on select."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Priority { low, medium, high }
            type Tasks { priorities: {string: Priority} }
            create Tasks(priorities={"task1": .high, "task2": .low})
        ''')
        result = _exec(executor, 'from Tasks select *')
        priorities = result.rows[0]["priorities"]
        assert isinstance(priorities, dict)
        assert isinstance(priorities["task1"], EnumValue)
        assert priorities["task1"].variant_name == "high"
        assert priorities["task2"].variant_name == "low"

    def test_dict_with_swift_style_enum_values(self, tmp_db):
        """Dict with Swift-style enum values resolves correctly."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Shape { none, circle(r: float32) }
            type Config { shapes: {string: Shape} }
            create Config(shapes={"bg": .circle(r=5.0), "empty": .none})
        ''')
        result = _exec(executor, 'from Config select *')
        shapes = result.rows[0]["shapes"]
        assert isinstance(shapes, dict)
        assert isinstance(shapes["bg"], EnumValue)
        assert shapes["bg"].variant_name == "circle"
        assert shapes["bg"].fields["r"] == pytest.approx(5.0)
        assert shapes["empty"].variant_name == "none"


# ──────────────────────────────────────────────────
# Nested enum display resolution (associated values)
# ──────────────────────────────────────────────────

class TestNestedEnumDisplay:
    """Test that nested enum values in variant fields display correctly."""

    def test_enum_field_in_variant_displays_resolved(self, tmp_db):
        """Enum-typed field inside a variant should display as EnumValue, not raw tuple."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            enum Styled { plain, colored(color: Color) }
            type Item { style: Styled }
            create Item(style=.colored(color=.green))
        ''')
        result = _exec(executor, 'from Item select *')
        style = result.rows[0]["style"]
        assert isinstance(style, EnumValue)
        assert style.variant_name == "colored"
        assert isinstance(style.fields["color"], EnumValue)
        assert style.fields["color"].variant_name == "green"

    def test_enum_array_in_variant_displays_resolved(self, tmp_db):
        """Enum[] field inside a variant should display as list of EnumValues."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            enum Pattern { solid, striped(colors: Color[]) }
            type Design { pattern: Pattern }
            create Design(pattern=.striped(colors=[.red, .blue]))
        ''')
        result = _exec(executor, 'from Design select *')
        pattern = result.rows[0]["pattern"]
        assert isinstance(pattern, EnumValue)
        assert pattern.variant_name == "striped"
        colors = pattern.fields["colors"]
        assert len(colors) == 2
        assert all(isinstance(c, EnumValue) for c in colors)
        assert colors[0].variant_name == "red"
        assert colors[1].variant_name == "blue"

    def test_dict_field_in_variant_displays_resolved(self, tmp_db):
        """Dict field inside a variant should display as resolved dict."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum JsonValue {
                null_val,
                str_val(value: string),
                object(entries: {string: JsonValue})
            }
            type Doc { root: JsonValue }
            create Doc(root=.object(entries={"name": .str_val(value="Alice")}))
        ''')
        result = _exec(executor, 'from Doc select *')
        root = result.rows[0]["root"]
        assert isinstance(root, EnumValue)
        assert root.variant_name == "object"
        entries = root.fields["entries"]
        assert isinstance(entries, dict)
        assert isinstance(entries["name"], EnumValue)
        assert entries["name"].variant_name == "str_val"
        assert entries["name"].fields["value"] == "Alice"

    def test_self_ref_enum_array_displays_resolved(self, tmp_db):
        """Self-referential enum array field should display correctly."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum JV {
                null_val,
                number(value: float64),
                array(elements: JV[])
            }
            type Doc { root: JV }
            create Doc(root=.array(elements=[.number(value=1.0), .number(value=2.0)]))
        ''')
        result = _exec(executor, 'from Doc select *')
        root = result.rows[0]["root"]
        assert isinstance(root, EnumValue)
        assert root.variant_name == "array"
        elements = root.fields["elements"]
        assert len(elements) == 2
        assert all(isinstance(e, EnumValue) for e in elements)
        assert elements[0].variant_name == "number"
        assert elements[0].fields["value"] == 1.0
        assert elements[1].fields["value"] == 2.0

    def test_string_array_in_variant_displays_as_strings(self, tmp_db):
        """String[] field inside a variant should display as list of strings."""
        executor, *_ = tmp_db
        _exec(executor, '''
            enum Data { empty, tagged(tags: string[]) }
            type Item { data: Data }
            create Item(data=.tagged(tags=["foo", "bar"]))
        ''')
        result = _exec(executor, 'from Item select *')
        data = result.rows[0]["data"]
        assert isinstance(data, EnumValue)
        assert data.variant_name == "tagged"
        tags = data.fields["tags"]
        assert tags == ["foo", "bar"]


# ──────────────────────────────────────────────────
# Dump roundtrip with enum arrays/sets
# ──────────────────────────────────────────────────

class TestEnumCollectionDumpRoundtrip:
    """Test dump/restore of enum arrays and sets."""

    def test_dump_enum_array_roundtrip(self, tmp_db):
        """Dump and restore of C-style enum arrays."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            type Palette { name: string, colors: Color[] }
            create Palette(name="warm", colors=[.red, .green])
        ''')
        result = _exec(executor, 'dump')
        dump_text = result.script

        db_dir2 = db_dir.parent / "test_db2"
        db_dir2.mkdir()
        registry2 = TypeRegistry()
        storage2 = StorageManager(db_dir2, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        _exec(executor2, dump_text)
        result2 = _exec(executor2, 'from Palette select *')
        assert len(result2.rows) == 1
        colors = result2.rows[0]["colors"]
        assert len(colors) == 2
        assert colors[0].variant_name == "red"

    def test_dump_swift_enum_array_roundtrip(self, tmp_db):
        """Dump and restore of Swift-style enum arrays."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum Shape { none, circle(r: float32) }
            type Drawing { shapes: Shape[] }
            create Drawing(shapes=[.circle(r=5.0), .none])
        ''')
        result = _exec(executor, 'dump')
        dump_text = result.script

        db_dir2 = db_dir.parent / "test_db2"
        db_dir2.mkdir()
        registry2 = TypeRegistry()
        storage2 = StorageManager(db_dir2, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        _exec(executor2, dump_text)
        result2 = _exec(executor2, 'from Drawing select *')
        shapes = result2.rows[0]["shapes"]
        assert len(shapes) == 2
        assert shapes[0].variant_name == "circle"

    def test_dump_enum_set_roundtrip(self, tmp_db):
        """Dump and restore of C-style enum sets."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum Color { red, green, blue }
            type Favorites { colors: {Color} }
            create Favorites(colors={.red, .blue})
        ''')
        result = _exec(executor, 'dump')
        dump_text = result.script

        db_dir2 = db_dir.parent / "test_db2"
        db_dir2.mkdir()
        registry2 = TypeRegistry()
        storage2 = StorageManager(db_dir2, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        _exec(executor2, dump_text)
        result2 = _exec(executor2, 'from Favorites select *')
        colors = result2.rows[0]["colors"]
        assert len(colors) == 2

    def test_dump_deep_nested_json_roundtrip(self, tmp_db):
        """Dump and restore of deeply nested JSON-like enum."""
        executor, db_dir, registry, storage = tmp_db
        _exec(executor, '''
            enum JsonValue {
                null_val,
                number(value: float64),
                str_val(value: string),
                array(elements: JsonValue[]),
                object(entries: {string: JsonValue})
            }
            type Doc { name: string, root: JsonValue }
            create Doc(name="nested", root=.object(entries={
                "nums": .array(elements=[.number(value=1.0), .number(value=2.0)]),
                "label": .str_val(value="test")
            }))
        ''')
        result = _exec(executor, 'dump')
        dump_text = result.script

        db_dir2 = db_dir.parent / "test_db2"
        db_dir2.mkdir()
        registry2 = TypeRegistry()
        storage2 = StorageManager(db_dir2, registry2)
        executor2 = QueryExecutor(storage2, registry2)

        _exec(executor2, dump_text)
        result2 = _exec(executor2, 'from Doc select *')
        assert len(result2.rows) == 1
        root = result2.rows[0]["root"]
        assert root.variant_name == "object"
