"""Tests for the TTGC config file parser."""

import pytest

from typed_tables.ttg.ttgc_parser import TTGCParser
from typed_tables.ttg.types import GraphConfig


@pytest.fixture
def parser():
    p = TTGCParser()
    p.build(debug=False, write_tables=False)
    return p


class TestTTGCEmpty:
    def test_empty_input(self, parser):
        config = parser.parse("")
        assert isinstance(config, GraphConfig)
        assert config.selectors == {}

    def test_comments_only(self, parser):
        config = parser.parse("-- just a comment\n-- another\n")
        assert config.selectors == {}


class TestTTGCSelector:
    def test_single_selector(self, parser):
        config = parser.parse('selector { composites: CompositeDef }')
        assert config.selectors == {"composites": "CompositeDef"}

    def test_multiple_selectors(self, parser):
        config = parser.parse("""
            selector {
                composites: CompositeDef,
                interfaces: InterfaceDef,
                enums: EnumDef
            }
        """)
        assert config.selectors == {
            "composites": "CompositeDef",
            "interfaces": "InterfaceDef",
            "enums": "EnumDef",
        }

    def test_trailing_comma(self, parser):
        config = parser.parse('selector { composites: CompositeDef, }')
        assert config.selectors == {"composites": "CompositeDef"}


class TestTTGCGroup:
    def test_single_group(self, parser):
        config = parser.parse('group { integers: [uint8, int8, uint16] }')
        assert config.groups == {"integers": ["uint8", "int8", "uint16"]}

    def test_nested_group(self, parser):
        config = parser.parse("""
            group {
                integers: [uint8, int8],
                primitives: [integers, bit]
            }
        """)
        assert config.groups == {
            "integers": ["uint8", "int8"],
            "primitives": ["integers", "bit"],
        }


class TestTTGCAxis:
    def test_single_axis_single_mapping(self, parser):
        config = parser.parse('axis { type: fields.type }')
        assert config.axes == {"type": ["fields.type"]}

    def test_single_axis_list_mapping(self, parser):
        config = parser.parse('axis { fields: [composites.fields, interfaces.fields] }')
        assert config.axes == {"fields": ["composites.fields", "interfaces.fields"]}

    def test_mixed_axes(self, parser):
        config = parser.parse("""
            axis {
                fields: [composites.fields, interfaces.fields, variants.fields],
                extends: [composites.parent, interfaces.extends],
                type: fields.type,
                key: dictionaries.key_type,
                value: dictionaries.value_type
            }
        """)
        assert "fields" in config.axes
        assert len(config.axes["fields"]) == 3
        assert config.axes["type"] == ["fields.type"]
        assert config.axes["key"] == ["dictionaries.key_type"]


class TestTTGCReverse:
    def test_reverse(self, parser):
        config = parser.parse("""
            reverse {
                children: extends,
                implementers: interfaces,
                typedBy: type
            }
        """)
        assert config.reverses == {
            "children": "extends",
            "implementers": "interfaces",
            "typedBy": "type",
        }


class TestTTGCAxisGroup:
    def test_axis_group(self, parser):
        config = parser.parse("""
            axis_group {
                all: [fields, extends, interfaces],
                allReverse: [children, implementers, typedBy]
            }
        """)
        assert config.axis_groups == {
            "all": ["fields", "extends", "interfaces"],
            "allReverse": ["children", "implementers", "typedBy"],
        }


class TestTTGCIdentity:
    def test_default_identity(self, parser):
        config = parser.parse('identity { default: name }')
        assert config.identity == {"default": "name"}


class TestTTGCShortcut:
    def test_shortcut_entries(self, parser):
        config = parser.parse("""
            shortcut {
                "": types + .fields{edge=.name, result=.type} + .extends + .interfaces
                "all": all
            }
        """)
        assert "" in config.shortcuts
        assert "types + .fields{edge=.name, result=.type} + .extends + .interfaces" == config.shortcuts[""]
        assert config.shortcuts["all"] == "all"


class TestTTGCMetaSchema:
    """Test parsing the actual meta-schema.ttgc file."""

    def test_parse_meta_schema(self, parser):
        import os
        meta_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "scratch", "schemas", "meta-schema.ttgc"
        )
        with open(meta_path) as f:
            text = f.read()
        config = parser.parse(text)

        # Selectors
        assert "composites" in config.selectors
        assert config.selectors["composites"] == "CompositeDef"
        assert "uint8" in config.selectors
        assert config.selectors["fraction"] == "FractionDef"
        assert len(config.selectors) == 30  # 10 user-defined + 20 primitives

        # Groups
        assert "integers" in config.groups
        assert len(config.groups["integers"]) == 12
        assert "types" in config.groups
        assert "all" in config.groups

        # Axes
        assert "fields" in config.axes
        assert len(config.axes["fields"]) == 3
        assert "extends" in config.axes
        assert config.axes["type"] == ["fields.type"]

        # Reverses
        assert config.reverses["children"] == "extends"
        assert config.reverses["typedBy"] == "type"
        assert len(config.reverses) == 12

        # Axis groups
        assert "all" in config.axis_groups
        assert "allReverse" in config.axis_groups
        assert "referencedBy" in config.axis_groups

        # Identity
        assert config.identity == {"default": "name"}

        # Shortcuts
        assert "all" in config.shortcuts
