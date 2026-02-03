"""Tests for the TTQ query parser."""

import pytest

from typed_tables.parsing.query_lexer import QueryLexer
from typed_tables.parsing.query_parser import (
    ArrayIndex,
    ArraySlice,
    CreateAliasQuery,
    CreateInstanceQuery,
    CreateTypeQuery,
    DeleteQuery,
    DescribeQuery,
    DropDatabaseQuery,
    EvalQuery,
    FieldDef,
    FunctionCall,
    CompositeRef,
    QueryParser,
    SelectQuery,
    ShowTablesQuery,
    UseQuery,
)


class TestQueryLexer:
    """Tests for the query lexer."""

    def test_tokenize_select(self):
        """Test tokenizing a select query."""
        lexer = QueryLexer()
        lexer.build()

        tokens = lexer.tokenize("from Person select name, age")
        token_types = [t.type for t in tokens]

        assert token_types == ["FROM", "IDENTIFIER", "SELECT", "IDENTIFIER", "COMMA", "IDENTIFIER"]

    def test_tokenize_where(self):
        """Test tokenizing a where clause."""
        lexer = QueryLexer()
        lexer.build()

        tokens = lexer.tokenize('from Person where age >= 18')
        token_types = [t.type for t in tokens]

        assert token_types == ["FROM", "IDENTIFIER", "WHERE", "IDENTIFIER", "GTE", "INTEGER"]

    def test_tokenize_brackets(self):
        """Test tokenizing brackets for array types."""
        lexer = QueryLexer()
        lexer.build()

        tokens = lexer.tokenize("create type Sensor readings:int8[]")
        token_types = [t.type for t in tokens]

        assert "LBRACKET" in token_types
        assert "RBRACKET" in token_types

    def test_tokenize_array_literal(self):
        """Test tokenizing an array literal."""
        lexer = QueryLexer()
        lexer.build()

        tokens = lexer.tokenize("create Sensor(readings=[1, 2, 3])")
        token_types = [t.type for t in tokens]

        assert "LBRACKET" in token_types
        assert "RBRACKET" in token_types


class TestQueryParser:
    """Tests for the query parser."""

    def test_parse_simple_select(self):
        """Test parsing a simple select query."""
        parser = QueryParser()
        query = parser.parse("from Person")

        assert isinstance(query, SelectQuery)
        assert query.table == "Person"
        assert len(query.fields) == 1
        assert query.fields[0].name == "*"

    def test_parse_select_with_fields(self):
        """Test parsing a select query with specific fields."""
        parser = QueryParser()
        query = parser.parse("from Person select name, age")

        assert isinstance(query, SelectQuery)
        assert query.table == "Person"
        assert len(query.fields) == 2
        assert query.fields[0].name == "name"
        assert query.fields[1].name == "age"

    def test_parse_select_with_where(self):
        """Test parsing a select query with where clause."""
        parser = QueryParser()
        query = parser.parse("from Person where age >= 18")

        assert isinstance(query, SelectQuery)
        assert query.where is not None
        assert query.where.field == "age"
        assert query.where.operator == "gte"
        assert query.where.value == 18

    def test_parse_select_with_quoted_table(self):
        """Test parsing a select from a quoted table name."""
        parser = QueryParser()
        query = parser.parse('from "character[]"')

        assert isinstance(query, SelectQuery)
        assert query.table == "character[]"

    def test_parse_select_with_dot_notation(self):
        """Test parsing a select with dot notation for nested fields."""
        parser = QueryParser()
        query = parser.parse("from Person select address.city, address.state")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 2
        assert query.fields[0].name == "address.city"
        assert query.fields[0].path == ["address", "city"]
        assert query.fields[1].name == "address.state"

    def test_parse_show_tables(self):
        """Test parsing show tables query."""
        parser = QueryParser()
        query = parser.parse("show tables")

        assert isinstance(query, ShowTablesQuery)

    def test_parse_describe(self):
        """Test parsing describe query."""
        parser = QueryParser()
        query = parser.parse("describe Person")

        assert isinstance(query, DescribeQuery)
        assert query.table == "Person"

    def test_parse_describe_quoted(self):
        """Test parsing describe with quoted table name."""
        parser = QueryParser()
        query = parser.parse('describe "character[]"')

        assert isinstance(query, DescribeQuery)
        assert query.table == "character[]"

    def test_parse_use(self):
        """Test parsing use query."""
        parser = QueryParser()
        query = parser.parse("use mydb")

        assert isinstance(query, UseQuery)
        assert query.path == "mydb"

    def test_parse_use_empty(self):
        """Test parsing use query without path (exit database)."""
        parser = QueryParser()
        query = parser.parse("use")

        assert isinstance(query, UseQuery)
        assert query.path == ""

    def test_parse_drop(self):
        """Test parsing drop database query."""
        parser = QueryParser()
        query = parser.parse("drop mydb")

        assert isinstance(query, DropDatabaseQuery)
        assert query.path == "mydb"

    def test_parse_create_alias(self):
        """Test parsing create alias query."""
        parser = QueryParser()
        query = parser.parse("create alias uuid as uint128")

        assert isinstance(query, CreateAliasQuery)
        assert query.name == "uuid"
        assert query.base_type == "uint128"

    def test_parse_create_type_multiline(self):
        """Test parsing create type with multiple lines."""
        parser = QueryParser()
        query = parser.parse("create type Person\nname: string\nage: uint8")

        assert isinstance(query, CreateTypeQuery)
        assert query.name == "Person"
        assert len(query.fields) == 2
        assert query.fields[0].name == "name"
        assert query.fields[0].type_name == "string"
        assert query.fields[1].name == "age"
        assert query.fields[1].type_name == "uint8"

    def test_parse_create_type_single_line(self):
        """Test parsing create type on a single line."""
        parser = QueryParser()
        query = parser.parse("create type Point x:float32 y:float32")

        assert isinstance(query, CreateTypeQuery)
        assert query.name == "Point"
        assert len(query.fields) == 2
        assert query.fields[0].name == "x"
        assert query.fields[1].name == "y"

    def test_parse_create_type_with_array_field(self):
        """Test parsing create type with array field."""
        parser = QueryParser()
        query = parser.parse("create type Sensor readings:int8[]")

        assert isinstance(query, CreateTypeQuery)
        assert query.name == "Sensor"
        assert len(query.fields) == 1
        assert query.fields[0].name == "readings"
        assert query.fields[0].type_name == "int8[]"

    def test_parse_create_type_with_inheritance(self):
        """Test parsing create type with inheritance."""
        parser = QueryParser()
        query = parser.parse("create type Employee from Person department:string")

        assert isinstance(query, CreateTypeQuery)
        assert query.name == "Employee"
        assert query.parent == "Person"
        assert len(query.fields) == 1
        assert query.fields[0].name == "department"

    def test_parse_create_instance(self):
        """Test parsing create instance query."""
        parser = QueryParser()
        query = parser.parse('create Person(name="Alice", age=30)')

        assert isinstance(query, CreateInstanceQuery)
        assert query.type_name == "Person"
        assert len(query.fields) == 2
        assert query.fields[0].name == "name"
        assert query.fields[0].value == "Alice"
        assert query.fields[1].name == "age"
        assert query.fields[1].value == 30

    def test_parse_create_instance_with_uuid(self):
        """Test parsing create instance with uuid() function."""
        parser = QueryParser()
        query = parser.parse('create Person(id=uuid(), name="Bob")')

        assert isinstance(query, CreateInstanceQuery)
        assert isinstance(query.fields[0].value, FunctionCall)
        assert query.fields[0].value.name == "uuid"

    def test_parse_create_instance_with_composite_ref(self):
        """Test parsing create instance with composite reference."""
        parser = QueryParser()
        query = parser.parse('create Person(address=Address(0))')

        assert isinstance(query, CreateInstanceQuery)
        assert isinstance(query.fields[0].value, CompositeRef)
        assert query.fields[0].value.type_name == "Address"
        assert query.fields[0].value.index == 0

    def test_parse_create_instance_with_array_literal(self):
        """Test parsing create instance with array literal."""
        parser = QueryParser()
        query = parser.parse('create Sensor(readings=[1, 2, 3])')

        assert isinstance(query, CreateInstanceQuery)
        assert query.type_name == "Sensor"
        assert query.fields[0].name == "readings"
        assert query.fields[0].value == [1, 2, 3]

    def test_parse_create_instance_with_empty_array(self):
        """Test parsing create instance with empty array."""
        parser = QueryParser()
        query = parser.parse('create Sensor(readings=[])')

        assert isinstance(query, CreateInstanceQuery)
        assert query.fields[0].value == []

    def test_parse_create_instance_with_string_array(self):
        """Test parsing create instance with string array."""
        parser = QueryParser()
        query = parser.parse('create Tags(values=["a", "b", "c"])')

        assert isinstance(query, CreateInstanceQuery)
        assert query.fields[0].value == ["a", "b", "c"]

    def test_parse_delete_with_where(self):
        """Test parsing delete query with where clause."""
        parser = QueryParser()
        query = parser.parse('delete Person where name = "Alice"')

        assert isinstance(query, DeleteQuery)
        assert query.table == "Person"
        assert query.where is not None
        assert query.where.field == "name"
        assert query.where.value == "Alice"

    def test_parse_delete_all(self):
        """Test parsing delete query without where (delete all)."""
        parser = QueryParser()
        query = parser.parse("delete Person")

        assert isinstance(query, DeleteQuery)
        assert query.table == "Person"
        assert query.where is None

    def test_parse_delete_quoted_table(self):
        """Test parsing delete with quoted table name."""
        parser = QueryParser()
        query = parser.parse('delete "character[]"')

        assert isinstance(query, DeleteQuery)
        assert query.table == "character[]"
        assert query.where is None

    def test_parse_delete_quoted_table_with_where(self):
        """Test parsing delete with quoted table name and where clause."""
        parser = QueryParser()
        query = parser.parse('delete "int8[]" where _index = 0')

        assert isinstance(query, DeleteQuery)
        assert query.table == "int8[]"
        assert query.where is not None

    def test_parse_eval_uuid(self):
        """Test parsing eval query with uuid()."""
        parser = QueryParser()
        query = parser.parse("select uuid()")

        assert isinstance(query, EvalQuery)
        assert len(query.expressions) == 1
        expr, alias = query.expressions[0]
        assert isinstance(expr, FunctionCall)
        assert expr.name == "uuid"

    def test_parse_eval_with_alias(self):
        """Test parsing eval query with alias."""
        parser = QueryParser()
        query = parser.parse('select uuid() as "id"')

        assert isinstance(query, EvalQuery)
        expr, alias = query.expressions[0]
        assert alias == "id"

    def test_parse_aggregates(self):
        """Test parsing aggregate functions."""
        parser = QueryParser()

        query = parser.parse("from Person select count()")
        assert query.fields[0].aggregate == "count"

        query = parser.parse("from Person select average(age)")
        assert query.fields[0].aggregate == "average"
        assert query.fields[0].name == "age"

        query = parser.parse("from Person select sum(age)")
        assert query.fields[0].aggregate == "sum"

        query = parser.parse("from Person select product(age)")
        assert query.fields[0].aggregate == "product"

    def test_parse_group_by(self):
        """Test parsing group by clause."""
        parser = QueryParser()
        query = parser.parse("from Person select age, count() group by age")

        assert isinstance(query, SelectQuery)
        assert query.group_by == ["age"]

    def test_parse_sort_by(self):
        """Test parsing sort by clause."""
        parser = QueryParser()
        query = parser.parse("from Person select name, age sort by age, name")

        assert isinstance(query, SelectQuery)
        assert query.sort_by == ["age", "name"]

    def test_parse_offset_limit(self):
        """Test parsing offset and limit clauses."""
        parser = QueryParser()
        query = parser.parse("from Person offset 10 limit 5")

        assert isinstance(query, SelectQuery)
        assert query.offset == 10
        assert query.limit == 5

    def test_syntax_error(self):
        """Test syntax error handling."""
        parser = QueryParser()

        with pytest.raises(SyntaxError):
            parser.parse("from where")

    def test_parse_array_index_single(self):
        """Test parsing array indexing with single index."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings[0]")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "readings"
        assert query.fields[0].array_index is not None
        assert query.fields[0].array_index.indices == [0]

    def test_parse_array_index_slice(self):
        """Test parsing array indexing with slice."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings[0:5]")

        assert isinstance(query, SelectQuery)
        field = query.fields[0]
        assert field.name == "readings"
        assert field.array_index is not None
        assert len(field.array_index.indices) == 1
        assert isinstance(field.array_index.indices[0], ArraySlice)
        assert field.array_index.indices[0].start == 0
        assert field.array_index.indices[0].end == 5

    def test_parse_array_index_slice_open_end(self):
        """Test parsing array slice with open end."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings[5:]")

        field = query.fields[0]
        assert isinstance(field.array_index.indices[0], ArraySlice)
        assert field.array_index.indices[0].start == 5
        assert field.array_index.indices[0].end is None

    def test_parse_array_index_slice_open_start(self):
        """Test parsing array slice with open start."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings[:5]")

        field = query.fields[0]
        assert isinstance(field.array_index.indices[0], ArraySlice)
        assert field.array_index.indices[0].start is None
        assert field.array_index.indices[0].end == 5

    def test_parse_array_index_multiple(self):
        """Test parsing array indexing with multiple indices."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings[0, 2, 4]")

        field = query.fields[0]
        assert field.array_index is not None
        assert field.array_index.indices == [0, 2, 4]

    def test_parse_array_index_mixed(self):
        """Test parsing array indexing with mixed indices and slices."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings[0, 2:5, 7]")

        field = query.fields[0]
        assert field.array_index is not None
        assert len(field.array_index.indices) == 3
        assert field.array_index.indices[0] == 0
        assert isinstance(field.array_index.indices[1], ArraySlice)
        assert field.array_index.indices[1].start == 2
        assert field.array_index.indices[1].end == 5
        assert field.array_index.indices[2] == 7
