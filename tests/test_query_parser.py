"""Tests for the TTQ query parser."""

import pytest

from typed_tables.parsing.query_lexer import QueryLexer
from typed_tables.parsing.query_parser import (
    ArrayIndex,
    ArraySlice,
    BinaryExpr,
    CollectQuery,
    CollectSource,
    Condition,
    CreateAliasQuery,
    CreateInstanceQuery,
    CreateTypeQuery,
    DeleteQuery,
    DescribeQuery,
    DropDatabaseQuery,
    DumpItem,
    DumpQuery,
    EnumValueExpr,
    EvalQuery,
    FieldDef,
    FieldValue,
    FunctionCall,
    CompositeRef,
    InlineInstance,
    MethodCall,
    NullValue,
    QueryParser,
    SelectField,
    SelectQuery,
    ShowTypesQuery,
    SortKeyExpr,
    TagReference,
    UnaryExpr,
    UpdateQuery,
    UseQuery,
    VariableAssignmentQuery,
    VariableReference,
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

        tokens = lexer.tokenize("type Sensor { readings: int8[] }")
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

    def test_tokenize_semicolon(self):
        """Test that semicolons are tokenized."""
        lexer = QueryLexer()
        lexer.build()

        tokens = lexer.tokenize("from Person;")
        token_types = [t.type for t in tokens]

        assert token_types == ["FROM", "IDENTIFIER", "SEMICOLON"]

    def test_newlines_ignored(self):
        """Test that newlines are not returned as tokens."""
        lexer = QueryLexer()
        lexer.build()

        tokens = lexer.tokenize("from Person\nselect name")
        token_types = [t.type for t in tokens]

        assert "NEWLINE" not in token_types
        assert token_types == ["FROM", "IDENTIFIER", "SELECT", "IDENTIFIER"]


class TestQueryParser:
    """Tests for the query parser."""

    def test_parse_simple_select(self):
        """Test parsing a simple select query."""
        parser = QueryParser()
        query = parser.parse("from Person select *")

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
        query = parser.parse("from Person select * where age >= 18")

        assert isinstance(query, SelectQuery)
        assert query.where is not None
        assert query.where.field == "age"
        assert query.where.operator == "gte"
        assert query.where.value == 18

    def test_parse_select_with_quoted_table(self):
        """Test parsing a select from a quoted table name."""
        parser = QueryParser()
        query = parser.parse('from "character[]" select *')

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

    def test_parse_show_types(self):
        """Test parsing show types query."""
        parser = QueryParser()
        query = parser.parse("show types")

        assert isinstance(query, ShowTypesQuery)

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
        """Test parsing alias query."""
        parser = QueryParser()
        query = parser.parse("alias uuid as uint128")

        assert isinstance(query, CreateAliasQuery)
        assert query.name == "uuid"
        assert query.base_type == "uint128"

    def test_parse_create_type_multiline(self):
        """Test parsing type with braces and comma-separated fields."""
        parser = QueryParser()
        query = parser.parse("type Person { name: string, age: uint8 }")

        assert isinstance(query, CreateTypeQuery)
        assert query.name == "Person"
        assert len(query.fields) == 2
        assert query.fields[0].name == "name"
        assert query.fields[0].type_name == "string"
        assert query.fields[1].name == "age"
        assert query.fields[1].type_name == "uint8"

    def test_parse_create_type_single_line(self):
        """Test parsing type on a single line."""
        parser = QueryParser()
        query = parser.parse("type Point { x: float32, y: float32 }")

        assert isinstance(query, CreateTypeQuery)
        assert query.name == "Point"
        assert len(query.fields) == 2
        assert query.fields[0].name == "x"
        assert query.fields[1].name == "y"

    def test_parse_create_type_with_array_field(self):
        """Test parsing type with array field."""
        parser = QueryParser()
        query = parser.parse("type Sensor { readings: int8[] }")

        assert isinstance(query, CreateTypeQuery)
        assert query.name == "Sensor"
        assert len(query.fields) == 1
        assert query.fields[0].name == "readings"
        assert query.fields[0].type_name == "int8[]"

    def test_parse_create_type_with_inheritance(self):
        """Test parsing type with inheritance."""
        parser = QueryParser()
        query = parser.parse("type Employee from Person { department: string }")

        assert isinstance(query, CreateTypeQuery)
        assert query.name == "Employee"
        assert query.parents == ["Person"]
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
        query = parser.parse("uuid()")

        assert isinstance(query, EvalQuery)
        assert len(query.expressions) == 1
        expr, alias = query.expressions[0]
        assert isinstance(expr, FunctionCall)
        assert expr.name == "uuid"

    def test_parse_eval_with_alias(self):
        """Test parsing eval query with alias."""
        parser = QueryParser()
        query = parser.parse('uuid() as "id"')

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
        query = parser.parse("from Person select * offset 10 limit 5")

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

    def test_parse_with_semicolon(self):
        """Test parsing queries terminated with a semicolon."""
        parser = QueryParser()

        query = parser.parse("from Person select *;")
        assert isinstance(query, SelectQuery)
        assert query.table == "Person"

        query = parser.parse("show types;")
        assert isinstance(query, ShowTypesQuery)

        query = parser.parse("describe Person;")
        assert isinstance(query, DescribeQuery)
        assert query.table == "Person"

        query = parser.parse("use mydb;")
        assert isinstance(query, UseQuery)
        assert query.path == "mydb"

    def test_parse_multiline_no_newline_token(self):
        """Test that newlines are treated as whitespace (no NEWLINE token)."""
        parser = QueryParser()

        # Multi-line select query with newlines between clauses
        query = parser.parse("from Person\nselect name, age\nwhere age >= 18")
        assert isinstance(query, SelectQuery)
        assert query.table == "Person"
        assert len(query.fields) == 2
        assert query.where is not None

    def test_parse_create_type_freeform(self):
        """Test that type works with free-form whitespace."""
        parser = QueryParser()
        query = parser.parse("type Person {\n  name: string,\n  age: uint8\n}")
        assert isinstance(query, CreateTypeQuery)
        assert query.name == "Person"
        assert len(query.fields) == 2

    def test_parse_inline_instance(self):
        """Test parsing create instance with inline nested instance."""
        parser = QueryParser()
        query = parser.parse('create Person(address=Address(street="123 Main", city="Springfield"))')

        assert isinstance(query, CreateInstanceQuery)
        assert query.type_name == "Person"
        assert len(query.fields) == 1
        assert query.fields[0].name == "address"

        inline = query.fields[0].value
        assert isinstance(inline, InlineInstance)
        assert inline.type_name == "Address"
        assert len(inline.fields) == 2
        assert inline.fields[0].name == "street"
        assert inline.fields[0].value == "123 Main"
        assert inline.fields[1].name == "city"
        assert inline.fields[1].value == "Springfield"

    def test_parse_nested_inline_instance(self):
        """Test parsing deeply nested inline instances."""
        parser = QueryParser()
        query = parser.parse(
            'create Person(address=Address(location=Location(lat=1.0, lng=2.0)))'
        )

        assert isinstance(query, CreateInstanceQuery)
        inline_addr = query.fields[0].value
        assert isinstance(inline_addr, InlineInstance)
        assert inline_addr.type_name == "Address"

        inline_loc = inline_addr.fields[0].value
        assert isinstance(inline_loc, InlineInstance)
        assert inline_loc.type_name == "Location"
        assert inline_loc.fields[0].name == "lat"
        assert inline_loc.fields[0].value == 1.0
        assert inline_loc.fields[1].name == "lng"
        assert inline_loc.fields[1].value == 2.0

    def test_parse_post_index_dot(self):
        """Test parsing post-index dot notation like arr[0].name."""
        parser = QueryParser()
        query = parser.parse("from Team select members[0].name")

        assert isinstance(query, SelectQuery)
        field = query.fields[0]
        assert field.name == "members"
        assert field.array_index is not None
        assert field.array_index.indices == [0]
        assert field.post_path == ["name"]

    def test_parse_post_index_dot_deep(self):
        """Test parsing post-index dot notation with deep path."""
        parser = QueryParser()
        query = parser.parse("from Team select members[0].address.city")

        assert isinstance(query, SelectQuery)
        field = query.fields[0]
        assert field.name == "members"
        assert field.array_index is not None
        assert field.array_index.indices == [0]
        assert field.post_path == ["address", "city"]

    def test_parse_dump_all(self):
        """Test parsing dump query without table name."""
        parser = QueryParser()
        query = parser.parse("dump")

        assert isinstance(query, DumpQuery)
        assert query.table is None
        assert query.output_file is None

    def test_parse_dump_table(self):
        """Test parsing dump query with table name."""
        parser = QueryParser()
        query = parser.parse("dump Person")

        assert isinstance(query, DumpQuery)
        assert query.table == "Person"
        assert query.output_file is None

    def test_parse_dump_to_file(self):
        """Test parsing dump query with output file."""
        parser = QueryParser()
        query = parser.parse('dump to "backup.ttq"')

        assert isinstance(query, DumpQuery)
        assert query.table is None
        assert query.output_file == "backup.ttq"

    def test_parse_dump_table_to_file(self):
        """Test parsing dump query with table name and output file."""
        parser = QueryParser()
        query = parser.parse('dump Person to "person.ttq"')

        assert isinstance(query, DumpQuery)
        assert query.table == "Person"
        assert query.output_file == "person.ttq"

    def test_parse_dump_quoted_table_to_file(self):
        """Test parsing dump query with quoted table name and output file."""
        parser = QueryParser()
        query = parser.parse('dump "character[]" to "chars.ttq"')

        assert isinstance(query, DumpQuery)
        assert query.table == "character[]"
        assert query.output_file == "chars.ttq"

    def test_parse_array_element_inline_instance(self):
        """Test parsing inline instances as array elements."""
        parser = QueryParser()
        query = parser.parse('create Team(employees=[Employee(name="Alice")])')

        assert isinstance(query, CreateInstanceQuery)
        assert query.type_name == "Team"
        arr = query.fields[0].value
        assert isinstance(arr, list)
        assert len(arr) == 1
        assert isinstance(arr[0], InlineInstance)
        assert arr[0].type_name == "Employee"
        assert arr[0].fields[0].name == "name"
        assert arr[0].fields[0].value == "Alice"

    def test_parse_variable_assignment(self):
        """Test parsing variable assignment: $addr = create Address(street="123")."""
        parser = QueryParser()
        query = parser.parse('$addr = create Address(street="123 Main")')

        assert isinstance(query, VariableAssignmentQuery)
        assert query.var_name == "addr"
        assert query.create_query.type_name == "Address"
        assert len(query.create_query.fields) == 1
        assert query.create_query.fields[0].name == "street"
        assert query.create_query.fields[0].value == "123 Main"

    def test_parse_variable_reference_in_field(self):
        """Test parsing variable reference in field value: create Person(address=$addr)."""
        parser = QueryParser()
        query = parser.parse("create Person(address=$addr)")

        assert isinstance(query, CreateInstanceQuery)
        assert query.type_name == "Person"
        assert len(query.fields) == 1
        assert query.fields[0].name == "address"
        assert isinstance(query.fields[0].value, VariableReference)
        assert query.fields[0].value.var_name == "addr"

    def test_parse_variable_reference_in_array(self):
        """Test parsing variable references in array: create Team(members=[$e1, $e2])."""
        parser = QueryParser()
        query = parser.parse("create Team(members=[$e1, $e2])")

        assert isinstance(query, CreateInstanceQuery)
        assert query.type_name == "Team"
        arr = query.fields[0].value
        assert isinstance(arr, list)
        assert len(arr) == 2
        assert isinstance(arr[0], VariableReference)
        assert arr[0].var_name == "e1"
        assert isinstance(arr[1], VariableReference)
        assert arr[1].var_name == "e2"

    def test_parse_variable_assignment_empty(self):
        """Test parsing variable assignment with empty field list: $x = create Empty()."""
        parser = QueryParser()
        query = parser.parse("$x = create Empty()")

        assert isinstance(query, VariableAssignmentQuery)
        assert query.var_name == "x"
        assert query.create_query.type_name == "Empty"
        assert len(query.create_query.fields) == 0

    def test_tokenize_variable(self):
        """Test that the lexer tokenizes $var correctly."""
        lexer = QueryLexer()
        lexer.build()

        tokens = lexer.tokenize("$addr = create Address()")
        token_types = [t.type for t in tokens]
        assert token_types[0] == "VARIABLE"
        assert tokens[0].value == "addr"

    def test_parse_collect_basic(self):
        """Test parsing collect query with where clause."""
        parser = QueryParser()
        query = parser.parse("$seniors = collect Person where age >= 65")

        assert isinstance(query, CollectQuery)
        assert query.var_name == "seniors"
        assert len(query.sources) == 1
        assert query.sources[0].table == "Person"
        assert query.sources[0].where is not None
        assert query.sources[0].where.field == "age"
        assert query.sources[0].where.operator == "gte"
        assert query.sources[0].where.value == 65

    def test_parse_collect_no_where(self):
        """Test parsing bare collect without any clauses."""
        parser = QueryParser()
        query = parser.parse("$all = collect Person")

        assert isinstance(query, CollectQuery)
        assert query.var_name == "all"
        assert len(query.sources) == 1
        assert query.sources[0].table == "Person"
        assert query.sources[0].where is None
        assert query.group_by == []
        assert query.sort_by == []
        assert query.offset == 0
        assert query.limit is None

    def test_parse_collect_with_sort_limit(self):
        """Test parsing collect with sort by and limit."""
        parser = QueryParser()
        query = parser.parse("$top10 = collect Score sort by value limit 10")

        assert isinstance(query, CollectQuery)
        assert query.var_name == "top10"
        assert len(query.sources) == 1
        assert query.sources[0].table == "Score"
        assert query.sort_by == ["value"]
        assert query.limit == 10

    def test_parse_collect_with_group_by(self):
        """Test parsing collect with group by."""
        parser = QueryParser()
        query = parser.parse("$grouped = collect Person group by age")

        assert isinstance(query, CollectQuery)
        assert query.var_name == "grouped"
        assert len(query.sources) == 1
        assert query.sources[0].table == "Person"
        assert query.group_by == ["age"]

    def test_parse_collect_quoted_table(self):
        """Test parsing collect with quoted table name."""
        parser = QueryParser()
        query = parser.parse('$items = collect "character[]"')

        assert isinstance(query, CollectQuery)
        assert query.var_name == "items"
        assert len(query.sources) == 1
        assert query.sources[0].table == "character[]"

    def test_parse_collect_with_semicolon(self):
        """Test parsing collect terminated with semicolon."""
        parser = QueryParser()
        query = parser.parse("$all = collect Person;")

        assert isinstance(query, CollectQuery)
        assert query.var_name == "all"
        assert len(query.sources) == 1
        assert query.sources[0].table == "Person"

    def test_parse_dump_variable(self):
        """Test parsing dump $var."""
        parser = QueryParser()
        query = parser.parse("dump $myvar")

        assert isinstance(query, DumpQuery)
        assert query.variable == "myvar"
        assert query.table is None
        assert query.output_file is None

    def test_parse_dump_variable_to_file(self):
        """Test parsing dump $var to file."""
        parser = QueryParser()
        query = parser.parse('dump $myvar to "output.ttq"')

        assert isinstance(query, DumpQuery)
        assert query.variable == "myvar"
        assert query.output_file == "output.ttq"
        assert query.table is None

    # --- from $var tests ---

    def test_parse_from_variable(self):
        """Test parsing from $var select ..."""
        parser = QueryParser()
        query = parser.parse("from $seniors select name, age")

        assert isinstance(query, SelectQuery)
        assert query.source_var == "seniors"
        assert query.table is None
        assert len(query.fields) == 2
        assert query.fields[0].name == "name"
        assert query.fields[1].name == "age"

    def test_parse_from_variable_with_where(self):
        """Test parsing from $var select * where ..."""
        parser = QueryParser()
        query = parser.parse("from $seniors select * where age > 70")

        assert isinstance(query, SelectQuery)
        assert query.source_var == "seniors"
        assert query.table is None
        assert query.where is not None
        assert query.where.field == "age"
        assert query.where.operator == "gt"
        assert query.where.value == 70

    # --- multi-source collect tests ---

    def test_parse_collect_multi_source(self):
        """Test parsing collect with multiple sources."""
        parser = QueryParser()
        query = parser.parse("$combined = collect Person where age >= 65, Person where age = 30")

        assert isinstance(query, CollectQuery)
        assert query.var_name == "combined"
        assert len(query.sources) == 2
        assert query.sources[0].table == "Person"
        assert query.sources[0].where is not None
        assert query.sources[0].where.field == "age"
        assert query.sources[0].where.operator == "gte"
        assert query.sources[0].where.value == 65
        assert query.sources[1].table == "Person"
        assert query.sources[1].where is not None
        assert query.sources[1].where.field == "age"
        assert query.sources[1].where.operator == "eq"
        assert query.sources[1].where.value == 30

    def test_parse_collect_variable_source(self):
        """Test parsing collect from a variable source."""
        parser = QueryParser()
        query = parser.parse('$subset = collect $seniors where city = "Springfield"')

        assert isinstance(query, CollectQuery)
        assert query.var_name == "subset"
        assert len(query.sources) == 1
        assert query.sources[0].variable == "seniors"
        assert query.sources[0].table is None
        assert query.sources[0].where is not None
        assert query.sources[0].where.field == "city"

    def test_parse_collect_mixed_sources(self):
        """Test parsing collect with mixed table and variable sources."""
        parser = QueryParser()
        query = parser.parse("$union = collect $seniors, Person where age = 30")

        assert isinstance(query, CollectQuery)
        assert query.var_name == "union"
        assert len(query.sources) == 2
        assert query.sources[0].variable == "seniors"
        assert query.sources[0].where is None
        assert query.sources[1].table == "Person"
        assert query.sources[1].where is not None

    def test_parse_collect_bare_variables(self):
        """Test parsing collect with bare variable sources."""
        parser = QueryParser()
        query = parser.parse("$union = collect $seniors, $young")

        assert isinstance(query, CollectQuery)
        assert query.var_name == "union"
        assert len(query.sources) == 2
        assert query.sources[0].variable == "seniors"
        assert query.sources[1].variable == "young"

    def test_parse_collect_single_source_compat(self):
        """Test that single-source collect still works."""
        parser = QueryParser()
        query = parser.parse("$all = collect Person")

        assert isinstance(query, CollectQuery)
        assert len(query.sources) == 1
        assert query.sources[0].table == "Person"
        assert query.sources[0].where is None

    def test_parse_collect_with_post_union_clauses(self):
        """Test parsing collect with post-union sort/limit."""
        parser = QueryParser()
        query = parser.parse("$top = collect Person where age >= 65, Person where age = 30 sort by age limit 10")

        assert isinstance(query, CollectQuery)
        assert len(query.sources) == 2
        assert query.sort_by == ["age"]
        assert query.limit == 10

    # --- dump list tests ---

    def test_parse_dump_list(self):
        """Test parsing dump list."""
        parser = QueryParser()
        query = parser.parse("dump [Person, $seniors, Employee]")

        assert isinstance(query, DumpQuery)
        assert query.items is not None
        assert len(query.items) == 3
        assert query.items[0].table == "Person"
        assert query.items[1].variable == "seniors"
        assert query.items[2].table == "Employee"
        assert query.output_file is None

    def test_parse_dump_list_to_file(self):
        """Test parsing dump list with output file."""
        parser = QueryParser()
        query = parser.parse('dump [$combined] to "backup.ttq"')

        assert isinstance(query, DumpQuery)
        assert query.items is not None
        assert len(query.items) == 1
        assert query.items[0].variable == "combined"
        assert query.output_file == "backup.ttq"

    def test_parse_dump_list_single_item(self):
        """Test parsing dump list with single item."""
        parser = QueryParser()
        query = parser.parse("dump [Person]")

        assert isinstance(query, DumpQuery)
        assert query.items is not None
        assert len(query.items) == 1
        assert query.items[0].table == "Person"

    def test_parse_null_value(self):
        """Test parsing create instance with null field value."""
        parser = QueryParser()
        query = parser.parse("create Node(value=1, next=null)")

        assert isinstance(query, CreateInstanceQuery)
        assert query.type_name == "Node"
        assert len(query.fields) == 2
        assert query.fields[0].name == "value"
        assert query.fields[0].value == 1
        assert query.fields[1].name == "next"
        assert isinstance(query.fields[1].value, NullValue)

    def test_parse_null_in_array(self):
        """Test parsing null as an array element."""
        parser = QueryParser()
        query = parser.parse("create Foo(items=[1, null, 3])")

        assert isinstance(query, CreateInstanceQuery)
        arr = query.fields[0].value
        assert isinstance(arr, list)
        assert len(arr) == 3
        assert arr[0] == 1
        assert isinstance(arr[1], NullValue)
        assert arr[2] == 3

    def test_parse_update_variable(self):
        """Test parsing update $var set field=value."""
        parser = QueryParser()
        query = parser.parse("update $n1 set next=$n2")

        assert isinstance(query, UpdateQuery)
        assert query.var_name == "n1"
        assert query.type_name == ""
        assert query.index is None
        assert len(query.fields) == 1
        assert query.fields[0].name == "next"
        assert isinstance(query.fields[0].value, VariableReference)
        assert query.fields[0].value.var_name == "n2"

    def test_parse_update_composite_ref(self):
        """Test parsing update Type(index) set field=value."""
        parser = QueryParser()
        query = parser.parse("update Node(0) set next=Node(1)")

        assert isinstance(query, UpdateQuery)
        assert query.type_name == "Node"
        assert query.index == 0
        assert query.var_name is None
        assert len(query.fields) == 1
        assert query.fields[0].name == "next"
        assert isinstance(query.fields[0].value, CompositeRef)
        assert query.fields[0].value.type_name == "Node"
        assert query.fields[0].value.index == 1

    def test_parse_update_null(self):
        """Test parsing update with null value."""
        parser = QueryParser()
        query = parser.parse("update $n set next=null")

        assert isinstance(query, UpdateQuery)
        assert query.var_name == "n"
        assert len(query.fields) == 1
        assert query.fields[0].name == "next"
        assert isinstance(query.fields[0].value, NullValue)

    def test_parse_update_multiple_fields(self):
        """Test parsing update with multiple fields."""
        parser = QueryParser()
        query = parser.parse('update $n set a=1, b="hello"')

        assert isinstance(query, UpdateQuery)
        assert query.var_name == "n"
        assert len(query.fields) == 2
        assert query.fields[0].name == "a"
        assert query.fields[0].value == 1
        assert query.fields[1].name == "b"
        assert query.fields[1].value == "hello"

    def test_parse_dump_pretty(self):
        """Test parsing dump pretty."""
        parser = QueryParser()
        query = parser.parse("dump pretty")

        assert isinstance(query, DumpQuery)
        assert query.pretty is True
        assert query.table is None
        assert query.output_file is None

    def test_parse_dump_pretty_table(self):
        """Test parsing dump pretty with table name."""
        parser = QueryParser()
        query = parser.parse("dump pretty Person")

        assert isinstance(query, DumpQuery)
        assert query.pretty is True
        assert query.table == "Person"

    def test_parse_dump_pretty_to_file(self):
        """Test parsing dump pretty with output file."""
        parser = QueryParser()
        query = parser.parse('dump pretty to "f.ttq"')

        assert isinstance(query, DumpQuery)
        assert query.pretty is True
        assert query.table is None
        assert query.output_file == "f.ttq"

    def test_parse_dump_pretty_table_to_file(self):
        """Test parsing dump pretty with table and output file."""
        parser = QueryParser()
        query = parser.parse('dump pretty Person to "p.ttq"')

        assert isinstance(query, DumpQuery)
        assert query.pretty is True
        assert query.table == "Person"
        assert query.output_file == "p.ttq"

    def test_parse_dump_not_pretty_by_default(self):
        """Test that regular dump has pretty=False."""
        parser = QueryParser()
        query = parser.parse("dump")

        assert isinstance(query, DumpQuery)
        assert query.pretty is False

    def test_parse_dump_table_not_pretty_by_default(self):
        """Test that regular dump table has pretty=False."""
        parser = QueryParser()
        query = parser.parse("dump Person")

        assert isinstance(query, DumpQuery)
        assert query.pretty is False

    def test_parse_dump_pretty_variable(self):
        """Test parsing dump pretty with variable."""
        parser = QueryParser()
        query = parser.parse("dump pretty $var")

        assert isinstance(query, DumpQuery)
        assert query.pretty is True
        assert query.variable == "var"

    def test_parse_dump_pretty_list(self):
        """Test parsing dump pretty with list."""
        parser = QueryParser()
        query = parser.parse("dump pretty [Person, $var]")

        assert isinstance(query, DumpQuery)
        assert query.pretty is True
        assert query.items is not None
        assert len(query.items) == 2

    def test_parse_create_with_tag(self):
        """Test parsing create with tag declaration."""
        parser = QueryParser()
        query = parser.parse('create Node(tag(X), name="A")')

        assert isinstance(query, CreateInstanceQuery)
        assert query.type_name == "Node"
        assert query.tag == "X"
        assert len(query.fields) == 1
        assert query.fields[0].name == "name"
        assert query.fields[0].value == "A"

    def test_parse_tag_reference_in_field(self):
        """Test parsing tag reference as field value."""
        parser = QueryParser()
        query = parser.parse('create Node(name="A", child=X)')

        assert isinstance(query, CreateInstanceQuery)
        assert query.type_name == "Node"
        assert len(query.fields) == 2
        assert query.fields[0].name == "name"
        assert query.fields[1].name == "child"
        assert isinstance(query.fields[1].value, TagReference)
        assert query.fields[1].value.name == "X"

    def test_parse_nested_with_tag_reference(self):
        """Test parsing nested inline instance with tag back-reference."""
        parser = QueryParser()
        query = parser.parse('create Node(tag(A), name="A", child=Node(name="B", child=A))')

        assert isinstance(query, CreateInstanceQuery)
        assert query.tag == "A"
        assert len(query.fields) == 2
        # The child field is an InlineInstance
        child = query.fields[1].value
        assert isinstance(child, InlineInstance)
        assert child.type_name == "Node"
        # Child's child field is a TagReference
        child_child = child.fields[1].value
        assert isinstance(child_child, TagReference)
        assert child_child.name == "A"

    def test_parse_variable_assignment_with_tag(self):
        """Test parsing variable assignment with tag."""
        parser = QueryParser()
        query = parser.parse('$n = create Node(tag(X), name="test")')

        assert isinstance(query, VariableAssignmentQuery)
        assert query.var_name == "n"
        assert query.create_query.tag == "X"
        assert query.create_query.type_name == "Node"

    def test_parse_inline_instance_with_tag(self):
        """Test parsing inline instance with tag in value position."""
        parser = QueryParser()
        query = parser.parse('create Parent(child=Node(tag(Y), name="B"))')

        assert isinstance(query, CreateInstanceQuery)
        assert query.type_name == "Parent"
        child = query.fields[0].value
        assert isinstance(child, InlineInstance)
        assert child.tag == "Y"
        assert child.type_name == "Node"

    def test_parse_create_without_tag_unchanged(self):
        """Test that create without tag still works (regression test)."""
        parser = QueryParser()
        query = parser.parse('create Node(name="A", value=1)')

        assert isinstance(query, CreateInstanceQuery)
        assert query.tag is None
        assert len(query.fields) == 2

    def test_parse_tag_reference_in_array(self):
        """Test parsing tag reference as array element."""
        parser = QueryParser()
        query = parser.parse('create Node(tag(A), children=[Node(name="B"), A])')

        assert isinstance(query, CreateInstanceQuery)
        assert query.tag == "A"
        # There's only 1 field (children) - tag is not a field
        assert len(query.fields) == 1
        # children field should be an array with two elements
        children = query.fields[0].value
        assert isinstance(children, list)
        assert len(children) == 2
        assert isinstance(children[0], InlineInstance)
        assert isinstance(children[1], TagReference)
        assert children[1].name == "A"

    def test_parse_program(self):
        """Test parsing multiple statements with parse_program."""
        parser = QueryParser()

        # Multiple statements without semicolons
        queries = parser.parse_program("from Person select *\nshow types")
        assert len(queries) == 2
        assert isinstance(queries[0], SelectQuery)
        assert isinstance(queries[1], ShowTypesQuery)

        # Multiple statements with semicolons
        queries = parser.parse_program("from Person select *; show types;")
        assert len(queries) == 2
        assert isinstance(queries[0], SelectQuery)
        assert isinstance(queries[1], ShowTypesQuery)

    def test_parse_create_type_trailing_comma(self):
        """Test that trailing comma is allowed in type field list."""
        parser = QueryParser()
        query = parser.parse("type Point { x: float32, y: float32, }")

        assert isinstance(query, CreateTypeQuery)
        assert query.name == "Point"
        assert len(query.fields) == 2
        assert query.fields[0].name == "x"
        assert query.fields[0].type_name == "float32"
        assert query.fields[1].name == "y"
        assert query.fields[1].type_name == "float32"

    def test_parse_create_type_empty_braces(self):
        """Test that type with empty braces works."""
        parser = QueryParser()
        query = parser.parse("type Empty { }")

        assert isinstance(query, CreateTypeQuery)
        assert query.name == "Empty"
        assert len(query.fields) == 0


class TestMethodCallParsing:
    """Tests for array method call parsing."""

    def test_parse_select_method_call_length(self):
        """Test parsing readings.length() in SELECT."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings.length()")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "readings"
        assert query.fields[0].method_name == "length"
        assert query.fields[0].method_args is None

    def test_parse_select_method_call_isEmpty(self):
        """Test parsing readings.isEmpty() in SELECT."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings.isEmpty()")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "readings"
        assert query.fields[0].method_name == "isEmpty"

    def test_parse_select_method_with_other_fields(self):
        """Test parsing method call alongside regular fields."""
        parser = QueryParser()
        query = parser.parse("from Sensor select name, readings.length()")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 2
        assert query.fields[0].name == "name"
        assert query.fields[0].method_name is None
        assert query.fields[1].name == "readings"
        assert query.fields[1].method_name == "length"

    def test_parse_where_method_comparison(self):
        """Test parsing method call in WHERE with comparison."""
        parser = QueryParser()
        query = parser.parse("from Sensor select * where readings.length() > 0")

        assert isinstance(query, SelectQuery)
        assert query.where is not None
        assert isinstance(query.where, Condition)
        assert query.where.field == "readings"
        assert query.where.method_name == "length"
        assert query.where.operator == "gt"
        assert query.where.value == 0

    def test_parse_where_method_boolean(self):
        """Test parsing bare method call as boolean condition."""
        parser = QueryParser()
        query = parser.parse("from Sensor select * where readings.isEmpty()")

        assert isinstance(query, SelectQuery)
        assert query.where is not None
        assert isinstance(query.where, Condition)
        assert query.where.field == "readings"
        assert query.where.method_name == "isEmpty"
        assert query.where.operator == "eq"
        assert query.where.value is True

    def test_parse_where_method_eq(self):
        """Test parsing method call with = comparison."""
        parser = QueryParser()
        query = parser.parse("from Sensor select * where readings.length() = 3")

        assert isinstance(query, SelectQuery)
        assert query.where.field == "readings"
        assert query.where.method_name == "length"
        assert query.where.operator == "eq"
        assert query.where.value == 3

    def test_parse_select_nested_field_method(self):
        """Test parsing nested field path with method call."""
        parser = QueryParser()
        query = parser.parse("from Team select dept.members.length()")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "dept.members"
        assert query.fields[0].method_name == "length"

    def test_parse_update_mutation_reverse(self):
        """Test parsing readings.reverse() in UPDATE SET."""
        parser = QueryParser()
        query = parser.parse("update $x set readings.reverse()")

        assert isinstance(query, UpdateQuery)
        assert len(query.fields) == 1
        fv = query.fields[0]
        assert fv.name == "readings"
        assert fv.method_name == "reverse"
        assert fv.method_args == []
        assert fv.value is None

    def test_parse_update_mutation_swap(self):
        """Test parsing readings.swap(0, 3) in UPDATE SET."""
        parser = QueryParser()
        query = parser.parse("update $x set readings.swap(0, 3)")

        assert isinstance(query, UpdateQuery)
        assert len(query.fields) == 1
        fv = query.fields[0]
        assert fv.name == "readings"
        assert fv.method_name == "swap"
        assert fv.method_args == [0, 3]

    def test_parse_update_mutation_mixed_with_assignment(self):
        """Test parsing mixed mutation and assignment in UPDATE SET."""
        parser = QueryParser()
        query = parser.parse('update $x set name = "foo", readings.reverse()')

        assert isinstance(query, UpdateQuery)
        assert len(query.fields) == 2
        assert query.fields[0].name == "name"
        assert query.fields[0].value == "foo"
        assert query.fields[0].method_name is None
        assert query.fields[1].name == "readings"
        assert query.fields[1].method_name == "reverse"

    def test_parse_bulk_update_mutation_with_where(self):
        """Test parsing bulk update with mutation and WHERE."""
        parser = QueryParser()
        query = parser.parse('update Sensor set readings.reverse() where name = "temp"')

        assert isinstance(query, UpdateQuery)
        assert query.type_name == "Sensor"
        assert len(query.fields) == 1
        assert query.fields[0].name == "readings"
        assert query.fields[0].method_name == "reverse"
        assert query.where is not None

    def test_parse_sort_no_args(self):
        """Test parsing readings.sort() with no arguments."""
        parser = QueryParser()
        query = parser.parse("update $x set readings.sort()")

        assert isinstance(query, UpdateQuery)
        fv = query.fields[0]
        assert fv.name == "readings"
        assert fv.method_name == "sort"
        assert fv.method_args == []

    def test_parse_sort_bare_desc(self):
        """Test parsing readings.sort(desc)."""
        parser = QueryParser()
        query = parser.parse("update $x set readings.sort(desc)")

        assert isinstance(query, UpdateQuery)
        fv = query.fields[0]
        assert fv.name == "readings"
        assert fv.method_name == "sort"
        assert len(fv.method_args) == 1
        assert isinstance(fv.method_args[0], SortKeyExpr)
        assert fv.method_args[0].field_name is None
        assert fv.method_args[0].descending is True

    def test_parse_sort_dot_field(self):
        """Test parsing members.sort(.salary) — dot shorthand parses as EnumValueExpr."""
        parser = QueryParser()
        query = parser.parse("update $x set members.sort(.salary)")

        assert isinstance(query, UpdateQuery)
        fv = query.fields[0]
        assert fv.name == "members"
        assert fv.method_name == "sort"
        assert len(fv.method_args) == 1
        arg = fv.method_args[0]
        assert isinstance(arg, EnumValueExpr)
        assert arg.enum_name is None
        assert arg.variant_name == "salary"

    def test_parse_sort_dot_field_desc(self):
        """Test parsing members.sort(.salary desc)."""
        parser = QueryParser()
        query = parser.parse("update $x set members.sort(.salary desc)")

        assert isinstance(query, UpdateQuery)
        fv = query.fields[0]
        assert fv.name == "members"
        assert fv.method_name == "sort"
        assert len(fv.method_args) == 1
        arg = fv.method_args[0]
        assert isinstance(arg, SortKeyExpr)
        assert arg.field_name == "salary"
        assert arg.descending is True

    def test_parse_sort_multi_key(self):
        """Test parsing members.sort(.age, .name desc)."""
        parser = QueryParser()
        query = parser.parse("update $x set members.sort(.age, .name desc)")

        assert isinstance(query, UpdateQuery)
        fv = query.fields[0]
        assert fv.name == "members"
        assert fv.method_name == "sort"
        assert len(fv.method_args) == 2
        # First arg: .age — parsed as EnumValueExpr (no direction)
        assert isinstance(fv.method_args[0], EnumValueExpr)
        assert fv.method_args[0].variant_name == "age"
        # Second arg: .name desc — parsed as SortKeyExpr
        assert isinstance(fv.method_args[1], SortKeyExpr)
        assert fv.method_args[1].field_name == "name"
        assert fv.method_args[1].descending is True

    def test_parse_select_contains_with_arg(self):
        """Test parsing readings.contains(5) in SELECT."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings.contains(5)")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "readings"
        assert query.fields[0].method_name == "contains"
        assert query.fields[0].method_args == [5]

    def test_parse_where_contains_boolean(self):
        """Test parsing readings.contains(5) as boolean condition in WHERE."""
        parser = QueryParser()
        query = parser.parse("from Sensor select * where readings.contains(5)")

        assert isinstance(query, SelectQuery)
        assert query.where is not None
        assert isinstance(query.where, Condition)
        assert query.where.field == "readings"
        assert query.where.method_name == "contains"
        assert query.where.method_args == [5]
        assert query.where.operator == "eq"
        assert query.where.value is True

    def test_parse_min_aggregate(self):
        """Test parsing min(age) as aggregate function."""
        parser = QueryParser()
        query = parser.parse("from Person select min(age)")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "age"
        assert query.fields[0].aggregate == "min"

    def test_parse_max_aggregate(self):
        """Test parsing max(age) as aggregate function."""
        parser = QueryParser()
        query = parser.parse("from Person select max(age)")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "age"
        assert query.fields[0].aggregate == "max"

    def test_parse_select_min_method(self):
        """Test parsing readings.min() as method call in SELECT."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings.min()")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "readings"
        assert query.fields[0].method_name == "min"

    def test_parse_select_max_method(self):
        """Test parsing readings.max() as method call in SELECT."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings.max()")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "readings"
        assert query.fields[0].method_name == "max"

    def test_parse_select_min_with_key(self):
        """Test parsing members.min(.salary) with key arg in SELECT."""
        parser = QueryParser()
        query = parser.parse("from Team select members.min(.salary)")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "members"
        assert query.fields[0].method_name == "min"
        assert len(query.fields[0].method_args) == 1
        arg = query.fields[0].method_args[0]
        assert isinstance(arg, EnumValueExpr)
        assert arg.enum_name is None
        assert arg.variant_name == "salary"

    def test_parse_chain_two_methods(self):
        """Test parsing readings.sort().reverse() — two-element chain."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings.sort().reverse()")

        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        f = query.fields[0]
        assert f.name == "readings"
        assert f.method_name is None  # chain, not single method
        assert f.method_chain is not None
        assert len(f.method_chain) == 2
        assert isinstance(f.method_chain[0], MethodCall)
        assert f.method_chain[0].method_name == "sort"
        assert f.method_chain[0].method_args is None
        assert f.method_chain[1].method_name == "reverse"
        assert f.method_chain[1].method_args is None

    def test_parse_chain_three_methods(self):
        """Test parsing readings.sort().reverse().length() — three-element chain."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings.sort().reverse().length()")

        assert isinstance(query, SelectQuery)
        f = query.fields[0]
        assert f.name == "readings"
        assert f.method_chain is not None
        assert len(f.method_chain) == 3
        assert f.method_chain[0].method_name == "sort"
        assert f.method_chain[1].method_name == "reverse"
        assert f.method_chain[2].method_name == "length"

    def test_parse_chain_with_args_on_first(self):
        """Test parsing readings.sort(desc).reverse() — chain with args on first call."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings.sort(desc).reverse()")

        assert isinstance(query, SelectQuery)
        f = query.fields[0]
        assert f.name == "readings"
        assert f.method_chain is not None
        assert len(f.method_chain) == 2
        assert f.method_chain[0].method_name == "sort"
        assert len(f.method_chain[0].method_args) == 1
        assert isinstance(f.method_chain[0].method_args[0], SortKeyExpr)
        assert f.method_chain[0].method_args[0].descending is True
        assert f.method_chain[1].method_name == "reverse"

    def test_parse_where_chain_comparison(self):
        """Test parsing chain in WHERE with comparison."""
        parser = QueryParser()
        query = parser.parse("from Sensor select * where readings.sort().length() > 5")

        assert isinstance(query, SelectQuery)
        cond = query.where
        assert isinstance(cond, Condition)
        assert cond.field == "readings"
        assert cond.method_name is None
        assert cond.method_chain is not None
        assert len(cond.method_chain) == 2
        assert cond.method_chain[0].method_name == "sort"
        assert cond.method_chain[1].method_name == "length"
        assert cond.operator == "gt"
        assert cond.value == 5

    def test_parse_where_chain_boolean(self):
        """Test parsing chain in WHERE as boolean condition."""
        parser = QueryParser()
        query = parser.parse("from Sensor select * where readings.sort().isEmpty()")

        assert isinstance(query, SelectQuery)
        cond = query.where
        assert isinstance(cond, Condition)
        assert cond.field == "readings"
        assert cond.method_chain is not None
        assert len(cond.method_chain) == 2
        assert cond.method_chain[0].method_name == "sort"
        assert cond.method_chain[1].method_name == "isEmpty"
        assert cond.operator == "eq"
        assert cond.value is True

    def test_parse_single_method_backward_compat(self):
        """Test that single method still uses method_name (backward compat)."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings.length()")

        f = query.fields[0]
        assert f.method_name == "length"
        assert f.method_chain is None

    # --- Update chain parsing ---

    def test_parse_update_mutation_chain(self):
        """Test parsing mutation chain (no =) in UPDATE SET."""
        parser = QueryParser()
        query = parser.parse("update Sensor(0) set readings.sort().reverse()")

        assert isinstance(query, UpdateQuery)
        fv = query.fields[0]
        assert fv.name == "readings"
        assert fv.value is None
        assert fv.method_name is None
        assert fv.method_chain is not None
        assert len(fv.method_chain) == 2
        assert fv.method_chain[0].method_name == "sort"
        assert fv.method_chain[1].method_name == "reverse"

    def test_parse_update_assignment_chain(self):
        """Test parsing assignment chain (with =) in UPDATE SET."""
        parser = QueryParser()
        query = parser.parse("update Sensor(0) set readings = readings.append(5).sort().reverse()")

        assert isinstance(query, UpdateQuery)
        fv = query.fields[0]
        assert fv.name == "readings"
        assert fv.value == "readings"
        assert fv.method_chain is not None
        assert len(fv.method_chain) == 3
        assert fv.method_chain[0].method_name == "append"
        assert fv.method_chain[0].method_args == [5]
        assert fv.method_chain[1].method_name == "sort"
        assert fv.method_chain[2].method_name == "reverse"

    def test_parse_update_single_method_assignment(self):
        """Test parsing single method assignment (keyword method)."""
        parser = QueryParser()
        query = parser.parse("update Sensor(0) set readings = readings.sort()")

        fv = query.fields[0]
        assert fv.name == "readings"
        assert fv.value == "readings"
        assert fv.method_name == "sort"
        assert fv.method_args == []
        assert fv.method_chain is None

    def test_parse_update_cross_field_assignment(self):
        """Test parsing cross-field chain assignment."""
        parser = QueryParser()
        query = parser.parse("update Sensor(0) set backup = readings.sort()")

        fv = query.fields[0]
        assert fv.name == "backup"
        assert fv.value == "readings"
        assert fv.method_name == "sort"


class TestExpressionParsing:
    """Tests for arithmetic expression parsing in eval queries."""

    def test_parse_addition(self):
        """5 + 3 → BinaryExpr(5, '+', 3)."""
        parser = QueryParser()
        query = parser.parse("5 + 3")
        assert isinstance(query, EvalQuery)
        expr, alias = query.expressions[0]
        assert isinstance(expr, BinaryExpr)
        assert expr.left == 5
        assert expr.op == "+"
        assert expr.right == 3

    def test_parse_precedence_mul_add(self):
        """5 * 3 + 1 → BinaryExpr(BinaryExpr(5, '*', 3), '+', 1)."""
        parser = QueryParser()
        query = parser.parse("5 * 3 + 1")
        expr, _ = query.expressions[0]
        assert isinstance(expr, BinaryExpr)
        assert expr.op == "+"
        assert isinstance(expr.left, BinaryExpr)
        assert expr.left.op == "*"
        assert expr.left.left == 5
        assert expr.left.right == 3
        assert expr.right == 1

    def test_parse_parenthesized(self):
        """(2 + 3) * 4 → BinaryExpr(BinaryExpr(2, '+', 3), '*', 4)."""
        parser = QueryParser()
        query = parser.parse("(2 + 3) * 4")
        expr, _ = query.expressions[0]
        assert isinstance(expr, BinaryExpr)
        assert expr.op == "*"
        assert isinstance(expr.left, BinaryExpr)
        assert expr.left.op == "+"
        assert expr.left.left == 2
        assert expr.left.right == 3
        assert expr.right == 4

    def test_parse_unary_minus(self):
        """-5 → UnaryExpr('-', 5)."""
        parser = QueryParser()
        query = parser.parse("-5")
        expr, _ = query.expressions[0]
        assert isinstance(expr, UnaryExpr)
        assert expr.op == "-"
        assert expr.operand == 5

    def test_parse_string_concat(self):
        """"hello" ++ " world" → BinaryExpr."""
        parser = QueryParser()
        query = parser.parse('"hello" ++ " world"')
        expr, _ = query.expressions[0]
        assert isinstance(expr, BinaryExpr)
        assert expr.op == "++"
        assert expr.left == "hello"
        assert expr.right == " world"

    def test_parse_modulo(self):
        """10 % 3 → BinaryExpr(10, '%', 3)."""
        parser = QueryParser()
        query = parser.parse("10 % 3")
        expr, _ = query.expressions[0]
        assert isinstance(expr, BinaryExpr)
        assert expr.op == "%"
        assert expr.left == 10
        assert expr.right == 3

    def test_parse_integer_division(self):
        """7 // 2 → BinaryExpr(7, '//', 2)."""
        parser = QueryParser()
        query = parser.parse("7 // 2")
        expr, _ = query.expressions[0]
        assert isinstance(expr, BinaryExpr)
        assert expr.op == "//"
        assert expr.left == 7
        assert expr.right == 2

    def test_parse_true_division(self):
        """10 / 3 → BinaryExpr(10, '/', 3) — SLASH not eaten by REGEX."""
        parser = QueryParser()
        query = parser.parse("10 / 3")
        expr, _ = query.expressions[0]
        assert isinstance(expr, BinaryExpr)
        assert expr.op == "/"
        assert expr.left == 10
        assert expr.right == 3

    def test_parse_negative_literal_in_where(self):
        """where age > -1 still parses."""
        parser = QueryParser()
        query = parser.parse("from Person select * where age > -1")
        assert isinstance(query, SelectQuery)
        assert query.where.value == -1

    def test_parse_regex_still_works(self):
        """where name matches /^K/ → Condition(matches, '^K')."""
        parser = QueryParser()
        query = parser.parse('from Person select * where name matches /^K/')
        assert isinstance(query, SelectQuery)
        assert query.where.operator == "matches"
        assert query.where.value == "^K"

    def test_parse_concat_lower_precedence_than_arithmetic(self):
        """"id:" ++ 5 + 3 → BinaryExpr("id:", "++", BinaryExpr(5, "+", 3))."""
        parser = QueryParser()
        query = parser.parse('"id:" ++ 5 + 3')
        expr, _ = query.expressions[0]
        assert isinstance(expr, BinaryExpr)
        assert expr.op == "++"
        assert expr.left == "id:"
        assert isinstance(expr.right, BinaryExpr)
        assert expr.right.op == "+"
        assert expr.right.left == 5
        assert expr.right.right == 3

    def test_parse_array_literal(self):
        """[1, 2, 3] → EvalQuery with list."""
        parser = QueryParser()
        query = parser.parse("[1, 2, 3]")
        assert isinstance(query, EvalQuery)
        expr, alias = query.expressions[0]
        assert isinstance(expr, list)
        assert expr == [1, 2, 3]
        assert alias is None

    def test_parse_empty_array(self):
        """[] → EvalQuery with empty list."""
        parser = QueryParser()
        query = parser.parse("[]")
        assert isinstance(query, EvalQuery)
        expr, _ = query.expressions[0]
        assert expr == []

    def test_parse_array_with_expressions(self):
        """[1+2, 3*4] → list of BinaryExpr nodes."""
        parser = QueryParser()
        query = parser.parse("[1+2, 3*4]")
        expr, _ = query.expressions[0]
        assert isinstance(expr, list)
        assert len(expr) == 2
        assert isinstance(expr[0], BinaryExpr)
        assert expr[0].op == "+"
        assert isinstance(expr[1], BinaryExpr)
        assert expr[1].op == "*"

    def test_parse_func_with_args(self):
        """sqrt(9) → FunctionCall with args."""
        parser = QueryParser()
        query = parser.parse("sqrt(9)")
        expr, _ = query.expressions[0]
        assert isinstance(expr, FunctionCall)
        assert expr.name == "sqrt"
        assert expr.args == [9]

    def test_parse_func_with_two_args(self):
        """pow(2, 3) → FunctionCall with two args."""
        parser = QueryParser()
        query = parser.parse("pow(2, 3)")
        expr, _ = query.expressions[0]
        assert isinstance(expr, FunctionCall)
        assert expr.name == "pow"
        assert expr.args == [2, 3]

    def test_parse_func_with_array_arg(self):
        """sqrt([1, 4, 9]) → FunctionCall with list arg."""
        parser = QueryParser()
        query = parser.parse("sqrt([1, 4, 9])")
        expr, _ = query.expressions[0]
        assert isinstance(expr, FunctionCall)
        assert expr.name == "sqrt"
        assert len(expr.args) == 1
        assert isinstance(expr.args[0], list)
        assert expr.args[0] == [1, 4, 9]

    def test_parse_array_binary_op(self):
        """[1, 2] + [3, 4] → BinaryExpr with list operands."""
        parser = QueryParser()
        query = parser.parse("[1, 2] + [3, 4]")
        expr, _ = query.expressions[0]
        assert isinstance(expr, BinaryExpr)
        assert expr.op == "+"
        assert isinstance(expr.left, list)
        assert isinstance(expr.right, list)

    def test_parse_scalar_broadcast(self):
        """5 * [1, 2, 3] → BinaryExpr(5, '*', list)."""
        parser = QueryParser()
        query = parser.parse("5 * [1, 2, 3]")
        expr, _ = query.expressions[0]
        assert isinstance(expr, BinaryExpr)
        assert expr.op == "*"
        assert expr.left == 5
        assert isinstance(expr.right, list)

    def test_parse_sum_eval(self):
        """sum([1, 2, 3]) → EvalQuery with FunctionCall(name='sum')."""
        parser = QueryParser()
        query = parser.parse("sum([1, 2, 3])")
        assert isinstance(query, EvalQuery)
        expr, alias = query.expressions[0]
        assert isinstance(expr, FunctionCall)
        assert expr.name == "sum"
        assert len(expr.args) == 1
        assert isinstance(expr.args[0], list)

    def test_parse_min_multi_arg_eval(self):
        """min(5, 3) → EvalQuery with FunctionCall(name='min', args=[5, 3])."""
        parser = QueryParser()
        query = parser.parse("min(5, 3)")
        assert isinstance(query, EvalQuery)
        expr, _ = query.expressions[0]
        assert isinstance(expr, FunctionCall)
        assert expr.name == "min"
        assert expr.args == [5, 3]

    def test_parse_count_eval(self):
        """count([1, 2, 3]) → EvalQuery with FunctionCall(name='count')."""
        parser = QueryParser()
        query = parser.parse("count([1, 2, 3])")
        assert isinstance(query, EvalQuery)
        expr, _ = query.expressions[0]
        assert isinstance(expr, FunctionCall)
        assert expr.name == "count"

    def test_parse_aggregate_count_in_select(self):
        """from Person select count() → SelectField(name='*', aggregate='count')."""
        parser = QueryParser()
        query = parser.parse("from Person select count()")
        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "*"
        assert query.fields[0].aggregate == "count"

    def test_parse_aggregate_sum_in_select(self):
        """from Person select sum(age) → SelectField(name='age', aggregate='sum')."""
        parser = QueryParser()
        query = parser.parse("from Person select sum(age)")
        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "age"
        assert query.fields[0].aggregate == "sum"

    def test_parse_aggregate_min_in_select(self):
        """from Person select min(age) → SelectField(name='age', aggregate='min')."""
        parser = QueryParser()
        query = parser.parse("from Person select min(age)")
        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "age"
        assert query.fields[0].aggregate == "min"

    def test_parse_aggregate_name_as_field(self):
        """from X select count → SelectField(name='count') — field, not function."""
        parser = QueryParser()
        query = parser.parse("from X select count")
        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "count"
        assert query.fields[0].aggregate is None

    def test_parse_method_min_still_works(self):
        """from Sensor select readings.min() → method chain still works."""
        parser = QueryParser()
        query = parser.parse("from Sensor select readings.min()")
        assert isinstance(query, SelectQuery)
        assert len(query.fields) == 1
        assert query.fields[0].name == "readings"
        assert query.fields[0].method_name == "min"
