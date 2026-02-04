"""Tests for the TTQ query parser."""

import pytest

from typed_tables.parsing.query_lexer import QueryLexer
from typed_tables.parsing.query_parser import (
    ArrayIndex,
    ArraySlice,
    CollectQuery,
    CollectSource,
    CreateAliasQuery,
    CreateInstanceQuery,
    CreateTypeQuery,
    DeleteQuery,
    DescribeQuery,
    DropDatabaseQuery,
    DumpItem,
    DumpQuery,
    EvalQuery,
    FieldDef,
    FunctionCall,
    CompositeRef,
    InlineInstance,
    NullValue,
    QueryParser,
    SelectQuery,
    ShowTablesQuery,
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

        query = parser.parse("show tables;")
        assert isinstance(query, ShowTablesQuery)

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
        """Test that create type works with free-form whitespace."""
        parser = QueryParser()
        query = parser.parse("create type Person\n  name: string\n  age: uint8")
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
