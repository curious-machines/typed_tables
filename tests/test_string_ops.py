"""Tests for string operations — fixed projection methods, new string methods, regex, mutations."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from typed_tables.parsing.query_parser import QueryParser
from typed_tables.query_executor import QueryExecutor, QueryResult, CreateResult, UpdateResult
from typed_tables.storage import StorageManager
from typed_tables.types import TypeRegistry


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary database directory and executor."""
    db_dir = tmp_path / "test_db"
    db_dir.mkdir()
    registry = TypeRegistry()
    storage = StorageManager(db_dir, registry)
    executor = QueryExecutor(storage, registry)
    return executor, db_dir, registry, storage


def _run(executor: QueryExecutor, query: str):
    """Run a query and return the result."""
    parser = QueryParser()
    parsed = parser.parse(query)
    return executor.execute(parsed)


def _run_all(executor: QueryExecutor, script: str):
    """Run multiple queries."""
    parser = QueryParser()
    stmts = parser.parse_program(script)
    results = []
    for stmt in stmts:
        results.append(executor.execute(stmt))
    return results


def _setup_item(executor):
    """Create the Item type and populate with test data."""
    _run_all(executor, '''
        type Item { name: string }
        create Item(name="Hello World")
        create Item(name="  spaces  ")
        create Item(name="abc")
    ''')


# ==============================================================
# Part 1: Fixed projection methods for strings
# ==============================================================

class TestFixedProjectionMethods:
    """Test the 8 methods that were broken for strings."""

    def test_sort(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.sort() where name="abc"')
        assert r.rows[0]["name.sort()"] == "abc"
        r = _run(executor, 'from Item select name.sort() where name="Hello World"')
        assert r.rows[0]["name.sort()"] == " HWdellloor"

    def test_insert(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.insert(5, "!") where name="Hello World"')
        assert r.rows[0]['name.insert(5, "!")'] == "Hello! World"

    def test_insert_at_start(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.insert(0, ">>") where name="abc"')
        assert r.rows[0]['name.insert(0, ">>")'] == ">>abc"

    def test_delete(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.delete(0) where name="abc"')
        assert r.rows[0]["name.delete(0)"] == "bc"

    def test_delete_last(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.delete(2) where name="abc"')
        assert r.rows[0]["name.delete(2)"] == "ab"

    def test_delete_out_of_range(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        with pytest.raises(RuntimeError, match="out of range"):
            _run(executor, 'from Item select name.delete(10) where name="abc"')

    def test_remove_substring(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.remove("World") where name="Hello World"')
        assert r.rows[0]['name.remove("World")'] == "Hello "

    def test_remove_first_only(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="abab")')
        r = _run(executor, 'from X select s.remove("ab")')
        assert r.rows[0]['s.remove("ab")'] == "ab"

    def test_remove_not_found(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.remove("xyz") where name="abc"')
        assert r.rows[0]['name.remove("xyz")'] == "abc"

    def test_removeAll(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="abcabc")')
        r = _run(executor, 'from X select s.removeAll("ab")')
        assert r.rows[0]['s.removeAll("ab")'] == "cc"

    def test_replace_first(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.replace("l", "L") where name="Hello World"')
        assert r.rows[0]['name.replace("l", "L")'] == "HeLlo World"

    def test_replaceAll(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.replaceAll("l", "L") where name="Hello World"')
        assert r.rows[0]['name.replaceAll("l", "L")'] == "HeLLo WorLd"

    def test_swap(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.swap(0, 2) where name="abc"')
        assert r.rows[0]["name.swap(0, 2)"] == "cba"

    def test_swap_out_of_range(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        with pytest.raises(RuntimeError, match="out of range"):
            _run(executor, 'from Item select name.swap(0, 10) where name="abc"')


# ==============================================================
# Part 2: New string-only methods
# ==============================================================

class TestNewStringMethods:
    """Test new string-only projection methods."""

    def test_uppercase(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.uppercase() where name="Hello World"')
        assert r.rows[0]["name.uppercase()"] == "HELLO WORLD"

    def test_lowercase(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.lowercase() where name="Hello World"')
        assert r.rows[0]["name.lowercase()"] == "hello world"

    def test_capitalize(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="hello world")')
        r = _run(executor, 'from X select s.capitalize()')
        assert r.rows[0]["s.capitalize()"] == "Hello world"

    def test_trim(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.trim() where name="  spaces  "')
        assert r.rows[0]["name.trim()"] == "spaces"

    def test_trimStart(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.trimStart() where name="  spaces  "')
        assert r.rows[0]["name.trimStart()"] == "spaces  "

    def test_trimEnd(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.trimEnd() where name="  spaces  "')
        assert r.rows[0]["name.trimEnd()"] == "  spaces"

    def test_startsWith(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.startsWith("Hello") where name="Hello World"')
        assert r.rows[0]['name.startsWith("Hello")'] is True

    def test_startsWith_false(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.startsWith("World") where name="Hello World"')
        assert r.rows[0]['name.startsWith("World")'] is False

    def test_endsWith(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.endsWith("World") where name="Hello World"')
        assert r.rows[0]['name.endsWith("World")'] is True

    def test_endsWith_false(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.endsWith("Hello") where name="Hello World"')
        assert r.rows[0]['name.endsWith("Hello")'] is False

    def test_indexOf(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.indexOf("World") where name="Hello World"')
        assert r.rows[0]['name.indexOf("World")'] == 6

    def test_indexOf_not_found(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.indexOf("xyz") where name="abc"')
        assert r.rows[0]['name.indexOf("xyz")'] == -1

    def test_lastIndexOf(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="abcabc")')
        r = _run(executor, 'from X select s.lastIndexOf("ab")')
        assert r.rows[0]['s.lastIndexOf("ab")'] == 3

    def test_padStart(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.padStart(6) where name="abc"')
        assert r.rows[0]["name.padStart(6)"] == "   abc"

    def test_padStart_with_char(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.padStart(6, "0") where name="abc"')
        assert r.rows[0]['name.padStart(6, "0")'] == "000abc"

    def test_padEnd(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.padEnd(6) where name="abc"')
        assert r.rows[0]["name.padEnd(6)"] == "abc   "

    def test_padEnd_with_char(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.padEnd(6, ".") where name="abc"')
        assert r.rows[0]['name.padEnd(6, ".")'] == "abc..."

    def test_repeat(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.repeat(3) where name="abc"')
        assert r.rows[0]["name.repeat(3)"] == "abcabcabc"

    def test_repeat_zero(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.repeat(0) where name="abc"')
        assert r.rows[0]["name.repeat(0)"] == ""

    def test_split(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.split(" ") where name="Hello World"')
        assert r.rows[0]['name.split(" ")'] == ["Hello", "World"]

    def test_split_multi(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="a,b,c")')
        r = _run(executor, 'from X select s.split(",")')
        assert r.rows[0]['s.split(",")'] == ["a", "b", "c"]

    def test_on_null(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X()')
        r = _run(executor, 'from X select s.uppercase()')
        assert r.rows[0]["s.uppercase()"] is None

    def test_on_non_string_error(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { n: uint8 }; create X(n=5)')
        with pytest.raises(RuntimeError, match="can only be applied to string"):
            _run(executor, 'from X select n.uppercase()')


# ==============================================================
# Part 3: Regex methods
# ==============================================================

class TestRegexMethods:
    """Test match() regex method. Note: matches() is a keyword (matches /regex/) so
    we use match() for method-based regex. Use match() != null for boolean checks."""

    def test_match_with_groups(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, r'from Item select name.match("(\w+)\s(\w+)") where name="Hello World"')
        result = r.rows[0]['name.match("(\\w+)\\s(\\w+)")']
        assert result == ["Hello World", "Hello", "World"]

    def test_match_no_groups(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, r'from Item select name.match("Hello") where name="Hello World"')
        assert r.rows[0]['name.match("Hello")'] == ["Hello"]

    def test_match_no_match(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, r'from Item select name.match("xyz") where name="abc"')
        assert r.rows[0]['name.match("xyz")'] is None

    def test_match_on_null(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X()')
        r = _run(executor, r'from X select s.match("abc")')
        assert r.rows[0]['s.match("abc")'] is None

    def test_match_partial(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, r'from Item select name.match("lo W") where name="Hello World"')
        assert r.rows[0]['name.match("lo W")'] == ["lo W"]

    def test_match_digits(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, r'type X { s: string }; create X(s="abc123def")')
        r = _run(executor, r'from X select s.match("(\d+)")')
        result = r.rows[0]['s.match("(\\d+)")']
        assert result == ["123", "123"]

    def test_existing_matches_operator_still_works(self, tmp_db):
        """Verify the existing 'matches /regex/' operator is unaffected."""
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, r'from Item select name where name matches /^Hello/')
        assert len(r.rows) == 1
        assert r.rows[0]["name"] == "Hello World"


# ==============================================================
# Part 4: WHERE clause with string methods
# ==============================================================

class TestStringMethodsInWhere:
    """Test string methods used in WHERE conditions."""

    def test_where_startsWith(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name where name.startsWith("Hello")')
        assert len(r.rows) == 1
        assert r.rows[0]["name"] == "Hello World"

    def test_where_endsWith(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name where name.endsWith("World")')
        assert len(r.rows) == 1
        assert r.rows[0]["name"] == "Hello World"

    def test_where_contains_string(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name where name.contains("llo")')
        assert len(r.rows) == 1

    def test_where_matches_operator(self, tmp_db):
        """Test the existing matches /regex/ operator in WHERE."""
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, r'from Item select name where name matches /^[a-z]+$/')
        assert len(r.rows) == 1
        assert r.rows[0]["name"] == "abc"

    def test_where_length_string(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name where name.length() = 3')
        assert len(r.rows) == 1
        assert r.rows[0]["name"] == "abc"

    def test_where_indexOf(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name where name.indexOf("World") >= 0')
        assert len(r.rows) == 1
        assert r.rows[0]["name"] == "Hello World"


# ==============================================================
# Part 5: Method chaining
# ==============================================================

class TestStringMethodChaining:
    """Test chaining string methods in SELECT."""

    def test_trim_then_uppercase(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.trim().uppercase() where name="  spaces  "')
        assert r.rows[0]["name.trim().uppercase()"] == "SPACES"

    def test_lowercase_then_replace(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.lowercase().replace("hello", "hi") where name="Hello World"')
        assert r.rows[0]['name.lowercase().replace("hello", "hi")'] == "hi world"

    def test_sort_then_reverse(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.sort().reverse() where name="abc"')
        assert r.rows[0]["name.sort().reverse()"] == "cba"

    def test_uppercase_then_startsWith(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.uppercase().startsWith("HELLO") where name="Hello World"')
        assert r.rows[0]['name.uppercase().startsWith("HELLO")'] is True

    def test_repeat_then_length(self, tmp_db):
        executor, *_ = tmp_db
        _setup_item(executor)
        r = _run(executor, 'from Item select name.repeat(2).length() where name="abc"')
        assert r.rows[0]["name.repeat(2).length()"] == 6

    def test_trim_then_split(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="  a b c  ")')
        r = _run(executor, 'from X select s.trim().split(" ")')
        assert r.rows[0]['s.trim().split(" ")'] == ["a", "b", "c"]


# ==============================================================
# Part 6: String mutations (UPDATE path)
# ==============================================================

class TestStringMutations:
    """Test string methods used as mutations in UPDATE."""

    def test_mutation_uppercase(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="hello")')
        _run(executor, 'update X(0) set s.uppercase()')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] == "HELLO"

    def test_mutation_lowercase(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="HELLO")')
        _run(executor, 'update X(0) set s.lowercase()')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] == "hello"

    def test_mutation_capitalize(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="hello world")')
        _run(executor, 'update X(0) set s.capitalize()')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] == "Hello world"

    def test_mutation_trim(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="  hello  ")')
        _run(executor, 'update X(0) set s.trim()')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] == "hello"

    def test_mutation_trimStart(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="  hello  ")')
        _run(executor, 'update X(0) set s.trimStart()')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] == "hello  "

    def test_mutation_trimEnd(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="  hello  ")')
        _run(executor, 'update X(0) set s.trimEnd()')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] == "  hello"

    def test_mutation_padStart(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="abc")')
        _run(executor, 'update X(0) set s.padStart(6, "0")')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] == "000abc"

    def test_mutation_padEnd(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="abc")')
        _run(executor, 'update X(0) set s.padEnd(6, ".")')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] == "abc..."

    def test_mutation_repeat(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="ab")')
        _run(executor, 'update X(0) set s.repeat(3)')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] == "ababab"

    def test_mutation_on_null(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X()')
        _run(executor, 'update X(0) set s.uppercase()')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] is None  # no-op on null

    def test_mutation_chain_uppercase_in_update(self, tmp_db):
        """Test chain mutation via assignment: s = s.trim().uppercase()."""
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="  hello  ")')
        _run(executor, 'update X(0) set s = s.trim().uppercase()')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] == "HELLO"

    def test_bulk_mutation_uppercase(self, tmp_db):
        executor, *_ = tmp_db
        _run_all(executor, '''
            type X { s: string }
            create X(s="hello")
            create X(s="world")
        ''')
        _run(executor, 'update X set s.uppercase()')
        r = _run(executor, 'from X select s')
        assert r.rows[0]["s"] == "HELLO"
        assert r.rows[1]["s"] == "WORLD"


# ==============================================================
# Part 7: Eval expression method calls
# ==============================================================

class TestEvalStringMethods:
    """Test string methods on bare eval expressions."""

    def test_eval_uppercase(self, tmp_db):
        executor, *_ = tmp_db
        r = _run(executor, '"hello world".uppercase()')
        assert r.rows[0][r.columns[0]] == "HELLO WORLD"

    def test_eval_lowercase(self, tmp_db):
        executor, *_ = tmp_db
        r = _run(executor, '"ABC".lowercase()')
        assert r.rows[0][r.columns[0]] == "abc"

    def test_eval_split(self, tmp_db):
        executor, *_ = tmp_db
        r = _run(executor, '"a,b,c".split(",")')
        assert r.rows[0][r.columns[0]] == ["a", "b", "c"]

    def test_eval_trim(self, tmp_db):
        executor, *_ = tmp_db
        r = _run(executor, '"  hello  ".trim()')
        assert r.rows[0][r.columns[0]] == "hello"

    def test_eval_contains(self, tmp_db):
        executor, *_ = tmp_db
        r = _run(executor, '"hello world".contains("world")')
        assert r.rows[0][r.columns[0]] is True

    def test_eval_length(self, tmp_db):
        executor, *_ = tmp_db
        r = _run(executor, '"hello".length()')
        assert r.rows[0][r.columns[0]] == 5

    def test_eval_chain(self, tmp_db):
        executor, *_ = tmp_db
        r = _run(executor, '"  Hello World  ".trim().lowercase()')
        assert r.rows[0][r.columns[0]] == "hello world"

    def test_eval_replace(self, tmp_db):
        executor, *_ = tmp_db
        r = _run(executor, '"hello world".replace("world", "there")')
        assert r.rows[0][r.columns[0]] == "hello there"

    def test_eval_repeat(self, tmp_db):
        executor, *_ = tmp_db
        r = _run(executor, '"abc".repeat(3)')
        assert r.rows[0][r.columns[0]] == "abcabcabc"

    def test_eval_match(self, tmp_db):
        executor, *_ = tmp_db
        r = _run(executor, r'"hello123".match("(\d+)")')
        result = r.rows[0][r.columns[0]]
        assert result == ["123", "123"]

    def test_eval_match_boolean_check(self, tmp_db):
        """Use match() != null for boolean regex checking."""
        executor, *_ = tmp_db
        _run_all(executor, 'type X { s: string }; create X(s="hello123")')
        # match returns non-null on match
        r = _run(executor, r'from X select s.match("\d+")')
        assert r.rows[0][r.columns[0]] is not None
