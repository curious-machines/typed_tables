"""Tests for the `set` command (session settings)."""

import pytest
import tempfile
from pathlib import Path

from typed_tables.parsing.query_parser import QueryParser, SetQuery
from typed_tables.query_executor import QueryExecutor, SetResult
from typed_tables.repl import format_value, print_result
from typed_tables.query_executor import QueryResult
from typed_tables.storage import StorageManager
from typed_tables.types import TypeRegistry


# --- Parser tests ---


class TestSetQueryParsing:
    def setup_method(self):
        self.parser = QueryParser()

    def test_set_max_width_reset(self):
        result = self.parser.parse("set max_width")
        assert isinstance(result, SetQuery)
        assert result.setting == "max_width"
        assert result.value is None

    def test_set_max_width_integer(self):
        result = self.parser.parse("set max_width 80")
        assert isinstance(result, SetQuery)
        assert result.setting == "max_width"
        assert result.value == "80"

    def test_set_max_width_large_integer(self):
        result = self.parser.parse("set max_width 200")
        assert isinstance(result, SetQuery)
        assert result.setting == "max_width"
        assert result.value == "200"

    def test_set_max_width_inf(self):
        result = self.parser.parse("set max_width inf")
        assert isinstance(result, SetQuery)
        assert result.setting == "max_width"
        assert result.value == "inf"

    def test_set_max_width_infinity(self):
        result = self.parser.parse("set max_width infinity")
        assert isinstance(result, SetQuery)
        assert result.setting == "max_width"
        assert result.value == "infinity"


# --- Executor tests ---


class TestSetExecution:
    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.registry = TypeRegistry()
        self.storage = StorageManager(Path(self.tmpdir), self.registry)
        self.executor = QueryExecutor(self.storage, self.registry)

    def test_set_max_width_default(self):
        query = SetQuery(setting="max_width")
        result = self.executor.execute(query)
        assert isinstance(result, SetResult)
        assert result.setting == "max_width"
        assert result.value == 40
        assert "default" in result.message.lower()

    def test_set_max_width_integer(self):
        query = SetQuery(setting="max_width", value="80")
        result = self.executor.execute(query)
        assert isinstance(result, SetResult)
        assert result.setting == "max_width"
        assert result.value == 80

    def test_set_max_width_inf(self):
        query = SetQuery(setting="max_width", value="inf")
        result = self.executor.execute(query)
        assert isinstance(result, SetResult)
        assert result.setting == "max_width"
        assert result.value is None
        assert "infinity" in result.message.lower()

    def test_set_max_width_infinity(self):
        query = SetQuery(setting="max_width", value="infinity")
        result = self.executor.execute(query)
        assert isinstance(result, SetResult)
        assert result.value is None

    def test_set_unknown_setting(self):
        query = SetQuery(setting="unknown_thing")
        with pytest.raises(ValueError, match="Unknown setting"):
            self.executor.execute(query)

    def test_set_max_width_negative(self):
        query = SetQuery(setting="max_width", value="-5")
        with pytest.raises(ValueError, match="positive integer"):
            self.executor.execute(query)

    def test_set_max_width_zero(self):
        query = SetQuery(setting="max_width", value="0")
        with pytest.raises(ValueError, match="positive integer"):
            self.executor.execute(query)


# --- Integration tests ---


class TestFormatValueMaxWidth:
    def test_default_truncation(self):
        long_str = "a" * 100
        result = format_value(long_str)
        assert len(result) <= 42  # 40 + quotes
        assert result.endswith("...'")

    def test_custom_width_wider(self):
        long_str = "a" * 100
        result = format_value(long_str, max_width=80)
        assert len(result) <= 82  # 80 + quotes
        assert result.endswith("...'")

    def test_custom_width_no_truncation(self):
        long_str = "a" * 100
        result = format_value(long_str, max_width=10_000_000)
        assert "..." not in result
        assert len(result) == 102  # quotes + 100 chars

    def test_long_list_truncation(self):
        long_list = list(range(5))
        result = format_value(long_list, max_width=10)
        assert len(result) <= 14  # some slack for truncation markers

    def test_long_list_no_truncation(self):
        long_list = list(range(5))
        result = format_value(long_list, max_width=10_000_000)
        assert result == "[0, 1, 2, 3, 4]"


class TestPrintResultMaxWidth:
    def test_print_result_default_caps_columns(self, capsys):
        result = QueryResult(
            columns=["value"],
            rows=[{"value": "x" * 100}],
        )
        print_result(result)
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        # The value line should be truncated to ~40 chars
        value_line = lines[2]  # header, separator, then value
        assert len(value_line.strip()) <= 45  # some padding slack

    def test_print_result_wide_max_width(self, capsys):
        result = QueryResult(
            columns=["value"],
            rows=[{"value": "x" * 100}],
        )
        print_result(result, max_width=200)
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        value_line = lines[2]
        # With max_width=200, the full value should be shown
        assert "x" * 50 in value_line

    def test_print_result_no_limit(self, capsys):
        result = QueryResult(
            columns=["value"],
            rows=[{"value": "x" * 100}],
        )
        print_result(result, max_width=None)
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        value_line = lines[2]
        assert "..." not in value_line
        assert "x" * 100 in value_line

    def test_print_result_no_truncate_overrides(self, capsys):
        """no_truncate flag on QueryResult should override max_width."""
        result = QueryResult(
            columns=["value"],
            rows=[{"value": "x" * 100}],
            no_truncate=True,
        )
        print_result(result, max_width=20)
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        value_line = lines[2]
        assert "x" * 100 in value_line
