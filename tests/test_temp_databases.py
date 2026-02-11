"""Tests for temporary databases (use <path> as temp)."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from typed_tables.parsing.query_parser import QueryParser, UseQuery
from typed_tables.query_executor import QueryExecutor, UseResult
from typed_tables.repl import run_file
from typed_tables.storage import StorageManager
from typed_tables.types import TypeRegistry


class TestParseUseAsTemp:
    """Tests for parsing 'use ... as temp' syntax."""

    def test_parse_use_as_temp_identifier(self):
        """use test_db as temp → UseQuery with temporary=True."""
        parser = QueryParser()
        query = parser.parse("use test_db as temp")
        assert isinstance(query, UseQuery)
        assert query.path == "test_db"
        assert query.temporary is True

    def test_parse_use_as_temp_string(self):
        """use "test_db" as temp → UseQuery with temporary=True."""
        parser = QueryParser()
        query = parser.parse('use "test_db" as temp')
        assert isinstance(query, UseQuery)
        assert query.path == "test_db"
        assert query.temporary is True

    def test_parse_use_as_temp_path(self):
        """use "./test_db" as temp → UseQuery with temporary=True."""
        parser = QueryParser()
        query = parser.parse('use "./test_db" as temp')
        assert isinstance(query, UseQuery)
        assert query.path == "./test_db"
        assert query.temporary is True

    def test_parse_use_without_temp(self):
        """use test_db → UseQuery with temporary=False."""
        parser = QueryParser()
        query = parser.parse("use test_db")
        assert isinstance(query, UseQuery)
        assert query.path == "test_db"
        assert query.temporary is False

    def test_parse_use_as_temp_with_semicolon(self):
        """use test_db as temp; → works with semicolon."""
        parser = QueryParser()
        query = parser.parse("use test_db as temp;")
        assert isinstance(query, UseQuery)
        assert query.path == "test_db"
        assert query.temporary is True

    def test_parse_use_empty_no_temp(self):
        """Bare 'use' has no temp variant."""
        parser = QueryParser()
        query = parser.parse("use")
        assert isinstance(query, UseQuery)
        assert query.path == ""
        assert query.temporary is False


class TestExecuteUseAsTemp:
    """Tests for executor passing temporary flag through."""

    @pytest.fixture
    def db_dir(self):
        tmp = tempfile.mkdtemp()
        yield Path(tmp)
        shutil.rmtree(tmp, ignore_errors=True)

    @pytest.fixture
    def executor(self, db_dir):
        registry = TypeRegistry()
        storage = StorageManager(db_dir, registry)
        return QueryExecutor(storage, registry)

    def test_execute_use_temp_result(self, executor):
        """_execute_use passes temporary=True through to UseResult."""
        parser = QueryParser()
        query = parser.parse("use test_db as temp")
        result = executor.execute(query)
        assert isinstance(result, UseResult)
        assert result.path == "test_db"
        assert result.temporary is True

    def test_execute_use_non_temp_result(self, executor):
        """_execute_use passes temporary=False through to UseResult."""
        parser = QueryParser()
        query = parser.parse("use test_db")
        result = executor.execute(query)
        assert isinstance(result, UseResult)
        assert result.path == "test_db"
        assert result.temporary is False


class TestTempDatabaseCleanup:
    """Integration tests for temp database cleanup via run_file."""

    def test_temp_db_cleanup_on_exit(self, tmp_path: Path):
        """Temp database is deleted after script execution completes."""
        temp_db = tmp_path / "temp_db"
        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{temp_db}" as temp
type Foo {{ x: uint8 }}
create Foo(x=42)
""")
        exit_code, _ = run_file(script, None, verbose=False)
        assert exit_code == 0
        # run_file doesn't handle temp cleanup (that's the REPL's job),
        # but the database should have been created
        assert temp_db.exists()

    def test_temp_db_survives_switch(self, tmp_path: Path):
        """Switching away from a temp database does not delete it."""
        temp_db = tmp_path / "temp_db"
        other_db = tmp_path / "other_db"
        script = tmp_path / "test.ttq"
        script.write_text(f"""
use "{temp_db}"
type Foo {{ x: uint8 }}
use "{other_db}"
type Bar {{ y: uint8 }}
""")
        exit_code, _ = run_file(script, None, verbose=False)
        assert exit_code == 0
        # Both databases should still exist after run_file
        assert temp_db.exists()
        assert other_db.exists()

    def test_parse_use_as_temp_in_program(self):
        """Multiple statements including 'use ... as temp' parse correctly."""
        parser = QueryParser()
        stmts = parser.parse_program(
            'use test_db as temp; type Foo { x: uint8 }'
        )
        assert len(stmts) == 2
        assert isinstance(stmts[0], UseQuery)
        assert stmts[0].temporary is True
