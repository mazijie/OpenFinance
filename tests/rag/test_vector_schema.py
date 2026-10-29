from __future__ import annotations

import pytest

from backend.rag.vector_schema import ensure_vector_dimension


class FakeCursor:
    def __init__(self, actual_type: str | None, has_rows: bool = False):
        self.actual_type = actual_type
        self.has_rows = has_rows
        self.commands = []
        self._result = None

    def execute(self, sql, params=None):
        self.commands.append(sql)
        if "format_type" in sql:
            self._result = (self.actual_type,) if self.actual_type else None
        elif "SELECT EXISTS" in sql:
            self._result = (self.has_rows,)

    def fetchone(self):
        return self._result


def test_empty_vector_table_is_rebuilt_for_new_dimension():
    cursor = FakeCursor("vector(1536)")

    ensure_vector_dimension(
        cursor,
        schema="openfinance",
        table="news_chunks",
        dimension=1024,
        create_table_sql=lambda: "CREATE TABLE openfinance.news_chunks (embedding vector(1024))",
        error_type=RuntimeError,
    )

    assert any(command.startswith("DROP TABLE openfinance.news_chunks") for command in cursor.commands)
    assert any("vector(1024)" in command for command in cursor.commands)


def test_non_empty_vector_table_requires_explicit_reindex():
    cursor = FakeCursor("vector(1536)", has_rows=True)

    with pytest.raises(RuntimeError, match="clear and rebuild"):
        ensure_vector_dimension(
            cursor,
            schema="openfinance",
            table="news_chunks",
            dimension=1024,
            create_table_sql=lambda: "CREATE TABLE openfinance.news_chunks",
            error_type=RuntimeError,
        )

    assert not any(command.startswith("DROP TABLE") for command in cursor.commands)
