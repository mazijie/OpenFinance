"""Daily PostgreSQL cache for the ordinary A-share universe."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import json
from typing import Any, Iterator

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None

from backend.config import settings


class AShareUniverseCacheError(Exception):
    """Raised when the universe cache cannot be accessed."""


class AShareUniverseCacheStore:
    _schema = "openfinance"
    _table = "a_share_universe_cache"

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or settings.DATABASE_URL
        if not self.dsn:
            raise AShareUniverseCacheError("DATABASE_URL is not configured")
        if psycopg is None:
            raise AShareUniverseCacheError("psycopg dependency is not installed")

    @property
    def fqtn(self) -> str:
        return f"{self._schema}.{self._table}"

    def get(self, *, provider: str, as_of_date: date) -> list[str] | None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT symbols FROM {self.fqtn} WHERE provider = %s AND as_of_date = %s",
                (provider, as_of_date),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return [str(symbol) for symbol in row[0]]

    def latest(self, *, provider: str) -> tuple[date, list[str]] | None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT as_of_date, symbols FROM {self.fqtn} WHERE provider = %s AND jsonb_array_length(symbols) > 0 ORDER BY as_of_date DESC, updated_at DESC LIMIT 1", (provider,))
            row = cur.fetchone()
        return (row[0], [str(symbol) for symbol in row[1]]) if row else None

    def put(self, *, provider: str, as_of_date: date, symbols: list[str]) -> None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.fqtn} (provider, as_of_date, symbols)
                VALUES (%s, %s, %s::jsonb)
                ON CONFLICT (provider, as_of_date)
                DO UPDATE SET symbols = EXCLUDED.symbols, updated_at = now()
                """,
                (provider, as_of_date, json.dumps(sorted(set(symbols)))),
            )

    def ensure_schema(self) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.fqtn} (
                    provider text NOT NULL,
                    as_of_date date NOT NULL,
                    symbols jsonb NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (provider, as_of_date)
                )
                """
            )

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        if psycopg is None:  # pragma: no cover
            raise AShareUniverseCacheError("psycopg dependency is not installed")
        conn = psycopg.connect(self.dsn, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()
