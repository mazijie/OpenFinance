"""PostgreSQL state for completed full-market A-share ingestions."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Any, Iterator

try:
    import psycopg
except ImportError:  # pragma: no cover - deployment dependency
    psycopg = None

from backend.config import settings


class AShareCoverageError(Exception):
    """Raised when A-share coverage state cannot be accessed."""


class AShareCoverageStore:
    """Record only universes whose every batch reached Silver successfully."""

    _schema = "openfinance"
    _table = "a_share_universe_coverage"

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or settings.DATABASE_URL
        if not self.dsn:
            raise AShareCoverageError("DATABASE_URL is not configured")
        if psycopg is None:
            raise AShareCoverageError("psycopg dependency is not installed")

    @property
    def fqtn(self) -> str:
        return f"{self._schema}.{self._table}"

    def is_covered(self, *, provider: str, adjustment_type: str, universe_as_of_date: date) -> bool:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM {self.fqtn} WHERE provider = %s AND adjustment_type = %s AND universe_as_of_date = %s",
                (provider, adjustment_type, universe_as_of_date),
            )
            return cur.fetchone() is not None

    def record_coverage(
        self,
        *,
        provider: str,
        adjustment_type: str,
        universe_as_of_date: date,
        universe_symbol_count: int,
        processed_batches: int,
        written_daily_bars: int,
        dag_id: str,
        dag_run_id: str,
    ) -> bool:
        if universe_symbol_count <= 0 or processed_batches <= 0 or written_daily_bars < 0:
            raise ValueError("coverage requires a positive universe and batch count, and a non-negative daily-bar count")
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.fqtn} (
                    provider, adjustment_type, universe_as_of_date, universe_symbol_count,
                    processed_batches, written_daily_bars, dag_id, dag_run_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (provider, adjustment_type, universe_as_of_date) DO NOTHING
                """,
                (
                    provider,
                    adjustment_type,
                    universe_as_of_date,
                    universe_symbol_count,
                    processed_batches,
                    written_daily_bars,
                    dag_id,
                    dag_run_id,
                ),
            )
            return cur.rowcount == 1

    def ensure_schema(self) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.fqtn} (
                    provider text NOT NULL,
                    adjustment_type text NOT NULL,
                    universe_as_of_date date NOT NULL,
                    universe_symbol_count integer NOT NULL,
                    processed_batches integer NOT NULL,
                    written_daily_bars integer NOT NULL,
                    dag_id text NOT NULL,
                    dag_run_id text NOT NULL,
                    completed_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (provider, adjustment_type, universe_as_of_date)
                )
                """
            )

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        if psycopg is None:  # pragma: no cover
            raise AShareCoverageError("psycopg dependency is not installed")
        conn = psycopg.connect(self.dsn, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()
