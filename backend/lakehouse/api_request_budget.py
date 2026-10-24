"""Persistent daily request budget for external finance data providers."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Iterator
from zoneinfo import ZoneInfo

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None

from backend.config import settings


class ApiRequestBudgetError(Exception):
    """Raised when the request budget cannot be accessed."""


class ApiRequestBudgetExceeded(ApiRequestBudgetError):
    """Raised when an external request would exceed the daily budget."""


class ApiRequestBudgetStore:
    """Atomically reserve external API requests against a China-local day."""

    _schema = "openfinance"
    _table = "api_request_budget"

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or settings.DATABASE_URL
        if not self.dsn:
            raise ApiRequestBudgetError("DATABASE_URL is not configured")
        if psycopg is None:
            raise ApiRequestBudgetError("psycopg dependency is not installed")

    @property
    def fqtn(self) -> str:
        return f"{self._schema}.{self._table}"

    @staticmethod
    def usage_date() -> date:
        return datetime.now(ZoneInfo("Asia/Shanghai")).date()

    def remaining(self, *, provider: str, daily_limit: int) -> int:
        self.ensure_schema()
        usage_day = self.usage_date()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.fqtn} (provider, usage_date, requests_used, request_limit)
                VALUES (%s, %s, 0, %s)
                ON CONFLICT (provider, usage_date) DO NOTHING
                """,
                (provider, usage_day, daily_limit),
            )
            cur.execute(
                f"UPDATE {self.fqtn} SET request_limit = %s "
                f"WHERE provider = %s AND usage_date = %s",
                (daily_limit, provider, usage_day),
            )
            cur.execute(
                f"SELECT requests_used, request_limit FROM {self.fqtn} "
                f"WHERE provider = %s AND usage_date = %s",
                (provider, usage_day),
            )
            used, stored_limit = cur.fetchone()
        return max(0, int(stored_limit) - int(used))

    def reserve(self, *, provider: str, count: int, daily_limit: int) -> int:
        if count <= 0:
            raise ValueError("request count must be positive")
        self.ensure_schema()
        usage_day = self.usage_date()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.fqtn} (provider, usage_date, requests_used, request_limit)
                VALUES (%s, %s, 0, %s)
                ON CONFLICT (provider, usage_date) DO NOTHING
                """,
                (provider, usage_day, daily_limit),
            )
            cur.execute(
                f"""
                UPDATE {self.fqtn}
                SET requests_used = requests_used + %s,
                    request_limit = %s,
                    updated_at = now()
                WHERE provider = %s AND usage_date = %s
                  AND requests_used + %s <= %s
                RETURNING request_limit - requests_used
                """,
                (count, daily_limit, provider, usage_day, count, daily_limit),
            )
            row = cur.fetchone()
        if row is None:
            raise ApiRequestBudgetExceeded(
                f"Daily request budget exhausted for {provider} on {usage_day}"
            )
        return int(row[0])

    def ensure_schema(self) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.fqtn} (
                    provider text NOT NULL,
                    usage_date date NOT NULL,
                    requests_used integer NOT NULL DEFAULT 0,
                    request_limit integer NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (provider, usage_date)
                )
                """
            )

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        if psycopg is None:  # pragma: no cover
            raise ApiRequestBudgetError("psycopg dependency is not installed")
        conn = psycopg.connect(self.dsn, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()
