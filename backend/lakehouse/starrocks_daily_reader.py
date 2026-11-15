"""Read pinned Iceberg daily-bar snapshots through StarRocks."""

from __future__ import annotations

import re
from datetime import UTC, datetime

from backend.config import settings
from backend.schemas.finance import DailyBarRecord

FIELDS = (
    "security_id", "symbol", "exchange", "trading_date", "open", "high", "low",
    "close", "volume", "amount", "turnover", "is_suspended", "is_st",
    "adjustment_factor", "adjustment_type", "provider", "source_record_id",
    "source_version", "payload_hash", "raw_object_path", "event_time",
    "available_at", "ingested_at", "schema_version",
)


class StarRocksUnavailable(RuntimeError):
    """A query could not be served by StarRocks."""


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Invalid StarRocks catalog, namespace, or table identifier")
    return f"`{value}`"


def _utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("A timezone-aware visibility cutoff is required")
    return value.astimezone(UTC).replace(tzinfo=None)


class StarRocksDailyReader:
    def __init__(self, *, allow_latest: bool = False) -> None:
        self.allow_latest = allow_latest

    def _table(self, snapshot_id: int | None, table_name: str | None = None) -> str:
        # Snapshot IDs cannot be bound as SQL expressions in every MySQL client.
        if snapshot_id is not None and (isinstance(snapshot_id, bool) or not isinstance(snapshot_id, int) or snapshot_id < 0):
            raise ValueError("A valid Iceberg snapshot ID is required")
        table = ".".join(_identifier(value) for value in (
            settings.STARROCKS_CATALOG, settings.ICEBERG_NAMESPACE, table_name or settings.ICEBERG_DAILY_BARS_TABLE,
        ))
        return f"{table} FOR VERSION AS OF {snapshot_id}" if snapshot_id is not None else table

    def _query(self, sql: str, args: tuple) -> list[dict]:
        try:
            import pymysql

            connection = pymysql.connect(
                host=settings.STARROCKS_HOST, port=settings.STARROCKS_PORT,
                user=settings.STARROCKS_USER, password=settings.STARROCKS_PASSWORD,
                connect_timeout=settings.STARROCKS_CONNECT_TIMEOUT_SECONDS,
                read_timeout=settings.STARROCKS_QUERY_TIMEOUT_SECONDS + 5,
                write_timeout=settings.STARROCKS_CONNECT_TIMEOUT_SECONDS,
                cursorclass=pymysql.cursors.DictCursor,
            )
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET time_zone = '+00:00'")
                    cursor.execute("SET query_timeout = %s", (settings.STARROCKS_QUERY_TIMEOUT_SECONDS,))
                    cursor.execute(sql, args)
                    return list(cursor.fetchall())
            finally:
                connection.close()
        except Exception as exc:
            raise StarRocksUnavailable("StarRocks query failed") from exc

    def scan_daily_bars_for_query(self, *, symbol, security_id, start_date, end_date,
                                  adjustment_type, as_of, limit, table=None, snapshot_id=None):
        if snapshot_id is None and not self.allow_latest:
            return []
        if not (security_id or symbol):
            raise ValueError("symbol or security_id is required")
        selector = "security_id" if security_id else "symbol"
        cutoff = _utc_naive(as_of)
        sql = (f"SELECT {', '.join(_identifier(field) for field in FIELDS)} FROM {self._table(snapshot_id)} "
               f"WHERE `{selector}` = %s AND `adjustment_type` = %s "
               "AND `trading_date` >= %s AND `trading_date` <= %s "
               "AND `available_at` <= %s AND `ingested_at` <= %s")
        rows = self._query(sql, (security_id or symbol, adjustment_type, start_date, end_date, cutoff, cutoff))
        for row in rows:
            for field in ("event_time", "available_at", "ingested_at"):
                if isinstance(row.get(field), datetime) and row[field].tzinfo is None:
                    row[field] = row[field].replace(tzinfo=UTC)
        return [DailyBarRecord.model_validate(row) for row in rows]

    def has_daily_bars_before(self, *, symbol, adjustment_type, before, as_of, table=None, snapshot_id=None):
        if snapshot_id is None and not self.allow_latest:
            return False
        cutoff = _utc_naive(as_of)
        sql = (f"SELECT 1 FROM {self._table(snapshot_id)} WHERE `symbol` = %s "
               "AND `adjustment_type` = %s AND `trading_date` < %s "
               "AND `available_at` <= %s AND `ingested_at` <= %s LIMIT 1")
        return bool(self._query(sql, (symbol, adjustment_type, before, cutoff, cutoff)))
