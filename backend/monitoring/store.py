"""PostgreSQL persistence for the investment-monitor control plane."""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from datetime import UTC, date, datetime
from typing import Any, Iterator
from uuid import UUID, uuid4

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - deployment dependency
    psycopg = None
    dict_row = None

from backend.config import settings
from backend.schemas.monitor import (
    MonitorReport,
    MonitorReportStatus,
    MonitorRun,
    MonitorRunStatus,
    WatchlistItem,
    WatchlistThresholds,
)


class MonitorStoreError(Exception):
    """Raised when monitor persistence is unavailable."""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class MonitorStore:
    """Create and query watchlists, runs, reports, and traces."""

    _schema = "openfinance"

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or settings.DATABASE_URL
        if not self.dsn:
            raise MonitorStoreError("DATABASE_URL is not configured")
        if psycopg is None:
            raise MonitorStoreError("psycopg dependency is not installed")

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        if psycopg is None:  # pragma: no cover
            raise MonitorStoreError("psycopg dependency is not installed")
        conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=10)
        try:
            yield conn
        finally:
            conn.close()

    def ensure_schema(self) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.watchlist_items (
                    symbol text PRIMARY KEY,
                    security_id text NOT NULL,
                    name text NOT NULL,
                    exchange text NOT NULL,
                    enabled boolean NOT NULL DEFAULT true,
                    price_move_threshold double precision NOT NULL DEFAULT 0.03,
                    volume_ratio_threshold double precision NOT NULL DEFAULT 1.8,
                    note text,
                    version integer NOT NULL DEFAULT 1,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.monitor_runs (
                    run_id uuid PRIMARY KEY,
                    run_key text NOT NULL UNIQUE,
                    trading_date date NOT NULL,
                    cutoff_at timestamptz NOT NULL,
                    status text NOT NULL CHECK (status IN ('queued','running','completed','degraded','skipped','failed')),
                    watchlist_version text NOT NULL,
                    config jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    stats jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    error text,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    started_at timestamptz,
                    finished_at timestamptz,
                    heartbeat_at timestamptz
                )
                """
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS monitor_runs_status_idx ON {self._schema}.monitor_runs (status, created_at)"
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.monitor_reports (
                    report_id uuid PRIMARY KEY,
                    run_id uuid NOT NULL,
                    symbol text NOT NULL,
                    name text NOT NULL,
                    trading_date date NOT NULL,
                    data_cutoff_at timestamptz NOT NULL,
                    data_status text NOT NULL CHECK (data_status IN ('complete','degraded')),
                    status text NOT NULL CHECK (status IN ('ready','degraded','failed')),
                    events jsonb NOT NULL DEFAULT '[]'::jsonb,
                    citations jsonb NOT NULL DEFAULT '[]'::jsonb,
                    recommendation_5d jsonb,
                    recommendation_20d jsonb,
                    analysis text NOT NULL DEFAULT '',
                    risk_notes jsonb NOT NULL DEFAULT '[]'::jsonb,
                    missing_data jsonb NOT NULL DEFAULT '[]'::jsonb,
                    model_metadata jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    generated_at timestamptz NOT NULL,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    CONSTRAINT monitor_reports_symbol_cutoff_key UNIQUE (symbol, data_cutoff_at)
                )
                """
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS monitor_reports_trading_date_idx ON {self._schema}.monitor_reports (trading_date DESC, symbol)"
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.monitor_agent_traces (
                    trace_id uuid PRIMARY KEY,
                    run_id uuid NOT NULL,
                    symbol text NOT NULL,
                    status text NOT NULL,
                    tool_calls jsonb NOT NULL DEFAULT '[]'::jsonb,
                    latency_ms integer NOT NULL DEFAULT 0,
                    error_stage text,
                    error text,
                    created_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )

    def insert_watchlist(self, item: WatchlistItem) -> WatchlistItem:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._schema}.watchlist_items
                    (symbol, security_id, name, exchange, enabled, price_move_threshold,
                     volume_ratio_threshold, note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO NOTHING
                RETURNING symbol, security_id, name, exchange, enabled,
                          price_move_threshold, volume_ratio_threshold, note,
                          version, created_at, updated_at
                """,
                (
                    item.symbol,
                    item.security_id,
                    item.name,
                    item.exchange,
                    item.enabled,
                    item.thresholds.price_move,
                    item.thresholds.volume_ratio,
                    item.note,
                ),
            )
            row = cur.fetchone()
        if row is None:
            raise ValueError(f"{item.symbol} is already in the watchlist")
        return self._watchlist_row(row)

    def update_watchlist(
        self,
        symbol: str,
        *,
        enabled: bool | None,
        thresholds: WatchlistThresholds | None,
        note: str | None,
        replace_note: bool = False,
    ) -> WatchlistItem | None:
        self.ensure_schema()
        current = self.get_watchlist(symbol)
        if current is None:
            return None
        price_threshold = thresholds.price_move if thresholds is not None else current.thresholds.price_move
        volume_threshold = thresholds.volume_ratio if thresholds is not None else current.thresholds.volume_ratio
        resolved_enabled = current.enabled if enabled is None else enabled
        resolved_note = note if replace_note else current.note if note is None else note
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self._schema}.watchlist_items
                SET enabled = %s, price_move_threshold = %s, volume_ratio_threshold = %s,
                    note = %s, version = version + 1, updated_at = now()
                WHERE symbol = %s
                RETURNING symbol, security_id, name, exchange, enabled,
                          price_move_threshold, volume_ratio_threshold, note,
                          version, created_at, updated_at
                """,
                (resolved_enabled, price_threshold, volume_threshold, resolved_note, symbol.upper()),
            )
            row = cur.fetchone()
        return self._watchlist_row(row) if row else None

    def delete_watchlist(self, symbol: str) -> bool:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"DELETE FROM {self._schema}.watchlist_items WHERE symbol = %s", (symbol.upper(),))
            return cur.rowcount > 0

    def list_watchlist(self, *, enabled_only: bool = False) -> list[WatchlistItem]:
        self.ensure_schema()
        where = "WHERE enabled = true" if enabled_only else ""
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT symbol, security_id, name, exchange, enabled,
                       price_move_threshold, volume_ratio_threshold, note,
                       version, created_at, updated_at
                FROM {self._schema}.watchlist_items
                {where}
                ORDER BY symbol
                """
            )
            rows = cur.fetchall()
        return [self._watchlist_row(row) for row in rows]

    def get_watchlist(self, symbol: str) -> WatchlistItem | None:
        items = self.list_watchlist()
        normalized = symbol.upper()
        return next((item for item in items if item.symbol == normalized), None)

    def watchlist_version(self, items: list[WatchlistItem]) -> str:
        payload = [
            {
                "symbol": item.symbol,
                "enabled": item.enabled,
                "price_move": item.thresholds.price_move,
                "volume_ratio": item.thresholds.volume_ratio,
                "version": item.version,
            }
            for item in sorted(items, key=lambda value: value.symbol)
        ]
        return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()

    def create_run(
        self,
        *,
        trading_date: date,
        cutoff_at: datetime,
        watchlist_version: str,
        config: dict[str, Any],
    ) -> tuple[MonitorRun, bool]:
        self.ensure_schema()
        run_id = uuid4()
        run_key = hashlib.sha256(
            _json(
                {
                    "trading_date": trading_date.isoformat(),
                    "cutoff_at": _utc(cutoff_at).isoformat(),
                    "watchlist_version": watchlist_version,
                    "config": config,
                }
            ).encode("utf-8")
        ).hexdigest()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._schema}.monitor_runs
                    (run_id, run_key, trading_date, cutoff_at, status, watchlist_version, config)
                VALUES (%s, %s, %s, %s, 'queued', %s, %s::jsonb)
                ON CONFLICT (run_key) DO NOTHING
                RETURNING run_id
                """,
                (run_id, run_key, trading_date, _utc(cutoff_at), watchlist_version, _json(config)),
            )
            inserted = cur.fetchone()
        run = self.get_run(str(inserted[0] if inserted else self._find_run_id(run_key)))
        if run is None:
            raise MonitorStoreError("Failed to create or load monitor run")
        return run, inserted is None

    def _find_run_id(self, run_key: str) -> UUID:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT run_id FROM {self._schema}.monitor_runs WHERE run_key = %s", (run_key,))
            row = cur.fetchone()
        if row is None:
            raise MonitorStoreError("Monitor run not found by idempotency key")
        return row[0]

    def get_run(self, run_id: str) -> MonitorRun | None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT run_id, run_key, trading_date, cutoff_at, status, watchlist_version,
                       config, stats, error, created_at, started_at, finished_at
                FROM {self._schema}.monitor_runs WHERE run_id = %s
                """,
                (UUID(run_id),),
            )
            row = cur.fetchone()
        return self._run_row(row) if row else None

    def latest_successful_cutoff(self) -> datetime | None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT max(cutoff_at) FROM {self._schema}.monitor_runs
                WHERE status IN ('completed', 'degraded')
                """
            )
            row = cur.fetchone()
        return row[0] if row and row[0] else None

    def claim_run(self, stale_after_seconds: int = 7200) -> MonitorRun | None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                WITH candidate AS (
                    SELECT run_id FROM {self._schema}.monitor_runs
                    WHERE status = 'queued'
                       OR (status = 'running' AND heartbeat_at < now() - interval '{int(stale_after_seconds)} seconds')
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE {self._schema}.monitor_runs AS run
                SET status = 'running', started_at = COALESCE(run.started_at, now()),
                    heartbeat_at = now(), error = NULL
                FROM candidate
                WHERE run.run_id = candidate.run_id
                RETURNING run.run_id, run.run_key, run.trading_date, run.cutoff_at,
                          run.status, run.watchlist_version, run.config, run.stats,
                          run.error, run.created_at, run.started_at, run.finished_at
                """
            )
            row = cur.fetchone()
        return self._run_row(row) if row else None

    def heartbeat_run(self, run_id: str) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self._schema}.monitor_runs SET heartbeat_at = now() WHERE run_id = %s AND status = 'running'",
                (UUID(run_id),),
            )

    def finish_run(
        self,
        run_id: str,
        *,
        status: MonitorRunStatus,
        stats: dict[str, Any],
        error: str | None = None,
    ) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self._schema}.monitor_runs
                SET status = %s, stats = %s::jsonb, error = %s,
                    finished_at = now(), heartbeat_at = now()
                WHERE run_id = %s
                """,
                (status, _json(stats), error, UUID(run_id)),
            )

    def upsert_report(self, report: MonitorReport) -> MonitorReport:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._schema}.monitor_reports (
                    report_id, run_id, symbol, name, trading_date, data_cutoff_at,
                    data_status, status, events, citations, recommendation_5d,
                    recommendation_20d, analysis, risk_notes, missing_data,
                    model_metadata, generated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb,
                    %s::jsonb, %s::jsonb, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s
                )
                ON CONFLICT (symbol, data_cutoff_at) DO UPDATE SET
                    run_id = EXCLUDED.run_id, name = EXCLUDED.name,
                    trading_date = EXCLUDED.trading_date, data_status = EXCLUDED.data_status,
                    status = EXCLUDED.status, events = EXCLUDED.events,
                    citations = EXCLUDED.citations, recommendation_5d = EXCLUDED.recommendation_5d,
                    recommendation_20d = EXCLUDED.recommendation_20d, analysis = EXCLUDED.analysis,
                    risk_notes = EXCLUDED.risk_notes, missing_data = EXCLUDED.missing_data,
                    model_metadata = EXCLUDED.model_metadata, generated_at = EXCLUDED.generated_at
                RETURNING report_id
                """,
                (
                    UUID(report.report_id),
                    UUID(report.run_id),
                    report.symbol,
                    report.name,
                    report.trading_date,
                    report.data_cutoff_at,
                    report.data_status,
                    report.status,
                    _json([item.model_dump(mode="json") for item in report.events]),
                    _json([item.model_dump(mode="json") for item in report.citations]),
                    _json(report.recommendation_5d.model_dump(mode="json")) if report.recommendation_5d else "null",
                    _json(report.recommendation_20d.model_dump(mode="json")) if report.recommendation_20d else "null",
                    report.analysis,
                    _json(report.risk_notes),
                    _json(report.missing_data),
                    _json(report.model_metadata),
                    report.generated_at,
                ),
            )
        return report

    def list_reports(
        self,
        *,
        trading_date: date | None = None,
        symbol: str | None = None,
        status: MonitorReportStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[MonitorReport], int]:
        self.ensure_schema()
        where: list[str] = []
        params: list[Any] = []
        if trading_date is not None:
            where.append("trading_date = %s")
            params.append(trading_date)
        if symbol:
            where.append("symbol = %s")
            params.append(symbol.upper())
        if status:
            where.append("status = %s")
            params.append(status)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT count(*) AS total FROM {self._schema}.monitor_reports {clause}", tuple(params))
            total = int(cur.fetchone()["total"])
            cur.execute(
                f"""
                SELECT * FROM {self._schema}.monitor_reports {clause}
                ORDER BY trading_date DESC, generated_at DESC, symbol
                LIMIT %s OFFSET %s
                """,
                tuple(params + [limit, offset]),
            )
            rows = cur.fetchall()
        return [self._report_row(row) for row in rows], total

    def get_report(self, report_id: str) -> MonitorReport | None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT * FROM {self._schema}.monitor_reports WHERE report_id = %s",
                (UUID(report_id),),
            )
            row = cur.fetchone()
        return self._report_row(row) if row else None

    def save_trace(
        self,
        *,
        run_id: str,
        symbol: str,
        status: str,
        tool_calls: list[dict[str, Any]],
        latency_ms: int,
        error_stage: str | None = None,
        error: str | None = None,
    ) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._schema}.monitor_agent_traces
                    (trace_id, run_id, symbol, status, tool_calls, latency_ms, error_stage, error)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                """,
                (uuid4(), UUID(run_id), symbol, status, _json(tool_calls), latency_ms, error_stage, error),
            )

    @staticmethod
    def _watchlist_row(row: Any) -> WatchlistItem:
        return WatchlistItem(
            symbol=str(row[0]),
            security_id=str(row[1]),
            name=str(row[2]),
            exchange=str(row[3]),
            enabled=bool(row[4]),
            thresholds=WatchlistThresholds(price_move=float(row[5]), volume_ratio=float(row[6])),
            note=row[7],
            version=int(row[8]),
            created_at=row[9],
            updated_at=row[10],
        )

    @staticmethod
    def _run_row(row: Any) -> MonitorRun:
        return MonitorRun(
            run_id=str(row[0]),
            run_key=str(row[1]),
            trading_date=row[2],
            cutoff_at=row[3],
            status=str(row[4]),
            watchlist_version=str(row[5]),
            config=dict(row[6] or {}),
            stats=dict(row[7] or {}),
            error=row[8],
            created_at=row[9],
            started_at=row[10],
            finished_at=row[11],
        )

    @staticmethod
    def _report_row(row: dict[str, Any]) -> MonitorReport:
        return MonitorReport.model_validate(
            {
                **row,
                "report_id": str(row["report_id"]),
                "run_id": str(row["run_id"]),
            }
        )
