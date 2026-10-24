"""Durable PostgreSQL queue for A-share ingestion jobs."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from hashlib import sha256
from typing import Any, Iterator
from uuid import UUID, uuid4

try:
    import psycopg
except ImportError:  # pragma: no cover - deployment dependency
    psycopg = None

from backend.config import settings


class AShareIngestionJobError(Exception):
    """Raised when the durable ingestion queue cannot be accessed."""


class AShareIngestionJobStore:
    """Create, claim and complete durable A-share ingestion jobs."""

    _schema = "openfinance"
    _table = "a_share_ingestion_jobs"

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or settings.DATABASE_URL
        if not self.dsn:
            raise AShareIngestionJobError("DATABASE_URL is not configured")
        if psycopg is None:
            raise AShareIngestionJobError("psycopg dependency is not installed")

    @property
    def fqtn(self) -> str:
        return f"{self._schema}.{self._table}"

    def ensure_schema(self) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.fqtn} (
                    job_id uuid PRIMARY KEY,
                    status text NOT NULL CHECK (status IN ('queued', 'running', 'success', 'failed')),
                    payload jsonb NOT NULL,
                    idempotency_key text,
                    result jsonb,
                    error text,
                    attempts integer NOT NULL DEFAULT 0,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    started_at timestamptz,
                    finished_at timestamptz,
                    heartbeat_at timestamptz
                )
                """
            )
            cur.execute(f"ALTER TABLE {self.fqtn} ADD COLUMN IF NOT EXISTS idempotency_key text")
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS a_share_ingestion_jobs_idempotency_idx "
                f"ON {self.fqtn} (idempotency_key)"
            )
            cur.execute(f"CREATE INDEX IF NOT EXISTS a_share_ingestion_jobs_status_idx ON {self.fqtn} (status, created_at)")

    @staticmethod
    def _payload_fingerprint(payload: dict[str, Any]) -> str:
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return sha256(canonical.encode("utf-8")).hexdigest()

    def create(self, payload: dict[str, Any], idempotency_key: str | None = None) -> str:
        self.ensure_schema()
        job_id = uuid4()
        resolved_key = (idempotency_key or "").strip() or self._payload_fingerprint(payload)
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.fqtn} (job_id, status, payload, idempotency_key)
                VALUES (%s, 'queued', %s::jsonb, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING job_id
                """,
                (job_id, json.dumps(payload, ensure_ascii=False, default=str), resolved_key),
            )
            inserted = cur.fetchone()
            if inserted is not None:
                return str(inserted[0])

            cur.execute(
                f"SELECT job_id, status FROM {self.fqtn} WHERE idempotency_key = %s",
                (resolved_key,),
            )
            existing = cur.fetchone()
            if existing is None:
                raise AShareIngestionJobError("Idempotent job lookup failed")
            existing_id, job_status = existing
            if job_status == "failed":
                cur.execute(
                    f"""
                    UPDATE {self.fqtn}
                    SET status = 'queued', result = NULL, error = NULL,
                        started_at = NULL, finished_at = NULL, heartbeat_at = NULL
                    WHERE job_id = %s
                    """,
                    (existing_id,),
                )
            return str(existing_id)

    def get(self, job_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT job_id, status, result, error, attempts, created_at, started_at, finished_at FROM {self.fqtn} WHERE job_id = %s",
                (UUID(job_id),),
            )
            row = cur.fetchone()
        if row is None:
            return None
        keys = ("job_id", "status", "result", "error", "attempts", "created_at", "started_at", "finished_at")
        return {key: value.isoformat() if isinstance(value, datetime) else str(value) if isinstance(value, UUID) else value for key, value in zip(keys, row, strict=True)}

    def claim(self, stale_after_seconds: int = 86400) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                WITH candidate AS (
                    SELECT job_id FROM {self.fqtn}
                    WHERE status = 'queued'
                       OR (status = 'running' AND heartbeat_at < now() - interval '{int(stale_after_seconds)} seconds')
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE {self.fqtn} AS job
                SET status = 'running', attempts = job.attempts + 1,
                    started_at = COALESCE(job.started_at, now()), heartbeat_at = now(), error = NULL
                FROM candidate
                WHERE job.job_id = candidate.job_id
                RETURNING job.job_id, job.payload, job.attempts
                """
            )
            row = cur.fetchone()
        if row is None:
            return None
        return {"job_id": str(row[0]), "payload": row[1], "attempts": row[2]}

    def complete(self, job_id: str, result: dict[str, Any]) -> None:
        self._update(job_id, status="success", result=result, error=None)

    def fail(self, job_id: str, error: str) -> None:
        self._update(job_id, status="failed", result=None, error=error)

    def heartbeat(self, job_id: str) -> None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"UPDATE {self.fqtn} SET heartbeat_at = now() WHERE job_id = %s AND status = 'running'", (UUID(job_id),))

    def _update(self, job_id: str, *, status: str, result: dict[str, Any] | None, error: str | None) -> None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self.fqtn} SET status = %s, result = %s::jsonb, error = %s, finished_at = now(), heartbeat_at = now() WHERE job_id = %s",
                (status, json.dumps(result, ensure_ascii=False, default=str) if result is not None else "null", error, UUID(job_id)),
            )

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        if psycopg is None:  # pragma: no cover
            raise AShareIngestionJobError("psycopg dependency is not installed")
        conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=10, options="-c statement_timeout=1800000 -c lock_timeout=300000", keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3)
        try:
            yield conn
        finally:
            conn.close()
