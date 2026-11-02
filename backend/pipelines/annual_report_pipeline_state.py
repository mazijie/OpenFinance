"""Durable control-plane state for annual-report processing and index generations."""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator
from uuid import uuid4

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None
    dict_row = None

from backend.config import settings


UTC = timezone.utc


class AnnualReportPipelineStateError(Exception):
    """Raised when durable processing state cannot be read or updated."""


class AnnualReportStageBusy(AnnualReportPipelineStateError):
    """Raised when another worker holds the lease for a report stage."""


class AnnualReportStageAttemptsExhausted(AnnualReportPipelineStateError):
    """Raised when a failed stage reached its automatic retry limit."""


@dataclass(frozen=True, slots=True)
class StageLease:
    report_id: str
    pdf_sha256: str
    stage: str
    stage_version: str
    lease_token: str


class AnnualReportPipelineStateStore:
    def __init__(self, dsn: str | None = None) -> None:
        if psycopg is None or dict_row is None:
            raise AnnualReportPipelineStateError("psycopg is required for annual-report pipeline state")
        self.dsn = dsn or settings.DATABASE_URL
        if not self.dsn:
            raise AnnualReportPipelineStateError("DATABASE_URL is required for annual-report pipeline state")

    def ensure_schema(self, connection=None) -> None:
        with (nullcontext(connection) if connection is not None else self._connection()) as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS openfinance")
            cur.execute(
                """CREATE TABLE IF NOT EXISTS openfinance.annual_report_processing_state (
                    report_id text NOT NULL,
                    pdf_sha256 text NOT NULL,
                    stage text NOT NULL,
                    stage_version text NOT NULL,
                    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
                    attempts integer NOT NULL DEFAULT 0,
                    output_ref text,
                    last_error text,
                    lease_token text,
                    lease_expires_at timestamptz,
                    started_at timestamptz,
                    finished_at timestamptz,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (report_id, pdf_sha256, stage, stage_version)
                )"""
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS annual_report_processing_pending_idx "
                "ON openfinance.annual_report_processing_state (status, stage, updated_at)"
            )
            cur.execute(
                """CREATE TABLE IF NOT EXISTS openfinance.annual_report_index_generations (
                    generation_id text PRIMARY KEY,
                    embedding_model text NOT NULL,
                    embedding_dimension integer NOT NULL,
                    chunker_version text NOT NULL,
                    expected_chunks integer NOT NULL,
                    vector_chunks integer NOT NULL DEFAULT 0,
                    lexical_chunks integer NOT NULL DEFAULT 0,
                    status text NOT NULL CHECK (status IN ('building', 'ready', 'active', 'retained', 'failed')),
                    created_at timestamptz NOT NULL DEFAULT now(),
                    activated_at timestamptz,
                    retain_until timestamptz,
                    last_error text
                )"""
            )
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS annual_report_one_active_generation_idx "
                "ON openfinance.annual_report_index_generations (status) WHERE status = 'active'"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS annual_report_generation_retention_idx "
                "ON openfinance.annual_report_index_generations (status, retain_until)"
            )

    def claim_stage(
        self,
        *,
        report_id: str,
        pdf_sha256: str,
        stage: str,
        stage_version: str,
        lease_seconds: int = 14_400,
    ) -> StageLease:
        now = datetime.now(UTC)
        token = str(uuid4())
        with self._connection() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """INSERT INTO openfinance.annual_report_processing_state
                    (report_id, pdf_sha256, stage, stage_version, status, attempts)
                    VALUES (%s, %s, %s, %s, 'failed', 0)
                    ON CONFLICT (report_id, pdf_sha256, stage, stage_version) DO NOTHING""",
                (report_id, pdf_sha256, stage, stage_version),
            )
            cur.execute(
                """SELECT status, attempts, lease_expires_at FROM openfinance.annual_report_processing_state
                    WHERE report_id=%s AND pdf_sha256=%s AND stage=%s AND stage_version=%s
                    FOR UPDATE""",
                (report_id, pdf_sha256, stage, stage_version),
            )
            row = cur.fetchone()
            if row["status"] == "running" and row["lease_expires_at"] and row["lease_expires_at"] > now:
                raise AnnualReportStageBusy(f"{stage} stage is already running for report {report_id}")
            if row["status"] != "succeeded" and row["attempts"] >= settings.ANNUAL_REPORT_MAX_STAGE_ATTEMPTS:
                raise AnnualReportStageAttemptsExhausted(
                    f"{stage} stage exhausted {settings.ANNUAL_REPORT_MAX_STAGE_ATTEMPTS} attempts for report {report_id}"
                )
            cur.execute(
                """UPDATE openfinance.annual_report_processing_state
                    SET status='running', attempts=attempts+1, last_error=NULL, lease_token=%s,
                        lease_expires_at=%s, started_at=%s, finished_at=NULL, updated_at=%s
                    WHERE report_id=%s AND pdf_sha256=%s AND stage=%s AND stage_version=%s""",
                (token, now + timedelta(seconds=lease_seconds), now, now, report_id, pdf_sha256, stage, stage_version),
            )
        return StageLease(report_id, pdf_sha256, stage, stage_version, token)

    def complete_stage(self, lease: StageLease, *, output_ref: str | None = None) -> None:
        self._finish_stage(lease, status="succeeded", output_ref=output_ref, error=None)

    def fail_stage(self, lease: StageLease, error: BaseException | str) -> None:
        message = str(error).strip()[:4000]
        if not message and isinstance(error, BaseException):
            message = type(error).__name__
        self._finish_stage(lease, status="failed", output_ref=None, error=message)

    def _finish_stage(self, lease: StageLease, *, status: str, output_ref: str | None, error: str | None) -> None:
        now = datetime.now(UTC)
        with self._connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """UPDATE openfinance.annual_report_processing_state
                    SET status=%s, output_ref=%s, last_error=%s, lease_token=NULL,
                        lease_expires_at=NULL, finished_at=%s, updated_at=%s
                    WHERE report_id=%s AND pdf_sha256=%s AND stage=%s AND stage_version=%s AND lease_token=%s""",
                (status, output_ref, error, now, now, lease.report_id, lease.pdf_sha256, lease.stage, lease.stage_version, lease.lease_token),
            )
            if cur.rowcount != 1:
                raise AnnualReportPipelineStateError("Annual-report stage lease was lost before commit")

    def pending_stages(self, *, limit: int = 1000) -> list[dict[str, Any]]:
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """SELECT report_id, pdf_sha256, stage, stage_version, status, output_ref, last_error
                    FROM openfinance.annual_report_processing_state
                    WHERE attempts < %s AND (status='failed' OR (status='running' AND lease_expires_at <= now()))
                    ORDER BY updated_at LIMIT %s""",
                (settings.ANNUAL_REPORT_MAX_STAGE_ATTEMPTS, limit),
            )
            return [dict(row) for row in cur.fetchall()]

    def list_stages(self, *, limit: int = 10000) -> list[dict[str, Any]]:
        """Return stage state for reconciliation, including successful rows."""
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """SELECT report_id, pdf_sha256, stage, stage_version, status,
                          attempts, output_ref, last_error, updated_at
                   FROM openfinance.annual_report_processing_state
                   ORDER BY updated_at DESC LIMIT %s""",
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]

    def enqueue_stage(
        self,
        *,
        report_id: str,
        pdf_sha256: str,
        stage: str,
        stage_version: str,
        error: str,
    ) -> bool:
        """Put a missing or invalid durable artifact back in the retry queue."""
        with self._connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """INSERT INTO openfinance.annual_report_processing_state
                    (report_id, pdf_sha256, stage, stage_version, status, attempts, last_error, updated_at)
                    VALUES (%s, %s, %s, %s, 'failed', 0, %s, now())
                    ON CONFLICT (report_id, pdf_sha256, stage, stage_version) DO UPDATE SET
                        status=CASE WHEN openfinance.annual_report_processing_state.status='running'
                                          AND openfinance.annual_report_processing_state.lease_expires_at > now()
                                    THEN openfinance.annual_report_processing_state.status ELSE 'failed' END,
                        last_error=CASE WHEN openfinance.annual_report_processing_state.status='running'
                                         AND openfinance.annual_report_processing_state.lease_expires_at > now()
                                         THEN openfinance.annual_report_processing_state.last_error ELSE EXCLUDED.last_error END,
                        lease_token=CASE WHEN openfinance.annual_report_processing_state.status='running'
                                         AND openfinance.annual_report_processing_state.lease_expires_at > now()
                                         THEN openfinance.annual_report_processing_state.lease_token ELSE NULL END,
                        lease_expires_at=CASE WHEN openfinance.annual_report_processing_state.status='running'
                                              AND openfinance.annual_report_processing_state.lease_expires_at > now()
                                              THEN openfinance.annual_report_processing_state.lease_expires_at ELSE NULL END,
                        updated_at=now()
                    RETURNING status, attempts""",
                (report_id, pdf_sha256, stage, stage_version, error[:4000]),
            )
            status, attempts = cur.fetchone()
            return status == "failed" and attempts < settings.ANNUAL_REPORT_MAX_STAGE_ATTEMPTS

    def record_verified_stage(self, *, report_id: str, pdf_sha256: str, stage: str, stage_version: str, output_ref: str) -> None:
        """Record a verified artifact discovered before state tracking existed."""
        now = datetime.now(UTC)
        with self._connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """INSERT INTO openfinance.annual_report_processing_state
                    (report_id, pdf_sha256, stage, stage_version, status, output_ref, started_at, finished_at, updated_at)
                    VALUES (%s, %s, %s, %s, 'succeeded', %s, %s, %s, %s)
                    ON CONFLICT (report_id, pdf_sha256, stage, stage_version) DO UPDATE SET
                        status=CASE WHEN openfinance.annual_report_processing_state.status='running'
                                    AND openfinance.annual_report_processing_state.lease_expires_at > now()
                                    THEN openfinance.annual_report_processing_state.status ELSE 'succeeded' END,
                        output_ref=CASE WHEN openfinance.annual_report_processing_state.status='running'
                                       AND openfinance.annual_report_processing_state.lease_expires_at > now()
                                       THEN openfinance.annual_report_processing_state.output_ref ELSE EXCLUDED.output_ref END,
                        last_error=CASE WHEN openfinance.annual_report_processing_state.status='running'
                                       AND openfinance.annual_report_processing_state.lease_expires_at > now()
                                       THEN openfinance.annual_report_processing_state.last_error ELSE NULL END,
                        lease_token=CASE WHEN openfinance.annual_report_processing_state.status='running'
                                       AND openfinance.annual_report_processing_state.lease_expires_at > now()
                                       THEN openfinance.annual_report_processing_state.lease_token ELSE NULL END,
                        lease_expires_at=CASE WHEN openfinance.annual_report_processing_state.status='running'
                                       AND openfinance.annual_report_processing_state.lease_expires_at > now()
                                       THEN openfinance.annual_report_processing_state.lease_expires_at ELSE NULL END,
                        finished_at=CASE WHEN openfinance.annual_report_processing_state.status='running'
                                       AND openfinance.annual_report_processing_state.lease_expires_at > now()
                                       THEN openfinance.annual_report_processing_state.finished_at ELSE EXCLUDED.finished_at END,
                        updated_at=EXCLUDED.updated_at""",
                (report_id, pdf_sha256, stage, stage_version, output_ref, now, now, now),
            )

    def get_active_generation(self) -> dict[str, Any] | None:
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM openfinance.annual_report_index_generations WHERE status='active'"
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def prepare_generation(
        self,
        *,
        generation_id: str,
        embedding_model: str,
        embedding_dimension: int,
        chunker_version: str,
        expected_chunks: int,
    ) -> None:
        with self._connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """INSERT INTO openfinance.annual_report_index_generations
                    (generation_id, embedding_model, embedding_dimension, chunker_version, expected_chunks, status)
                    VALUES (%s, %s, %s, %s, %s, 'building')
                    ON CONFLICT (generation_id) DO UPDATE SET
                        status='building',
                        expected_chunks=EXCLUDED.expected_chunks,
                        last_error=NULL
                    WHERE openfinance.annual_report_index_generations.status IN ('building', 'failed', 'ready', 'retained')""",
                (generation_id, embedding_model, embedding_dimension, chunker_version, expected_chunks),
            )

    def mark_generation_ready(self, generation_id: str, *, vector_chunks: int, lexical_chunks: int) -> None:
        with self._connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """UPDATE openfinance.annual_report_index_generations
                    SET vector_chunks=%s, lexical_chunks=%s, status='ready', last_error=NULL
                    WHERE generation_id=%s AND status='building'""",
                (vector_chunks, lexical_chunks, generation_id),
            )
            if cur.rowcount != 1:
                raise AnnualReportPipelineStateError("Index generation is not in building state")

    def activate_generation(self, generation_id: str, *, retention_days: int = 7) -> None:
        now = datetime.now(UTC)
        with self._connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "UPDATE openfinance.annual_report_index_generations SET status='retained', retain_until=%s WHERE status='active'",
                (now + timedelta(days=retention_days),),
            )
            cur.execute(
                """UPDATE openfinance.annual_report_index_generations
                    SET status='active', activated_at=%s, retain_until=NULL
                    WHERE generation_id=%s AND status='ready' AND vector_chunks=expected_chunks AND lexical_chunks=expected_chunks""",
                (now, generation_id),
            )
            if cur.rowcount != 1:
                raise AnnualReportPipelineStateError("Index generation failed readiness checks")

    def fail_generation(self, generation_id: str, error: BaseException | str) -> None:
        message = str(error)[:4000]
        with self._connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "UPDATE openfinance.annual_report_index_generations SET status='failed', last_error=%s WHERE generation_id=%s AND status IN ('building', 'ready')",
                (message, generation_id),
            )

    def expired_generations(self) -> list[str]:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT generation_id FROM openfinance.annual_report_index_generations WHERE status='retained' AND retain_until <= now()"
            )
            return [str(row["generation_id"]) for row in cur.fetchall()]

    def delete_generation_record(self, generation_id: str) -> None:
        with self._connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "DELETE FROM openfinance.annual_report_index_generations WHERE generation_id=%s AND status='retained' AND retain_until <= now()",
                (generation_id,),
            )

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        conn = psycopg.connect(self.dsn, row_factory=dict_row)
        try:
            yield conn
        finally:
            conn.close()
