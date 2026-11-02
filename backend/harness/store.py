"""PostgreSQL persistence for threads, turns, tools, approvals, and events."""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from typing import Any, Callable, Iterator
from uuid import UUID, uuid4

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - deployment dependency
    psycopg = None
    dict_row = None

from backend.config import settings
from backend.schemas.harness import (
    AgentApproval,
    AgentCitation,
    AgentEvent,
    AgentMessage,
    AgentThread,
    AgentToolCall,
    AgentTurn,
    WebDocument,
    WebDomain,
)


class HarnessStoreError(Exception):
    """Raised when harness persistence is unavailable."""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


class HarnessStore:
    _schema = "openfinance"

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or settings.DATABASE_URL
        if not self.dsn:
            raise HarnessStoreError("DATABASE_URL is not configured")
        if psycopg is None:
            raise HarnessStoreError("psycopg dependency is not installed")

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        if psycopg is None:  # pragma: no cover
            raise HarnessStoreError("psycopg dependency is not installed")
        conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=10)
        try:
            yield conn
        finally:
            conn.close()

    def ensure_schema(self, connection=None) -> None:
        with (nullcontext(connection) if connection is not None else self._connection()) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_threads (
                    thread_id uuid PRIMARY KEY,
                    title text NOT NULL,
                    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active','archived')),
                    kind text NOT NULL DEFAULT 'user' CHECK (kind IN ('user','system')),
                    rolling_summary text NOT NULL DEFAULT '',
                    last_seq bigint NOT NULL DEFAULT 0,
                    last_read_seq bigint NOT NULL DEFAULT 0,
                    last_message_at timestamptz,
                    metadata jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_turns (
                    turn_id uuid PRIMARY KEY,
                    thread_id uuid NOT NULL REFERENCES {self._schema}.agent_threads(thread_id) ON DELETE CASCADE,
                    question text NOT NULL,
                    mode text NOT NULL CHECK (mode IN ('standard','deep')),
                    status text NOT NULL CHECK (status IN ('queued','running','waiting_approval','completed','failed','cancelled')),
                    checkpoint_id text,
                    budget jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    stats jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    error text,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    started_at timestamptz,
                    finished_at timestamptz
                )
                """
            )
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS agent_one_active_turn_idx ON {self._schema}.agent_turns (thread_id) "
                f"WHERE status IN ('queued','running','waiting_approval')"
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_messages (
                    message_id uuid PRIMARY KEY,
                    thread_id uuid NOT NULL REFERENCES {self._schema}.agent_threads(thread_id) ON DELETE CASCADE,
                    seq bigint NOT NULL,
                    role text NOT NULL CHECK (role IN ('user','assistant','system')),
                    kind text NOT NULL CHECK (kind IN ('text','tool','approval','monitor_report','system')),
                    content text NOT NULL DEFAULT '',
                    citations jsonb NOT NULL DEFAULT '[]'::jsonb,
                    payload jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    turn_id uuid,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    UNIQUE (thread_id, seq)
                )
                """
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS agent_messages_thread_idx ON {self._schema}.agent_messages (thread_id, seq)"
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_tool_calls (
                    call_id text PRIMARY KEY,
                    turn_id uuid NOT NULL REFERENCES {self._schema}.agent_turns(turn_id) ON DELETE CASCADE,
                    thread_id uuid NOT NULL,
                    tool_name text NOT NULL,
                    args jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    result_summary jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    status text NOT NULL CHECK (status IN ('started','completed','failed','rejected')),
                    requires_approval boolean NOT NULL DEFAULT false,
                    error text,
                    started_at timestamptz NOT NULL DEFAULT now(),
                    finished_at timestamptz
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_approvals (
                    approval_id uuid PRIMARY KEY,
                    thread_id uuid NOT NULL,
                    turn_id uuid NOT NULL REFERENCES {self._schema}.agent_turns(turn_id) ON DELETE CASCADE,
                    call_id text NOT NULL,
                    tool_name text NOT NULL,
                    proposed_action jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    status text NOT NULL CHECK (status IN ('pending','approved','rejected','expired')),
                    idempotency_key text NOT NULL UNIQUE,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    decided_at timestamptz
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_events (
                    event_id uuid PRIMARY KEY,
                    event_seq bigserial UNIQUE,
                    thread_id uuid NOT NULL,
                    turn_id uuid,
                    event_type text NOT NULL,
                    payload jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS agent_events_thread_idx ON {self._schema}.agent_events (thread_id, event_seq)"
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_web_domains (
                    host_pattern text PRIMARY KEY,
                    enabled boolean NOT NULL DEFAULT true,
                    created_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.web_documents (
                    document_id uuid PRIMARY KEY,
                    url text NOT NULL,
                    canonical_url text NOT NULL,
                    domain text NOT NULL,
                    title text NOT NULL,
                    text text NOT NULL,
                    content_hash text NOT NULL,
                    fetched_at timestamptz NOT NULL,
                    metadata jsonb NOT NULL DEFAULT '{{}}'::jsonb
                )
                """
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS web_documents_canonical_idx ON {self._schema}.web_documents (canonical_url, fetched_at DESC)"
            )
            cur.execute(
                f"ALTER TABLE {self._schema}.monitor_runs ADD COLUMN IF NOT EXISTS origin_thread_id uuid"
            )
            self._seed_web_domains(cur)

    def _seed_web_domains(self, cur: Any) -> None:
        for host in (
            "www.cninfo.com.cn",
            "*.cninfo.com.cn",
            "www.sse.com.cn",
            "www.szse.cn",
            "www.bse.cn",
            "www.csrc.gov.cn",
            "www.pbc.gov.cn",
            "www.stats.gov.cn",
            "www.gov.cn",
            "*.eastmoney.com",
            "*.sina.com.cn",
            "*.10jqka.com.cn",
            "*.stcn.com",
            "*.yicai.com",
            "*.cls.cn",
            "*.gmw.cn",
            "*.cnstock.com",
            "*.people.com.cn",
            "*.xinhuanet.com",
        ):
            cur.execute(
                f"INSERT INTO {self._schema}.agent_web_domains (host_pattern) VALUES (%s) ON CONFLICT DO NOTHING",
                (host,),
            )

    def create_thread(self, title: str | None = None, *, kind: str = "user") -> AgentThread:
        self.ensure_schema()
        thread_id = uuid4()
        resolved_title = (title or "新研究").strip() or "新研究"
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._schema}.agent_threads (thread_id, title, kind)
                VALUES (%s, %s, %s)
                RETURNING thread_id, title, status, kind, rolling_summary, last_seq,
                          last_read_seq, last_message_at, created_at, updated_at, metadata
                """,
                (thread_id, resolved_title, kind),
            )
            row = cur.fetchone()
        return self._thread_row(row)

    def get_or_create_system_thread(self, *, system_key: str, title: str) -> AgentThread:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT t.thread_id, t.title, t.status, t.kind, t.rolling_summary, t.last_seq,
                       t.last_read_seq, t.last_message_at, t.created_at, t.updated_at, t.metadata,
                       (SELECT count(*) FROM {self._schema}.agent_messages m
                        WHERE m.thread_id = t.thread_id AND m.seq > t.last_read_seq AND m.role <> 'user')
                FROM {self._schema}.agent_threads t
                WHERE t.kind = 'system' AND t.metadata->>'system_key' = %s
                ORDER BY t.created_at LIMIT 1
                """,
                (system_key,),
            )
            row = cur.fetchone()
            if row:
                return self._thread_row(row)
            thread_id = uuid4()
            cur.execute(
                f"""
                INSERT INTO {self._schema}.agent_threads (thread_id, title, kind, metadata)
                VALUES (%s, %s, 'system', %s::jsonb)
                RETURNING thread_id, title, status, kind, rolling_summary, last_seq,
                          last_read_seq, last_message_at, created_at, updated_at, metadata
                """,
                (thread_id, title, _json({"system_key": system_key})),
            )
            row = cur.fetchone()
        return self._thread_row((*row, 0))

    def list_threads(self, *, include_archived: bool = False, limit: int = 100) -> list[AgentThread]:
        self.ensure_schema()
        where = "" if include_archived else "WHERE t.status = 'active'"
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT t.thread_id, t.title, t.status, t.kind, t.rolling_summary, t.last_seq,
                       t.last_read_seq, t.last_message_at, t.created_at, t.updated_at, t.metadata,
                       (SELECT count(*) FROM {self._schema}.agent_messages m
                        WHERE m.thread_id = t.thread_id AND m.seq > t.last_read_seq AND m.role <> 'user')
                FROM {self._schema}.agent_threads t
                {where}
                ORDER BY COALESCE(t.last_message_at, t.updated_at) DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return [self._thread_row(row) for row in rows]

    def get_thread(self, thread_id: str) -> AgentThread | None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT t.thread_id, t.title, t.status, t.kind, t.rolling_summary, t.last_seq,
                       t.last_read_seq, t.last_message_at, t.created_at, t.updated_at, t.metadata,
                       (SELECT count(*) FROM {self._schema}.agent_messages m
                        WHERE m.thread_id = t.thread_id AND m.seq > t.last_read_seq AND m.role <> 'user')
                FROM {self._schema}.agent_threads t WHERE t.thread_id = %s
                """,
                (UUID(thread_id),),
            )
            row = cur.fetchone()
        return self._thread_row(row) if row else None

    def update_thread(
        self,
        thread_id: str,
        *,
        title: str | None = None,
        status: str | None = None,
    ) -> AgentThread | None:
        current = self.get_thread(thread_id)
        if current is None:
            return None
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self._schema}.agent_threads
                SET title = %s, status = %s, updated_at = now()
                WHERE thread_id = %s
                """,
                (title or current.title, status or current.status, UUID(thread_id)),
            )
        return self.get_thread(thread_id)

    def delete_thread(self, thread_id: str) -> bool:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass(%s)",
                (f"{self._schema}.checkpoints",),
            )
            checkpoints_exist = cur.fetchone()[0] is not None
            if checkpoints_exist:
                cur.execute(
                    f"DELETE FROM {self._schema}.checkpoint_writes WHERE thread_id = %s",
                    (thread_id,),
                )
                cur.execute(
                    f"DELETE FROM {self._schema}.checkpoint_blobs WHERE thread_id = %s",
                    (thread_id,),
                )
                cur.execute(
                    f"DELETE FROM {self._schema}.checkpoints WHERE thread_id = %s",
                    (thread_id,),
                )
            cur.execute(
                f"DELETE FROM {self._schema}.agent_events WHERE thread_id = %s",
                (UUID(thread_id),),
            )
            cur.execute(
                f"DELETE FROM {self._schema}.agent_threads WHERE thread_id = %s",
                (UUID(thread_id),),
            )
            return cur.rowcount > 0

    def mark_thread_read(self, thread_id: str) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self._schema}.agent_threads SET last_read_seq = last_seq WHERE thread_id = %s",
                (UUID(thread_id),),
            )

    def set_rolling_summary(self, thread_id: str, summary: str) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self._schema}.agent_threads SET rolling_summary = %s, updated_at = now() WHERE thread_id = %s",
                (summary, UUID(thread_id)),
            )

    def append_message(
        self,
        *,
        thread_id: str,
        role: str,
        kind: str,
        content: str,
        citations: list[AgentCitation] | None = None,
        payload: dict[str, Any] | None = None,
        turn_id: str | None = None,
    ) -> AgentMessage:
        message_id = uuid4()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self._schema}.agent_threads
                SET last_seq = last_seq + 1, last_message_at = now(), updated_at = now()
                WHERE thread_id = %s
                RETURNING last_seq
                """,
                (UUID(thread_id),),
            )
            seq_row = cur.fetchone()
            if seq_row is None:
                raise HarnessStoreError("Thread not found")
            seq = int(seq_row[0])
            cur.execute(
                f"""
                INSERT INTO {self._schema}.agent_messages
                    (message_id, thread_id, seq, role, kind, content, citations, payload, turn_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    message_id,
                    UUID(thread_id),
                    seq,
                    role,
                    kind,
                    content,
                    _json([item.model_dump(mode="json") for item in citations or []]),
                    _json(payload or {}),
                    UUID(turn_id) if turn_id else None,
                ),
            )
        message = self.get_message(str(message_id))
        if message is None:
            raise HarnessStoreError("Message was not persisted")
        return message

    def get_message(self, message_id: str) -> AgentMessage | None:
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT * FROM {self._schema}.agent_messages WHERE message_id = %s",
                (UUID(message_id),),
            )
            row = cur.fetchone()
        return self._message_row(row) if row else None

    def find_turn_message(self, *, thread_id: str, turn_id: str, kind: str) -> AgentMessage | None:
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT * FROM {self._schema}.agent_messages
                WHERE thread_id = %s AND turn_id = %s AND kind = %s
                ORDER BY seq ASC LIMIT 1
                """,
                (UUID(thread_id), UUID(turn_id), kind),
            )
            row = cur.fetchone()
        return self._message_row(row) if row else None

    def update_message_payload(self, message_id: str, payload: dict[str, Any]) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self._schema}.agent_messages SET payload = %s::jsonb WHERE message_id = %s",
                (_json(payload), UUID(message_id)),
            )

    def list_messages(
        self,
        thread_id: str,
        *,
        after_seq: int = 0,
        limit: int = 100,
    ) -> list[AgentMessage]:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT * FROM {self._schema}.agent_messages
                WHERE thread_id = %s AND seq > %s
                ORDER BY seq ASC LIMIT %s
                """,
                (UUID(thread_id), after_seq, limit),
            )
            rows = cur.fetchall()
        return [self._message_row(row) for row in rows]

    def create_turn(
        self,
        *,
        thread_id: str,
        question: str,
        mode: str,
        budget: dict[str, Any],
    ) -> AgentTurn:
        self.ensure_schema()
        turn_id = uuid4()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._schema}.agent_turns
                    (turn_id, thread_id, question, mode, status, budget)
                VALUES (%s, %s, %s, %s, 'queued', %s::jsonb)
                RETURNING turn_id, thread_id, question, mode, status, checkpoint_id,
                          budget, stats, error, created_at, started_at, finished_at
                """,
                (turn_id, UUID(thread_id), question, mode, _json(budget)),
            )
            row = cur.fetchone()
        return self._turn_row(row)

    def get_turn(self, turn_id: str) -> AgentTurn | None:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT turn_id, thread_id, question, mode, status, checkpoint_id,
                       budget, stats, error, created_at, started_at, finished_at
                FROM {self._schema}.agent_turns WHERE turn_id = %s
                """,
                (UUID(turn_id),),
            )
            row = cur.fetchone()
        return self._turn_row(row) if row else None

    def update_turn(
        self,
        turn_id: str,
        *,
        status: str,
        checkpoint_id: str | None = None,
        stats: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self._schema}.agent_turns
                SET status = %s,
                    checkpoint_id = COALESCE(%s, checkpoint_id),
                    stats = COALESCE(%s::jsonb, stats),
                    error = %s,
                    started_at = CASE WHEN %s = 'running' THEN COALESCE(started_at, now()) ELSE started_at END,
                    finished_at = CASE WHEN %s IN ('completed','failed','cancelled') THEN now() ELSE finished_at END
                WHERE turn_id = %s
                """,
                (
                    status,
                    checkpoint_id,
                    _json(stats) if stats is not None else None,
                    error,
                    status,
                    status,
                    UUID(turn_id),
                ),
            )

    def update_turn_budget(self, turn_id: str, budget: dict[str, Any]) -> None:
        """Persist budget assessments and revisions without a schema migration."""

        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self._schema}.agent_turns
                SET budget = %s::jsonb
                WHERE turn_id = %s
                """,
                (_json(budget), UUID(turn_id)),
            )

    def list_turns(self, thread_id: str, *, limit: int = 100) -> list[AgentTurn]:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT turn_id, thread_id, question, mode, status, checkpoint_id,
                       budget, stats, error, created_at, started_at, finished_at
                FROM {self._schema}.agent_turns WHERE thread_id = %s
                ORDER BY created_at DESC LIMIT %s
                """,
                (UUID(thread_id), limit),
            )
            rows = cur.fetchall()
        return [self._turn_row(row) for row in rows]

    def record_tool_call(
        self,
        *,
        call_id: str,
        turn_id: str,
        thread_id: str,
        tool_name: str,
        args: dict[str, Any],
        status: str,
        requires_approval: bool = False,
        result_summary: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._schema}.agent_tool_calls
                    (call_id, turn_id, thread_id, tool_name, args, result_summary,
                     status, requires_approval, error, finished_at)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s,
                        CASE WHEN %s IN ('completed','failed','rejected') THEN now() ELSE NULL END)
                ON CONFLICT (call_id) DO UPDATE SET
                    result_summary = EXCLUDED.result_summary,
                    status = EXCLUDED.status,
                    error = EXCLUDED.error,
                    finished_at = EXCLUDED.finished_at
                """,
                (
                    call_id,
                    UUID(turn_id),
                    UUID(thread_id),
                    tool_name,
                    _json(args),
                    _json(result_summary or {}),
                    status,
                    requires_approval,
                    error,
                    status,
                ),
            )

    def list_tool_calls(self, turn_id: str) -> list[AgentToolCall]:
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT * FROM {self._schema}.agent_tool_calls WHERE turn_id = %s ORDER BY started_at",
                (UUID(turn_id),),
            )
            rows = cur.fetchall()
        return [AgentToolCall.model_validate({**row, "call_id": str(row["call_id"])}) for row in rows]

    def create_approval(
        self,
        *,
        thread_id: str,
        turn_id: str,
        call_id: str,
        tool_name: str,
        proposed_action: dict[str, Any],
    ) -> AgentApproval:
        approval_id = uuid4()
        idempotency_key = hashlib.sha256(
            _json(
                {
                    "thread_id": thread_id,
                    "turn_id": turn_id,
                    "call_id": call_id,
                    "tool_name": tool_name,
                    "args": proposed_action,
                }
            ).encode("utf-8")
        ).hexdigest()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._schema}.agent_approvals
                    (approval_id, thread_id, turn_id, call_id, tool_name, proposed_action, status, idempotency_key)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, 'pending', %s)
                ON CONFLICT (idempotency_key) DO UPDATE SET tool_name = EXCLUDED.tool_name
                RETURNING approval_id, thread_id, turn_id, call_id, tool_name,
                          proposed_action, status, idempotency_key, created_at, decided_at
                """,
                (
                    approval_id,
                    UUID(thread_id),
                    UUID(turn_id),
                    call_id,
                    tool_name,
                    _json(proposed_action),
                    idempotency_key,
                ),
            )
            row = cur.fetchone()
        return self._approval_row(row)

    def get_approval(self, approval_id: str) -> AgentApproval | None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self._schema}.agent_approvals
                SET status = 'expired', decided_at = now()
                WHERE approval_id = %s AND status = 'pending'
                  AND created_at < now() - interval '{int(settings.HARNESS_APPROVAL_TTL_SECONDS)} seconds'
                """,
                (UUID(approval_id),),
            )
            cur.execute(
                f"""
                SELECT approval_id, thread_id, turn_id, call_id, tool_name,
                       proposed_action, status, idempotency_key, created_at, decided_at
                FROM {self._schema}.agent_approvals WHERE approval_id = %s
                """,
                (UUID(approval_id),),
            )
            row = cur.fetchone()
        return self._approval_row(row) if row else None

    def decide_approval(self, approval_id: str, *, approved: bool) -> AgentApproval | None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self._schema}.agent_approvals
                SET status = %s, decided_at = now()
                WHERE approval_id = %s AND status = 'pending'
                RETURNING approval_id, thread_id, turn_id, call_id, tool_name,
                          proposed_action, status, idempotency_key, created_at, decided_at
                """,
                ("approved" if approved else "rejected", UUID(approval_id)),
            )
            row = cur.fetchone()
        return self._approval_row(row) if row else self.get_approval(approval_id)

    def expire_turn_approvals(self, turn_id: str) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self._schema}.agent_approvals
                SET status = 'expired', decided_at = now()
                WHERE turn_id = %s AND status = 'pending'
                """,
                (UUID(turn_id),),
            )

    def list_approvals(self, turn_id: str) -> list[AgentApproval]:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT approval_id, thread_id, turn_id, call_id, tool_name,
                       proposed_action, status, idempotency_key, created_at, decided_at
                FROM {self._schema}.agent_approvals WHERE turn_id = %s ORDER BY created_at
                """,
                (UUID(turn_id),),
            )
            rows = cur.fetchall()
        return [self._approval_row(row) for row in rows]

    def append_event(
        self,
        *,
        thread_id: str,
        turn_id: str | None,
        event_type: str,
        payload: dict[str, Any],
    ) -> AgentEvent:
        event_id = uuid4()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._schema}.agent_events
                    (event_id, thread_id, turn_id, event_type, payload)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                RETURNING event_id, event_seq, thread_id, turn_id, event_type, payload, created_at
                """,
                (
                    event_id,
                    UUID(thread_id),
                    UUID(turn_id) if turn_id else None,
                    event_type,
                    _json(payload),
                ),
            )
            row = cur.fetchone()
        return self._event_row(row)

    def list_events(
        self,
        *,
        thread_id: str,
        turn_id: str | None = None,
        after_seq: int = 0,
        limit: int = 200,
    ) -> list[AgentEvent]:
        where = "thread_id = %s AND event_seq > %s"
        params: list[Any] = [UUID(thread_id), after_seq]
        if turn_id:
            where += " AND turn_id = %s"
            params.append(UUID(turn_id))
        params.append(limit)
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT event_id, event_seq, thread_id, turn_id, event_type, payload, created_at
                FROM {self._schema}.agent_events
                WHERE {where} ORDER BY event_seq LIMIT %s
                """,
                tuple(params),
            )
            rows = cur.fetchall()
        return [self._event_row(row) for row in rows]

    def list_web_domains(self) -> list[WebDomain]:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT host_pattern, enabled, created_at FROM {self._schema}.agent_web_domains ORDER BY host_pattern"
            )
            rows = cur.fetchall()
        return [WebDomain(host_pattern=row[0], enabled=bool(row[1]), created_at=row[2]) for row in rows]

    def add_web_domain(self, host_pattern: str) -> WebDomain:
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._schema}.agent_web_domains (host_pattern)
                VALUES (%s) ON CONFLICT (host_pattern) DO UPDATE SET enabled = true
                RETURNING host_pattern, enabled, created_at
                """,
                (host_pattern,),
            )
            row = cur.fetchone()
        return WebDomain(host_pattern=row[0], enabled=bool(row[1]), created_at=row[2])

    def delete_web_domain(self, host_pattern: str) -> bool:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._schema}.agent_web_domains WHERE host_pattern = %s",
                (host_pattern,),
            )
            return cur.rowcount > 0

    def set_web_domain_enabled(self, host_pattern: str, enabled: bool) -> WebDomain | None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self._schema}.agent_web_domains SET enabled = %s
                WHERE host_pattern = %s
                RETURNING host_pattern, enabled, created_at
                """,
                (enabled, host_pattern),
            )
            row = cur.fetchone()
        return WebDomain(host_pattern=row[0], enabled=bool(row[1]), created_at=row[2]) if row else None

    def is_allowed_web_host(self, hostname: str) -> bool:
        host = hostname.casefold().rstrip(".")
        domains = self.list_web_domains()
        for domain in domains:
            if not domain.enabled:
                continue
            pattern = domain.host_pattern
            if host == pattern or (pattern.startswith("*.") and host.endswith(pattern[1:])):
                return True
        return False

    def save_web_document(self, document: WebDocument) -> WebDocument:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._schema}.web_documents
                    (document_id, url, canonical_url, domain, title, text, content_hash, fetched_at, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    UUID(document.document_id),
                    document.url,
                    document.canonical_url,
                    document.domain,
                    document.title,
                    document.text,
                    document.content_hash,
                    document.fetched_at,
                    _json(document.metadata),
                ),
            )
        return document

    def latest_web_document(self, canonical_url: str, *, ttl_seconds: int = 3600) -> WebDocument | None:
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT * FROM {self._schema}.web_documents
                WHERE canonical_url = %s AND fetched_at >= now() - interval '{int(ttl_seconds)} seconds'
                ORDER BY fetched_at DESC LIMIT 1
                """,
                (canonical_url,),
            )
            row = cur.fetchone()
        return WebDocument.model_validate({**row, "document_id": str(row["document_id"])}) if row else None

    def append_monitor_report_message(
        self,
        *,
        thread_id: str,
        report: dict[str, Any],
    ) -> AgentMessage:
        report_id = str(report.get("report_id") or "")
        if report_id:
            with self._connection() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT message_id FROM {self._schema}.agent_messages
                    WHERE thread_id = %s AND kind = 'monitor_report'
                      AND payload->>'report_id' = %s
                    LIMIT 1
                    """,
                    (UUID(thread_id), report_id),
                )
                existing = cur.fetchone()
            if existing:
                message = self.get_message(str(existing[0]))
                if message is not None:
                    return message
        message_report = dict(report)
        if not settings.INVESTMENT_MONITOR_RECOMMENDATIONS_ENABLED:
            message_report["recommendation_5d"] = None
            message_report["recommendation_20d"] = None
            message_report["model_metadata"] = {
                **dict(message_report.get("model_metadata") or {}),
                "shadow_mode": True,
            }
        summary = (
            f"{message_report.get('trading_date')} · {message_report.get('symbol')} {message_report.get('name')}\n"
            f"状态：{message_report.get('status')} / {message_report.get('data_status')}\n"
            f"{message_report.get('analysis') or '未生成分析文本'}"
        )
        return self.append_message(
            thread_id=thread_id,
            role="assistant",
            kind="monitor_report",
            content=summary,
            payload={"report_id": message_report.get("report_id"), "report": message_report},
        )

    def prune_archived_checkpoints(self, *, older_than_days: int) -> int:
        """Remove LangGraph checkpoints for archived, inactive threads."""

        if older_than_days < 1:
            raise ValueError("older_than_days must be positive")
        self.ensure_schema()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT thread_id FROM {self._schema}.agent_threads
                WHERE status = 'archived'
                  AND updated_at < now() - interval '{int(older_than_days)} days'
                """
            )
            thread_ids = [str(row[0]) for row in cur.fetchall()]
            if not thread_ids:
                return 0
            cur.execute(
                f"DELETE FROM {self._schema}.checkpoint_writes WHERE thread_id = ANY(%s)",
                (thread_ids,),
            )
            cur.execute(
                f"DELETE FROM {self._schema}.checkpoint_blobs WHERE thread_id = ANY(%s)",
                (thread_ids,),
            )
            cur.execute(
                f"DELETE FROM {self._schema}.checkpoints WHERE thread_id = ANY(%s)",
                (thread_ids,),
            )
        return len(thread_ids)

    @staticmethod
    def _thread_row(row: Any) -> AgentThread:
        return AgentThread(
            thread_id=str(row[0]),
            title=str(row[1]),
            status=row[2],
            kind=row[3],
            rolling_summary=row[4] or "",
            last_seq=int(row[5]),
            unread_count=int(row[11] or 0) if len(row) > 11 else 0,
            last_message_at=row[7],
            created_at=row[8],
            updated_at=row[9],
            metadata=dict(row[10] or {}),
        )

    @staticmethod
    def _message_row(row: dict[str, Any]) -> AgentMessage:
        return AgentMessage.model_validate(
            {
                **row,
                "message_id": str(row["message_id"]),
                "thread_id": str(row["thread_id"]),
                "turn_id": str(row["turn_id"]) if row.get("turn_id") else None,
            }
        )

    @staticmethod
    def _turn_row(row: Any) -> AgentTurn:
        return AgentTurn(
            turn_id=str(row[0]),
            thread_id=str(row[1]),
            question=row[2],
            mode=row[3],
            status=row[4],
            checkpoint_id=row[5],
            budget=dict(row[6] or {}),
            stats=dict(row[7] or {}),
            error=row[8],
            created_at=row[9],
            started_at=row[10],
            finished_at=row[11],
        )

    @staticmethod
    def _approval_row(row: Any) -> AgentApproval:
        return AgentApproval(
            approval_id=str(row[0]),
            thread_id=str(row[1]),
            turn_id=str(row[2]),
            call_id=str(row[3]),
            tool_name=str(row[4]),
            proposed_action=dict(row[5] or {}),
            status=row[6],
            idempotency_key=str(row[7]),
            created_at=row[8],
            decided_at=row[9],
        )

    @staticmethod
    def _event_row(row: Any) -> AgentEvent:
        return AgentEvent(
            event_id=str(row[0]),
            event_seq=int(row[1]),
            thread_id=str(row[2]),
            turn_id=str(row[3]) if row[3] else None,
            event_type=row[4],
            payload=dict(row[5] or {}),
            created_at=row[6],
        )
