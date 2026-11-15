"""Create unified agent-harness tables and monitor thread linkage.

Revision ID: 0002_agent_harness
Revises: 0001_monitor
Create Date: 2026-10-26
"""

from alembic import op

from contextlib import nullcontext
from typing import Any


class HistoricalHarnessSchema:
    _schema = "openfinance"

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



revision = "0002_agent_harness"
down_revision = "0001_monitor"
branch_labels = None
depends_on = None


def upgrade() -> None:
    HistoricalHarnessSchema().ensure_schema(connection=op.get_bind().connection.driver_connection)


def downgrade() -> None:
    op.execute("ALTER TABLE openfinance.monitor_runs DROP COLUMN IF EXISTS origin_thread_id")
    for table in (
        "web_documents",
        "agent_web_domains",
        "agent_events",
        "agent_approvals",
        "agent_tool_calls",
        "agent_messages",
        "agent_turns",
        "agent_threads",
    ):
        op.execute(f"DROP TABLE IF EXISTS openfinance.{table}")

