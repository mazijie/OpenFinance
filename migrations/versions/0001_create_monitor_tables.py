"""Create investment-monitor control-plane tables.

Revision ID: 0001_monitor
Revises:
Create Date: 2026-10-25
"""

from alembic import op

"""SQLAlchemy metadata for monitor control-plane tables."""


from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID


MONITOR_METADATA = MetaData(schema="openfinance")

WATCHLIST_TABLE = Table(
    "watchlist_items",
    MONITOR_METADATA,
    Column("symbol", String(16), primary_key=True),
    Column("security_id", String(32), nullable=False),
    Column("name", String(256), nullable=False),
    Column("exchange", String(16), nullable=False),
    Column("enabled", Boolean, nullable=False, server_default=text("true")),
    Column("price_move_threshold", Float, nullable=False, server_default=text("0.03")),
    Column("volume_ratio_threshold", Float, nullable=False, server_default=text("1.8")),
    Column("note", Text),
    Column("version", Integer, nullable=False, server_default=text("1")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
)

MONITOR_RUNS_TABLE = Table(
    "monitor_runs",
    MONITOR_METADATA,
    Column("run_id", UUID(as_uuid=True), primary_key=True),
    Column("run_key", Text, nullable=False, unique=True),
    Column("trading_date", Date, nullable=False),
    Column("cutoff_at", DateTime(timezone=True), nullable=False),
    Column("status", String(24), nullable=False, server_default=text("'queued'")),
    Column("watchlist_version", Text, nullable=False),
    Column("config", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("stats", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("error", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("heartbeat_at", DateTime(timezone=True)),
    Column("origin_thread_id", UUID(as_uuid=True)),
)

MONITOR_REPORTS_TABLE = Table(
    "monitor_reports",
    MONITOR_METADATA,
    Column("report_id", UUID(as_uuid=True), primary_key=True),
    Column("run_id", UUID(as_uuid=True), nullable=False),
    Column("symbol", String(16), nullable=False),
    Column("name", String(256), nullable=False),
    Column("trading_date", Date, nullable=False),
    Column("data_cutoff_at", DateTime(timezone=True), nullable=False),
    Column("data_status", String(16), nullable=False),
    Column("status", String(16), nullable=False),
    Column("events", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("citations", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("recommendation_5d", JSONB),
    Column("recommendation_20d", JSONB),
    Column("analysis", Text, nullable=False, server_default=text("''")),
    Column("risk_notes", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("missing_data", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("model_metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("generated_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    UniqueConstraint("symbol", "data_cutoff_at", name="monitor_reports_symbol_cutoff_key"),
)

MONITOR_AGENT_TRACES_TABLE = Table(
    "monitor_agent_traces",
    MONITOR_METADATA,
    Column("trace_id", UUID(as_uuid=True), primary_key=True),
    Column("run_id", UUID(as_uuid=True), nullable=False),
    Column("symbol", String(16), nullable=False),
    Column("status", String(24), nullable=False),
    Column("tool_calls", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("latency_ms", Integer, nullable=False, server_default=text("0")),
    Column("error_stage", String(64)),
    Column("error", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
)


revision = "0001_monitor"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    op.execute("CREATE SCHEMA IF NOT EXISTS openfinance")
    MONITOR_METADATA.create_all(bind=bind)


def downgrade() -> None:
    bind = op.get_bind()
    MONITOR_METADATA.drop_all(bind=bind)
