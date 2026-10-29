"""Create unified agent-harness tables and monitor thread linkage.

Revision ID: 0002_agent_harness
Revises: 0001_monitor
Create Date: 2026-10-26
"""

from alembic import op

from backend.harness.store import HarnessStore

revision = "0002_agent_harness"
down_revision = "0001_monitor"
branch_labels = None
depends_on = None


def upgrade() -> None:
    HarnessStore().ensure_schema()


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

