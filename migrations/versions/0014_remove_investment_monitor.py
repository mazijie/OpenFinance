"""Remove investment monitoring and clear all existing agent sessions.

This migration deletes monitor data and conversation history irreversibly.
"""
from alembic import op

revision = "0014_remove_monitor"
down_revision = "0013_market_history_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # LangGraph uses the database search path (usually public); older deployments
    # may have configured openfinance. Check each table independently.
    for schema in ("public", "openfinance"):
        for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
            op.execute(f"""DO $$ BEGIN
                IF to_regclass('{schema}.{table}') IS NOT NULL THEN
                    DELETE FROM {schema}.{table}
                    WHERE thread_id IN (SELECT thread_id::text FROM openfinance.agent_threads);
                END IF;
            END $$""")
    op.execute("DELETE FROM openfinance.agent_events")
    op.execute("DELETE FROM openfinance.agent_threads")
    for table in ("monitor_agent_traces", "monitor_reports", "monitor_runs", "watchlist_items"):
        op.execute(f"DROP TABLE IF EXISTS openfinance.{table}")
    op.execute("ALTER TABLE openfinance.agent_messages DROP CONSTRAINT IF EXISTS agent_messages_kind_check")
    op.execute("ALTER TABLE openfinance.agent_messages ADD CONSTRAINT agent_messages_kind_check CHECK (kind IN ('text','tool','approval','system'))")


def downgrade() -> None:
    raise RuntimeError("Monitor data and conversation history cannot be restored by downgrade")
