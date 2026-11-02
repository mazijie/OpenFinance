"""Create investment-monitor control-plane tables.

Revision ID: 0001_monitor
Revises:
Create Date: 2026-10-25
"""

from alembic import op

from backend.monitoring.db_schema import MONITOR_METADATA

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
