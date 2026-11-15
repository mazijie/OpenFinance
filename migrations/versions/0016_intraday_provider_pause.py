"""Persist a provider-wide pause when Tencent blocks requests."""
from alembic import op

revision = "0016_intraday_pause"
down_revision = "0015_intraday_backfill"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE openfinance.intraday_provider_pause (
        provider text PRIMARY KEY, paused_until timestamptz NOT NULL,
        reason text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())""")


def downgrade():
    op.execute("DROP TABLE openfinance.intraday_provider_pause")
