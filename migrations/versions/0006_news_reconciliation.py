"""Record news index reconciliation without changing existing indexes."""
from alembic import op

revision = "0006_news_reconciliation"
down_revision = "0005_realtime_market"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE openfinance.news_index_reconciliation (
        id integer PRIMARY KEY CHECK (id = 1), status text NOT NULL,
        updated_at timestamptz NOT NULL, last_success_at timestamptz,
        details jsonb NOT NULL DEFAULT '{}'::jsonb, error text)""")


def downgrade():
    op.execute("DROP TABLE openfinance.news_index_reconciliation")
