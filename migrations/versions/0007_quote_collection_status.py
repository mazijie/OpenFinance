"""Keep observable collection and Kafka publishing gaps."""
from alembic import op

revision = "0007_quote_collection_status"
down_revision = "0006_news_reconciliation"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE openfinance.realtime_collection_rounds (
        round_id timestamptz PRIMARY KEY, status text NOT NULL, expected integer,
        failed_batches integer, acknowledged integer NOT NULL DEFAULT 0,
        error text, updated_at timestamptz NOT NULL DEFAULT now())""")


def downgrade():
    op.execute("DROP TABLE openfinance.realtime_collection_rounds")
