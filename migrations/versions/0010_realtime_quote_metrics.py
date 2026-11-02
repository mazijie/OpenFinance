"""Store optional metrics from the same upstream quote snapshot."""
from alembic import op

revision = "0010_quote_metrics"
down_revision = "0009_realtime_indices"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE openfinance.realtime_quote_metrics (
        symbol text PRIMARY KEY, source_time timestamptz NOT NULL,
        captured_at timestamptz NOT NULL, volume_lots double precision,
        market_cap_cny double precision, pe_ratio double precision)""")


def downgrade():
    op.execute("DROP TABLE openfinance.realtime_quote_metrics")
