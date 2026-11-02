"""Store the upstream trading day's open, high and low alongside quote metrics."""
from alembic import op

revision = "0012_quote_daily_ohlc"
down_revision = "0011_quote_metric_history"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE openfinance.realtime_quote_metrics ADD COLUMN day_open double precision")
    op.execute("ALTER TABLE openfinance.realtime_quote_metrics ADD COLUMN day_high double precision")
    op.execute("ALTER TABLE openfinance.realtime_quote_metrics ADD COLUMN day_low double precision")


def downgrade():
    op.execute("ALTER TABLE openfinance.realtime_quote_metrics DROP COLUMN day_low")
    op.execute("ALTER TABLE openfinance.realtime_quote_metrics DROP COLUMN day_high")
    op.execute("ALTER TABLE openfinance.realtime_quote_metrics DROP COLUMN day_open")
