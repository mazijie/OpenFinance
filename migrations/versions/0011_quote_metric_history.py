"""Keep overlapping quote metrics while Flink processes the next snapshot."""
from alembic import op

revision = "0011_quote_metric_history"
down_revision = "0010_quote_metrics"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE openfinance.realtime_quote_metrics DROP CONSTRAINT realtime_quote_metrics_pkey")
    op.execute("ALTER TABLE openfinance.realtime_quote_metrics ADD PRIMARY KEY (symbol, source_time)")
    op.execute("CREATE INDEX ON openfinance.realtime_quote_metrics (captured_at)")


def downgrade():
    op.execute("DROP INDEX openfinance.realtime_quote_metrics_captured_at_idx")
    op.execute("DELETE FROM openfinance.realtime_quote_metrics a USING openfinance.realtime_quote_metrics b WHERE a.symbol=b.symbol AND a.source_time < b.source_time")
    op.execute("ALTER TABLE openfinance.realtime_quote_metrics DROP CONSTRAINT realtime_quote_metrics_pkey")
    op.execute("ALTER TABLE openfinance.realtime_quote_metrics ADD PRIMARY KEY (symbol)")
