"""Add optional streaming quote read models."""
from alembic import op

revision = "0004_realtime_quotes"
down_revision = "0003_annual_report_state"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE openfinance.realtime_quotes (
        symbol text PRIMARY KEY, name text NOT NULL, price double precision NOT NULL,
        source text NOT NULL, source_time timestamptz, captured_at timestamptz NOT NULL)""")
    op.execute("""CREATE TABLE openfinance.realtime_quote_windows (
        symbol text NOT NULL, window_start timestamp NOT NULL, window_end timestamp NOT NULL,
        sample_min double precision NOT NULL, sample_max double precision NOT NULL,
        sample_avg double precision NOT NULL, sample_count bigint NOT NULL,
        PRIMARY KEY (symbol, window_start))""")


def downgrade():
    op.execute("DROP TABLE openfinance.realtime_quote_windows")
    op.execute("DROP TABLE openfinance.realtime_quotes")
