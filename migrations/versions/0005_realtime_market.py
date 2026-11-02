"""Add market rounds and source-aware quote state."""
from alembic import op

revision = "0005_realtime_market"
down_revision = "0004_realtime_quotes"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE openfinance.realtime_quotes ADD COLUMN previous_close double precision, ADD COLUMN quote_status text, ADD COLUMN round_id text, ADD COLUMN status_at timestamptz")
    op.execute("""CREATE TABLE openfinance.realtime_market_rounds (
      round_id text PRIMARY KEY, expected bigint NOT NULL, received bigint NOT NULL,
      valid bigint NOT NULL, rising bigint NOT NULL, falling bigint NOT NULL,
      flat bigint NOT NULL, failed_batches integer NOT NULL)""")
    op.execute("""CREATE TABLE openfinance.realtime_quote_status (
      symbol text PRIMARY KEY, status text NOT NULL, observed_at timestamptz NOT NULL,
      round_id text NOT NULL)""")


def downgrade():
    op.execute("DROP TABLE openfinance.realtime_quote_status")
    op.execute("DROP TABLE openfinance.realtime_market_rounds")
    op.execute("ALTER TABLE openfinance.realtime_quotes DROP COLUMN status_at, DROP COLUMN round_id, DROP COLUMN quote_status, DROP COLUMN previous_close")
