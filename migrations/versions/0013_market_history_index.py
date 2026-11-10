"""Store a snapshot-bound index of the earliest visible market history."""
from alembic import op

revision = "0013_market_history_index"
down_revision = "0012_quote_daily_ohlc"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE openfinance.market_history_index_state (
        singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
        table_uuid uuid NOT NULL, snapshot_id bigint NOT NULL,
        valid_from timestamptz NOT NULL, valid_until timestamptz,
        published_at timestamptz NOT NULL DEFAULT now()
    )""")
    op.execute("""CREATE TABLE openfinance.market_history_index (
        symbol text NOT NULL, adjustment_type text NOT NULL,
        earliest_date date,
        PRIMARY KEY (symbol, adjustment_type)
    )""")


def downgrade():
    op.execute("DROP TABLE openfinance.market_history_index")
    op.execute("DROP TABLE openfinance.market_history_index_state")
