"""Store index snapshots separately from the A-share stock universe."""
from alembic import op

revision = "0009_realtime_indices"
down_revision = "0008_quote_event_time"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE openfinance.realtime_indices (
        symbol text PRIMARY KEY, name text NOT NULL, price double precision NOT NULL,
        previous_close double precision NOT NULL, source text NOT NULL,
        source_time timestamptz NOT NULL, captured_at timestamptz NOT NULL,
        quote_status text NOT NULL)""")


def downgrade():
    op.execute("DROP TABLE openfinance.realtime_indices")
