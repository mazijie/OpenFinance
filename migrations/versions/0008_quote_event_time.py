"""Record late event-time samples and protect recomputed windows."""
from alembic import op

revision = "0008_quote_event_time"
down_revision = "0007_quote_collection_status"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE openfinance.realtime_quote_windows ADD COLUMN recomputed_at timestamptz")
    op.execute("ALTER TABLE openfinance.realtime_quote_windows ADD COLUMN window_basis text NOT NULL DEFAULT 'legacy_processing_time'")
    op.execute("ALTER TABLE openfinance.realtime_quote_windows ADD COLUMN recompute_stale boolean NOT NULL DEFAULT false")
    op.execute("""CREATE TABLE openfinance.realtime_quote_late (
        symbol text NOT NULL, round_id text NOT NULL, captured_at timestamptz NOT NULL,
        window_start timestamp NOT NULL, price double precision, watermark_time timestamp,
        first_seen_at timestamptz NOT NULL DEFAULT now(), resolved_at timestamptz,
        PRIMARY KEY (symbol, round_id, captured_at))""")
    op.execute("CREATE INDEX ON openfinance.realtime_quote_late (window_start, symbol) WHERE resolved_at IS NULL")
    op.execute("""CREATE TABLE openfinance.realtime_quote_recomputations (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, range_start timestamptz NOT NULL,
        range_end timestamptz NOT NULL, window_count integer NOT NULL, offsets jsonb NOT NULL,
        completed_at timestamptz NOT NULL DEFAULT now())""")


def downgrade():
    op.execute("DROP TABLE openfinance.realtime_quote_recomputations")
    op.execute("DROP TABLE openfinance.realtime_quote_late")
    op.execute("ALTER TABLE openfinance.realtime_quote_windows DROP COLUMN recompute_stale")
    op.execute("ALTER TABLE openfinance.realtime_quote_windows DROP COLUMN window_basis")
    op.execute("ALTER TABLE openfinance.realtime_quote_windows DROP COLUMN recomputed_at")
