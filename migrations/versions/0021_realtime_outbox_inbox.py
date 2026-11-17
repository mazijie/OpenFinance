"""Durable publication and consumer deduplication for realtime quotes."""
from alembic import op

revision = "0021_realtime_outbox_inbox"
down_revision = "0020_news_embedding_model"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE openfinance.realtime_quote_outbox (
        event_id text PRIMARY KEY, round_id timestamptz NOT NULL, symbol text NOT NULL,
        captured_at timestamptz NOT NULL, payload jsonb NOT NULL,
        state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','published','expired')),
        created_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz,
        UNIQUE (round_id, symbol))""")
    op.execute("CREATE INDEX realtime_quote_outbox_pending ON openfinance.realtime_quote_outbox (created_at, event_id) WHERE state = 'pending'")
    op.execute("CREATE INDEX realtime_quote_outbox_pending_round ON openfinance.realtime_quote_outbox (round_id) WHERE state = 'pending'")
    op.execute("""CREATE TABLE openfinance.realtime_quote_inbox (
        topic text NOT NULL, partition_id integer NOT NULL, kafka_offset bigint NOT NULL,
        processed_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (topic, partition_id, kafka_offset))""")
    op.execute("""CREATE TABLE openfinance.realtime_quote_expired (
        event_id text PRIMARY KEY, captured_at timestamptz NOT NULL,
        observed_at timestamptz NOT NULL DEFAULT now())""")


def downgrade():
    op.execute("DROP TABLE openfinance.realtime_quote_expired")
    op.execute("DROP TABLE openfinance.realtime_quote_inbox")
    op.execute("DROP TABLE openfinance.realtime_quote_outbox")
