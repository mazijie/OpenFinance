"""Manual intraday backfill runs and durable request pacing."""
from alembic import op

revision = '0017_manual_intraday'
down_revision = '0016_intraday_pause'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE openfinance.intraday_manual_runs (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, trading_day date NOT NULL,
        scope text NOT NULL CHECK (scope IN ('symbol','batch')),
        status text NOT NULL CHECK (status IN ('queued','running','completed','blocked','interrupted','failed')),
        requested_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz,
        error text)""")
    op.execute("""CREATE UNIQUE INDEX intraday_one_active_run ON openfinance.intraday_manual_runs ((true))
        WHERE status IN ('queued','running')""")
    op.execute("""CREATE TABLE openfinance.intraday_manual_items (
        run_id bigint NOT NULL REFERENCES openfinance.intraday_manual_runs(id), symbol text NOT NULL,
        status text NOT NULL DEFAULT 'queued', requested_at timestamptz, completed_at timestamptz,
        points_added integer NOT NULL DEFAULT 0, error text, PRIMARY KEY(run_id,symbol))""")
    op.execute("""CREATE TABLE openfinance.intraday_request_clock (
        provider text PRIMARY KEY, last_requested_at timestamptz NOT NULL)""")


def downgrade():
    op.execute('DROP TABLE openfinance.intraday_request_clock')
    op.execute('DROP TABLE openfinance.intraday_manual_items')
    op.execute('DROP TABLE openfinance.intraday_manual_runs')
