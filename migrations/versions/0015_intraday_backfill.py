"""Persist intraday backfill data, daily universe, and resumable jobs."""
from alembic import op

revision = "0015_intraday_backfill"
down_revision = "0014_remove_monitor"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE openfinance.intraday_universe (
        trading_day date NOT NULL, symbol text NOT NULL, PRIMARY KEY (trading_day, symbol))""")
    op.execute("""CREATE TABLE openfinance.intraday_backfill_points (
        symbol text NOT NULL, window_start timestamp NOT NULL, price double precision NOT NULL CHECK (price > 0),
        source text NOT NULL DEFAULT 'tencent_minute', fetched_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (symbol, window_start))""")
    op.execute("""CREATE TABLE openfinance.intraday_backfill_jobs (
        trading_day date NOT NULL, symbol text NOT NULL, status text NOT NULL DEFAULT 'pending',
        priority_at timestamptz, next_attempt_at timestamptz NOT NULL DEFAULT now(),
        attempts integer NOT NULL DEFAULT 0, last_error text, updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (trading_day, symbol))""")
    op.execute("CREATE INDEX ON openfinance.intraday_backfill_jobs (trading_day, status, next_attempt_at, priority_at)")
    op.execute("""CREATE TABLE openfinance.intraday_scan_progress (
        trading_day date PRIMARY KEY, scanned_through timestamp NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())""")
    op.execute("""CREATE FUNCTION openfinance.invalidate_intraday_scan() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            DELETE FROM openfinance.intraday_scan_progress
              WHERE trading_day=((OLD.window_start AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai')::date;
            RETURN OLD;
        END $$""")
    op.execute("""CREATE TRIGGER intraday_window_deleted AFTER DELETE ON openfinance.realtime_quote_windows
        FOR EACH ROW EXECUTE FUNCTION openfinance.invalidate_intraday_scan()""")
    op.execute("""CREATE TRIGGER intraday_backfill_deleted AFTER DELETE ON openfinance.intraday_backfill_points
        FOR EACH ROW EXECUTE FUNCTION openfinance.invalidate_intraday_scan()""")
    op.execute("""CREATE FUNCTION openfinance.invalidate_stale_intraday_scan() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.recompute_stale AND NOT OLD.recompute_stale THEN
                DELETE FROM openfinance.intraday_scan_progress
                  WHERE trading_day=((NEW.window_start AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai')::date;
            END IF;
            RETURN NEW;
        END $$""")
    op.execute("""CREATE TRIGGER intraday_window_stale AFTER UPDATE OF recompute_stale ON openfinance.realtime_quote_windows
        FOR EACH ROW EXECUTE FUNCTION openfinance.invalidate_stale_intraday_scan()""")


def downgrade():
    op.execute("DROP TRIGGER intraday_window_stale ON openfinance.realtime_quote_windows")
    op.execute("DROP TRIGGER intraday_backfill_deleted ON openfinance.intraday_backfill_points")
    op.execute("DROP TRIGGER intraday_window_deleted ON openfinance.realtime_quote_windows")
    op.execute("DROP FUNCTION openfinance.invalidate_stale_intraday_scan()")
    op.execute("DROP FUNCTION openfinance.invalidate_intraday_scan()")
    op.execute("DROP TABLE openfinance.intraday_scan_progress")
    op.execute("DROP TABLE openfinance.intraday_backfill_jobs")
    op.execute("DROP TABLE openfinance.intraday_backfill_points")
    op.execute("DROP TABLE openfinance.intraday_universe")
