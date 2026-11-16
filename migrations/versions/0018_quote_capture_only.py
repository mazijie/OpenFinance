"""Require capture-time quote windows after the legacy data cleanup."""
from alembic import op

revision = "0018_quote_capture_only"
down_revision = "0017_manual_intraday"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM openfinance.realtime_quote_windows WHERE window_basis <> 'capture_time') THEN
            RAISE EXCEPTION 'Legacy quote windows remain: recompute or delete them before upgrading';
        END IF;
    END $$""")
    op.execute("ALTER TABLE openfinance.realtime_quote_windows ALTER COLUMN window_basis SET DEFAULT 'capture_time'")
    op.execute("ALTER TABLE openfinance.realtime_quote_windows ADD CONSTRAINT realtime_quote_windows_capture_only CHECK (window_basis = 'capture_time')")


def downgrade():
    op.execute("ALTER TABLE openfinance.realtime_quote_windows DROP CONSTRAINT realtime_quote_windows_capture_only")
    op.execute("ALTER TABLE openfinance.realtime_quote_windows ALTER COLUMN window_basis SET DEFAULT 'legacy_processing_time'")
