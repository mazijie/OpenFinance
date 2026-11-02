"""Run only against an explicitly configured disposable database."""
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
import psycopg

from backend.services.realtime_quote_service import RealtimeQuoteService


@unittest.skipUnless(os.getenv("CONSISTENCY_TEST_DATABASE_URL"), "requires isolated PostgreSQL")
class QuoteServiceIntegration(unittest.TestCase):
    def test_late_markers_and_legacy_windows(self):
        dsn = os.environ["CONSISTENCY_TEST_DATABASE_URL"]
        symbol = "098765.SH"
        now = datetime.now(timezone.utc)
        window = (now - timedelta(minutes=3)).replace(second=0, microsecond=0, tzinfo=None)
        try:
            with psycopg.connect(dsn) as conn:
                conn.execute("INSERT INTO openfinance.realtime_quotes (symbol,name,price,source,source_time,captured_at,quote_status) VALUES (%s,'test',10,'mock',%s,%s,'valid')", (symbol, now, now))
                conn.execute("""INSERT INTO openfinance.realtime_quote_windows
                    (symbol,window_start,window_end,sample_min,sample_max,sample_avg,sample_count,window_basis)
                    VALUES (%s,%s,%s,10,10,10,1,'legacy_processing_time')""", (symbol, window, window + timedelta(minutes=1)))
            with patch("backend.services.realtime_quote_service.trading_day", return_value=True):
                self.assertIsNone(RealtimeQuoteService().lookup(symbol, now=now)["quote"]["window_start"])
            with psycopg.connect(dsn) as conn:
                conn.execute("UPDATE openfinance.realtime_quote_windows SET window_basis='capture_time' WHERE symbol=%s", (symbol,))
                conn.execute("""INSERT INTO openfinance.realtime_quote_late
                    (symbol,round_id,captured_at,window_start,price,first_seen_at) VALUES (%s,'late',%s,%s,10,%s)""", (symbol, window.replace(tzinfo=timezone.utc), window, now - timedelta(seconds=1)))
            with patch("backend.services.realtime_quote_service.trading_day", return_value=True):
                result = RealtimeQuoteService().lookup(symbol, now=now)
            self.assertTrue(result["quote"]["window_incomplete"])
            self.assertEqual(len(result["late_windows"]), 1)
            with psycopg.connect(dsn) as conn:
                conn.execute("UPDATE openfinance.realtime_quote_late SET resolved_at=%s WHERE symbol=%s", (now - timedelta(milliseconds=1), symbol))
                conn.execute("UPDATE openfinance.realtime_quote_windows SET recompute_stale=true WHERE symbol=%s", (symbol,))
            with patch("backend.services.realtime_quote_service.trading_day", return_value=True):
                self.assertTrue(RealtimeQuoteService().lookup(symbol, now=now)["quote"]["window_incomplete"])
        finally:
            with psycopg.connect(dsn) as conn:
                conn.execute("DELETE FROM openfinance.realtime_quote_late WHERE symbol=%s", (symbol,))
                conn.execute("DELETE FROM openfinance.realtime_quote_windows WHERE symbol=%s", (symbol,))
                conn.execute("DELETE FROM openfinance.realtime_quotes WHERE symbol=%s", (symbol,))


if __name__ == "__main__": unittest.main()
