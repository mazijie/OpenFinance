"""Run with CONSISTENCY_TEST_DATABASE_URL against a disposable migrated database."""
import os
import unittest
from datetime import datetime, timezone
from uuid import uuid4

import psycopg
from realtime.collector import record_round
from realtime.consumer import persist


@unittest.skipUnless(os.getenv("CONSISTENCY_TEST_DATABASE_URL"), "requires isolated PostgreSQL")
class PersistenceTest(unittest.TestCase):
    def test_legacy_window_cannot_replace_capture_time_window(self):
        dsn = os.environ["CONSISTENCY_TEST_DATABASE_URL"]
        symbol = "TEST-" + uuid4().hex
        row = {"symbol": symbol, "window_start": "2020-01-01T00:00:00", "window_end": "2020-01-01T00:01:00",
               "sample_min": 10.0, "sample_max": 10.0, "sample_avg": 10.0, "sample_count": 1}
        try:
            with psycopg.connect(dsn) as conn:
                persist(conn, "quotes.windows", row)
                self.assertEqual(conn.execute("SELECT window_basis FROM openfinance.realtime_quote_windows WHERE symbol=%s", (symbol,)).fetchone()[0], "legacy_processing_time")
                persist(conn, "quotes.windows", {**row, "sample_avg": 12.0, "window_basis": "capture_time"})
                persist(conn, "quotes.windows", row)
                self.assertEqual(conn.execute("SELECT window_basis,sample_avg FROM openfinance.realtime_quote_windows WHERE symbol=%s", (symbol,)).fetchone(), ("capture_time", 12.0))
        finally:
            with psycopg.connect(dsn) as conn:
                conn.execute("DELETE FROM openfinance.realtime_quote_windows WHERE symbol=%s", (symbol,))

    def test_database_replay_and_visible_failed_round(self):
        dsn = os.environ["CONSISTENCY_TEST_DATABASE_URL"]
        symbol = "TEST-" + uuid4().hex
        instant = datetime.now(timezone.utc)
        row = {"symbol": symbol, "name": "test", "price": 12.0, "source": "mock",
               "source_time": instant.isoformat(), "captured_at": instant.isoformat(),
               "previous_close": 10.0, "status": "valid", "round_id": instant.isoformat()}
        try:
            with psycopg.connect(dsn) as conn:
                # Emulate process death after database commit but before Kafka offset commit.
                with conn.transaction(): persist(conn, "quotes.latest", row)
                with conn.transaction(): persist(conn, "quotes.latest", row)
                result = conn.execute("SELECT count(*), max(price) FROM openfinance.realtime_quotes WHERE symbol=%s", (symbol,)).fetchone()
                self.assertEqual(result, (1, 12.0))
            record_round(dsn, instant, "failed", expected=2, acknowledged=1, error="injected send failure")
            with psycopg.connect(dsn) as conn:
                result = conn.execute("SELECT status, expected, acknowledged, error FROM openfinance.realtime_collection_rounds WHERE round_id=%s", (instant,)).fetchone()
                self.assertEqual(result, ("failed", 2, 1, "injected send failure"))
        finally:
            with psycopg.connect(dsn) as conn:
                conn.execute("DELETE FROM openfinance.realtime_quote_status WHERE symbol=%s", (symbol,))
                conn.execute("DELETE FROM openfinance.realtime_quotes WHERE symbol=%s", (symbol,))
                conn.execute("DELETE FROM openfinance.realtime_collection_rounds WHERE round_id=%s", (instant,))


if __name__ == "__main__":
    unittest.main()
