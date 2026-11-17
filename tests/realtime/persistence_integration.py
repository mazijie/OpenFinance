"""Run with CONSISTENCY_TEST_DATABASE_URL against a disposable migrated database."""
import os
import unittest
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
from realtime.collector import record_round, store_round
from realtime.consumer import persist, process_message
from realtime.outbox import publish_one


@unittest.skipUnless(os.getenv("CONSISTENCY_TEST_DATABASE_URL"), "requires isolated PostgreSQL")
class PersistenceTest(unittest.TestCase):
    def test_round_rolls_back_on_invalid_event(self):
        dsn = os.environ["CONSISTENCY_TEST_DATABASE_URL"]
        instant = datetime.now(timezone.utc)
        symbol = uuid4().hex
        row = {"symbol": symbol, "round_id": instant.isoformat(), "captured_at": instant.isoformat(),
               "source_time": None, "price": None, "status": "missing"}
        try:
            record_round(dsn, instant, "collecting")
            with self.assertRaises(KeyError):
                store_round(dsn, instant, [row, {"symbol": "invalid"}], 0)
            with psycopg.connect(dsn) as conn:
                self.assertEqual(conn.execute("SELECT count(*) FROM openfinance.realtime_quote_outbox WHERE round_id=%s", (instant,)).fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT status FROM openfinance.realtime_collection_rounds WHERE round_id=%s", (instant,)).fetchone()[0], "collecting")
            store_round(dsn, instant, [row], 1)
            store_round(dsn, instant, [row], 1)
            with psycopg.connect(dsn) as conn:
                self.assertEqual(conn.execute("SELECT count(*) FROM openfinance.realtime_quote_outbox WHERE round_id=%s", (instant,)).fetchone()[0], 1)
        finally:
            with psycopg.connect(dsn) as conn:
                conn.execute("DELETE FROM openfinance.realtime_quote_outbox WHERE round_id=%s", (instant,))
                conn.execute("DELETE FROM openfinance.realtime_collection_rounds WHERE round_id=%s", (instant,))

    def test_expired_outbox_event_is_not_sent(self):
        from psycopg.types.json import Jsonb
        dsn = os.environ["CONSISTENCY_TEST_DATABASE_URL"]
        instant = datetime.now(timezone.utc) - timedelta(hours=25)
        event_id = uuid4().hex
        class Producer:
            def send(self, *args, **kwargs):
                raise AssertionError("expired event was sent")
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute("INSERT INTO openfinance.realtime_collection_rounds (round_id,status) VALUES (%s,'publishing')", (instant,))
                conn.execute("""INSERT INTO openfinance.realtime_quote_outbox
                    (event_id,round_id,symbol,captured_at,payload) VALUES (%s,%s,%s,%s,%s)""",
                    (event_id, instant, event_id, instant, Jsonb({"status": "valid"})))
                self.assertTrue(publish_one(conn, Producer()))
                self.assertEqual(conn.execute("SELECT state FROM openfinance.realtime_quote_outbox WHERE event_id=%s", (event_id,)).fetchone()[0], "expired")
                self.assertEqual(conn.execute("SELECT status FROM openfinance.realtime_collection_rounds WHERE round_id=%s", (instant,)).fetchone()[0], "incomplete")
        finally:
            with psycopg.connect(dsn) as conn:
                conn.execute("DELETE FROM openfinance.realtime_quote_outbox WHERE event_id=%s", (event_id,))
                conn.execute("DELETE FROM openfinance.realtime_collection_rounds WHERE round_id=%s", (instant,))

    def test_inbox_and_business_write_commit_together(self):
        dsn = os.environ["CONSISTENCY_TEST_DATABASE_URL"]
        topic = "test-" + uuid4().hex
        message = SimpleNamespace(topic=topic, partition=0, offset=7, value={"value": 1})
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                with patch("realtime.consumer.persist", side_effect=RuntimeError("injected")):
                    with self.assertRaises(RuntimeError):
                        process_message(conn, message)
                self.assertEqual(conn.execute("SELECT count(*) FROM openfinance.realtime_quote_inbox WHERE topic=%s", (topic,)).fetchone()[0], 0)
                with patch("realtime.consumer.persist") as persist_mock:
                    self.assertTrue(process_message(conn, message))
                    self.assertFalse(process_message(conn, message))
                    self.assertEqual(persist_mock.call_count, 1)
        finally:
            with psycopg.connect(dsn) as conn:
                conn.execute("DELETE FROM openfinance.realtime_quote_inbox WHERE topic=%s", (topic,))

    def test_outbox_retry_after_uncertain_ack(self):
        from psycopg.types.json import Jsonb
        dsn = os.environ["CONSISTENCY_TEST_DATABASE_URL"]
        event_id = uuid4().hex
        instant = datetime.now(timezone.utc)
        payload = {"event_id": event_id, "symbol": event_id, "status": "valid"}
        class Producer:
            sent = []
            fail = True
            def send(self, topic, key, value):
                self.sent.append(value)
                return self
            def get(self, timeout):
                if self.fail:
                    raise RuntimeError("injected after acknowledgement")
        producer = Producer()
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute("INSERT INTO openfinance.realtime_collection_rounds (round_id,status) VALUES (%s,'publishing')", (instant,))
                conn.execute("""INSERT INTO openfinance.realtime_quote_outbox
                    (event_id,round_id,symbol,captured_at,payload) VALUES (%s,%s,%s,%s,%s)""",
                    (event_id, instant, event_id, instant, Jsonb(payload)))
                with self.assertRaises(RuntimeError):
                    publish_one(conn, producer)
                self.assertEqual(conn.execute("SELECT state FROM openfinance.realtime_quote_outbox WHERE event_id=%s", (event_id,)).fetchone()[0], "pending")
                producer.fail = False
                self.assertTrue(publish_one(conn, producer))
                self.assertEqual(producer.sent, [payload, payload])
                self.assertEqual(conn.execute("SELECT state FROM openfinance.realtime_quote_outbox WHERE event_id=%s", (event_id,)).fetchone()[0], "published")
        finally:
            with psycopg.connect(dsn) as conn:
                conn.execute("DELETE FROM openfinance.realtime_quote_outbox WHERE event_id=%s", (event_id,))
                conn.execute("DELETE FROM openfinance.realtime_collection_rounds WHERE round_id=%s", (instant,))

    def test_legacy_window_cannot_replace_capture_time_window(self):
        dsn = os.environ["CONSISTENCY_TEST_DATABASE_URL"]
        symbol = "TEST-" + uuid4().hex
        row = {"symbol": symbol, "window_start": "2020-01-01T00:00:00", "window_end": "2020-01-01T00:01:00",
               "sample_min": 10.0, "sample_max": 10.0, "sample_avg": 10.0, "sample_count": 1}
        try:
            with psycopg.connect(dsn) as conn:
                persist(conn, "quotes.windows", row)
                self.assertIsNone(conn.execute("SELECT window_basis FROM openfinance.realtime_quote_windows WHERE symbol=%s", (symbol,)).fetchone())
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
