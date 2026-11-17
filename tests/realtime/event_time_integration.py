"""Exercise event-time windows against disposable Kafka, Flink and PostgreSQL."""
import json
import os
import time
import unittest
from datetime import datetime, timedelta, timezone

import psycopg
from kafka import KafkaConsumer, KafkaProducer
from realtime.recompute import aggregate, read_snapshot, write_results
from realtime.consumer import persist

UTC = timezone.utc
BASE = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=5)
ROUND1 = BASE.isoformat()
ROUND2 = (BASE + timedelta(minutes=1)).isoformat()
ROUND3 = (BASE + timedelta(minutes=2)).isoformat()


@unittest.skipUnless(os.getenv("CONSISTENCY_ISOLATED_KAFKA") == "1", "requires isolated services")
class EventTimeIntegration(unittest.TestCase):
    def test_out_of_order_idle_close_late_and_recompute(self):
        bootstrap, dsn = os.environ["KAFKA_BOOTSTRAP"], os.environ["DATABASE_URL"]
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", enable_idempotence=True,
                                 value_serializer=lambda row: json.dumps(row).encode())
        def quote(seconds, price, round_id):
            captured = (BASE + timedelta(seconds=seconds)).isoformat()
            row = {"symbol": "TEST.SIM", "name": "fixture", "price": price, "previous_close": 10.0,
                   "source": "mock", "source_time": captured, "captured_at": captured,
                   "status": "valid", "round_id": round_id, "event_id": round_id + "/TEST.SIM", "expected": 1, "failed_batches": 0}
            producer.send("quotes.raw", key=b"TEST.SIM", value=row).get(timeout=20)
        def heartbeat(seconds):
            for part in producer.partitions_for("quotes.raw"):
                producer.send("quotes.raw", partition=part, key=b"__watermark__",
                              value={"symbol": "__watermark__", "captured_at": (BASE + timedelta(seconds=seconds)).isoformat()}).get(timeout=20)
        def wait_for(sql, params, predicate):
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                with psycopg.connect(dsn) as conn:
                    rows = conn.execute(sql, params).fetchall()
                if predicate(rows): return rows
                time.sleep(1)
            self.fail(f"timed out waiting for {sql}: {rows}")
        try:
            quote(10, 10.0, ROUND1)
            quote(10, 10.0, ROUND1)
            heartbeat(140)  # watermark is BASE+20 seconds
            time.sleep(3)
            quote(40, 14.0, ROUND2)  # out of order, within tolerance
            heartbeat(190)  # closes the 00:00 window with no later real quote
            rows = wait_for("SELECT sample_min,sample_max,sample_avg,sample_count FROM openfinance.realtime_quote_windows WHERE symbol=%s AND window_start=%s",
                            ("TEST.SIM", BASE.replace(tzinfo=None)), lambda value: bool(value))
            self.assertEqual(rows[0], (10.0, 14.0, 12.0, 2))
            quote(10, 10.0, ROUND1)  # replayed duplicate after watermark
            quote(30, 20.0, ROUND3)
            late = wait_for("SELECT round_id FROM openfinance.realtime_quote_late WHERE symbol=%s AND resolved_at IS NULL", ("TEST.SIM",), bool)
            self.assertEqual(late, [(ROUND3,)])
            raw = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False, group_id=None)
            try: messages, offsets = read_snapshot(raw, BASE)
            finally: raw.close()
            samples, identities = aggregate(messages, BASE, BASE + timedelta(minutes=1))
            write_results(dsn, samples, identities, BASE, BASE + timedelta(minutes=1), offsets)
            with psycopg.connect(dsn) as conn:
                row = conn.execute("SELECT sample_min,sample_max,sample_avg,sample_count,recomputed_at FROM openfinance.realtime_quote_windows WHERE symbol=%s AND window_start=%s", ("TEST.SIM", BASE.replace(tzinfo=None))).fetchone()
                self.assertEqual(row[:4], (10.0, 20.0, 44.0/3, 3))
                self.assertIsNotNone(row[4])
                self.assertEqual(conn.execute("SELECT count(*) FROM openfinance.realtime_quote_late WHERE symbol=%s AND resolved_at IS NULL", ("TEST.SIM",)).fetchone()[0], 0)
                persist(conn, "quotes.windows", {"symbol": "TEST.SIM", "window_start": BASE.isoformat(),
                    "window_end": (BASE + timedelta(minutes=1)).isoformat(), "sample_min": 10.0,
                    "sample_max": 14.0, "sample_avg": 12.0, "sample_count": 2, "window_basis": "capture_time"})
                self.assertEqual(conn.execute("SELECT sample_count,recompute_stale FROM openfinance.realtime_quote_windows WHERE symbol=%s AND window_start=%s", ("TEST.SIM", BASE.replace(tzinfo=None))).fetchone(), (3, True))
        finally:
            producer.close()


if __name__ == "__main__":
    unittest.main()
