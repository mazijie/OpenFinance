"""Publish durable quote events; uncertain sends are retried with the same event ID."""
import json
import logging
import os
import time

import psycopg
from kafka import KafkaProducer

from realtime.collector import send_watermarks
from threading import Thread

log = logging.getLogger(__name__)


def publish_one(conn, producer):
    # Hold the row lock across send and state update. A crash after the Kafka
    # acknowledgement rolls back the DB transaction and causes a safe retry.
    with conn.transaction():
        row = conn.execute("""SELECT event_id, round_id, symbol, payload,
            captured_at <= now() - interval '24 hours' AS expired
            FROM openfinance.realtime_quote_outbox WHERE state='pending'
            ORDER BY created_at, event_id FOR UPDATE SKIP LOCKED LIMIT 1""").fetchone()
        if row is None:
            return False
        event_id, round_id, symbol, payload, expired = row
        if not expired:
            producer.send("quotes.raw", key=symbol.encode(), value=payload).get(timeout=30)
        conn.execute("""UPDATE openfinance.realtime_quote_outbox SET state=%s, completed_at=now()
            WHERE event_id=%s""", ("expired" if expired else "published", event_id))
        conn.execute("""UPDATE openfinance.realtime_collection_rounds r SET
            acknowledged=acknowledged + %s,
            status=CASE WHEN EXISTS (SELECT 1 FROM openfinance.realtime_quote_outbox
                                     WHERE round_id=%s AND state='pending') THEN 'publishing'
              WHEN r.failed_batches > 0 OR EXISTS (SELECT 1 FROM openfinance.realtime_quote_outbox
                  WHERE round_id=%s AND (state='expired' OR payload->>'status' IN ('missing','unsupported')))
              THEN 'incomplete' ELSE 'published' END, updated_at=now()
            WHERE r.round_id=%s""", (0 if expired else 1, round_id, round_id, round_id))
        if expired:
            log.warning("expired unprocessed quote event %s", event_id)
        return True


def main():
    logging.basicConfig(level=logging.INFO)
    producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                             acks="all", retries=3, enable_idempotence=True,
                             value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode())
    Thread(target=send_watermarks, args=(producer,), daemon=True, name="quote-watermark").start()
    while True:
        try:
            with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as conn:
                locked = conn.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                                      ("openfinance_quote_outbox_publisher",)).fetchone()[0]
                if not locked:
                    raise RuntimeError("another outbox publisher is already active")
                while True:
                    if not publish_one(conn, producer):
                        time.sleep(1)
        except Exception:
            log.exception("outbox publisher failed; retrying")
            time.sleep(5)


if __name__ == "__main__":
    main()
