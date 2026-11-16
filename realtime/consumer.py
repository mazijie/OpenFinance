"""Persist processed Kafka output, committing offsets only after database commit."""
import json
import logging
import os
import time

import psycopg

log = logging.getLogger(__name__)


def persist(conn, topic, row):
    if topic == "quotes.windows" and row.get("window_basis") != "capture_time":
        log.warning("Discarding obsolete quote window without capture-time basis")
        return
    with conn.cursor() as cur:
        if topic in ("quotes.windows", "quotes.late"):
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", ("openfinance_quote_window_recompute",))
        if topic == "quotes.latest":
            cur.execute("""INSERT INTO openfinance.realtime_quote_status (symbol, status, observed_at, round_id)
                VALUES (%s,%s,%s,%s) ON CONFLICT (symbol) DO UPDATE SET status=EXCLUDED.status,
                observed_at=EXCLUDED.observed_at, round_id=EXCLUDED.round_id
                WHERE openfinance.realtime_quote_status.observed_at <= EXCLUDED.observed_at""",
                (row["symbol"], row.get("status") or "unknown", row["captured_at"], row.get("round_id") or ""))
            if row.get("price") is None:
                cur.execute("""UPDATE openfinance.realtime_quotes SET quote_status=%s, round_id=%s, status_at=%s
                    WHERE symbol=%s AND (status_at IS NULL OR status_at <= %s)""",
                    (row.get("status"), row.get("round_id"), row["captured_at"], row["symbol"], row["captured_at"]))
                return
            cur.execute("""INSERT INTO openfinance.realtime_quotes
                (symbol, name, price, source, source_time, captured_at, previous_close, quote_status, round_id, status_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol) DO UPDATE SET name=EXCLUDED.name, price=EXCLUDED.price,
                source=EXCLUDED.source, source_time=EXCLUDED.source_time, captured_at=EXCLUDED.captured_at,
                previous_close=EXCLUDED.previous_close, quote_status=EXCLUDED.quote_status, round_id=EXCLUDED.round_id, status_at=EXCLUDED.status_at
                WHERE (openfinance.realtime_quotes.source_time IS NULL OR EXCLUDED.source_time >= openfinance.realtime_quotes.source_time)
                AND openfinance.realtime_quotes.captured_at <= EXCLUDED.captured_at
                AND (openfinance.realtime_quotes.status_at IS NULL OR openfinance.realtime_quotes.status_at <= EXCLUDED.status_at)""",
                (row["symbol"], row["name"], row["price"], row["source"], row.get("source_time"), row["captured_at"], row.get("previous_close"), row.get("status"), row.get("round_id"), row["captured_at"]))
        elif topic == "quotes.windows":
            cur.execute("""INSERT INTO openfinance.realtime_quote_windows
                (symbol, window_start, window_end, sample_min, sample_max, sample_avg, sample_count, window_basis)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (symbol, window_start)
                DO UPDATE SET
                window_end=CASE WHEN openfinance.realtime_quote_windows.recomputed_at IS NULL THEN EXCLUDED.window_end ELSE openfinance.realtime_quote_windows.window_end END,
                sample_min=CASE WHEN openfinance.realtime_quote_windows.recomputed_at IS NULL THEN EXCLUDED.sample_min ELSE openfinance.realtime_quote_windows.sample_min END,
                sample_max=CASE WHEN openfinance.realtime_quote_windows.recomputed_at IS NULL THEN EXCLUDED.sample_max ELSE openfinance.realtime_quote_windows.sample_max END,
                sample_avg=CASE WHEN openfinance.realtime_quote_windows.recomputed_at IS NULL THEN EXCLUDED.sample_avg ELSE openfinance.realtime_quote_windows.sample_avg END,
                sample_count=CASE WHEN openfinance.realtime_quote_windows.recomputed_at IS NULL THEN EXCLUDED.sample_count ELSE openfinance.realtime_quote_windows.sample_count END,
                window_basis=EXCLUDED.window_basis,
                recompute_stale=openfinance.realtime_quote_windows.recompute_stale OR
                    (openfinance.realtime_quote_windows.recomputed_at IS NOT NULL AND
                     (openfinance.realtime_quote_windows.sample_min,openfinance.realtime_quote_windows.sample_max,
                      openfinance.realtime_quote_windows.sample_avg,openfinance.realtime_quote_windows.sample_count)
                     IS DISTINCT FROM (EXCLUDED.sample_min,EXCLUDED.sample_max,EXCLUDED.sample_avg,EXCLUDED.sample_count))
                """,
                (row["symbol"], row["window_start"], row["window_end"], row["sample_min"], row["sample_max"], row["sample_avg"], row["sample_count"],
                 "capture_time"))
        elif topic == "quotes.late":
            cur.execute("""INSERT INTO openfinance.realtime_quote_late
                (symbol, round_id, captured_at, window_start, price, watermark_time)
                VALUES (%s, %s, %s, date_trunc('minute', %s::timestamptz AT TIME ZONE 'UTC'), %s, %s)
                ON CONFLICT (symbol, round_id, captured_at) DO NOTHING""",
                (row["symbol"], row["round_id"], row["captured_at"], row["captured_at"], row.get("price"), row.get("watermark_time")))
        elif topic == "quotes.market":
            cur.execute("""INSERT INTO openfinance.realtime_market_rounds
                (round_id, expected, received, valid, rising, falling, flat, failed_batches)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (round_id) DO UPDATE SET
                expected=EXCLUDED.expected, received=EXCLUDED.received, valid=EXCLUDED.valid,
                rising=EXCLUDED.rising, falling=EXCLUDED.falling, flat=EXCLUDED.flat,
                failed_batches=EXCLUDED.failed_batches
                WHERE openfinance.realtime_market_rounds.received <= EXCLUDED.received""",
                (row["round_id"], row["expected"], row["received"], row["valid"], row["rising"], row["falling"], row["flat"], row["failed_batches"]))


def main():
    from kafka import KafkaConsumer, TopicPartition
    from kafka.structs import OffsetAndMetadata

    logging.basicConfig(level=logging.INFO)
    while True:
        try:
            with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
                consumer = KafkaConsumer("quotes.latest", "quotes.windows", "quotes.market", "quotes.late", bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                    group_id="quotes-postgres-v1", enable_auto_commit=False, auto_offset_reset="earliest",
                    value_deserializer=lambda value: json.loads(value.decode()) if value is not None else None)
                try:
                    for message in consumer:
                        if message.value is None:
                            consumer.commit({TopicPartition(message.topic, message.partition): OffsetAndMetadata(message.offset + 1, "", -1)})
                            continue
                        with conn.transaction():
                            persist(conn, message.topic, message.value)
                        consumer.commit({TopicPartition(message.topic, message.partition):
                                         OffsetAndMetadata(message.offset + 1, "", -1)})
                finally:
                    consumer.close()
        except Exception:
            log.exception("consumer failed; retrying")
            time.sleep(5)


if __name__ == "__main__":
    main()
