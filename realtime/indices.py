"""Collect index snapshots without publishing them to stock aggregation."""
import logging
import os

from realtime.tencent import fetch_batch

log = logging.getLogger(__name__)
INDICES = ("000001.SH", "399001.SZ", "399006.SZ")


def collect_indices(minute, fetch=fetch_batch):
    import psycopg

    try:
        rows, _ = fetch(INDICES, minute)
        if not rows:
            return
        with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
            for row in rows.values():
                conn.execute("""INSERT INTO openfinance.realtime_indices
                    (symbol, name, price, previous_close, source, source_time, captured_at, quote_status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (symbol) DO UPDATE SET name=EXCLUDED.name, price=EXCLUDED.price,
                    previous_close=EXCLUDED.previous_close, source=EXCLUDED.source,
                    source_time=EXCLUDED.source_time, captured_at=EXCLUDED.captured_at,
                    quote_status=EXCLUDED.quote_status
                    WHERE openfinance.realtime_indices.source_time <= EXCLUDED.source_time
                    AND openfinance.realtime_indices.captured_at <= EXCLUDED.captured_at""",
                    (row["symbol"], row["name"], row["price"], row["previous_close"],
                     row["source"], row["source_time"], row["captured_at"], row["status"]))
    except Exception:
        log.exception("index snapshot collection failed")
