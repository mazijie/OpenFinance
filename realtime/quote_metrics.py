"""Persist optional quote metrics separately from Flink aggregation."""
import time

_last_cleanup = 0.0


def persist_quote_metrics(conn, rows):
    global _last_cleanup

    values = [(row["symbol"], row["source_time"], row["captured_at"],
               row.get("volume_lots"), row.get("market_cap_cny"), row.get("pe_ratio"),
               row.get("day_open"), row.get("day_high"), row.get("day_low"))
              for row in rows if row.get("source_time") and row.get("price") is not None]
    if not values:
        return
    with conn.cursor() as cursor:
        cursor.executemany("""INSERT INTO openfinance.realtime_quote_metrics
                    (symbol, source_time, captured_at, volume_lots, market_cap_cny, pe_ratio, day_open, day_high, day_low)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (symbol, source_time) DO UPDATE SET
                    source_time=EXCLUDED.source_time, captured_at=EXCLUDED.captured_at,
                    volume_lots=EXCLUDED.volume_lots, market_cap_cny=EXCLUDED.market_cap_cny,
                    pe_ratio=EXCLUDED.pe_ratio, day_open=EXCLUDED.day_open,
                    day_high=EXCLUDED.day_high, day_low=EXCLUDED.day_low
                    WHERE openfinance.realtime_quote_metrics.source_time <= EXCLUDED.source_time
                    AND openfinance.realtime_quote_metrics.captured_at <= EXCLUDED.captured_at""", values)
        if time.monotonic() - _last_cleanup > 86400:
            cursor.execute("DELETE FROM openfinance.realtime_quote_metrics WHERE captured_at < now() - interval '3 days'")
            _last_cleanup = time.monotonic()
