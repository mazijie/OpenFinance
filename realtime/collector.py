"""Poll AKShare snapshots and publish configured symbols to Kafka."""
import json
import logging
import math
import os
import time
from datetime import datetime, timezone, timedelta
from multiprocessing import get_context
from threading import Thread
from realtime.tencent import CHINA, collect_round
from realtime.indices import collect_indices
from realtime.quote_metrics import persist_quote_metrics

log = logging.getLogger(__name__)


def scheduled_minute(now, last_minute):
    minute = now.replace(second=0, microsecond=0)
    if minute == last_minute or now.second > 2:
        return None
    return minute


class ProviderRateLimitError(RuntimeError):
    """The upstream provider requested a pause in collection."""


def normalize(rows, symbols, captured_at):
    result = []
    for row in rows:
        symbol = str(row.get("代码", "")).zfill(6)
        if symbol not in symbols:
            continue
        try:
            price = float(row.get("最新价"))
            if not math.isfinite(price) or price <= 0:
                continue
        except (TypeError, ValueError):
            continue
        result.append({"symbol": symbol, "name": str(row.get("名称") or ""),
                       "price": price, "source": "akshare_eastmoney",
                       "source_time": None, "captured_at": captured_at})
    return result


def fetch(queue, provider):
    try:
        import akshare as ak
        fn = getattr(ak, {"eastmoney": "stock_zh_a_spot_em", "tencent": "stock_zh_a_spot_tx",
                          "sina": "stock_zh_a_spot"}[provider])
        frame = fn()
        queue.put((True, frame.to_dict(orient="records"), list(frame.columns)))
    except Exception as exc:
        limited = "ak" in locals() and isinstance(exc, ak.RateLimitError)
        queue.put((False, f"{type(exc).__name__}: {exc}", limited))


def timed_fetch(provider, timeout):
    ctx = get_context("spawn")
    queue = ctx.Queue()
    process = ctx.Process(target=fetch, args=(queue, provider))
    process.start()
    try:
        # Read before join: a whole-market dataframe can exceed the pipe buffer.
        result = queue.get(timeout=timeout)
    finally:
        if process.is_alive():
            process.terminate()
        process.join()
        queue.close()
    if not result[0]:
        raise (ProviderRateLimitError if result[2] else RuntimeError)(result[1])
    return result[1], result[2]


def record_round(dsn, minute, status, *, expected=None, failed_batches=None, acknowledged=0, error=None):
    import psycopg
    with psycopg.connect(dsn) as conn:
        conn.execute("""INSERT INTO openfinance.realtime_collection_rounds
            (round_id,status,expected,failed_batches,acknowledged,error,updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,now()) ON CONFLICT (round_id) DO UPDATE SET
            status=EXCLUDED.status, expected=EXCLUDED.expected, failed_batches=EXCLUDED.failed_batches,
            acknowledged=EXCLUDED.acknowledged, error=EXCLUDED.error, updated_at=now()""",
            (minute, status, expected, failed_batches, acknowledged, error))


def send_watermark_once(producer, now=None):
    partitions = producer.partitions_for("quotes.raw")
    if not partitions:
        return
    captured = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    futures = [producer.send("quotes.raw", partition=partition, key=b"__watermark__",
                             value={"symbol": "__watermark__", "captured_at": captured})
               for partition in sorted(partitions)]
    for future in futures: future.get(timeout=30)


def send_watermarks(producer, *, interval=15):
    """Advance each Kafka split during idle periods, without contacting the provider."""
    while True:
        try:
            send_watermark_once(producer)
        except Exception:
            log.exception("failed to publish watermark heartbeat")
        time.sleep(interval)


def main():
    from kafka import KafkaProducer
    import psycopg
    import requests
    logging.basicConfig(level=logging.INFO)
    mode = os.getenv("QUOTE_MODE", "live")
    if mode not in ("live", "mock"):
        raise ValueError("invalid collector configuration")
    producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                             acks="all", retries=3, enable_idempotence=True,
                             value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode())
    Thread(target=send_watermarks, args=(producer,), daemon=True, name="quote-watermark").start()
    last_minute = None
    index_thread = None
    while True:
        now = datetime.now(CHINA)
        minute = scheduled_minute(now, last_minute)
        if minute is None:
            time.sleep(0.5)
            continue
        last_minute = minute
        started = False
        acknowledged = 0
        expected = None
        failed_batches = None
        try:
            if mode == "live":
                if not ((9, 30) <= (now.hour, now.minute) <= (11, 30) or (13, 0) <= (now.hour, now.minute) <= (15, 0)):
                    continue
            record_round(os.environ["DATABASE_URL"], minute, "collecting")
            started = True
            if mode == "live":
                calendar = requests.get(os.environ["CALENDAR_URL"], params={"trading_date": now.date().isoformat()},
                    headers={"X-Openfinance-Token": os.environ["OPENFINANCE_INTERNAL_API_TOKEN"]}, timeout=15)
                calendar.raise_for_status()
                if calendar.json().get("is_open") is not True:
                    log.warning("calendar closed or unavailable; no upstream request")
                    record_round(os.environ["DATABASE_URL"], minute, "calendar_not_open_or_unknown")
                    continue
                if index_thread is None or not index_thread.is_alive():
                    index_thread = Thread(target=collect_indices, args=(minute,), daemon=True)
                    index_thread.start()
                with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
                    row = conn.execute("SELECT symbols FROM openfinance.a_share_universe_cache WHERE jsonb_array_length(symbols)>0 ORDER BY as_of_date DESC, updated_at DESC LIMIT 1").fetchone()
                if not row:
                    raise RuntimeError("security universe cache is empty")
                items = collect_round(row[0], minute)
            else:
                symbols = [s.strip().upper() for s in os.getenv("QUOTE_SYMBOLS", "600519.SH,000001.SZ").split(",")]
                def mock_fetch(batch, instant):
                    return ({symbol: {"symbol": symbol, "name": "模拟数据", "price": 10.0+i,
                        "previous_close": 10.0, "source": "mock", "source_time": instant.astimezone(timezone.utc).isoformat(),
                        "captured_at": datetime.now(timezone.utc).isoformat(), "status": "valid"}
                        for i, symbol in enumerate(batch)}, [])
                if index_thread is None or not index_thread.is_alive():
                    index_thread = Thread(target=collect_indices, args=(minute, mock_fetch), daemon=True)
                    index_thread.start()
                items = collect_round(symbols, minute, fetch=mock_fetch)
            items = list(items)
            persist_quote_metrics(items)
            expected = len(items)
            failed_batches = max((item.get("failed_batches", 0) for item in items), default=0)
            record_round(os.environ["DATABASE_URL"], minute, "publishing", expected=expected, failed_batches=failed_batches)
            pending = []
            for item in items:
                pending.append(producer.send("quotes.raw", key=item["symbol"].encode(), value=item))
            for future in pending:
                future.get(timeout=30)
                acknowledged += 1
            status = "incomplete" if failed_batches or any(item.get("status") in ("missing", "unsupported") for item in items) else "published"
            record_round(os.environ["DATABASE_URL"], minute, status, expected=expected,
                         failed_batches=failed_batches, acknowledged=acknowledged)
            log.info("published round %s records=%d", minute.isoformat(), len(pending))
        except Exception as exc:
            log.exception("snapshot collection failed; next round will be attempted")
            if started:
                try:
                    record_round(os.environ["DATABASE_URL"], minute, "failed", expected=expected,
                                 failed_batches=failed_batches, acknowledged=acknowledged,
                                 error=f"{type(exc).__name__}: {exc}"[:1000])
                except Exception:
                    log.exception("could not persist collection gap")


if __name__ == "__main__":
    main()
