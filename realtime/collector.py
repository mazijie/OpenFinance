"""Poll AKShare snapshots and publish configured symbols to Kafka."""
import logging
import math
import os
import time
from datetime import datetime, timezone, timedelta
from multiprocessing import get_context
from threading import Lock, Thread
from realtime.tencent import CHINA, collect_round
from realtime.indices import collect_indices
from realtime.quote_metrics import persist_quote_metrics
from realtime.intraday_backfill import record_universe

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


class IndexCollector:
    def __init__(self):
        self.lock = Lock()
        self.thread = None

    def start(self, minute, fetch=None):
        with self.lock:
            if self.thread is not None and self.thread.is_alive():
                return
            args = (minute,) if fetch is None else (minute, fetch)
            self.thread = Thread(target=collect_indices, args=args, daemon=True)
            self.thread.start()


def store_round(dsn, minute, items, failed_batches):
    """Atomically persist a round, its metrics, and its Kafka outbox entries."""
    import psycopg
    from psycopg.types.json import Jsonb

    inserted = []
    with psycopg.connect(dsn) as conn:
        for item in items:
            item = dict(item)
            item["event_id"] = item["round_id"] + "/" + item["symbol"]
            result = conn.execute("""INSERT INTO openfinance.realtime_quote_outbox
                (event_id,round_id,symbol,captured_at,payload) VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (round_id,symbol) DO NOTHING RETURNING event_id""",
                (item["event_id"], item["round_id"], item["symbol"], item["captured_at"], Jsonb(item)))
            if result.fetchone():
                inserted.append(item)
        persist_quote_metrics(conn, inserted)
        conn.execute("""UPDATE openfinance.realtime_collection_rounds SET
            status=CASE WHEN s.pending > 0 THEN 'publishing'
              WHEN s.expired > 0 OR s.missing > 0 OR %s > 0 THEN 'incomplete'
              ELSE 'published' END,
            expected=%s, failed_batches=%s, acknowledged=s.published,
            error=NULL, updated_at=now()
            FROM (SELECT count(*) FILTER (WHERE state='published') AS published,
              count(*) FILTER (WHERE state='pending') AS pending,
              count(*) FILTER (WHERE state='expired') AS expired,
              count(*) FILTER (WHERE payload->>'status' IN ('missing','unsupported')) AS missing
              FROM openfinance.realtime_quote_outbox WHERE round_id=%s) s
            WHERE round_id=%s""", (failed_batches, len(items), failed_batches, minute, minute))


def collect_quote_round(minute, mode, producer, start_indices):
    import requests
    started = False
    acknowledged = 0
    expected = None
    failed_batches = None
    try:
        if mode == "live":
            if not ((9, 30) <= (minute.hour, minute.minute) <= (11, 30) or (13, 0) <= (minute.hour, minute.minute) <= (15, 0)):
                return
        record_round(os.environ["DATABASE_URL"], minute, "collecting")
        started = True
        if mode == "live":
            calendar = requests.get(os.environ["CALENDAR_URL"], params={"trading_date": minute.date().isoformat()},
                headers={"X-Openfinance-Token": os.environ["OPENFINANCE_INTERNAL_API_TOKEN"]}, timeout=15)
            calendar.raise_for_status()
            if calendar.json().get("is_open") is not True:
                log.warning("calendar closed or unavailable; no upstream request")
                record_round(os.environ["DATABASE_URL"], minute, "calendar_not_open_or_unknown")
                return
            start_indices(minute)
            response = requests.get(os.environ["UNIVERSE_URL"], headers={"X-Openfinance-Token": os.environ["OPENFINANCE_INTERNAL_API_TOKEN"]}, timeout=120)
            response.raise_for_status()
            universe = response.json()
            if not universe.get("symbols"):
                raise RuntimeError("security universe is empty")
            if universe.get("stale"):
                log.warning("using stale security universe from %s", universe.get("as_of_date"))
            items = collect_round(universe["symbols"], minute)
        else:
            symbols = [s.strip().upper() for s in os.getenv("QUOTE_SYMBOLS", "600519.SH,000001.SZ").split(",")]
            def mock_fetch(batch, instant):
                return ({symbol: {"symbol": symbol, "name": "模拟数据", "price": 10.0+i,
                    "previous_close": 10.0, "source": "mock", "source_time": instant.astimezone(timezone.utc).isoformat(),
                    "captured_at": datetime.now(timezone.utc).isoformat(), "status": "valid"}
                    for i, symbol in enumerate(batch)}, [])
            start_indices(minute, mock_fetch)
            items = collect_round(symbols, minute, fetch=mock_fetch)
        items = list(items)
        expected = len(items)
        failed_batches = max((item.get("failed_batches", 0) for item in items), default=0)
        store_round(os.environ["DATABASE_URL"], minute, items, failed_batches)
        if mode == "live":
            try:
                record_universe(os.environ["DATABASE_URL"], minute.date(), items)
            except Exception:
                log.exception("could not record intraday universe for backfill")
        log.info("stored round %s records=%d", minute.isoformat(), len(items))
    except Exception as exc:
        log.exception("snapshot collection failed; next round will be attempted")
        if started:
            try:
                record_round(os.environ["DATABASE_URL"], minute, "failed", expected=expected,
                             failed_batches=failed_batches, acknowledged=acknowledged,
                             error=f"{type(exc).__name__}: {exc}"[:1000])
            except Exception:
                log.exception("could not persist collection gap")


def start_quote_round(minute, mode, producer, start_indices):
    thread = Thread(target=collect_quote_round, args=(minute, mode, producer, start_indices), daemon=True)
    thread.start()
    return thread


def main():
    logging.basicConfig(level=logging.INFO)
    mode = os.getenv("QUOTE_MODE", "live")
    if mode not in ("live", "mock"):
        raise ValueError("invalid collector configuration")
    last_minute = None
    index_collector = IndexCollector()
    while True:
        now = datetime.now(CHINA)
        minute = scheduled_minute(now, last_minute)
        if minute is None:
            time.sleep(0.5)
            continue
        last_minute = minute
        start_quote_round(minute, mode, None, index_collector.start)


if __name__ == "__main__":
    main()
