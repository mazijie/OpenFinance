"""Shared, read-only realtime quote access for UI and agent."""
from datetime import datetime, timezone, timedelta
from functools import lru_cache
import os
import re

import psycopg
from psycopg.rows import dict_row
from backend.config import settings
from backend.lakehouse.starrocks_daily_reader import StarRocksUnavailable

CHINA = timezone(timedelta(hours=8))
SYMBOL_PATTERN = re.compile(r"^\d{6}(?:\.(?:SH|SZ|BJ))?$")


def symbol_candidates(symbol):
    normalized = symbol.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(normalized):
        raise ValueError("证券代码应为六位代码或带 SH、SZ、BJ 后缀的代码")
    return [normalized] if "." in normalized else [f"{normalized}.{exchange}" for exchange in ("SH", "SZ", "BJ")]


@lru_cache(maxsize=32)
def _trading_day(day):
    from backend.services.finance_service import FinanceService
    result = FinanceService().is_trading_day(day)
    if result is None:
        raise LookupError("trading calendar is unavailable")
    return result


def trading_day(day):
    try:
        return _trading_day(day)
    except StarRocksUnavailable:
        raise
    except Exception:
        return None


def quote_status(row, now, is_open):
    local = now.astimezone(CHINA)
    if row.get("quote_status") in {"missing", "unsupported"}:
        return "source_error"
    if is_open is False or (is_open is True and not ((9, 30) <= (local.hour, local.minute) <= (11, 30) or (13, 0) <= (local.hour, local.minute) <= (15, 0))):
        return "closed"
    if is_open is None:
        return "unknown"
    source = row.get("source_time")
    if not source or source > now or (now - source).total_seconds() > int(os.getenv("QUOTE_STALE_SECONDS", "300")):
        return "not_updated"
    if row.get("quote_status") != "valid":
        return "not_updated"
    return "fresh"


class RealtimeQuoteService:
    def intraday(self, symbol, now=None):
        cutoff = now or datetime.now(timezone.utc)
        candidates = symbol_candidates(symbol)
        if len(candidates) != 1:
            raise ValueError("分时行情请提供交易所后缀")
        with psycopg.connect(settings.DATABASE_URL, row_factory=dict_row) as conn:
            rows = conn.execute("""WITH points AS (
                SELECT w.symbol, w.window_start, w.sample_avg AS price, 'sample'::text AS source,
                    (w.recompute_stale OR EXISTS (SELECT 1 FROM openfinance.realtime_quote_late l
                      WHERE l.symbol=w.symbol AND l.window_start=w.window_start AND l.resolved_at IS NULL)) AS incomplete, 0 AS rank
                FROM openfinance.realtime_quote_windows w
                WHERE w.symbol=ANY(%s) AND w.window_basis='capture_time' AND w.window_end <= (%s AT TIME ZONE 'UTC')
                UNION ALL
                SELECT b.symbol,b.window_start,b.price,b.source,false,1
                FROM openfinance.intraday_backfill_points b
                WHERE b.symbol=ANY(%s) AND b.window_start + interval '1 minute' <= (%s AT TIME ZONE 'UTC')
                  AND b.fetched_at <= %s
            ), latest AS (SELECT ((max(window_start) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai')::date AS day FROM points),
            chosen AS (SELECT DISTINCT ON (symbol, window_start) * FROM points ORDER BY symbol,window_start,rank)
            SELECT c.symbol,c.window_start AT TIME ZONE 'UTC' AS time,c.price,c.source,c.incomplete
            FROM chosen c, latest WHERE ((c.window_start AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai')::date=latest.day
            ORDER BY c.window_start""", (candidates, cutoff, candidates, cutoff, cutoff)).fetchall()
        return {"symbol": candidates[0], "points": rows}

    def lookup(self, symbol=None, as_of=None, now=None):
        now = now or datetime.now(timezone.utc)
        cutoff = min(as_of or now, now)
        with psycopg.connect(settings.DATABASE_URL, row_factory=dict_row) as conn:
            quote = None
            if symbol:
                candidates = symbol_candidates(symbol)
                rows = conn.execute("""SELECT q.*, m.volume_lots, m.market_cap_cny, m.pe_ratio,
                    m.day_open, m.day_high, m.day_low, w.window_start, w.window_end, w.sample_min,
                    w.sample_max, w.sample_avg, w.sample_count, w.recomputed_at,
                    (COALESCE(w.recompute_stale, false) OR EXISTS (SELECT 1 FROM openfinance.realtime_quote_late l WHERE l.symbol=q.symbol
                            AND l.window_start=w.window_start AND l.first_seen_at <= %s
                            AND (l.resolved_at IS NULL OR l.resolved_at > %s))) AS window_incomplete
                    FROM openfinance.realtime_quotes q
                    LEFT JOIN openfinance.realtime_quote_metrics m ON m.symbol=q.symbol
                        AND m.source_time=q.source_time AND m.captured_at <= %s
                    LEFT JOIN LATERAL (SELECT * FROM openfinance.realtime_quote_windows
                    WHERE symbol=q.symbol AND window_basis='capture_time' AND window_end <= (%s AT TIME ZONE 'UTC')
                    AND (recomputed_at IS NULL OR recomputed_at <= %s)
                    ORDER BY window_start DESC LIMIT 1) w ON true
                    WHERE q.symbol = ANY(%s) AND q.captured_at <= %s AND (q.source_time IS NULL OR q.source_time <= %s)
                    AND (q.status_at IS NULL OR q.status_at <= %s)""",
                    (cutoff, cutoff, cutoff, cutoff, cutoff, candidates, cutoff, cutoff, cutoff)).fetchall()
                if len(rows) > 1:
                    raise ValueError("证券代码存在多个交易所，请提供交易所后缀")
                quote = rows[0] if rows else None
                if quote is None:
                    states = conn.execute("""SELECT symbol, status, observed_at FROM openfinance.realtime_quote_status
                        WHERE symbol = ANY(%s) AND observed_at <= %s""", (candidates, cutoff)).fetchall()
                    if len(states) > 1:
                        raise ValueError("证券代码存在多个交易所，请提供交易所后缀")
                    state = states[0] if states else None
                    if state and state["status"] in {"missing", "unsupported"}:
                        quote = {"symbol": state["symbol"], "name": "", "price": None, "source": "tencent",
                                 "source_time": None, "captured_at": state["observed_at"], "quote_status": state["status"]}
            market = conn.execute("""SELECT *, (received >= expected AND valid = expected AND failed_batches = 0) AS complete
                FROM openfinance.realtime_market_rounds WHERE round_id::timestamptz <= %s
                ORDER BY round_id::timestamptz DESC LIMIT 1""", (cutoff,)).fetchone()
            collection = conn.execute("""SELECT * FROM openfinance.realtime_collection_rounds
                WHERE round_id <= %s ORDER BY round_id DESC LIMIT 1""", (cutoff,)).fetchone()
            indices = conn.execute("""SELECT symbol, name, price, previous_close, source, source_time,
                captured_at, quote_status FROM openfinance.realtime_indices
                WHERE captured_at <= %s AND source_time <= %s""", (cutoff, cutoff)).fetchall()
            late_windows = []
            if symbol:
                late_windows = conn.execute("""SELECT symbol, window_start, count(*) AS late_count
                    FROM openfinance.realtime_quote_late WHERE symbol=ANY(%s) AND first_seen_at <= %s
                    AND (resolved_at IS NULL OR resolved_at > %s) GROUP BY symbol,window_start
                    ORDER BY window_start DESC LIMIT 20""", (candidates, cutoff, cutoff)).fetchall()
        day = trading_day(now.astimezone(CHINA).date())
        if quote:
            quote["status"] = quote_status(quote, now, day)
        for index in indices:
            index["status"] = quote_status(index, now, day)
            index["change"] = index["price"] - index["previous_close"]
            index["change_percent"] = index["change"] / index["previous_close"] * 100
        return {"quote": quote, "market": market, "trading_day": day, "collection": collection,
                "late_windows": late_windows, "indices": indices}
