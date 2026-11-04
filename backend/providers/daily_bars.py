"""Independent daily-bar providers; metadata continues to come from BaoStock."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
import json
import math
import re
import time as time_module
from typing import Any, Callable

import httpx

from backend.config import settings
from backend.logging_utils import get_logger
from backend.providers.finance import BaseFinanceProvider, BaoStockFinanceProvider, FinanceProviderError
from backend.schemas.finance import DailyBarRecord

logger = get_logger(__name__)
ENDPOINT = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
MAX_BARS = 640
WINDOW_DAYS = 730  # Fewer than 640 trading days, including at year boundaries.


def daily_windows(start: date, end: date):
    if start > end:
        raise ValueError("start_date must not be after end_date")
    while start <= end:
        stop = min(end, start + timedelta(days=WINDOW_DAYS - 1))
        yield start, stop
        if stop == end:
            break
        start = stop + timedelta(days=1)


def daily_request_limit(provider: str) -> int:
    if provider == "tencent":
        return settings.TENCENT_DAILY_REQUEST_LIMIT
    if provider == "baostock":
        return settings.BAOSTOCK_DAILY_REQUEST_LIMIT
    raise ValueError(f"Unsupported daily provider: {provider}")


def estimate_daily_requests(provider: str, start: date, end: date) -> int:
    if provider == "baostock":
        return 2
    if provider == "tencent":
        return sum(1 for _ in daily_windows(start, end))
    raise ValueError(f"Unsupported daily provider: {provider}")


def create_daily_provider(name: str | None = None, *, budget_store=None, check_lease=None):
    name = name or settings.FINANCE_DAILY_PROVIDER
    if name == "tencent":
        return TencentDailyBarsProvider(budget_store=budget_store, check_lease=check_lease)
    if name == "baostock":
        return BaoStockFinanceProvider(budget_store=budget_store)
    raise ValueError(f"Unsupported daily provider: {name}")


class TencentDailyBarsProvider(BaseFinanceProvider):
    """Tencent HTTP daily bars, without login, BaoStock locks or implicit fallback."""

    name = "tencent"
    daily_only = True
    handles_retries = True

    def __init__(self, *, client=None, budget_store=None, check_lease=None,
                 observer: Callable[[dict[str, Any]], None] | None = None,
                 sleep: Callable[[float], None] = time_module.sleep):
        self.client = client or httpx.Client(
            timeout=httpx.Timeout(settings.TENCENT_READ_TIMEOUT_SECONDS,
                                  connect=settings.TENCENT_CONNECT_TIMEOUT_SECONDS),
        )
        self._owns_client = client is None
        self.budget_store, self.check_lease = budget_store, check_lease
        self.observer, self.sleep = observer, sleep

    def close(self):
        if self._owns_client:
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @staticmethod
    def normalize_symbol(symbol: str) -> tuple[str, str]:
        symbol = symbol.strip().upper()
        if match := re.fullmatch(r"(\d{6})\.(SH|SZ|BJ)", symbol):
            code, exchange = match.groups()
        elif match := re.fullmatch(r"(SH|SZ|BJ)[.]?(\d{6})", symbol):
            exchange, code = match.groups()
        elif re.fullmatch(r"\d{6}", symbol):
            code = symbol
            exchange = "BJ" if code.startswith(("43", "83", "87", "88", "92")) else "SH" if code.startswith(("5", "6", "9")) else "SZ"
        else:
            raise FinanceProviderError(f"Unsupported symbol: {symbol}")
        return f"{code}.{exchange}", exchange.lower() + code

    def fetch_daily_bars(self, *, symbols: list[str], start_date, end_date,
                         adjustment_type="qfq", limit=MAX_BARS) -> list[DailyBarRecord]:
        adjustment_type = {"forward": "qfq", "backward": "hfq"}.get(adjustment_type, adjustment_type)
        if adjustment_type not in {"none", "qfq", "hfq"}:
            raise FinanceProviderError(f"Unsupported adjustment_type: {adjustment_type}")
        if not 1 <= limit <= MAX_BARS:
            raise ValueError("limit must be between 1 and 640")
        start = BaoStockFinanceProvider._coerce_date(start_date)
        end = BaoStockFinanceProvider._coerce_date(end_date)
        windows = list(daily_windows(start, end))
        found = {}
        for symbol in dict.fromkeys(symbols):
            internal, code = self.normalize_symbol(symbol)
            for first, last in windows:
                for bar in self._fetch_window(internal, code, first, last, adjustment_type, limit):
                    found[(internal, bar.trading_date)] = bar
        return sorted(found.values(), key=lambda bar: (bar.security_id, bar.trading_date))

    def _fetch_window(self, symbol, code, start, end, adjustment, limit):
        started = time_module.perf_counter()
        attempts, rows, error = [], [], None
        adjust = "" if adjustment == "none" else adjustment
        params = {"param": f"{code},day,{start},{end},{limit},{adjust}"}
        try:
            for attempt in range(settings.TENCENT_MAX_RETRIES + 1):
                if self.check_lease:
                    self.check_lease()
                if settings.TENCENT_DAILY_REQUEST_LIMIT > 0:
                    if self.budget_store is None:
                        from backend.lakehouse.api_request_budget import ApiRequestBudgetStore
                        self.budget_store = ApiRequestBudgetStore()
                    self.budget_store.reserve(provider=self.name, count=1,
                                              daily_limit=settings.TENCENT_DAILY_REQUEST_LIMIT)
                item = {"status_code": None, "raw_text": None, "error": None}
                tick = time_module.perf_counter()
                retry = False
                try:
                    response = self.client.get(ENDPOINT, params=params)
                    item.update(status_code=response.status_code, raw_text=response.text)
                    retry = response.status_code == 429 or 500 <= response.status_code <= 599
                    response.raise_for_status()
                except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                    item["error"] = str(exc)
                    retry = True
                except httpx.HTTPStatusError as exc:
                    item["error"] = str(exc)
                finally:
                    item["network_seconds"] = time_module.perf_counter() - tick
                    attempts.append(item)
                if item["error"] is None:
                    break
                if not retry or attempt >= settings.TENCENT_MAX_RETRIES:
                    raise FinanceProviderError(f"Tencent request failed for {symbol}: {item['error']}")
                self.sleep(settings.TENCENT_RETRY_BACKOFF_SECONDS * 2 ** attempt)

            try:
                text = response.text.strip()
                if not text.startswith("{") and "={" in text:
                    text = text[text.index("={") + 1:].rstrip("; ")
                payload = json.loads(text)
                if not isinstance(payload, dict):
                    raise ValueError("Expected a JSON object")
                if payload.get("code") != 0:
                    raise ValueError(f"upstream code {payload.get('code')}: {payload.get('msg', '')}")
                node = payload["data"][code]
                if not isinstance(node, dict):
                    raise ValueError("Expected daily-bar object")
                raw_bars = node.get(adjust + "day", node.get("day"))
                if not isinstance(raw_bars, list):
                    raise ValueError("Missing daily-bar array")
                captured = datetime.now(UTC)
                for raw in raw_bars:
                    bar = self._parse_bar(symbol, raw, adjustment, captured)
                    if start <= bar.trading_date <= end:
                        rows.append(bar)
            except (ValueError, KeyError, TypeError, IndexError) as exc:
                raise FinanceProviderError(f"Invalid Tencent daily bars for {symbol}: {exc}") from exc
            return rows
        except Exception as exc:
            error = str(exc)
            raise
        finally:
            event = {"symbol": symbol, "start_date": str(start), "end_date": str(end),
                     "params": params, "attempts": attempts, "retries": max(0, len(attempts) - 1),
                     "network_seconds": sum(a["network_seconds"] for a in attempts),
                     "total_seconds": time_module.perf_counter() - started,
                     "row_count": len(rows), "error": error}
            logger.info("Tencent daily request completed", event="tencent_daily_request_completed",
                        extra_fields={k: v for k, v in event.items() if k not in {"attempts", "params"}})
            if self.observer:
                self.observer(event)

    @staticmethod
    def _parse_bar(symbol, row, adjustment, captured):
        if not isinstance(row, list) or len(row) < 6:
            raise ValueError("Daily row must have at least six columns")
        day = date.fromisoformat(row[0])
        open_, close, high, low, volume = map(float, row[1:6])
        if not all(math.isfinite(v) for v in (open_, close, high, low, volume)):
            raise ValueError("Non-finite price or volume")
        if volume < 0 or high < max(open_, close, low) or low > min(open_, close, high):
            raise ValueError("Inconsistent OHLC or negative volume")
        if adjustment != "qfq" and min(open_, close, high, low) <= 0:
            raise ValueError("Non-positive unadjusted/backward-adjusted price")
        if open_ == close == high == low == 0:
            raise ValueError("Empty zero-price bar")
        def optional(index):
            value = row[index] if len(row) > index else None
            if value in (None, ""):
                return None
            value = float(value)
            if not math.isfinite(value) or value < 0:
                raise ValueError("Invalid amount or turnover")
            return value
        code, exchange = symbol.split(".")
        multiplier = 1 if exchange == "SH" and code.startswith(("688", "689")) else 100
        amount = optional(8)
        return DailyBarRecord(
            security_id=symbol, symbol=symbol, exchange=exchange, trading_date=day,
            open=open_, high=high, low=low, close=close, volume=volume * multiplier,
            amount=amount * 10000 if amount is not None else None, turnover=optional(7),
            is_suspended=None, is_st=None, adjustment_factor=None, adjustment_type=adjustment,
            provider="tencent", source_record_id=f"{symbol}-{day}-{adjustment}",
            source_version=captured.isoformat(), payload_hash=sha256(json.dumps(row, ensure_ascii=False).encode()).hexdigest(),
            raw_object_path=f"{ENDPOINT}?param={exchange.lower()}{code},day,{day},{day},1,{'' if adjustment == 'none' else adjustment}",
            event_time=datetime.combine(day, time.min, tzinfo=UTC),
            available_at=captured, ingested_at=captured,
        )
