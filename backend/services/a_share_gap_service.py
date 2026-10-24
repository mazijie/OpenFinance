"""Discover missing A-share daily bars without spending a request per date."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from backend.config import settings
from backend.lakehouse.api_request_budget import ApiRequestBudgetStore
from backend.lakehouse.a_share_universe_cache import AShareUniverseCacheStore
from backend.lakehouse.finance_writer import FinanceWriter
from backend.providers.finance import BaoStockFinanceProvider
from backend.schemas.finance import SecurityMasterRecord, TradingCalendarRecord


class AShareGapService:
    """Plan gap repair using local metadata and a persistent request budget."""

    def __init__(
        self,
        *,
        writer: FinanceWriter | None = None,
        provider: BaoStockFinanceProvider | None = None,
        budget_store: ApiRequestBudgetStore | None = None,
        universe_cache: AShareUniverseCacheStore | None = None,
    ) -> None:
        self.writer = writer or FinanceWriter()
        self.budget_store = budget_store or ApiRequestBudgetStore()
        self.universe_cache = universe_cache or AShareUniverseCacheStore()
        self.provider = provider or BaoStockFinanceProvider(budget_store=self.budget_store)

    def plan(
        self,
        *,
        adjustment_type: str = "qfq",
        lookback_days: int | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        provider_name = settings.FINANCE_DEFAULT_PROVIDER
        request_limit = settings.BAOSTOCK_DAILY_REQUEST_LIMIT
        resolved_end = end_date or (datetime.now(ZoneInfo("Asia/Shanghai")).date() - timedelta(days=1))
        while resolved_end.weekday() >= 5:
            resolved_end -= timedelta(days=1)
        resolved_lookback = lookback_days or settings.A_SHARE_GAP_LOOKBACK_DAYS
        if resolved_lookback <= 0:
            raise ValueError("lookback_days must be positive")
        range_start = resolved_end - timedelta(days=resolved_lookback - 1)

        securities = self.writer.scan_security_master()
        security_by_id = {security.security_id: security for security in securities}
        calendar = self.writer.scan_trading_calendar()
        known_exchanges = {security.exchange for security in securities}

        calendar_refreshes = self._refresh_stale_calendar(
            calendar=calendar,
            exchanges=sorted(known_exchanges),
            range_start=range_start,
            range_end=resolved_end,
        )
        if calendar_refreshes:
            calendar = self.writer.scan_trading_calendar()

        universe, universe_refresh_requests = self._load_universe(provider_name, resolved_end)
        if not universe:
            raise ValueError("A-share universe is empty")
        expected_symbols = sorted(set(universe) | set(security_by_id))
        exchanges = {
            security_by_id[symbol].exchange
            if symbol in security_by_id
            else symbol.rsplit(".", 1)[-1]
            for symbol in expected_symbols
        }
        calendar_refreshes += self._refresh_stale_calendar(
            calendar=calendar,
            exchanges=sorted(exchanges),
            range_start=range_start,
            range_end=resolved_end,
        )
        if calendar_refreshes:
            calendar = self.writer.scan_trading_calendar()

        open_dates = self._open_dates_by_exchange(calendar, range_start, resolved_end)
        actual = self.writer.scan_daily_bar_keys(
            provider=provider_name,
            adjustment_type=adjustment_type,
            start_date=range_start,
            end_date=resolved_end,
        )

        missing: list[dict[str, Any]] = []
        missing_pair_count = 0
        for symbol in expected_symbols:
            security = security_by_id.get(symbol)
            exchange = security.exchange if security else symbol.rsplit(".", 1)[-1]
            dates = open_dates.get(exchange, [])
            absent = [trading_date for trading_date in dates if (symbol, trading_date) not in actual]
            if not absent:
                continue
            missing_pair_count += len(absent)
            missing.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "start_date": absent[0].isoformat(),
                    "end_date": absent[-1].isoformat(),
                    "missing_date_count": len(absent),
                    "needs_metadata": security is None,
                }
            )

        missing.sort(key=lambda item: (item["start_date"], -item["missing_date_count"], item["symbol"]))
        remaining_budget = self.budget_store.remaining(provider=provider_name, daily_limit=request_limit)
        base_requests_per_symbol = max(1, settings.A_SHARE_REQUESTS_PER_SYMBOL)
        selected: list[dict[str, Any]] = []
        estimated_requests = 0
        for item in missing:
            if len(selected) >= settings.A_SHARE_MAX_SYMBOLS_PER_RUN:
                break
            item_requests = base_requests_per_symbol + (1 if item["needs_metadata"] else 0)
            if estimated_requests + item_requests > remaining_budget:
                continue
            selected.append(item)
            estimated_requests += item_requests

        status = "available" if selected else "complete" if not missing else "quota_exhausted"
        return {
            "status": status,
            "provider": provider_name,
            "adjustment_type": adjustment_type,
            "range_start": range_start.isoformat(),
            "range_end": resolved_end.isoformat(),
            "calendar_refresh_requests": calendar_refreshes,
            "universe_refresh_requests": universe_refresh_requests,
            "universe_symbol_count": len(expected_symbols),
            "missing_symbol_count": len(missing),
            "missing_pair_count": missing_pair_count,
            "selected_symbol_count": len(selected),
            "pending_symbol_count": max(0, len(missing) - len(selected)),
            "estimated_requests": estimated_requests,
            "base_requests_per_existing_symbol": base_requests_per_symbol,
            "remaining_budget": remaining_budget,
            "plans": selected,
        }

    def _load_universe(self, provider_name: str, as_of_date: date) -> tuple[list[str], int]:
        cached = self.universe_cache.get(provider=provider_name, as_of_date=as_of_date)
        if cached is not None:
            return cached, 0
        symbols = self.provider.fetch_a_share_symbols_for_day(day=as_of_date)
        self.universe_cache.put(provider=provider_name, as_of_date=as_of_date, symbols=symbols)
        return symbols, 1

    def _refresh_stale_calendar(
        self,
        *,
        calendar: list[TradingCalendarRecord],
        exchanges: list[str],
        range_start: date,
        range_end: date,
    ) -> int:
        latest_by_exchange: dict[str, date] = {}
        for record in calendar:
            latest_by_exchange[record.exchange] = max(
                record.trading_date,
                latest_by_exchange.get(record.exchange, date.min),
            )

        stale_ranges: list[tuple[str, date, date]] = []
        for exchange in exchanges:
            latest = latest_by_exchange.get(exchange)
            if latest is None:
                stale_ranges.append((exchange, range_start, range_end))
            elif latest < range_end:
                stale_ranges.append((exchange, max(range_start, latest + timedelta(days=1)), range_end))

        if not stale_ranges:
            return 0
        remaining = self.budget_store.remaining(
            provider=settings.FINANCE_DEFAULT_PROVIDER,
            daily_limit=settings.BAOSTOCK_DAILY_REQUEST_LIMIT,
        )
        if remaining < len(stale_ranges):
            return 0
        for exchange, start, end in stale_ranges:
            records = self.provider.fetch_trading_calendar(
                exchange=exchange,
                start_date=start,
                end_date=end,
            )
            self.writer.append_trading_calendar(records)
        return len(stale_ranges)

    @staticmethod
    def _open_dates_by_exchange(
        calendar: list[TradingCalendarRecord],
        start_date: date,
        end_date: date,
    ) -> dict[str, list[date]]:
        result: dict[str, list[date]] = {}
        for record in calendar:
            if record.is_open and start_date <= record.trading_date <= end_date:
                result.setdefault(record.exchange, []).append(record.trading_date)
        for dates in result.values():
            dates.sort()
        return result
