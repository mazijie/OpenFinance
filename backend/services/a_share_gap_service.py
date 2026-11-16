"""Discover missing A-share daily bars without spending a request per date."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from backend.config import settings
from backend.job_store.api_request_budget import ApiRequestBudgetStore
from backend.job_store.a_share_universe_cache import AShareUniverseCacheStore
from backend.lakehouse.finance_writer import FinanceWriter
from backend.providers.finance import BaoStockFinanceProvider
from backend.providers.tencent_securities import TencentSecurityProvider
from backend.providers.daily_bars import daily_request_limit, estimate_daily_requests
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
        self.provider = provider or TencentSecurityProvider()
        self.calendar_provider = provider or BaoStockFinanceProvider(budget_store=self.budget_store)

    def plan(
        self,
        *,
        adjustment_type: str = "none",
        lookback_days: int | None = None,
        end_date: date | None = None,
        start_date: date | None = None,
        symbols: list[str] | None = None,
        mode: str = "daily",
    ) -> dict[str, Any]:
        provider_name = settings.FINANCE_DAILY_PROVIDER
        if adjustment_type != "none":
            raise ValueError("Only unadjusted daily bars are supported")
        request_limit = daily_request_limit(provider_name)
        resolved_end = end_date or (datetime.now(ZoneInfo("Asia/Shanghai")).date() - timedelta(days=1))
        while resolved_end.weekday() >= 5:
            resolved_end -= timedelta(days=1)
        resolved_lookback = lookback_days or settings.A_SHARE_GAP_LOOKBACK_DAYS
        if resolved_lookback <= 0:
            raise ValueError("lookback_days must be positive")
        range_start = start_date or resolved_end - timedelta(days=resolved_lookback - 1)
        if range_start > resolved_end:
            raise ValueError("start_date must not be after end_date")
        if mode not in {"daily", "backfill"}:
            raise ValueError("mode must be daily or backfill")

        securities = self.writer.scan_security_master()
        security_by_id = {security.security_id: security for security in securities}
        calendar = self.writer.scan_trading_calendar()
        universe_as_of = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        universe, universe_refresh_requests = self._load_universe("tencent", universe_as_of)
        if not universe:
            raise ValueError("A-share universe is empty")
        requested_symbols = {symbol.strip().upper() for symbol in symbols or [] if symbol.strip()}
        if symbols is not None and (not requested_symbols or not requested_symbols <= set(universe)):
            raise ValueError("symbols must contain only securities in the current A-share universe")
        expected_symbols = sorted(requested_symbols if symbols is not None else set(universe))
        exchanges = {
            security_by_id[symbol].exchange
            if symbol in security_by_id
            else symbol.rsplit(".", 1)[-1]
            for symbol in expected_symbols
        }
        calendar_refreshes = self._refresh_stale_calendar(
            calendar=calendar,
            exchanges=sorted(exchanges),
            range_start=range_start,
            range_end=resolved_end,
        )
        if calendar_refreshes:
            calendar = self.writer.scan_trading_calendar()

        missing_calendar = self._missing_calendar_days(calendar, sorted(exchanges), range_start, resolved_end)
        if missing_calendar:
            return {"status": "calendar_incomplete", "mode": mode, "provider": provider_name,
                    "adjustment_type": adjustment_type, "range_start": range_start.isoformat(),
                    "range_end": resolved_end.isoformat(), "pending_symbol_count": len(expected_symbols),
                    "missing_calendar_days": len(missing_calendar), "plans": []}

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
            absent = [
                trading_date for trading_date in dates
                if (security is None or security.list_date is None or trading_date >= security.list_date)
                and (security is None or security.delist_date is None or trading_date <= security.delist_date)
                and (symbol, trading_date) not in actual
            ]
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
        remaining_budget = self.budget_store.remaining(provider=provider_name, daily_limit=request_limit) if request_limit > 0 else None
        base_requests_per_symbol = estimate_daily_requests(provider_name, range_start, resolved_end)
        selected: list[dict[str, Any]] = []
        estimated_requests = 0
        for item in missing:
            if len(selected) >= settings.A_SHARE_MAX_SYMBOLS_PER_RUN:
                break
            item_requests = estimate_daily_requests(provider_name, date.fromisoformat(item["start_date"]), date.fromisoformat(item["end_date"]))
            item_requests += int(item["needs_metadata"])
            if remaining_budget is not None and estimated_requests + item_requests > remaining_budget:
                continue
            selected.append(item)
            estimated_requests += item_requests

        unknown_metadata = [symbol for symbol in expected_symbols if symbol not in security_by_id or security_by_id[symbol].list_date is None]
        status = "available" if selected else "quota_exhausted" if missing else "partial_metadata" if unknown_metadata else "complete"
        return {
            "status": status,
            "mode": mode,
            "provider": provider_name,
            "adjustment_type": adjustment_type,
            "range_start": range_start.isoformat(),
            "range_end": resolved_end.isoformat(),
            "calendar_refresh_requests": calendar_refreshes,
            "universe_refresh_requests": universe_refresh_requests,
            "universe_symbol_count": len(expected_symbols),
            "universe_as_of_date": universe_as_of.isoformat(),
            "historical_universe_complete": False,
            "missing_symbol_count": len(missing),
            "missing_pair_count": missing_pair_count,
            "metadata_incomplete_symbols": unknown_metadata,
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
        missing = self._missing_calendar_days(calendar, exchanges, range_start, range_end)
        if not missing:
            return 0
        remaining = self.budget_store.remaining(
            provider="baostock", daily_limit=settings.BAOSTOCK_DAILY_REQUEST_LIMIT
        )
        refreshed = 0
        for exchange in exchanges:
            dates = [day for item_exchange, day in missing if item_exchange == exchange]
            if not dates or refreshed >= remaining:
                continue
            try:
                records = self.calendar_provider.fetch_trading_calendar(exchange=exchange, start_date=min(dates), end_date=max(dates))
                self.writer.append_trading_calendar(records)
                refreshed += 1
            except Exception:
                # Incomplete or unavailable calendars must never imply complete coverage.
                continue
        return refreshed

    @staticmethod
    def _missing_calendar_days(calendar: list[TradingCalendarRecord], exchanges: list[str], start_date: date, end_date: date) -> list[tuple[str, date]]:
        present = {(item.exchange, item.trading_date) for item in calendar}
        return [(exchange, start_date + timedelta(days=offset))
                for exchange in exchanges
                for offset in range((end_date - start_date).days + 1)
                if (exchange, start_date + timedelta(days=offset)) not in present]

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
