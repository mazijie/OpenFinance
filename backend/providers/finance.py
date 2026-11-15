"""Finance provider abstractions."""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
import socket
import threading
from typing import Any, Iterator

from backend.config import settings
from backend.schemas.finance import DailyBarRecord, SecurityMasterRecord, TradingCalendarRecord


class FinanceProviderError(Exception):
    """Raised when a finance provider cannot be accessed or parsed."""


@dataclass(frozen=True, slots=True)
class AShareUniverseResult:
    """A security universe returned by a market data provider."""

    status: str
    as_of_date: date
    symbols: list[str]


class BaseFinanceProvider:
    """Abstract provider for finance data sources."""

    def fetch_security_master(self, *, symbols: list[str] | None = None) -> list[SecurityMasterRecord]:
        raise NotImplementedError

    def fetch_trading_calendar(self, *, exchange: str, start_date, end_date) -> list[TradingCalendarRecord]:
        raise NotImplementedError

    def fetch_daily_bars(
        self,
        *,
        symbols: list[str],
        start_date,
        end_date,
        adjustment_type: str = "qfq",
    ) -> list[DailyBarRecord]:
        raise NotImplementedError


class BaoStockFinanceProvider(BaseFinanceProvider):
    """BaoStock trading-calendar provider."""

    name = "baostock"
    _session_lock = threading.Lock()

    def __init__(self, client: Any | None = None, budget_store: Any | None = None) -> None:
        self.budget_store = budget_store
        if client is not None:
            self.client = client
            return
        if self.budget_store is None:
            from backend.lakehouse.api_request_budget import ApiRequestBudgetStore

            self.budget_store = ApiRequestBudgetStore()
        try:  # pragma: no cover - optional dependency
            import baostock as bs  # type: ignore
        except ImportError as exc:  # pragma: no cover - test fallback
            raise FinanceProviderError("baostock dependency is not installed") from exc
        self.client = bs

    def fetch_trading_calendar(self, *, exchange: str, start_date, end_date) -> list[TradingCalendarRecord]:
        start = self._coerce_date(start_date)
        end = self._coerce_date(end_date)
        rows: list[dict[str, str]]
        with self._session():
            self._reserve_requests(1)
            rows = self._query_rows(self.client.query_trade_dates(start_date=start.isoformat(), end_date=end.isoformat()))
        records: list[TradingCalendarRecord] = []
        for row in rows:
            trading_date = self._coerce_date(row["calendar_date"])
            raw_status = str(row.get("is_trading_day") or "").strip()
            if raw_status not in {"0", "1"}:
                raise FinanceProviderError(f"Invalid BaoStock trading-day status for {trading_date}")
            is_open = raw_status == "1"
            session_open_at = None
            session_close_at = None
            if is_open:
                session_open_at = datetime.combine(trading_date, time(1, 30), tzinfo=UTC)
                session_close_at = datetime.combine(trading_date, time(7, 0), tzinfo=UTC)
            records.append(
                TradingCalendarRecord(
                    exchange=str(exchange).strip().upper(),
                    trading_date=trading_date,
                    is_open=is_open,
                    session_open_at=session_open_at,
                    session_close_at=session_close_at,
                )
            )
        return records

    @contextmanager
    def _session(self) -> Iterator[None]:
        session_lock = getattr(self.budget_store, "session_lock", None)
        shared_lock = (
            session_lock(provider=self.name)
            if callable(session_lock)
            else nullcontext()
        )
        with self._session_lock, shared_lock:
            previous_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(settings.BAOSTOCK_SOCKET_TIMEOUT_SECONDS)
            try:
                login_result = self.client.login()
                error_code = str(getattr(login_result, "error_code", "0"))
                if error_code != "0":
                    error_msg = str(getattr(login_result, "error_msg", "unknown error"))
                    raise FinanceProviderError(f"BaoStock login failed: {error_code} {error_msg}")
                try:
                    yield
                finally:
                    logout = getattr(self.client, "logout", None)
                    if callable(logout):
                        logout()
            finally:
                socket.setdefaulttimeout(previous_timeout)

    def _query_rows(self, result_set: Any) -> list[dict[str, str]]:
        error_code = str(getattr(result_set, "error_code", "0"))
        if error_code != "0":
            error_msg = str(getattr(result_set, "error_msg", "unknown error"))
            raise FinanceProviderError(f"BaoStock query failed: {error_code} {error_msg}")

        fields = list(getattr(result_set, "fields", []))
        rows: list[dict[str, str]] = []
        while result_set.next():
            row_data = list(result_set.get_row_data())
            rows.append({field: value for field, value in zip(fields, row_data, strict=False)})

        # BaoStock returns False both at EOF and when fetching the next page
        # fails. A full final page without an EOF response is incomplete too:
        # socket failures can leave error_code unchanged at "0".
        error_code = str(getattr(result_set, "error_code", "0"))
        if error_code != "0":
            error_msg = str(getattr(result_set, "error_msg", "unknown error"))
            raise FinanceProviderError(f"BaoStock pagination failed: {error_code} {error_msg}")
        page = getattr(result_set, "data", None)
        page_size = int(getattr(result_set, "per_page_count", 0) or 0)
        if page_size > 0 and page is not None and len(page) == page_size:
            raise FinanceProviderError("BaoStock pagination stopped on a full page without confirming EOF")
        return rows

    def _reserve_requests(self, count: int) -> None:
        if self.budget_store is not None:
            self.budget_store.reserve(
                provider="baostock",
                count=count,
                daily_limit=settings.BAOSTOCK_DAILY_REQUEST_LIMIT,
            )

    @staticmethod
    def _coerce_date(value: Any) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        return date.fromisoformat(str(value))


class InMemoryFinanceProvider(BaseFinanceProvider):
    """Test provider backed by static records."""

    def __init__(
        self,
        *,
        security_master: list[SecurityMasterRecord] | None = None,
        trading_calendar: list[TradingCalendarRecord] | None = None,
        daily_bars: list[DailyBarRecord] | None = None,
    ) -> None:
        self._security_master = security_master or []
        self._trading_calendar = trading_calendar or []
        self._daily_bars = daily_bars or []

    def fetch_security_master(self, *, symbols: list[str] | None = None) -> list[SecurityMasterRecord]:
        if not symbols:
            return list(self._security_master)
        wanted = {symbol.strip().upper() for symbol in symbols if symbol}
        return [record for record in self._security_master if record.symbol.upper() in wanted]

    def fetch_trading_calendar(self, *, exchange: str, start_date, end_date) -> list[TradingCalendarRecord]:
        return [
            record
            for record in self._trading_calendar
            if record.exchange == exchange and start_date <= record.trading_date <= end_date
        ]

    def fetch_daily_bars(
        self,
        *,
        symbols: list[str],
        start_date,
        end_date,
        adjustment_type: str = "qfq",
    ) -> list[DailyBarRecord]:
        wanted = {symbol.strip().upper() for symbol in symbols if symbol}
        return [
            record
            for record in self._daily_bars
            if record.symbol.upper() in wanted and start_date <= record.trading_date <= end_date and record.adjustment_type == adjustment_type
        ]
