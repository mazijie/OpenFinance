"""Finance provider abstractions."""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
import socket
import threading
import time as time_module
from typing import Any, Iterator

from backend.config import settings
from backend.logging_utils import get_logger
from backend.schemas.finance import DailyBarRecord, SecurityMasterRecord, TradingCalendarRecord


class FinanceProviderError(Exception):
    """Raised when a finance provider cannot be accessed or parsed."""


@dataclass(frozen=True, slots=True)
class AShareUniverseResult:
    """A universe available from BaoStock or a completed coverage watermark."""

    status: str
    as_of_date: date
    symbols: list[str]


logger = get_logger(__name__)


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
    """BaoStock-backed provider."""

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

    def fetch_security_master(self, *, symbols: list[str] | None = None) -> list[SecurityMasterRecord]:
        ingested_at = datetime.now(UTC)
        requested = [self.to_baostock_code(symbol) for symbol in symbols] if symbols else None
        records: list[SecurityMasterRecord] = []
        with self._session():
            if requested:
                for code in requested:
                    self._reserve_requests(1)
                    rows = self._query_rows(self.client.query_stock_basic(code=code))
                    for row in rows:
                        records.append(self._build_security_master_record(row=row, ingested_at=ingested_at))
                return records

            self._reserve_requests(1)
            all_stock_rows = self._query_rows(self.client.query_all_stock(day=ingested_at.date().isoformat()))
            for row in all_stock_rows:
                code = str(row.get("code") or "").strip()
                if not code or not self._is_a_share_code(code):
                    continue
                self._reserve_requests(1)
                basic_rows = self._query_rows(self.client.query_stock_basic(code=code))
                if not basic_rows:
                    continue
                records.append(self._build_security_master_record(row=basic_rows[0], ingested_at=ingested_at))
        return records

    def list_a_share_symbols(
        self,
        *,
        day: date | None = None,
        coverage_store: Any | None = None,
        adjustment_type: str = "qfq",
        ignore_coverage: bool = False,
        coverage_provider: str | None = None,
    ) -> AShareUniverseResult:
        """Find a listed A-share universe, stopping at a completed coverage date."""

        if coverage_store is None and not ignore_coverage:
            from backend.lakehouse.a_share_coverage import AShareCoverageStore

            coverage_store = AShareCoverageStore()
        candidate = day or datetime.now(UTC).date()
        max_days = settings.A_SHARE_UNIVERSE_LOOKBACK_DAYS
        if max_days <= 0:
            raise FinanceProviderError("A_SHARE_UNIVERSE_LOOKBACK_DAYS must be positive")
        for _ in range(max_days):
            if not ignore_coverage and coverage_store.is_covered(
                provider=coverage_provider or settings.FINANCE_DAILY_PROVIDER,
                adjustment_type=adjustment_type,
                universe_as_of_date=candidate,
            ):
                return AShareUniverseResult(status="already_covered", as_of_date=candidate, symbols=[])
            symbols = self._list_a_share_symbols_for_day(candidate)
            if symbols:
                return AShareUniverseResult(status="available", as_of_date=candidate, symbols=symbols)
            candidate -= timedelta(days=1)
        raise FinanceProviderError(
            f"No A-share universe found from {(day or datetime.now(UTC).date()).isoformat()} "
            f"within {max_days} calendar days"
        )

    def fetch_a_share_symbols_for_day(self, *, day: date) -> list[str]:
        """Fetch one day's ordinary A-share universe with one provider request."""

        return self._list_a_share_symbols_for_day(day)

    def _list_a_share_symbols_for_day(self, query_date: date) -> list[str]:
        """Query one day. Provider failures must propagate instead of resembling holidays."""

        query_day = query_date.isoformat()
        rows: list[dict[str, str]] | None = None
        last_error: Exception | None = None
        for attempt in range(settings.BAOSTOCK_MAX_RETRIES + 1):
            try:
                with self._session():
                    self._reserve_requests(1)
                    rows = self._query_rows(self.client.query_all_stock(day=query_day))
                break
            except Exception as exc:  # pragma: no cover - provider response dependent
                last_error = exc
                logger.warning(
                    "BaoStock universe query failed",
                    event="baostock_universe_query_failed",
                    extra_fields={"attempt": attempt + 1, "error": str(exc)},
                )
                if attempt < settings.BAOSTOCK_MAX_RETRIES:
                    time_module.sleep(settings.BAOSTOCK_RETRY_BACKOFF_SECONDS * (attempt + 1))
        if rows is None:
            raise FinanceProviderError(f"BaoStock universe query failed after retries: {last_error}") from last_error

        symbols: set[str] = set()
        for row in rows:
            code = str(row.get("code") or "").strip()
            security_type = str(row.get("type") or row.get("security_type") or "1").strip()
            trade_status = str(row.get("tradeStatus") or row.get("status") or "1").strip()
            if security_type not in {"", "1"} or trade_status not in {"", "1"} or not self._is_a_share_code(code):
                continue
            symbols.add(self.to_internal_symbol(code))
        return sorted(symbols)

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

    def fetch_daily_bars(
        self,
        *,
        symbols: list[str],
        start_date,
        end_date,
        adjustment_type: str = "qfq",
    ) -> list[DailyBarRecord]:
        if not symbols:
            return []

        start = self._coerce_date(start_date)
        end = self._coerce_date(end_date)
        adjustflag = self._map_adjustment_flag(adjustment_type)
        ingested_at = datetime.now(UTC)
        all_records: list[DailyBarRecord] = []

        with self._session():
            for symbol in symbols:
                self._reserve_requests(2)
                baostock_code = self.to_baostock_code(symbol)
                bar_rows = self._query_rows(
                    self.client.query_history_k_data_plus(
                        baostock_code,
                        "date,code,open,high,low,close,volume,amount,turn,tradestatus,isST",
                        start_date=start.isoformat(),
                        end_date=end.isoformat(),
                        frequency="d",
                        adjustflag=adjustflag,
                    )
                )
                print(baostock_code, start.isoformat(), end.isoformat(), len(bar_rows))
                factor_rows = self._query_rows(
                    self.client.query_adjust_factor(
                        code=baostock_code,
                        start_date=start.isoformat(),
                        end_date=end.isoformat(),
                    )
                )
                factor_by_date = self._build_adjustment_factor_map(factor_rows, adjustment_type)
                for row in bar_rows:
                    record = self._build_daily_bar_record(
                        row=row,
                        ingested_at=ingested_at,
                        adjustment_type=adjustment_type,
                        factor_by_date=factor_by_date,
                    )
                    all_records.append(record)
        return all_records

    @staticmethod
    def to_baostock_code(symbol: str) -> str:
        normalized = str(symbol).strip().upper()
        if not normalized:
            raise FinanceProviderError("symbol cannot be empty")
        if "." in normalized:
            left, right = normalized.split(".", 1)
            if left in {"SH", "SZ", "BJ"}:
                return f"{left.lower()}.{right}"
            if right in {"SH", "SZ", "BJ"}:
                return f"{right.lower()}.{left}"
        if normalized.isdigit() and len(normalized) == 6:
            exchange = "sh" if normalized.startswith(("5", "6", "9")) else "sz"
            return f"{exchange}.{normalized}"
        raise FinanceProviderError(f"unsupported symbol format: {symbol}")

    @staticmethod
    def to_internal_symbol(code: str) -> str:
        normalized = str(code).strip().lower()
        if "." not in normalized:
            raise FinanceProviderError(f"unsupported BaoStock code format: {code}")
        exchange, numeric = normalized.split(".", 1)
        return f"{numeric.upper()}.{exchange.upper()}"

    def _build_security_master_record(self, *, row: dict[str, str], ingested_at: datetime) -> SecurityMasterRecord:
        baostock_code = str(row["code"]).strip()
        symbol = self.to_internal_symbol(baostock_code)
        exchange = symbol.split(".", 1)[1]
        list_date = self._coerce_optional_date(row.get("ipoDate"))
        delist_date = self._coerce_optional_date(row.get("outDate"))
        event_date = list_date or ingested_at.date()
        return SecurityMasterRecord(
            security_id=symbol,
            symbol=symbol,
            exchange=exchange,
            security_type=self._map_security_type(row.get("type")),
            name=str(row.get("code_name") or "").strip() or symbol,
            list_date=list_date,
            delist_date=delist_date,
            status=self._map_stock_status(row.get("status")),
            provider="baostock",
            source_record_id=baostock_code,
            source_version=ingested_at.date().isoformat(),
            payload_hash=self._payload_hash(row),
            raw_object_path=f"baostock://query_stock_basic/{baostock_code}",
            event_time=datetime.combine(event_date, time.min, tzinfo=UTC),
            available_at=ingested_at,
            ingested_at=ingested_at,
            schema_version=1,
        )

    def _build_daily_bar_record(
        self,
        *,
        row: dict[str, str],
        ingested_at: datetime,
        adjustment_type: str,
        factor_by_date: dict[date, float],
    ) -> DailyBarRecord:
        symbol = self.to_internal_symbol(row["code"])
        exchange = symbol.split(".", 1)[1]
        trading_date = self._coerce_date(row["date"])
        adjustment_factor = factor_by_date.get(trading_date, 1.0)
        trade_status = str(row.get("tradestatus") or "1")
        return DailyBarRecord(
            security_id=symbol,
            symbol=symbol,
            exchange=exchange,
            trading_date=trading_date,
            open=self._coerce_float(row.get("open")),
            high=self._coerce_float(row.get("high")),
            low=self._coerce_float(row.get("low")),
            close=self._coerce_float(row.get("close")),
            volume=self._coerce_float(row.get("volume")),
            amount=self._coerce_optional_float(row.get("amount")),
            turnover=self._coerce_optional_float(row.get("turn")),
            is_suspended=trade_status != "1",
            is_st=str(row.get("isST") or "0") == "1",
            adjustment_factor=adjustment_factor,
            adjustment_type=adjustment_type,
            provider="baostock",
            source_record_id=f"{row['code']}-{row['date']}-{adjustment_type}",
            source_version=ingested_at.date().isoformat(),
            payload_hash=self._payload_hash(row),
            raw_object_path=f"baostock://query_history_k_data_plus/{row['code']}/{row['date']}",
            event_time=datetime.combine(trading_date, time.min, tzinfo=UTC),
            available_at=ingested_at,
            ingested_at=ingested_at,
            schema_version=1,
        )

    @staticmethod
    def _build_adjustment_factor_map(rows: list[dict[str, str]], adjustment_type: str) -> dict[date, float]:
        factor_field = "adjustFactor"
        if adjustment_type in {"qfq", "forward"}:
            factor_field = "foreAdjustFactor"
        elif adjustment_type in {"hfq", "backward"}:
            factor_field = "backAdjustFactor"
        factors: dict[date, float] = {}
        for row in rows:
            operate_date = row.get("dividOperateDate")
            if not operate_date:
                continue
            factors[date.fromisoformat(str(operate_date))] = float(row.get(factor_field) or row.get("adjustFactor") or 1.0)
        return factors

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

    def _coerce_optional_date(self, value: Any) -> date | None:
        if value in {None, ""}:
            return None
        return self._coerce_date(value)

    @staticmethod
    def _coerce_float(value: Any) -> float:
        if value in {None, ""}:
            return 0.0
        return float(value)

    def _coerce_optional_float(self, value: Any) -> float | None:
        if value in {None, ""}:
            return None
        return self._coerce_float(value)

    @staticmethod
    def _map_adjustment_flag(adjustment_type: str) -> str:
        normalized = str(adjustment_type).strip().lower()
        mapping = {
            "hfq": "1",
            "backward": "1",
            "qfq": "2",
            "forward": "2",
            "none": "3",
        }
        try:
            return mapping[normalized]
        except KeyError as exc:
            raise FinanceProviderError(f"unsupported adjustment_type: {adjustment_type}") from exc

    @staticmethod
    def _map_security_type(value: Any) -> str:
        mapping = {
            "1": "stock",
            "2": "index",
            "3": "other",
        }
        return mapping.get(str(value or "").strip(), "stock")

    @staticmethod
    def _map_stock_status(value: Any) -> str:
        mapping = {
            "1": "active",
            "0": "inactive",
        }
        return mapping.get(str(value or "").strip(), "active")

    @staticmethod
    def _is_a_share_code(code: str) -> bool:
        normalized = str(code).strip().lower()
        return normalized.startswith(("sh.6", "sh.688", "sz.0", "sz.3", "bj."))

    @staticmethod
    def _payload_hash(row: dict[str, Any]) -> str:
        return sha256(repr(sorted(row.items())).encode("utf-8")).hexdigest()


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
