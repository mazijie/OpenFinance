from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import pytest

from backend.providers.finance import AShareUniverseResult, BaoStockFinanceProvider, FinanceProviderError


class FakeLoginResult:
    error_code = "0"
    error_msg = "success"


class FakeResultSet:
    def __init__(self, fields, rows) -> None:
        self.error_code = "0"
        self.error_msg = "success"
        self.fields = fields
        self._rows = list(rows)
        self._index = -1

    def next(self) -> bool:
        self._index += 1
        return self._index < len(self._rows)

    def get_row_data(self):
        return self._rows[self._index]


class FakeBaoStockClient:
    def __init__(self) -> None:
        self.logged_in = 0
        self.logged_out = 0

    def login(self):
        self.logged_in += 1
        return FakeLoginResult()

    def logout(self):
        self.logged_out += 1
        return None

    def query_stock_basic(self, code="", code_name=""):
        return FakeResultSet(
            ["code", "code_name", "ipoDate", "outDate", "type", "status"],
            [[code, "贵州茅台", "2001-08-27", "", "1", "1"]],
        )

    def query_all_stock(self, day=None):
        return FakeResultSet(
            ["code", "tradeStatus", "type"],
            [
                ["sh.600519", "1", "1"],
                ["sz.000001", "1", "1"],
                ["sh.000001", "1", "2"],
                ["sz.159001", "1", "3"],
                ["sh.600000", "0", "1"],
                ["bj.430001", "1", "1"],
            ],
        )

    def query_trade_dates(self, start_date=None, end_date=None):
        return FakeResultSet(
            ["calendar_date", "is_trading_day"],
            [["2026-07-01", "1"], ["2026-07-02", "0"]],
        )

    def query_history_k_data_plus(self, code, fields, start_date=None, end_date=None, frequency="d", adjustflag="2"):
        return FakeResultSet(
            fields.split(","),
            [
                ["2026-07-01", code, "1450", "1460", "1440", "1455", "10000", "15000000", "0.01", "1", "0"],
                ["2026-07-02", code, "0", "0", "0", "0", "0", "0", "", "0", "1"],
            ],
        )

    def query_adjust_factor(self, code=None, start_date=None, end_date=None):
        return FakeResultSet(
            ["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"],
            [
                [code, "2026-07-01", "1.01", "0.99", "1.0"],
                [code, "2026-07-02", "1.02", "0.98", "1.0"],
            ],
        )


class FakeCoverageStore:
    def __init__(self, covered: set[date] | None = None) -> None:
        self.covered = covered or set()
        self.queries: list[date] = []

    def is_covered(self, *, universe_as_of_date: date, **kwargs) -> bool:
        self.queries.append(universe_as_of_date)
        return universe_as_of_date in self.covered


def test_baostock_provider_fetches_security_master() -> None:
    client = FakeBaoStockClient()
    provider = BaoStockFinanceProvider(client=client)

    records = provider.fetch_security_master(symbols=["600519.SH"])

    assert len(records) == 1
    assert records[0].symbol == "600519.SH"
    assert records[0].security_type == "stock"
    assert client.logged_in == 1
    assert client.logged_out == 1


def test_baostock_provider_lists_current_a_share_symbols() -> None:
    client = FakeBaoStockClient()
    provider = BaoStockFinanceProvider(client=client)

    result = provider.list_a_share_symbols(day=date(2026, 8, 20), coverage_store=FakeCoverageStore())

    assert result == AShareUniverseResult("available", date(2026, 8, 20), ["000001.SZ", "430001.BJ", "600519.SH"])


def test_baostock_universe_falls_back_after_empty_day(monkeypatch) -> None:
    client = FakeBaoStockClient()
    original = client.query_all_stock
    client.query_all_stock = lambda day=None: FakeResultSet(["code", "tradeStatus", "type"], []) if day == "2026-08-21" else original(day)  # type: ignore[method-assign]

    result = BaoStockFinanceProvider(client=client).list_a_share_symbols(day=date(2026, 8, 21), coverage_store=FakeCoverageStore())

    assert result.status == "available"
    assert result.as_of_date == date(2026, 8, 20)


def test_baostock_universe_stops_at_covered_date() -> None:
    store = FakeCoverageStore({date(2026, 8, 21)})
    client = FakeBaoStockClient()

    result = BaoStockFinanceProvider(client=client).list_a_share_symbols(day=date(2026, 8, 21), coverage_store=store)

    assert result == AShareUniverseResult("already_covered", date(2026, 8, 21), [])
    assert client.logged_in == 0


def test_baostock_universe_can_ignore_daily_coverage() -> None:
    store = FakeCoverageStore({date(2026, 8, 20)})
    client = FakeBaoStockClient()

    result = BaoStockFinanceProvider(client=client).list_a_share_symbols(
        day=date(2026, 8, 20), coverage_store=store, ignore_coverage=True
    )

    assert result.status == "available"
    assert result.symbols
    assert store.queries == []


def test_baostock_universe_empty_range_fails(monkeypatch) -> None:
    client = FakeBaoStockClient()
    client.query_all_stock = lambda day=None: FakeResultSet(["code", "tradeStatus"], [])  # type: ignore[method-assign]
    monkeypatch.setattr("backend.providers.finance.settings.A_SHARE_UNIVERSE_LOOKBACK_DAYS", 2)

    with pytest.raises(FinanceProviderError, match="No A-share universe found"):
        BaoStockFinanceProvider(client=client).list_a_share_symbols(day=date(2026, 8, 21), coverage_store=FakeCoverageStore())


def test_baostock_provider_fetches_calendar_and_daily_bars() -> None:
    client = FakeBaoStockClient()
    provider = BaoStockFinanceProvider(client=client)

    calendar = provider.fetch_trading_calendar(exchange="SH", start_date=date(2026, 7, 1), end_date=date(2026, 7, 2))
    bars = provider.fetch_daily_bars(
        symbols=["600519.SH"],
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
        adjustment_type="qfq",
    )

    assert len(calendar) == 2
    assert calendar[0].is_open is True
    assert len(bars) == 2
    assert bars[0].adjustment_factor == 1.01
    assert bars[1].is_suspended is True
    assert bars[1].is_st is True


def test_calendar_rejects_unknown_status_instead_of_assuming_closed() -> None:
    client = FakeBaoStockClient()
    client.query_trade_dates = lambda **kwargs: FakeResultSet(["calendar_date", "is_trading_day"], [["2026-07-01", ""]])
    with pytest.raises(FinanceProviderError, match="Invalid BaoStock trading-day status"):
        BaoStockFinanceProvider(client=client).fetch_trading_calendar(exchange="SH", start_date=date(2026, 7, 1), end_date=date(2026, 7, 1))


def test_query_rejects_error_reported_only_on_next_page() -> None:
    class FailedPage(FakeResultSet):
        def next(self):
            if super().next():
                return True
            self.error_code = "10002007"
            self.error_msg = "network receive failed"
            return False

    result = FailedPage(["code"], [["sh.600519"]])
    with pytest.raises(FinanceProviderError, match="pagination failed"):
        BaoStockFinanceProvider(client=FakeBaoStockClient())._query_rows(result)


def test_query_rejects_silent_disconnect_after_full_page() -> None:
    result = FakeResultSet(["code"], [["sh.600519"], ["sz.000001"]])
    result.data = result._rows
    result.per_page_count = "2"
    with pytest.raises(FinanceProviderError, match="without confirming EOF"):
        BaoStockFinanceProvider(client=FakeBaoStockClient())._query_rows(result)


@pytest.mark.parametrize("last_page", [[], [["sz.000001"]]])
def test_query_accepts_confirmed_end_after_full_page(last_page) -> None:
    class CompletePages(FakeResultSet):
        per_page_count = "2"

        def next(self):
            if super().next():
                return True
            self.data = last_page
            return False

    result = CompletePages(["code"], [["sh.600519"], ["sz.000001"]])
    result.data = result._rows
    assert len(BaoStockFinanceProvider(client=FakeBaoStockClient())._query_rows(result)) == 2


def test_provider_holds_shared_lock_through_logout_on_failure() -> None:
    events = []

    class Budget:
        @contextmanager
        def session_lock(self, *, provider):
            events.append("lock")
            try:
                yield
            finally:
                events.append("unlock")

    class Client(FakeBaoStockClient):
        def login(self):
            events.append("login")
            return super().login()

        def logout(self):
            events.append("logout")
            return super().logout()

    p = BaoStockFinanceProvider(client=Client(), budget_store=Budget())
    with pytest.raises(RuntimeError, match="request failed"):
        with p._session():
            events.append("query")
            raise RuntimeError("request failed")
    assert events == ["lock", "login", "query", "logout", "unlock"]
