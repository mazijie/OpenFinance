from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import pytest

from backend.providers.finance import BaoStockFinanceProvider, FinanceProviderError


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


    def query_trade_dates(self, start_date=None, end_date=None):
        return FakeResultSet(
            ["calendar_date", "is_trading_day"],
            [["2026-07-01", "1"], ["2026-07-02", "0"]],
        )


def test_baostock_provider_fetches_calendar() -> None:
    client = FakeBaoStockClient()
    provider = BaoStockFinanceProvider(client=client)

    calendar = provider.fetch_trading_calendar(exchange="SH", start_date=date(2026, 7, 1), end_date=date(2026, 7, 2))
    assert len(calendar) == 2
    assert calendar[0].is_open is True
    assert calendar[1].is_open is False


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
