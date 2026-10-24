from __future__ import annotations

from datetime import UTC, date, datetime

from backend.schemas.finance import SecurityMasterRecord, TradingCalendarRecord
from backend.services.a_share_gap_service import AShareGapService


class FakeBudget:
    def __init__(self, remaining_value: int) -> None:
        self.remaining_value = remaining_value

    def remaining(self, *, provider: str, daily_limit: int) -> int:
        assert provider == "baostock"
        return self.remaining_value


class FakeUniverseCache:
    def __init__(self, symbols: list[str] | None = None) -> None:
        self.symbols = symbols or ["000001.SZ", "000002.SZ"]

    def get(self, *, provider: str, as_of_date: date):
        return list(self.symbols)

    def put(self, *, provider: str, as_of_date: date, symbols: list[str]) -> None:
        self.symbols = list(symbols)


class FakeWriter:
    def __init__(self) -> None:
        metadata = datetime(2026, 1, 1, tzinfo=UTC)
        self.security_master = [
            SecurityMasterRecord(
                security_id="000001.SZ",
                symbol="000001.SZ",
                exchange="SZ",
                security_type="stock",
                name="A",
                status="active",
                provider="baostock",
                raw_object_path="test://a",
                event_time=metadata,
                available_at=metadata,
                ingested_at=metadata,
            ),
            SecurityMasterRecord(
                security_id="000002.SZ",
                symbol="000002.SZ",
                exchange="SZ",
                security_type="stock",
                name="B",
                status="active",
                provider="baostock",
                raw_object_path="test://b",
                event_time=metadata,
                available_at=metadata,
                ingested_at=metadata,
            ),
        ]
        self.calendar = [
            TradingCalendarRecord(exchange="SZ", trading_date=date(2026, 1, 5), is_open=True)
        ]
        self.actual = {("000001.SZ", date(2026, 1, 5))}

    def scan_security_master(self):
        return self.security_master

    def scan_trading_calendar(self):
        return self.calendar

    def scan_daily_bar_keys(self, **kwargs):
        return self.actual

    def append_trading_calendar(self, records):
        self.calendar.extend(records)
        return len(records)


def test_gap_plan_uses_one_range_per_symbol(monkeypatch) -> None:
    monkeypatch.setattr("backend.services.a_share_gap_service.settings.BAOSTOCK_DAILY_REQUEST_LIMIT", 50)
    monkeypatch.setattr("backend.services.a_share_gap_service.settings.A_SHARE_REQUESTS_PER_SYMBOL", 2)
    monkeypatch.setattr("backend.services.a_share_gap_service.settings.A_SHARE_MAX_SYMBOLS_PER_RUN", 24)
    service = AShareGapService(
        writer=FakeWriter(),
        provider=object(),
        budget_store=FakeBudget(10),
        universe_cache=FakeUniverseCache(),
    )

    result = service.plan(
        adjustment_type="qfq",
        lookback_days=1,
        end_date=date(2026, 1, 5),
    )

    assert result["status"] == "available"
    assert result["missing_pair_count"] == 1
    assert result["selected_symbol_count"] == 1
    assert result["pending_symbol_count"] == 0
    assert result["estimated_requests"] == 2
    assert result["plans"] == [
        {
            "symbol": "000002.SZ",
            "exchange": "SZ",
            "start_date": "2026-01-05",
            "end_date": "2026-01-05",
            "missing_date_count": 1,
            "needs_metadata": False,
        }
    ]


def test_gap_plan_returns_quota_exhausted_without_requesting_api(monkeypatch) -> None:
    monkeypatch.setattr("backend.services.a_share_gap_service.settings.BAOSTOCK_DAILY_REQUEST_LIMIT", 50)
    monkeypatch.setattr("backend.services.a_share_gap_service.settings.A_SHARE_REQUESTS_PER_SYMBOL", 2)
    service = AShareGapService(
        writer=FakeWriter(),
        provider=object(),
        budget_store=FakeBudget(1),
        universe_cache=FakeUniverseCache(),
    )

    result = service.plan(
        adjustment_type="qfq",
        lookback_days=1,
        end_date=date(2026, 1, 5),
    )

    assert result["status"] == "quota_exhausted"
    assert result["selected_symbol_count"] == 0
    assert result["pending_symbol_count"] == 1


def test_gap_plan_does_not_refresh_calendar_when_closed_day_is_latest(monkeypatch) -> None:
    monkeypatch.setattr("backend.services.a_share_gap_service.settings.A_SHARE_REQUESTS_PER_SYMBOL", 2)
    writer = FakeWriter()
    writer.calendar.append(
        TradingCalendarRecord(exchange="SZ", trading_date=date(2026, 1, 6), is_open=False)
    )
    service = AShareGapService(
        writer=writer,
        provider=object(),
        budget_store=FakeBudget(10),
        universe_cache=FakeUniverseCache(),
    )

    result = service.plan(
        adjustment_type="qfq",
        lookback_days=2,
        end_date=date(2026, 1, 6),
    )

    assert result["calendar_refresh_requests"] == 0
