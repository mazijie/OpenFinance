from __future__ import annotations

from datetime import UTC, date, datetime

from backend.repositories.finance_repository import FinanceRepository
from backend.schemas.finance import DailyBarRecord
from backend.services.finance_service import FinanceService
from backend.tools.market import MarketDataTool


class FakeFinanceWriter:
    def __init__(self, rows: list[DailyBarRecord]) -> None:
        self.rows = rows
        self.query_calls: list[dict] = []

    def scan_daily_bars(self) -> list[DailyBarRecord]:
        return list(self.rows)

    def scan_daily_bars_for_query(self, **kwargs) -> list[DailyBarRecord]:
        self.query_calls.append(kwargs)
        return [
            row
            for row in self.rows
            if (not kwargs["security_id"] or row.security_id == kwargs["security_id"])
            and (not kwargs["symbol"] or row.symbol.upper() == kwargs["symbol"].upper())
            and row.adjustment_type == kwargs["adjustment_type"]
            and kwargs["start_date"] <= row.trading_date <= kwargs["end_date"]
            and row.available_at <= kwargs["as_of"]
            and row.ingested_at <= kwargs["as_of"]
        ]


def _bars() -> list[DailyBarRecord]:
    return [
        DailyBarRecord(
            security_id="600519.SH",
            symbol="600519.SH",
            exchange="SH",
            trading_date=date(2026, 7, 10),
            open=1450.0,
            high=1460.0,
            low=1440.0,
            close=1455.0,
            volume=10000,
            amount=15000000,
            turnover=0.01,
            is_suspended=False,
            is_st=False,
            adjustment_factor=1.0,
            adjustment_type="qfq",
            provider="baostock",
            source_record_id="600519.SH-2026-07-10",
            source_version="v1",
            payload_hash="hash-1",
            raw_object_path="finance/provider=baostock/one.json",
            event_time=datetime(2026, 7, 10, 0, 0, tzinfo=UTC),
            available_at=datetime(2026, 7, 10, 16, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 7, 10, 16, 5, tzinfo=UTC),
            schema_version=1,
        ),
        DailyBarRecord(
            security_id="600519.SH",
            symbol="600519.SH",
            exchange="SH",
            trading_date=date(2026, 7, 11),
            open=1460.0,
            high=1475.0,
            low=1458.0,
            close=1470.0,
            volume=12000,
            amount=18000000,
            turnover=0.012,
            is_suspended=False,
            is_st=False,
            adjustment_factor=1.01,
            adjustment_type="qfq",
            provider="baostock",
            source_record_id="600519.SH-2026-07-11",
            source_version="v1",
            payload_hash="hash-2",
            raw_object_path="finance/provider=baostock/two.json",
            event_time=datetime(2026, 7, 11, 0, 0, tzinfo=UTC),
            available_at=datetime(2026, 7, 11, 16, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 7, 11, 16, 5, tzinfo=UTC),
            schema_version=1,
        ),
        DailyBarRecord(
            security_id="600519.SH",
            symbol="600519.SH",
            exchange="SH",
            trading_date=date(2026, 7, 11),
            open=1460.0,
            high=1480.0,
            low=1458.0,
            close=1478.0,
            volume=12500,
            amount=18100000,
            turnover=0.013,
            is_suspended=False,
            is_st=False,
            adjustment_factor=1.02,
            adjustment_type="qfq",
            provider="baostock",
            source_record_id="600519.SH-2026-07-11",
            source_version="v2",
            payload_hash="hash-3",
            raw_object_path="finance/provider=baostock/two-revision.json",
            event_time=datetime(2026, 7, 11, 0, 0, tzinfo=UTC),
            available_at=datetime(2026, 7, 12, 2, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 7, 12, 2, 5, tzinfo=UTC),
            schema_version=1,
        ),
    ]


def test_market_tool_uses_internal_lakehouse_summary() -> None:
    writer = FakeFinanceWriter(_bars())
    repository = FinanceRepository(writer=writer)
    service = FinanceService(repository=repository)
    tool = MarketDataTool(service=service)

    result = tool.run(
        symbol="600519.SH",
        start_date="2026-07-10",
        end_date="2026-07-11",
        adjustment_type="qfq",
        as_of="2026-07-11T23:59:59+00:00",
    )

    assert result["success"] is True
    assert result["data"]["source"] == "internal_lakehouse"
    assert result["data"]["last_close"] == 1470.0
    assert result["data"]["interval_return"] == (1470.0 / 1455.0) - 1.0
    assert writer.query_calls[0]["symbol"] == "600519.SH"
    assert writer.query_calls[0]["start_date"] == date(2026, 7, 10)


def test_market_tool_respects_as_of_for_revisions() -> None:
    repository = FinanceRepository(writer=FakeFinanceWriter(_bars()))
    service = FinanceService(repository=repository)
    tool = MarketDataTool(service=service)

    result = tool.run(
        symbol="600519.SH",
        start_date="2026-07-10",
        end_date="2026-07-11",
        adjustment_type="qfq",
        as_of="2026-07-12T03:00:00+00:00",
    )

    assert result["success"] is True
    assert result["data"]["last_close"] == 1478.0
    assert result["data"]["bars"][-1]["raw_object_path"].endswith("two-revision.json")
