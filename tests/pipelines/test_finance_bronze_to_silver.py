from __future__ import annotations

from datetime import UTC, datetime

from backend.pipelines.finance_bronze_to_silver import FinanceBronzeToSilverPipeline
from backend.schemas.finance import DailyBarRecord, SecurityMasterRecord, TradingCalendarRecord


class FakeBronzeWriter:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def read_json(self, object_path: str) -> dict:
        return self.payload


class FakeFinanceWriter:
    def __init__(self) -> None:
        self.security_master_records: list[SecurityMasterRecord] = []
        self.trading_calendar_records: list[TradingCalendarRecord] = []
        self.daily_bar_records: list[DailyBarRecord] = []

    def append_security_master(self, records: list[SecurityMasterRecord]) -> int:
        inserted = sum(1 for record in records if record not in self.security_master_records)
        self.security_master_records.extend(records)
        return inserted

    def append_trading_calendar(self, records: list[TradingCalendarRecord]) -> int:
        inserted = sum(1 for record in records if record not in self.trading_calendar_records)
        self.trading_calendar_records.extend(records)
        return inserted

    def append_daily_bars(self, records: list[DailyBarRecord]) -> int:
        by_key = {
            (record.provider, record.adjustment_type, record.security_id, record.trading_date): record
            for record in self.daily_bar_records
        }
        inserted = 0
        for record in records:
            key = (record.provider, record.adjustment_type, record.security_id, record.trading_date)
            if key not in by_key:
                inserted += 1
            by_key[key] = record
        self.daily_bar_records = list(by_key.values())
        return inserted

    def existing_daily_keys(self, keys):
        wanted = set(keys)
        return {
            (record.provider, record.adjustment_type, record.security_id, record.trading_date)
            for record in self.daily_bar_records
            if (record.provider, record.adjustment_type, record.security_id, record.trading_date) in wanted
        }


def _bronze_payload() -> dict:
    return {
        "domain": "finance",
        "provider": "baostock",
        "ingested_at": "2026-07-14T09:00:00Z",
        "source_version": "snapshot-1",
        "request_params": {"adjustment_type": "qfq"},
        "raw_response": {
            "security_master": [
                {
                    "symbol": "600519.SH",
                    "exchange": "SH",
                    "security_type": "stock",
                    "name": "Kweichow Moutai",
                    "list_date": "2001-08-27",
                }
            ],
            "trading_calendar": [
                {
                    "exchange": "SH",
                    "trading_date": "2026-07-13",
                    "is_open": True,
                }
            ],
            "daily_bars": [
                {
                    "symbol": "600519.SH",
                    "exchange": "SH",
                    "trading_date": "2026-07-13",
                    "open": 1450.0,
                    "high": 1468.0,
                    "low": 1448.0,
                    "close": 1460.0,
                    "volume": 12345,
                    "amount": 18000000,
                    "turnover": 0.012,
                    "adjustment_factor": 1.02,
                },
                {
                    "symbol": "600519.SH",
                    "exchange": "SH",
                    "trading_date": "2026-07-13",
                    "open": 1450.0,
                    "high": 1468.0,
                    "low": 1448.0,
                    "close": 1460.0,
                    "volume": 12345,
                    "amount": 18000000,
                    "turnover": 0.012,
                    "adjustment_factor": 1.02,
                },
                {
                    "symbol": "600519.SH",
                    "exchange": "SH",
                    "trading_date": "2026-07-14",
                    "open": 0,
                    "high": 0,
                    "low": 0,
                    "close": 0,
                    "volume": 0,
                },
                {
                    "symbol": "600519.SH",
                    "exchange": "SH",
                    "trading_date": "2026-07-15",
                    "open": 1500.0,
                    "high": 1490.0,
                    "low": 1495.0,
                    "close": 1510.0,
                    "volume": 5000,
                },
            ],
        },
    }


def test_finance_pipeline_cleans_deduplicates_and_flags_suspension() -> None:
    writer = FakeFinanceWriter()
    pipeline = FinanceBronzeToSilverPipeline(bronze_writer=FakeBronzeWriter(_bronze_payload()), finance_writer=writer)

    stats = pipeline.process_object("finance/provider=baostock/ingest_date=2026-07-14/hour=09/sample.json")

    assert stats.read_count == 6
    assert stats.duplicate_count == 1
    assert stats.filtered_count == 1
    assert stats.written_security_master == 1
    assert stats.written_trading_calendar == 1
    assert stats.written_daily_bars == 2
    assert writer.daily_bar_records[1].is_suspended is True
    assert writer.security_master_records[0].security_id == "600519.SH"
    assert writer.security_master_records[0].symbol == "600519"


def test_finance_pipeline_does_not_scan_entire_daily_bars_table() -> None:
    class NoScanFinanceWriter(FakeFinanceWriter):
        def scan_daily_bars(self) -> list[DailyBarRecord]:
            raise AssertionError("process_object must not scan daily_bars")

    writer = NoScanFinanceWriter()
    stats = FinanceBronzeToSilverPipeline(
        bronze_writer=FakeBronzeWriter(_bronze_payload()), finance_writer=writer
    ).process_object("finance/provider=baostock/ingest_date=2026-07-14/hour=09/sample.json")

    assert stats.written_daily_bars == 2


def test_finance_pipeline_is_idempotent_for_same_snapshot() -> None:
    writer = FakeFinanceWriter()
    pipeline = FinanceBronzeToSilverPipeline(bronze_writer=FakeBronzeWriter(_bronze_payload()), finance_writer=writer)

    first = pipeline.process_object("finance/provider=baostock/ingest_date=2026-07-14/hour=09/sample.json")
    second = pipeline.process_object("finance/provider=baostock/ingest_date=2026-07-14/hour=09/sample.json")

    assert first.written_daily_bars == 2
    assert second.written_daily_bars == 0
    assert second.duplicate_count == 1
