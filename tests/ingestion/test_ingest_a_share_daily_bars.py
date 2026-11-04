from __future__ import annotations

from datetime import date
import importlib

from backend.ingestion.jobs.ingest_a_share_daily_bars import ingest_a_share_daily_bars
from backend.providers.finance import InMemoryFinanceProvider
from backend.schemas.finance import DailyBarRecord, SecurityMasterRecord, TradingCalendarRecord


class FakeBronzeWriter:
    def __init__(self) -> None:
        self.calls = []

    def write_finance_raw(self, **kwargs):
        self.calls.append(kwargs)
        return "finance/provider=baostock/ingest_date=2026-08-03/hour=10/sample.json"


def test_ingest_a_share_daily_bars_writes_bronze_payload() -> None:
    provider = InMemoryFinanceProvider(
        security_master=[
            SecurityMasterRecord(
                security_id="600519.SH",
                symbol="600519.SH",
                exchange="SH",
                security_type="stock",
                name="贵州茅台",
                provider="baostock",
                source_record_id="sh.600519",
                source_version="2026-08-03",
                payload_hash="hash-security",
                raw_object_path="baostock://query_stock_basic/sh.600519",
                event_time="2026-08-03T00:00:00+00:00",
                available_at="2026-08-03T10:00:00+00:00",
                ingested_at="2026-08-03T10:00:00+00:00",
                schema_version=1,
            )
        ],
        trading_calendar=[
            TradingCalendarRecord(
                exchange="SH",
                trading_date=date(2026, 8, 3),
                is_open=True,
                session_open_at="2026-08-03T01:30:00+00:00",
                session_close_at="2026-08-03T07:00:00+00:00",
            )
        ],
        daily_bars=[
            DailyBarRecord(
                security_id="600519.SH",
                symbol="600519.SH",
                exchange="SH",
                trading_date=date(2026, 8, 3),
                open=1450,
                high=1460,
                low=1440,
                close=1455,
                volume=10000,
                amount=15000000,
                turnover=0.01,
                is_suspended=False,
                is_st=False,
                adjustment_factor=1.0,
                adjustment_type="qfq",
                provider="baostock",
                source_record_id="sh.600519-2026-08-03-qfq",
                source_version="2026-08-03",
                payload_hash="hash-bar",
                raw_object_path="baostock://query_history_k_data_plus/sh.600519/2026-08-03",
                event_time="2026-08-03T00:00:00+00:00",
                available_at="2026-08-03T10:00:00+00:00",
                ingested_at="2026-08-03T10:00:00+00:00",
                schema_version=1,
            )
        ],
    )
    writer = FakeBronzeWriter()

    result = ingest_a_share_daily_bars(
        symbols=["600519.SH"],
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 3),
        adjustment_type="qfq",
        provider=provider,
        writer=writer,
    )

    assert result.daily_bar_count == 1
    assert result.no_data_symbols == []
    assert writer.calls[0]["provider"] == "baostock"
    assert writer.calls[0]["raw_response"]["security_master"][0]["symbol"] == "600519.SH"


def test_ingest_a_share_daily_bars_retries_one_symbol(monkeypatch) -> None:
    ingestion_module = importlib.import_module("backend.ingestion.jobs.ingest_a_share_daily_bars")

    class FlakyProvider(InMemoryFinanceProvider):
        def __init__(self) -> None:
            super().__init__()
            self.daily_calls = 0

        def fetch_security_master(self, *, symbols=None):
            return []

        def fetch_daily_bars(self, *, symbols, start_date, end_date, adjustment_type="qfq"):
            self.daily_calls += 1
            if self.daily_calls == 1:
                raise RuntimeError("temporary BaoStock failure")
            return []

        def fetch_trading_calendar(self, *, exchange, start_date, end_date):
            return []

    provider = FlakyProvider()
    writer = FakeBronzeWriter()
    monkeypatch.setattr("backend.ingestion.jobs.ingest_a_share_daily_bars.settings.BAOSTOCK_MAX_RETRIES", 1)
    monkeypatch.setattr(ingestion_module.time_module, "sleep", lambda _: None)

    result = ingest_a_share_daily_bars(
        symbols=["600519.SH"],
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 3),
        provider=provider,
        writer=writer,
    )

    assert provider.daily_calls == 2
    assert result.processed_symbol_count == 1
    assert result.no_data_symbols == ["600519.SH"]
    assert writer.calls[0]["raw_response"]["daily_bars"] == []


def test_tencent_daily_and_baostock_metadata_are_independent(monkeypatch):
    import httpx
    from backend.providers.daily_bars import TencentDailyBarsProvider

    requests = []
    def handler(req):
        requests.append(req)
        code = req.url.params["param"].split(",")[0]
        if code == "sz000001":
            return httpx.Response(403, text="denied")
        return httpx.Response(200, json={"code": 0, "data": {code: {"qfqday": [
            ["2026-08-03", "10", "11", "12", "9", "100", {}, "1", "10"]
        ]}}})
    metadata = InMemoryFinanceProvider()
    writer = FakeBronzeWriter()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = ingest_a_share_daily_bars(
            symbols=["600519.SH", "000001.SZ"], start_date=date(2026, 8, 1), end_date=date(2026, 8, 3),
            provider=TencentDailyBarsProvider(client=client), metadata_provider=metadata, writer=writer,
            idempotency_key="tencent-job-1",
        )
    assert result.provider == "tencent" and result.failed_symbols == ["000001.SZ"]
    assert result.processed_symbol_count == 1 and result.daily_bar_count == 1
    assert len(requests) == 2  # The ingestion layer must not retry a permanent Tencent failure.
    assert writer.calls[0]["provider"] == "tencent"
    assert writer.calls[0]["object_key"] == "tencent-job-1"
    assert writer.calls[0]["raw_response"]["daily_bars"][0]["is_st"] is None


def test_lease_loss_is_not_retried_or_written():
    import pytest
    from backend.lakehouse.job_lease import JobLeaseLost

    writer = FakeBronzeWriter()
    class Provider(InMemoryFinanceProvider):
        def fetch_daily_bars(self, **kwargs):
            raise JobLeaseLost("lease expired")
    with pytest.raises(JobLeaseLost):
        ingest_a_share_daily_bars(symbols=["600519.SH"], start_date=date(2026, 8, 1), end_date=date(2026, 8, 3),
                                 provider=Provider(), writer=writer)
    assert writer.calls == []
