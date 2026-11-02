from datetime import date, timedelta
from types import SimpleNamespace

import pytest
from backend.ingestion.jobs.sync_trading_calendar import validate_calendar
from backend.schemas.finance import TradingCalendarRecord


def row(day, is_open=True):
    return TradingCalendarRecord(exchange="SH", trading_date=day, is_open=is_open)


def test_calendar_validation_requires_tomorrow_and_contiguous_dates():
    today = date(2026, 11, 2)
    with pytest.raises(ValueError, match="today and tomorrow"):
        validate_calendar([row(today)], today, today + timedelta(days=3))
    with pytest.raises(ValueError, match="gap"):
        validate_calendar([row(today), row(today + timedelta(days=1)), row(today + timedelta(days=3))], today, today + timedelta(days=3))
    assert len(validate_calendar([row(today), row(today + timedelta(days=1))], today, today + timedelta(days=3))) == 2


def test_each_manual_or_scheduled_run_fetches_and_writes_calendar(monkeypatch):
    import backend.ingestion.jobs.sync_trading_calendar as module
    today = date(2040, 1, 2)
    objects, stored, calls = {}, [], []
    class Provider:
        def fetch_trading_calendar(self, *, exchange, start_date, end_date):
            calls.append((exchange, start_date, end_date))
            return [row(start_date + timedelta(days=i)) for i in range(4)]
    class Bronze:
        def write_finance_raw(self, *, raw_response, **kwargs):
            path = f"fixture-{len(objects)}"
            objects[path] = {"raw_response": raw_response}
            return path
        def read_json(self, path): return objects[path]
    class Writer:
        def scan_trading_calendar(self): return stored
    class Pipeline:
        def __init__(self, **kwargs): pass
        def process_object(self, path):
            stored.clear()
            stored.extend(TradingCalendarRecord.model_validate(item) for item in objects[path]["raw_response"]["trading_calendar"])
            return SimpleNamespace(failed_count=0, filtered_count=0)
    monkeypatch.setattr(module, "BaoStockFinanceProvider", Provider)
    monkeypatch.setattr(module, "BronzeWriter", Bronze)
    monkeypatch.setattr(module, "FinanceWriter", Writer)
    monkeypatch.setattr(module, "FinanceBronzeToSilverPipeline", Pipeline)
    result = module.run(today=today)
    assert result["status"] == "success" and result["record_count"] == 4
    assert module.run(today=today)["status"] == "success"
    assert len(calls) == 2
