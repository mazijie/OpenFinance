from datetime import datetime, timezone, timedelta

import pytest
from backend.services.realtime_quote_service import RealtimeQuoteService, quote_status, symbol_candidates


def test_quote_status_respects_calendar_source_failure_and_timestamp():
    now = datetime(2026, 11, 2, 2, 0, tzinfo=timezone.utc)
    row = {"source_time": now - timedelta(seconds=10), "quote_status": "valid"}
    assert quote_status(row, now, True) == "fresh"
    assert quote_status(row, now, False) == "closed"
    assert quote_status(row, now, None) == "unknown"
    assert quote_status({**row, "quote_status": "missing"}, now, True) == "source_error"
    assert quote_status({**row, "source_time": now - timedelta(hours=1)}, now, True) == "not_updated"
    assert quote_status({**row, "source_time": now + timedelta(seconds=1)}, now, True) == "not_updated"


def test_six_digit_and_qualified_symbols():
    assert symbol_candidates("600519") == ["600519.SH", "600519.SZ", "600519.BJ"]
    assert symbol_candidates(" 000001.sz ") == ["000001.SZ"]
    with pytest.raises(ValueError):
        symbol_candidates("600519.XX")


def test_intraday_requires_exchange_and_returns_sample_windows(monkeypatch):
    now = datetime(2026, 11, 2, 2, 0, tzinfo=timezone.utc)
    points = [{"symbol": "600519.SH", "time": now, "price": 123.4, "incomplete": False}]

    class Result:
        def fetchall(self): return points

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params):
            assert "window_basis='capture_time'" in sql
            assert params == (["600519.SH"], now, now)
            return Result()

    monkeypatch.setattr("backend.services.realtime_quote_service.psycopg.connect", lambda *a, **kw: Connection())
    assert RealtimeQuoteService().intraday("600519.SH", now)["points"] == points
    with pytest.raises(ValueError):
        RealtimeQuoteService().intraday("600519", now)
