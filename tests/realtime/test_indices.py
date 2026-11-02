from datetime import datetime, timedelta, timezone

from backend.services.realtime_quote_service import RealtimeQuoteService
from realtime.tencent import CHINA, parse_response


def test_index_response_parses_separately():
    fields = [""] * 31
    fields[1:5] = ["上证指数", "000001", "3624.10", "3599.02"]
    fields[30] = "20261102093000"
    moment = datetime(2026, 11, 2, 9, 30, tzinfo=CHINA)
    response = ('v_sh000001="' + '~'.join(fields) + '";').encode("gb18030")
    result = parse_response(response, ["000001.SH"], moment, moment)
    assert result["000001.SH"]["previous_close"] == 3599.02
    assert result["000001.SH"]["status"] == "valid"


def test_lookup_returns_index_changes_and_stale_status_without_changing_market(monkeypatch):
    now = datetime(2026, 11, 2, 2, 0, tzinfo=timezone.utc)
    index = {"symbol": "000001.SH", "name": "上证指数", "price": 110.0,
             "previous_close": 100.0, "source_time": now - timedelta(hours=1),
             "captured_at": now - timedelta(hours=1), "quote_status": "valid"}
    market = {"rising": 3, "falling": 2, "flat": 1}

    class Result:
        def __init__(self, value): self.value = value
        def fetchone(self): return self.value
        def fetchall(self): return self.value

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params):
            if "realtime_market_rounds" in sql: return Result(market)
            if "realtime_collection_rounds" in sql: return Result(None)
            if "realtime_indices" in sql: return Result([index])
            raise AssertionError(sql)

    monkeypatch.setattr("backend.services.realtime_quote_service.psycopg.connect", lambda *a, **kw: Connection())
    monkeypatch.setattr("backend.services.realtime_quote_service.trading_day", lambda day: True)
    result = RealtimeQuoteService().lookup(now=now)
    assert result["indices"][0]["change"] == 10
    assert result["indices"][0]["change_percent"] == 10
    assert result["indices"][0]["status"] == "not_updated"
    assert result["market"] == market
    assert result["quote"] is None
