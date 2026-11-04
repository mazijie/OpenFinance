from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
from threading import Barrier

import httpx
import pytest

from backend.config import settings
from backend.providers.daily_bars import TencentDailyBarsProvider, daily_windows, estimate_daily_requests
from backend.providers.finance import FinanceProviderError


def row(day="2026-11-04", *, negative=False):
    prices = ["-10", "-9", "-8", "-11"] if negative else ["10", "11", "12", "9"]
    return [day, *prices, "20", {}, "1.25", "3.5", ""]


def response(request, bars=None, *, key="qfqday", status=200):
    code = request.url.params["param"].split(",")[0]
    return httpx.Response(status, json={"code": 0, "data": {code: {key: [row()] if bars is None else bars}}})


def fetch(provider, **kw):
    return provider.fetch_daily_bars(symbols=kw.pop("symbols", ["600519.SH"]),
                                    start_date=kw.pop("start_date", date(2026, 11, 1)),
                                    end_date=kw.pop("end_date", date(2026, 11, 4)), **kw)


@pytest.mark.parametrize("adjust,key", [("qfq", "qfqday"), ("hfq", "hfqday"), ("none", "day"), ("qfq", "day")])
def test_adjustments_and_optional_fields(adjust, key):
    with httpx.Client(transport=httpx.MockTransport(lambda req: response(req, key=key))) as client:
        bars = fetch(TencentDailyBarsProvider(client=client), adjustment_type=adjust)
    bar = bars[0]
    assert (bar.open, bar.close, bar.high, bar.low) == (10, 11, 12, 9)
    assert bar.volume == 2000 and bar.amount == 35000 and bar.turnover == 1.25
    assert bar.adjustment_type == adjust and bar.provider == "tencent"
    assert bar.is_st is None and bar.is_suspended is None and bar.adjustment_factor is None
    assert bar.available_at >= bar.event_time


@pytest.mark.parametrize("symbol,multiplier", [("688981.SH", 1), ("sh689009", 1), ("300750.SZ", 100), ("920799.BJ", 100)])
def test_volume_units_and_codes(symbol, multiplier):
    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        bar = fetch(TencentDailyBarsProvider(client=client), symbols=[symbol])[0]
    assert bar.volume == 20 * multiplier
    assert bar.security_id.endswith((".SH", ".SZ", ".BJ"))


def test_segments_filter_sort_and_deduplicate():
    requests = []
    def handler(req):
        requests.append(req)
        _, _, start, end, count, _ = req.url.params["param"].split(",")
        assert count == "640"
        return response(req, [row(start), row(end), row(end), row("2020-01-01"), row("2030-01-01")])
    start, end = date(2023, 1, 1), date(2026, 11, 4)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        bars = fetch(TencentDailyBarsProvider(client=client), start_date=start, end_date=end)
    assert len(requests) == estimate_daily_requests("tencent", start, end) == 2
    assert len(bars) == 4
    assert all(start <= bar.trading_date <= end for bar in bars)
    assert [bar.trading_date for bar in bars] == sorted({bar.trading_date for bar in bars})
    assert all((b - a).days < 730 for a, b in daily_windows(start, end))


def test_empty_response_never_synthesizes_a_bar():
    with httpx.Client(transport=httpx.MockTransport(lambda req: response(req, []))) as client:
        assert fetch(TencentDailyBarsProvider(client=client)) == []


def test_negative_forward_adjusted_prices_are_valid():
    with httpx.Client(transport=httpx.MockTransport(lambda req: response(req, [row(negative=True)]))) as client:
        assert fetch(TencentDailyBarsProvider(client=client))[0].close == -9
        with pytest.raises(FinanceProviderError):
            fetch(TencentDailyBarsProvider(client=client), adjustment_type="none")


@pytest.mark.parametrize("failure", [429, 503, "timeout"])
def test_retry_only_transient_errors_and_observe_attempts(monkeypatch, failure):
    monkeypatch.setattr(settings, "TENCENT_MAX_RETRIES", 2)
    requests, sleeps, events = [], [], []
    def handler(req):
        requests.append(req)
        if len(requests) < 3:
            if failure == "timeout":
                raise httpx.ReadTimeout("temporary timeout", request=req)
            return response(req, status=failure)
        return response(req)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert len(fetch(TencentDailyBarsProvider(client=client, sleep=sleeps.append, observer=events.append))) == 1
    assert len(requests) == 3 and sleeps == [2, 4]
    assert events[0]["retries"] == 2 and events[0]["error"] is None
    assert len(events[0]["attempts"]) == 3
    assert events[0]["total_seconds"] >= events[0]["network_seconds"]


@pytest.mark.parametrize("status,text", [(403, "denied"), (200, "not json"), (200, '{"code":1,"msg":"invalid"}'), (200, '{"code":0,"data":{}}')])
def test_permanent_and_malformed_errors_are_not_retried(status, text):
    requests = []
    def handler(req):
        requests.append(req)
        return httpx.Response(status, text=text)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(FinanceProviderError):
            fetch(TencentDailyBarsProvider(client=client, sleep=lambda _: pytest.fail("unexpected retry")))
    assert len(requests) == 1


def test_retries_stop_at_limit(monkeypatch):
    monkeypatch.setattr(settings, "TENCENT_MAX_RETRIES", 2)
    events = []
    with httpx.Client(transport=httpx.MockTransport(lambda req: response(req, status=500))) as client:
        with pytest.raises(FinanceProviderError):
            fetch(TencentDailyBarsProvider(client=client, sleep=lambda _: None, observer=events.append))
    assert events[0]["retries"] == 2 and len(events[0]["attempts"]) == 3
    assert events[0]["error"]


def test_concurrency_does_not_take_baostock_session_lock(monkeypatch):
    monkeypatch.setattr(settings, "TENCENT_DAILY_REQUEST_LIMIT", 100)
    barrier = Barrier(8, timeout=5)
    class Budget:
        def session_lock(self, **_):
            pytest.fail("Tencent must not acquire a provider session lock")
        def reserve(self, **kw):
            assert kw == {"provider": "tencent", "count": 1, "daily_limit": 100}
    def handler(req):
        barrier.wait()
        return response(req)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: fetch(TencentDailyBarsProvider(client=client, budget_store=Budget())), range(8)))
    assert all(len(bars) == 1 for bars in results)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1"])
def test_invalid_volume_is_rejected(value):
    raw = row()
    raw[5] = value
    with httpx.Client(transport=httpx.MockTransport(lambda req: response(req, [raw]))) as client:
        with pytest.raises(FinanceProviderError):
            fetch(TencentDailyBarsProvider(client=client))
