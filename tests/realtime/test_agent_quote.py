from datetime import datetime, timezone
import json
from backend.harness.tools import ToolContext, ToolRegistry


def test_agent_uses_cutoff_and_does_not_present_stale_price(monkeypatch):
    cutoff = datetime(2026, 11, 2, 2, tzinfo=timezone.utc)
    calls = []
    class Service:
        def lookup(self, symbol, as_of):
            calls.append((symbol, as_of))
            return {"quote": {"status": "not_updated", "price": 12}}
    monkeypatch.setattr("backend.harness.tools.RealtimeQuoteService", Service)
    context = ToolContext(thread_id="t", turn_id="r", run_kind="interactive", as_of=cutoff,
                          store=object(), evidence_service=object(), web_service=object())
    tools = ToolRegistry(context=context, watchlist_service=object(), monitor_store=object(), finance_service=object()).tool_map
    response = json.loads(tools["realtime_quote"].invoke({"symbol": "600519.SH"}))
    assert calls == [("600519.SH", cutoff)]
    assert response["message"] == "没有更新"
    assert "12" not in response["documents"][0]["text"]


def test_agent_returns_citable_fresh_quote(monkeypatch):
    instant = datetime(2026, 11, 2, 2, tzinfo=timezone.utc)
    class Service:
        def lookup(self, symbol, as_of):
            assert symbol == "600519" and as_of == instant
            return {"quote": {"status": "fresh", "symbol": "600519.SH", "name": "贵州茅台",
                              "price": 1234.5, "source": "tencent", "source_time": instant, "captured_at": instant}}
    monkeypatch.setattr("backend.harness.tools.RealtimeQuoteService", Service)
    context = ToolContext(thread_id="t", turn_id="r", run_kind="interactive", as_of=instant,
                          store=object(), evidence_service=object(), web_service=object())
    tools = ToolRegistry(context=context, watchlist_service=object(), monitor_store=object(), finance_service=object()).tool_map
    result = json.loads(tools["realtime_quote"].invoke({"symbol": "600519"}))
    assert result["documents"][0]["source_type"] == "market"
    assert "1234.5" in result["documents"][0]["text"]
    assert result["documents"][0]["published_at"] == instant.isoformat().replace("+00:00", "Z")
