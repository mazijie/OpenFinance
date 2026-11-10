from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

from fastapi.testclient import TestClient

from backend.api.market import get_finance_service
from backend.main import app
from backend.schemas.finance import MarketSearchResult, MarketSummary, SecurityOptionsResult


class FakeFinanceService:
    def __init__(self, result: MarketSearchResult) -> None:
        self.result = result
        self.calls: list[dict] = []

    def search_market(self, **kwargs):
        self.calls.append(kwargs)
        return self.result

    def list_security_options(self):
        return SecurityOptionsResult(items=[])

    def history_table(self):
        return SimpleNamespace(
            metadata=SimpleNamespace(table_uuid="00000000-0000-0000-0000-000000000001"),
            current_snapshot=lambda: SimpleNamespace(snapshot_id=1234567890123456789),
            snapshot_by_id=lambda snapshot_id: object() if snapshot_id == 1234567890123456789 else None,
        )


def _summary() -> MarketSummary:
    now = datetime(2026, 8, 3, 8, 0, tzinfo=UTC)
    return MarketSummary(
        symbol="600519.SH",
        security_id="600519.SH",
        exchange="SH",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 31),
        adjustment_type="none",
        first_close=1200,
        last_close=1250,
        interval_return=0.0416,
        bars=[
            {
                "trading_date": "2026-07-31",
                "open": 1240,
                "high": 1260,
                "low": 1230,
                "close": 1250,
                "volume": 1000,
                "provider": "tencent",
                "available_at": now.isoformat(),
                "ingested_at": now.isoformat(),
                "raw_object_path": "finance/provider=tencent/market.json",
            }
        ],
    )


def test_market_api_forwards_query_and_returns_summary() -> None:
    service = FakeFinanceService(MarketSearchResult(items=[_summary()]))
    app.dependency_overrides[get_finance_service] = lambda: service
    client = TestClient(app)

    try:
        response = client.get(
            "/api/market",
            params={
                "symbol": "600519.sh",
                "start_date": "2026-07-01",
                "end_date": "2026-07-31",
                "adjustment_type": "none",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["items"][0]["symbol"] == "600519.SH"
    assert service.calls[0]["symbol"] == "600519.SH"
    assert service.calls[0]["start_date"] == date(2026, 7, 1)
    assert "as_of" not in service.calls[0]


def test_market_api_returns_explicit_empty_result() -> None:
    service = FakeFinanceService(MarketSearchResult(items=[], message="No internal market data found"))
    app.dependency_overrides[get_finance_service] = lambda: service
    client = TestClient(app)
    try:
        response = client.get("/api/market", params={"symbol": "600519.SH"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {"items": [], "message": "No internal market data found"}


def test_market_api_lists_security_options() -> None:
    service = FakeFinanceService(MarketSearchResult())
    service.list_security_options = lambda: SecurityOptionsResult(items=[
        {"symbol": "600519.SH", "name": "贵州茅台", "exchange": "SH"},
    ])
    app.dependency_overrides[get_finance_service] = lambda: service
    client = TestClient(app)
    try:
        response = client.get("/api/market/securities")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["items"] == [{"symbol": "600519.SH", "name": "贵州茅台", "exchange": "SH"}]


def test_market_api_validates_selector_and_date_range() -> None:
    app.dependency_overrides[get_finance_service] = lambda: FakeFinanceService(MarketSearchResult())
    client = TestClient(app)
    try:
        assert client.get("/api/market").status_code == 422
        assert client.get("/api/market", params={"symbol": "600519.SH", "start_date": "2026-08-03", "end_date": "2026-07-01"}).status_code == 422
        assert client.get("/api/market", params={"symbol": "600519.SH", "adjustment_type": "bad"}).status_code == 422
        assert client.get("/api/market", params={"symbol": "600519.SH", "as_of": "2026-08-03T08:00:00Z"}).status_code == 422
        assert client.get("/api/market/history", params={"symbol": "600519.SH", "as_of": "2026-08-03T08:00:00Z"}).status_code == 422
    finally:
        app.dependency_overrides.clear()


def test_history_pages_align_to_period_and_preserve_empty_gap(monkeypatch) -> None:
    service = FakeFinanceService(MarketSearchResult(items=[]))
    monkeypatch.setattr("backend.api.market.has_earlier", lambda **kwargs: True)
    app.dependency_overrides[get_finance_service] = lambda: service
    try:
        client = TestClient(app)
        response = client.get("/api/market/history", params={"symbol": "600519.SH", "period": "weekly", "before": "2026-08-03"})
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == [] and data["has_more"] is True
        assert date.fromisoformat(data["start_date"]).weekday() == 0
        assert data["end_date"] == "2026-08-02"
        response = client.get("/api/market/history", params={"symbol": "600519.SH", "period": "monthly", "before": "2026-08-01"})
        assert response.json()["start_date"].endswith("-01")
        assert response.json()["end_date"] == "2026-07-31"
    finally:
        app.dependency_overrides.clear()


def test_history_returns_pending_and_checks_same_snapshot(monkeypatch) -> None:
    service = FakeFinanceService(MarketSearchResult(items=[_summary()]))
    calls = []
    service.has_history_before = lambda *args, **kwargs: calls.append((args, kwargs)) or True
    monkeypatch.setattr("backend.api.market.has_earlier", lambda **kwargs: None)
    app.dependency_overrides[get_finance_service] = lambda: service
    try:
        client = TestClient(app)
        data = client.get("/api/market/history", params={"symbol": "600519.SH"}).json()
        assert data["has_more"] is None
        assert data["snapshot_id"] == "1234567890123456789"
        assert not calls
        response = client.get("/api/market/history/availability", params={
            "symbol": "600519.SH", "before": data["next_before"],
            "snapshot_id": data["snapshot_id"], "table_uuid": data["table_uuid"],
            "query_time": data["query_time"],
        })
        assert response.status_code == 200 and response.json() == {"has_more": True}
        assert calls[0][1]["snapshot_id"] == 1234567890123456789
        assert service.calls[0]["snapshot_id"] == 1234567890123456789
        bad = client.get("/api/market/history/availability", params={
            "symbol": "600519.SH", "before": data["next_before"], "snapshot_id": "99",
            "table_uuid": data["table_uuid"], "query_time": data["query_time"],
        })
        assert bad.status_code == 410
    finally:
        app.dependency_overrides.clear()


def test_history_uses_index_without_synchronous_fallback(monkeypatch) -> None:
    service = FakeFinanceService(MarketSearchResult(items=[_summary()]))
    monkeypatch.setattr("backend.api.market.has_earlier", lambda **kwargs: False)
    app.dependency_overrides[get_finance_service] = lambda: service
    try:
        data = TestClient(app).get("/api/market/history", params={"symbol": "600519.SH"}).json()
        assert data["has_more"] is False
    finally:
        app.dependency_overrides.clear()


def test_history_returns_busy_if_scan_queue_is_full(monkeypatch) -> None:
    from contextlib import contextmanager

    class BusyGate:
        @contextmanager
        def acquire(self, *, page):
            raise TimeoutError("Market history is busy; please retry")
            yield

    monkeypatch.setattr("backend.api.market.market_query_gate", BusyGate())
    app.dependency_overrides[get_finance_service] = lambda: FakeFinanceService(MarketSearchResult())
    try:
        response = TestClient(app).get("/api/market/history", params={"symbol": "600519.SH"})
        assert response.status_code == 503
    finally:
        app.dependency_overrides.clear()
