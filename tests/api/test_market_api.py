from __future__ import annotations

from datetime import UTC, date, datetime

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


def test_history_pages_align_to_period_and_preserve_empty_gap() -> None:
    service = FakeFinanceService(MarketSearchResult(items=[]))
    service.has_history_before = lambda symbol, adjustment_type, before, as_of: True
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
