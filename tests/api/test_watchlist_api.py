from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from backend.api.watchlist import get_watchlist_service
from backend.main import app
from backend.schemas.monitor import WatchlistItem, WatchlistItemCreate, WatchlistThresholds


class FakeWatchlistService:
    def __init__(self) -> None:
        self.items = []

    def list(self):
        return self.items

    def create(self, request: WatchlistItemCreate):
        now = datetime.now(UTC)
        item = WatchlistItem(
            symbol=request.symbol,
            security_id=request.symbol,
            name="贵州茅台",
            exchange="SH",
            thresholds=request.thresholds,
            created_at=now,
            updated_at=now,
        )
        self.items.append(item)
        return item

    def update(self, symbol, request):
        item = next((value for value in self.items if value.symbol == symbol), None)
        if item is None:
            return None
        updated = item.model_copy(update={"enabled": request.enabled if request.enabled is not None else item.enabled})
        self.items = [updated if value.symbol == symbol else value for value in self.items]
        return updated

    def delete(self, symbol):
        before = len(self.items)
        self.items = [value for value in self.items if value.symbol != symbol]
        return len(self.items) != before


def test_watchlist_crud_api() -> None:
    service = FakeWatchlistService()
    app.dependency_overrides[get_watchlist_service] = lambda: service
    client = TestClient(app)
    try:
        created = client.post(
            "/api/watchlist",
            json={"symbol": "600519.SH", "thresholds": {"price_move": 0.04, "volume_ratio": 2.0}},
        )
        assert created.status_code == 201
        assert created.json()["symbol"] == "600519.SH"
        assert len(client.get("/api/watchlist").json()) == 1
        patched = client.patch("/api/watchlist/600519.SH", json={"enabled": False})
        assert patched.status_code == 200
        assert patched.json()["enabled"] is False
        assert client.delete("/api/watchlist/600519.SH").status_code == 204
    finally:
        app.dependency_overrides.clear()
