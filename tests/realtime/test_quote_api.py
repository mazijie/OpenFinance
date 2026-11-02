import psycopg
from fastapi import FastAPI
from fastapi.testclient import TestClient
from backend.api.realtime_quotes import router


def test_quote_endpoint_returns_selected_symbol_and_overview(monkeypatch):
    class Service:
        def lookup(self, symbol):
            return {"quote": {"symbol": symbol, "status": "fresh"}, "market": {"expected": 2, "valid": 1}}
    monkeypatch.setattr("backend.api.realtime_quotes.RealtimeQuoteService", Service)
    app = FastAPI()
    app.include_router(router)
    response = TestClient(app).get("/api/realtime-quotes", params={"symbol": "600519.SH"})
    assert response.status_code == 200
    assert response.json()["quote"]["symbol"] == "600519.SH"
    assert response.json()["market"]["valid"] == 1


def test_quote_endpoint_reports_invalid_and_unavailable(monkeypatch):
    class Service:
        def lookup(self, symbol):
            if symbol == "invalid":
                raise ValueError("证券代码无效")
            raise psycopg.OperationalError("database unavailable")
    monkeypatch.setattr("backend.api.realtime_quotes.RealtimeQuoteService", Service)
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    assert client.get("/api/realtime-quotes?symbol=invalid").status_code == 422
    assert client.get("/api/realtime-quotes?symbol=600519.SH").status_code == 503


def test_intraday_endpoint(monkeypatch):
    class Service:
        def intraday(self, symbol):
            return {"symbol": symbol, "points": [{"price": 10.2}]}
    monkeypatch.setattr("backend.api.realtime_quotes.RealtimeQuoteService", Service)
    app = FastAPI()
    app.include_router(router)
    response = TestClient(app).get("/api/realtime-quotes/intraday?symbol=600519.SH")
    assert response.status_code == 200
    assert response.json()["points"][0]["price"] == 10.2
