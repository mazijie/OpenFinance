from fastapi.testclient import TestClient
from backend.main import app


class FakeService:
    def screen(self, **kwargs):
        return {'items': [{'symbol': '600519.SH', 'return_5d': .1}], 'method': 'test', 'trading_date': '2026-11-10', 'as_of': kwargs['as_of'].isoformat()}
    def overview(self, **kwargs):
        return {'symbol': kwargs['symbol'], 'market': None, 'documents': [], 'missing': ['market: unavailable']}


def test_screen_and_overview_endpoints(monkeypatch):
    monkeypatch.setattr('backend.api.market_gold.MarketGoldService', FakeService)
    client = TestClient(app)
    response = client.get('/api/market/gold/screen', params={'sort_by': 'return_5d', 'limit': 2})
    assert response.status_code == 200 and response.json()['items'][0]['return_5d'] == .1
    assert client.get('/api/market/gold/screen', params={'sort_by': 'invalid'}).status_code == 422
    response = client.get('/api/market/gold/overview', params={'symbol': '600519.sh'})
    assert response.status_code == 200 and response.json()['symbol'] == '600519.SH'
    assert client.get('/api/market/gold/overview', params={'symbol': 'A', 'as_of': '2026-01-01T00:00:00'}).status_code == 422
