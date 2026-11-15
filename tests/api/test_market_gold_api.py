from fastapi.testclient import TestClient
from backend.main import app


class FakeService:
    def screen(self, **kwargs):
        return {'items': [{'symbol': '600519.SH', 'return_5d': .1}], 'method': 'test', 'trading_date': '2026-11-10', 'as_of': kwargs['as_of'].isoformat()}
    def ranking(self, **kwargs):
        return {'period': kwargs['period'], 'side': kwargs['side'], 'return_field': 'return_1y', 'items': []}
    def overview(self, **kwargs):
        return {'symbol': kwargs['symbol'], 'market': None, 'documents': [], 'missing': ['market: unavailable']}


def test_screen_and_overview_endpoints(monkeypatch):
    monkeypatch.setattr('backend.api.market_gold.MarketGoldService', FakeService)
    client = TestClient(app)
    calls = []
    def capture_screen(self, **kwargs):
        calls.append(kwargs)
        return {'items': [], 'method': 'test', 'trading_date': None, 'as_of': kwargs['as_of'].isoformat()}
    monkeypatch.setattr(FakeService, 'screen', capture_screen)
    response = client.get('/api/market/gold/screen', params={'sort_by': 'return_5d', 'limit': 2, 'symbol': '600519.sh'})
    assert response.status_code == 200
    assert calls[0]['symbol'] == '600519.SH'
    assert client.get('/api/market/gold/screen', params={'symbol': 'x' * 17}).status_code == 422
    assert client.get('/api/market/gold/screen', params={'sort_by': 'invalid'}).status_code == 422
    assert client.get('/api/market/gold/ranking', params={'period': 'year'}).json()['period'] == 'year'
    assert client.get('/api/market/gold/ranking', params={'period': 'year', 'side': 'negative'}).json()['side'] == 'negative'
    assert client.get('/api/market/gold/ranking', params={'period': 'day', 'side': 'invalid'}).status_code == 422
    assert client.get('/api/market/gold/ranking', params={'period': 'invalid'}).status_code == 422
    response = client.get('/api/market/gold/overview', params={'symbol': '600519.sh'})
    assert response.status_code == 200 and response.json()['symbol'] == '600519.SH'
    assert client.get('/api/market/gold/overview', params={'symbol': 'A', 'as_of': '2026-01-01T00:00:00'}).status_code == 422
