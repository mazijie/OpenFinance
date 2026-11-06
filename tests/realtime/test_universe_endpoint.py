from datetime import date, timedelta

from fastapi.testclient import TestClient
from backend.main import app


def test_realtime_universe_refresh_and_stale_fallback(monkeypatch):
    from backend.config import settings
    from backend.lakehouse.a_share_universe_cache import AShareUniverseCacheStore
    from backend.providers.tencent_securities import TencentSecurityProvider

    monkeypatch.setattr(settings, 'OPENFINANCE_INTERNAL_API_TOKEN', 'test-token')
    old = date.today() - timedelta(days=2)
    saved = []
    monkeypatch.setattr(AShareUniverseCacheStore, 'latest', lambda self, **kw: (old, ['000001.SZ']))
    monkeypatch.setattr(AShareUniverseCacheStore, 'put', lambda self, **kw: saved.append(kw))
    monkeypatch.setattr(TencentSecurityProvider, '__init__', lambda self: None)
    monkeypatch.setattr(TencentSecurityProvider, 'close', lambda self: None)
    monkeypatch.setattr(TencentSecurityProvider, 'fetch_a_share_symbols_for_day', lambda self, **kw: ['600519.SH'])
    client = TestClient(app)
    assert client.get('/internal/jobs/realtime-universe').status_code == 403
    headers = {'X-Openfinance-Token': 'test-token'}
    result = client.get('/internal/jobs/realtime-universe', headers=headers).json()
    assert result['symbols'] == ['600519.SH'] and result['stale'] is False
    assert saved[0]['provider'] == 'tencent'
    def fail(*args, **kwargs):
        raise RuntimeError('provider down')
    monkeypatch.setattr(TencentSecurityProvider, 'fetch_a_share_symbols_for_day', fail)
    result = client.get('/internal/jobs/realtime-universe', headers=headers).json()
    assert result == {'symbols': ['000001.SZ'], 'as_of_date': old.isoformat(), 'stale': True}
