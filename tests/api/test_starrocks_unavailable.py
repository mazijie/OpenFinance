from fastapi.testclient import TestClient

from backend.api.market import get_finance_service
from backend.api.news import get_news_service
from backend.lakehouse.starrocks_daily_reader import StarRocksUnavailable
from backend.main import app


class Unavailable:
    def list_security_options(self):
        raise StarRocksUnavailable('StarRocks query failed')

    def list_news(self, **kwargs):
        raise StarRocksUnavailable('StarRocks query failed')


def test_unavailable_online_reads_return_503():
    app.dependency_overrides[get_finance_service] = Unavailable
    app.dependency_overrides[get_news_service] = Unavailable
    try:
        with TestClient(app) as client:
            assert client.get('/api/market/securities').status_code == 503
            assert client.get('/api/news').status_code == 503
    finally:
        app.dependency_overrides.clear()
