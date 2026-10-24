from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime

from backend.rag.lexical_store import OpenSearchNewsStore
from backend.rag.vector_store import PgVectorNewsStore


class FakeCursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params = ()

    def execute(self, sql, params=()):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


class FakeConnection:
    def __init__(self, cursor):
        self.value = cursor

    def cursor(self, **kwargs):
        return self.value

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


def test_vector_search_filters_by_ingested_at_cutoff() -> None:
    store = PgVectorNewsStore.__new__(PgVectorNewsStore)
    store.schema = "openfinance"
    store.table = "news_chunks"
    store.dsn = "unused"
    store.dimension = 2
    cursor = FakeCursor()

    @contextmanager
    def connection():
        yield FakeConnection(cursor)

    store._connection = connection
    cutoff = datetime(2026, 10, 23, 12, tzinfo=UTC)
    store.search_chunks(
        query_embedding=[1.0, 0.0],
        symbol="600519.SH",
        start_date=date(2026, 10, 20),
        end_date=date(2026, 10, 23),
        as_of=cutoff,
        limit=5,
    )

    assert "ingested_at <= %s" in cursor.sql
    assert cutoff in cursor.params


class FakeHttpClient:
    def __init__(self) -> None:
        self.payload = None

    def post(self, url, json):
        self.payload = json
        response = type("Response", (), {"status_code": 200})()
        response.json = lambda: {"hits": {"hits": []}}
        return response


def test_lexical_search_filters_by_ingested_at_cutoff() -> None:
    store = OpenSearchNewsStore.__new__(OpenSearchNewsStore)
    store.url = "http://unused"
    store.index = "news"
    store.enabled = True
    store.client = FakeHttpClient()
    cutoff = datetime(2026, 10, 23, 12, tzinfo=UTC)

    store.search_chunks(
        question="风险事件",
        symbol="600519.SH",
        start_date=date(2026, 10, 20),
        end_date=date(2026, 10, 23),
        as_of=cutoff,
        limit=5,
    )

    filters = store.client.payload["query"]["bool"]["filter"]
    assert {"range": {"ingested_at": {"lte": cutoff.isoformat()}}} in filters
    assert filters[0]["range"]["published_at"]["lte"] == cutoff.isoformat()
