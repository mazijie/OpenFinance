from datetime import UTC, date, datetime
from contextlib import contextmanager
from backend.rag.news.vector_store import PgVectorNewsStore


def test_news_search_binds_vector_before_where_parameters():
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params): self.params = params
        def fetchall(self): return []
    cursor = Cursor()
    class Connection:
        def cursor(self, **kwargs): return cursor
    @contextmanager
    def connection(): yield Connection()
    store = object.__new__(PgVectorNewsStore)
    store.schema = 'openfinance'
    store.table = 'news_chunks'
    store._connection = connection
    store.search_chunks(query_embedding=[1.0, 2.0], symbol='A', start_date=date(2026, 1, 1),
                        end_date=date(2026, 1, 2), as_of=datetime(2026, 1, 3, tzinfo=UTC), limit=3)
    assert cursor.params[0].to_list() == [1.0, 2.0]
    assert cursor.params[1].date() == date(2026, 1, 1)
    assert cursor.params[4] == ['A']
    assert cursor.params[-2].to_list() == [1.0, 2.0]


def test_news_search_converts_pgvector_result():
    from pgvector import Vector
    now = datetime(2026, 1, 1, tzinfo=UTC)
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params): pass
        def fetchall(self):
            return [{"chunk_id": "c", "news_id": "n", "provider": "test", "title": "title",
                     "url": "https://example.test", "published_at": now, "ingested_at": now,
                     "content_hash": "hash", "raw_object_path": "raw", "chunk_index": 0,
                     "chunk_text": "text", "similarity": 0.5, "embedding": Vector([1.0, 2.0])}]
    class Connection:
        def cursor(self, **kwargs): return Cursor()
    @contextmanager
    def connection(): yield Connection()
    store = object.__new__(PgVectorNewsStore)
    store.schema, store.table, store._connection = 'openfinance', 'news_chunks', connection
    hits = store.search_chunks(query_embedding=[1.0, 2.0], symbol=None,
                               start_date=date(2026, 1, 1), end_date=date(2026, 1, 2), limit=3)
    assert hits[0].embedding == [1.0, 2.0]
