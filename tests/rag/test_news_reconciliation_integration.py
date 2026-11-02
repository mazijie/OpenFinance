"""Use an explicitly configured disposable PostgreSQL database."""
import os
import pytest
from datetime import datetime, timezone
from uuid import uuid4
from backend.rag.indexer import NewsRAGIndexer
from backend.rag.vector_store import PgVectorNewsStore, RagStoreError
from backend.schemas.news import NewsArticleRecord


@pytest.mark.skipif(not os.getenv("CONSISTENCY_TEST_DATABASE_URL"), reason="requires isolated PostgreSQL")
def test_reconciliation_lock_and_success_status():
    class Writer:
        def scan_records(self): return []
    class Lexical:
        enabled = False
        def ensure_index(self): pass
        def close(self): pass
    store = PgVectorNewsStore(dsn=os.environ["CONSISTENCY_TEST_DATABASE_URL"])
    indexer = NewsRAGIndexer(news_writer=Writer(), vector_store=store, lexical_store=Lexical())
    with store._connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", ("openfinance_news_index",))
        try:
            with pytest.raises(RagStoreError, match="Another news"):
                indexer.run()
        finally:
            cur.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", ("openfinance_news_index",))
    indexer.run()
    with store._connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT status, last_success_at FROM openfinance.news_index_reconciliation WHERE id=1")
        status, last_success = cur.fetchone()
    assert status == "success" and last_success is not None


@pytest.mark.skipif(not os.getenv("CONSISTENCY_TEST_DATABASE_URL"), reason="requires isolated PostgreSQL")
def test_real_vector_chunks_can_be_reconciled_after_lexical_failure():
    content_hash = uuid4().hex
    instant = datetime.now(timezone.utc)
    article = NewsArticleRecord(news_id=content_hash, provider="test", title="测试新闻标题",
        summary="摘要内容" * 40, content="正文内容" * 200, url="https://example.com/test",
        published_at=instant, ingested_at=instant, content_hash=content_hash, raw_object_path="test")
    class Writer:
        def scan_records(self): return [article]
    class Embeddings:
        def embed_documents(self, texts): return [[0.1] * store.dimension for _ in texts]
    class Lexical:
        enabled = True
        def __init__(self): self.ids = set(); self.fail = True
        def ensure_index(self): pass
        def existing_chunk_ids(self, ids): return self.ids.intersection(ids)
        def upsert_chunks(self, chunks):
            if self.fail:
                self.fail = False
                raise RuntimeError("injected lexical failure")
            self.ids.update(chunk.chunk_id for chunk in chunks)
        def close(self): pass
    store = PgVectorNewsStore(dsn=os.environ["CONSISTENCY_TEST_DATABASE_URL"])
    lexical = Lexical()
    indexer = NewsRAGIndexer(news_writer=Writer(), vector_store=store, lexical_store=lexical, embedding_service=Embeddings())
    try:
        with pytest.raises(RuntimeError, match="injected"):
            indexer.run()
        result = indexer.run()
        assert result.details["missing_vector_chunks"] == 0
        assert result.details["repaired_lexical_chunks"] == len(lexical.ids) > 0
    finally:
        with store._connection() as conn, conn.cursor() as cur:
            cur.execute(f"DELETE FROM {store.fqtn} WHERE content_hash=%s", (content_hash,))
