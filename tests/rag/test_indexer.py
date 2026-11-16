from __future__ import annotations

from datetime import UTC, datetime

from backend.rag.news.indexer import NewsRAGIndexer
from backend.schemas.news import NewsArticleRecord
import pytest


class FakeNewsWriter:
    def __init__(self, records):
        self._records = records

    def scan_records(self):
        return self._records


class FakeVectorStore:
    def __init__(self, existing_hashes=None):
        self.existing_hashes = set(existing_hashes or [])
        self.upserted = []
        self.schema_ensured = False

    def ensure_schema(self):
        self.schema_ensured = True

    def existing_content_hashes(self, content_hashes):
        return self.existing_hashes.intersection(set(content_hashes))

    def upsert_chunks(self, chunks):
        self.upserted.extend(chunks)
        return len(chunks)

    def chunks_for_content_hashes(self, content_hashes):
        return [item for item in self.upserted if item.content_hash in content_hashes]

    def clear(self):
        self.existing_hashes.clear()


class FakeEmbeddingService:
    def embed_documents(self, texts):
        return [[0.1, 0.2, 0.3] for _ in texts]


def test_indexer_skips_weak_articles_and_index_eligible_news() -> None:
    strong_article = NewsArticleRecord(
        news_id="news-1",
        provider="juhe",
        source_name="Source",
        title="NVIDIA 发布新产品",
        summary="这是一个足够长的摘要，用来测试 RAG 分块是否正常工作。",
        content=(
            "这里是更长的正文内容，足以被切成多个向量块。为了测试，我们补充更多文本。"
            "这段内容继续展开，以确保长度超过阈值并进入向量化流程。"
            "此外，这里再增加一段新闻背景描述，说明公司产品发布、市场反馈和后续影响。"
            "继续补充一些分析性文字，确保整段文本足够长，满足向量化和分块的最低要求。"
        ),
        url="https://example.com/news-1",
        published_at=datetime(2026, 7, 18, 1, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 18, 2, 0, tzinfo=UTC),
        language="zh-cn",
        symbols=["NVDA"],
        entities=["NVIDIA"],
        content_hash="hash-strong",
        raw_object_path="news/provider=juhe/strong.json",
    )
    weak_article = NewsArticleRecord(
        news_id="news-2",
        provider="juhe",
        source_name="Source",
        title="快讯",
        summary="短",
        content="",
        url="https://example.com/news-2",
        published_at=datetime(2026, 7, 18, 1, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 18, 2, 0, tzinfo=UTC),
        language="zh-cn",
        symbols=["NVDA"],
        entities=["NVIDIA"],
        content_hash="hash-weak",
        raw_object_path="news/provider=juhe/weak.json",
    )

    vector_store = FakeVectorStore()
    indexer = NewsRAGIndexer(
        news_writer=FakeNewsWriter([strong_article, weak_article]),
        vector_store=vector_store,
        embedding_service=FakeEmbeddingService(),
    )

    stats = indexer.run()

    assert vector_store.schema_ensured is True
    assert stats.scanned_articles == 2
    assert stats.skipped_articles == 1
    assert stats.written_chunks >= 1
    assert len(vector_store.upserted) == stats.written_chunks


def test_reconciliation_repairs_missing_chunks_after_partial_failure():
    article = NewsArticleRecord(
        news_id="n", provider="test", title="足够长的新闻标题用于测试",
        summary="摘要内容" * 25, content="正文内容" * 150, url="https://example.com/n",
        published_at=datetime(2026, 7, 18, tzinfo=UTC), ingested_at=datetime(2026, 7, 18, tzinfo=UTC),
        content_hash="test-hash", raw_object_path="test.json")

    class Lexical:
        enabled = True
        def __init__(self): self.ids = set(); self.fail = True
        def ensure_index(self): pass
        def existing_chunk_ids(self, ids): return self.ids.intersection(ids)
        def upsert_chunks(self, chunks):
            if chunks and self.fail:
                self.ids.add(chunks[0].chunk_id)
                self.fail = False
                raise RuntimeError("partial bulk failure")
            self.ids.update(chunk.chunk_id for chunk in chunks)
        def close(self): pass

    vector, lexical = FakeVectorStore(), Lexical()
    indexer = NewsRAGIndexer(news_writer=FakeNewsWriter([article]), vector_store=vector,
                             embedding_service=FakeEmbeddingService(), lexical_store=lexical)
    with pytest.raises(RuntimeError, match="partial bulk"):
        indexer.run()
    result = indexer.run()
    assert lexical.ids == {chunk.chunk_id for chunk in vector.upserted}
    assert result.details["missing_vector_chunks"] == 0
    assert indexer.run().details["repaired_lexical_chunks"] == 0
