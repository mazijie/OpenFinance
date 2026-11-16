from datetime import UTC, datetime

from backend.rag.common.reranker import CohereCompatibleReranker
from backend.schemas.rag import RagSearchHit


def test_disabled_reranker_keeps_rrf_score(monkeypatch) -> None:
    monkeypatch.setattr("backend.rag.common.reranker.settings.RAG_RERANK_ENABLED", False)
    candidate = RagSearchHit(
        chunk_id="chunk", news_id="news", provider="juhe", title="title", url="https://example.com",
        published_at=datetime.now(UTC), ingested_at=datetime.now(UTC), content_hash="hash", raw_object_path="raw",
        chunk_index=0, chunk_text="evidence", similarity=0.8, rrf_score=0.03,
    )
    result = CohereCompatibleReranker().rerank(question="question", candidates=[candidate])
    assert result[0].rerank_score == 0.03
