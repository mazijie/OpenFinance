from datetime import date
import pytest

from backend.rag.news.vector_store import RagStoreError
from backend.repositories.rag_repository import RagRepository
from backend.harness.evidence import EvidenceService


class Store:
    enabled = True
    def __init__(self, fails=False): self.fails = fails
    def search_chunks(self, **kwargs):
        if self.fails: raise RuntimeError("injected failure")
        return []


class Embeddings:
    def embed_query(self, question): return [1.0]


class Reranker:
    def rerank(self, *, question, candidates): return candidates


@pytest.mark.parametrize("vector_failed,lexical_failed", [(True, False), (False, True)])
def test_one_side_failure_returns_warning(vector_failed, lexical_failed):
    repo = RagRepository(vector_store=Store(vector_failed), lexical_store=Store(lexical_failed),
                         embedding_service=Embeddings(), reranker=Reranker())
    hits, warnings = repo.search_with_warnings(question="test", symbol=None,
                                                start_date=date(2026, 1, 1), end_date=date(2026, 1, 2))
    assert hits == []
    assert len(warnings) == 1


def test_both_sides_fail_explicitly():
    repo = RagRepository(vector_store=Store(True), lexical_store=Store(True),
                         embedding_service=Embeddings(), reranker=Reranker())
    with pytest.raises(RagStoreError):
        repo.search_with_warnings(question="test", symbol=None, start_date=date(2026, 1, 1), end_date=date(2026, 1, 2))


def test_degraded_warning_reaches_evidence_pack():
    service = EvidenceService(news_search=lambda **_: {"success": True, "data": {"articles": [], "warnings": ["索引不同步"]}})
    pack = service.search(query="test", sources=["news"])
    assert any("索引不同步" in warning for warning in pack.missing)
