"""Service layer for RAG news retrieval."""

from __future__ import annotations

from datetime import date, datetime

from backend.config import settings
from backend.repositories.rag_repository import RagRepository
from backend.rag.retrieval import aggregate_articles, filter_by_threshold, mmr_select
from backend.schemas.rag import RagSearchResult
from backend.rag.embeddings import RagEmbeddingError
from backend.rag.lexical_store import LexicalStoreError
from backend.rag.reranker import RerankerError
from backend.rag.vector_store import RagStoreError


class RagService:
    """Application service used by the agent RAG tool."""

    def __init__(self, repository: RagRepository | None = None) -> None:
        self.repository = repository or RagRepository()

    def search(
        self,
        *,
        question: str,
        symbol: str | None,
        start_date: date,
        end_date: date,
        as_of: datetime | None = None,
        limit: int = 5,
    ) -> RagSearchResult:
        """Run semantic retrieval and format the result."""

        try:
            hits = self.repository.search(
                question=question,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                as_of=as_of,
                limit=limit,
            )
        except (RagEmbeddingError, RagStoreError, LexicalStoreError, RerankerError) as exc:
            return RagSearchResult(message="检索服务暂不可用，未使用新闻证据生成结论。", error_type=type(exc).__name__, error_stage="retrieval")
        filtered_hits = filter_by_threshold(hits, reranker_enabled=settings.RAG_RERANK_ENABLED)
        articles = mmr_select(aggregate_articles(filtered_hits), filtered_hits, limit=limit)
        if not articles:
            return RagSearchResult(hits=[], message=settings.NEWS_EMPTY_RESULT_MESSAGE)
        return RagSearchResult(hits=filtered_hits, articles=articles, message=None)
