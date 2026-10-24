"""Repository for semantic news retrieval over pgvector."""

from __future__ import annotations

from datetime import date, datetime

from backend.config import settings
from backend.rag.embeddings import EmbeddingService
from backend.rag.lexical_store import OpenSearchNewsStore
from backend.rag.reranker import CohereCompatibleReranker, Reranker
from backend.rag.retrieval import reciprocal_rank_fusion
from backend.rag.vector_store import PgVectorNewsStore
from backend.schemas.rag import RagSearchHit


class RagRepository:
    """Read-only repository for vector search over news chunks."""

    def __init__(
        self,
        *,
        vector_store: PgVectorNewsStore | None = None,
        embedding_service: EmbeddingService | None = None,
        lexical_store: OpenSearchNewsStore | None = None,
        reranker: Reranker | None = None,
    ) -> None:
        self.vector_store = vector_store or PgVectorNewsStore()
        self.embedding_service = embedding_service or EmbeddingService()
        self.lexical_store = lexical_store or OpenSearchNewsStore()
        self.reranker = reranker or CohereCompatibleReranker()
        self.vector_store.ensure_schema()
        self.lexical_store.ensure_index()

    def search(
        self,
        *,
        question: str,
        symbol: str | None,
        start_date: date,
        end_date: date,
        as_of: datetime | None = None,
        limit: int = 5,
    ) -> list[RagSearchHit]:
        """Search news chunks semantically."""

        query_embedding = self.embedding_service.embed_query(question)
        vector_hits = self.vector_store.search_chunks(
            query_embedding=query_embedding,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            as_of=as_of,
            limit=max(limit, settings.RAG_CANDIDATE_LIMIT),
        )
        lexical_hits = self.lexical_store.search_chunks(
            question=question,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            as_of=as_of,
            limit=max(limit, settings.RAG_CANDIDATE_LIMIT),
        )
        fused = reciprocal_rank_fusion(vector_hits, lexical_hits)[: settings.RAG_CANDIDATE_LIMIT]
        return self.reranker.rerank(question=question, candidates=fused)
