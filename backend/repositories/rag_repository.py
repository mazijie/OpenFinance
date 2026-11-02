"""Repository for semantic news retrieval over pgvector."""

from __future__ import annotations

from datetime import date, datetime
import logging

from backend.config import settings
from backend.rag.embeddings import EmbeddingService
from backend.rag.lexical_store import OpenSearchNewsStore
from backend.rag.reranker import CohereCompatibleReranker, Reranker
from backend.rag.retrieval import reciprocal_rank_fusion
from backend.rag.vector_store import PgVectorNewsStore, RagStoreError
from backend.schemas.rag import RagSearchHit


logger = logging.getLogger(__name__)


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

        hits, _ = self.search_with_warnings(question=question, symbol=symbol, start_date=start_date, end_date=end_date, as_of=as_of, limit=limit)
        return hits

    def search_with_warnings(self, *, question: str, symbol: str | None, start_date: date,
                             end_date: date, as_of: datetime | None = None,
                             limit: int = 5) -> tuple[list[RagSearchHit], list[str]]:
        warnings: list[str] = []
        vector_hits: list[RagSearchHit] = []
        lexical_hits: list[RagSearchHit] = []
        succeeded = 0
        try:
            query_embedding = self.embedding_service.embed_query(question)
            vector_hits = self.vector_store.search_chunks(
                query_embedding=query_embedding, symbol=symbol, start_date=start_date,
                end_date=end_date, as_of=as_of, limit=max(limit, settings.RAG_CANDIDATE_LIMIT),
            )
            succeeded += 1
        except Exception:
            logger.exception("News vector search failed")
            warnings.append("新闻向量索引不可用，检索结果可能不完整。")
        if getattr(self.lexical_store, "enabled", True):
            try:
                lexical_hits = self.lexical_store.search_chunks(
                    question=question, symbol=symbol, start_date=start_date,
                    end_date=end_date, as_of=as_of, limit=max(limit, settings.RAG_CANDIDATE_LIMIT),
                )
                succeeded += 1
            except Exception:
                logger.exception("News lexical search failed")
                warnings.append("新闻关键词索引不可用，检索结果可能不完整。")
        if not succeeded:
            raise RagStoreError("All enabled news indexes are unavailable")
        if isinstance(self.vector_store, PgVectorNewsStore) and getattr(self.lexical_store, "enabled", True):
            try:
                with self.vector_store._connection() as conn, conn.cursor() as cur:
                    cur.execute("SELECT status, last_success_at FROM openfinance.news_index_reconciliation WHERE id = 1")
                    state = cur.fetchone()
                if not state or state[0] != "success":
                    warnings.append("新闻索引对账尚未完成或最近一次失败，索引可能不同步。")
                else:
                    warnings.append(f"新闻索引可能短暂不同步，最近完成对账时间：{state[1]}。")
            except Exception:
                logger.exception("News reconciliation status unavailable")
                warnings.append("无法确认新闻索引对账状态，索引可能不同步。")
        fused = reciprocal_rank_fusion(vector_hits, lexical_hits)[: settings.RAG_CANDIDATE_LIMIT]
        return self.reranker.rerank(question=question, candidates=fused), warnings
