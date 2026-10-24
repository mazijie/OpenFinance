"""Annual-report semantic search service with citable results."""

from __future__ import annotations

from datetime import UTC, datetime

from backend.rag.annual_report_store import AnnualReportVectorStore
from backend.rag.embeddings import EmbeddingService
from backend.schemas.rag import RagArticleResult, RagEvidence


class AnnualReportRagService:
    def __init__(self, store: AnnualReportVectorStore | None = None, embeddings: EmbeddingService | None = None) -> None:
        self.store = store or AnnualReportVectorStore()
        self.embeddings = embeddings or EmbeddingService()
        self.store.ensure_schema()

    def search(
        self,
        *,
        question: str,
        stock_code: str | None,
        report_year: int | None,
        as_of: datetime | None = None,
        published_after: datetime | None = None,
        limit: int,
    ) -> list[RagArticleResult]:
        hits = self.store.search(
            embedding=self.embeddings.embed_query(question),
            stock_code=stock_code,
            report_year=report_year,
            as_of=as_of,
            published_after=published_after,
            limit=limit,
        )
        articles: list[RagArticleResult] = []
        for index, hit in enumerate(hits, start=1):
            articles.append(RagArticleResult(
                citation_id=index,
                news_id=hit.report_id,
                content_hash=hit.chunk_id,
                title=f"{hit.stock_code} {hit.report_year} 年报 - {hit.section_path}（第 {hit.page_start}-{hit.page_end} 页）",
                source_name="CNInfo annual report",
                provider="cninfo",
                published_at=hit.announcement_time or hit.ingested_at or as_of or datetime.now(UTC),
                url=f"/api/reports/{hit.report_id}/download",
                final_score=hit.similarity,
                evidence_chunks=[RagEvidence(chunk_id=hit.chunk_id, chunk_text=hit.chunk_text, rerank_score=hit.similarity, final_score=hit.similarity)],
            ))
        return articles
