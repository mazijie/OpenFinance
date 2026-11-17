"""Annual-report semantic search service with citable results."""

from __future__ import annotations

from datetime import datetime

from backend.rag.annual_reports.annual_report_store import AnnualReportVectorStore
from backend.rag.annual_reports.annual_report_lexical_store import AnnualReportLexicalStore, AnnualReportLexicalStoreError
from backend.rag.common.embeddings import EmbeddingService
from backend.config import settings
from backend.pipelines.annual_report_pipeline_state import AnnualReportPipelineStateStore
from backend.schemas.rag import RagArticleResult, RagEvidence


class AnnualReportRagService:
    def __init__(self, store: AnnualReportVectorStore | None = None, embeddings: EmbeddingService | None = None, lexical: AnnualReportLexicalStore | None = None, state_store: AnnualReportPipelineStateStore | None = None) -> None:
        self.store = store or AnnualReportVectorStore()
        self.embeddings = embeddings or EmbeddingService()
        self.lexical = lexical or AnnualReportLexicalStore()
        self.state_store = state_store
        if self.state_store is None and all(item is None for item in (store, embeddings, lexical)):
            self.state_store = AnnualReportPipelineStateStore()
        self.store.ensure_schema()
        self.lexical.ensure_index()
        if self.state_store is not None:
            self.state_store.ensure_schema()

    def close(self) -> None:
        self.embeddings.close()
        self.lexical.close()

    def search(
        self,
        *,
        question: str,
        stock_code: str | None,
        report_year: int | None,
        section_path: str | None = None,
        content_types: list[str] | None = None,
        page_start: int | None = None,
        page_end: int | None = None,
        as_of: datetime | None = None,
        published_after: datetime | None = None,
        limit: int,
    ) -> list[RagArticleResult]:
        candidate_limit = max(limit * 5, settings.RAG_CANDIDATE_LIMIT)
        active = self.state_store.get_active_generation() if self.state_store is not None else None
        generation_id = str(active["generation_id"]) if active else None
        vector_hits = []
        # Never compare Qwen queries with a generation produced by another model.
        if self.state_store is None or (active and str(active["embedding_model"]) == self.embeddings.model_name and int(active["embedding_dimension"]) == self.embeddings.dimension):
            try:
                query_vector = self.embeddings.embed_query(question)
                vector_hits = self.store.search(
                    embedding=query_vector, stock_code=stock_code, report_year=report_year,
                    section_path=section_path, content_types=content_types, page_start=page_start,
                    page_end=page_end, as_of=as_of, published_after=published_after,
                    limit=candidate_limit, generation_id=generation_id,
                )
            except Exception:
                if not self.lexical.enabled:
                    raise
        lexical_hits: list = []
        try:
            lexical_hits = self.lexical.search(
                question=question,
                stock_code=stock_code,
                report_year=report_year,
                section_path=section_path,
                content_types=content_types,
                page_start=page_start,
                page_end=page_end,
                as_of=as_of,
                published_after=published_after,
                limit=candidate_limit,
                generation_id=generation_id,
            )
        except AnnualReportLexicalStoreError:
            lexical_hits = []
        hits = _rrf(vector_hits, lexical_hits, k=settings.RAG_RRF_K)[:limit]
        articles: list[RagArticleResult] = []
        for index, hit in enumerate(hits, start=1):
            score = float(hit.final_score or hit.rrf_score or hit.similarity or 0.0)
            articles.append(RagArticleResult(
                citation_id=index,
                news_id=hit.report_id,
                content_hash=hit.chunk_id,
                title=f"{hit.stock_code} {hit.report_year} 年报 - {hit.section_path}（第 {hit.page_start}-{hit.page_end} 页）",
                source_name="CNInfo annual report",
                provider="cninfo",
                published_at=hit.announcement_time,
                url=f"/api/reports/{hit.report_id}/download",
                final_score=score,
                metadata={
                    "report_id": hit.report_id,
                    "source_pdf_url": f"/api/reports/{hit.report_id}/download",
                    "chunk_text": hit.chunk_text,
                    "parse_run_id": hit.parse_run_id,
                    "page_start": hit.page_start,
                    "page_end": hit.page_end,
                    "section_path": hit.section_path,
                    "content_type": hit.content_type,
                    "region_ids": hit.region_ids,
                },
                evidence_chunks=[RagEvidence(chunk_id=hit.chunk_id, chunk_text=hit.chunk_text, rerank_score=score, final_score=score)],
            ))
        return articles


def _rrf(vector_hits: list, lexical_hits: list, k: int = 60) -> list:
    by_id: dict[str, object] = {}
    scores: dict[str, float] = {}
    for rank, hit in enumerate(vector_hits, start=1):
        by_id[hit.chunk_id] = hit
        scores[hit.chunk_id] = scores.get(hit.chunk_id, 0.0) + 1.0 / (k + rank)
    for rank, hit in enumerate(lexical_hits, start=1):
        by_id.setdefault(hit.chunk_id, hit)
        scores[hit.chunk_id] = scores.get(hit.chunk_id, 0.0) + 1.0 / (k + rank)
    result = []
    for chunk_id, hit in by_id.items():
        result.append(hit.model_copy(update={"rrf_score": scores[chunk_id], "final_score": scores[chunk_id]}))
    return sorted(result, key=lambda item: float(item.rrf_score or 0.0), reverse=True)
