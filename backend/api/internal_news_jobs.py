"""Internal job trigger APIs used by orchestrators."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Header, status

from backend.schemas.jobs import BronzeToSilverRequest, IngestFinanceNewsRequest, RagIndexRequest

from backend.api.internal_auth import _require_internal_token

router = APIRouter(prefix="/internal/jobs", tags=["internal-jobs"])


@router.post("/ingest-finance-news", status_code=status.HTTP_200_OK)
def trigger_ingest_finance_news(
    request: IngestFinanceNewsRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Run one ingestion cycle inside the backend runtime."""

    from backend.ingestion.jobs.ingest_news import ingest_finance_news

    _require_internal_token(x_openfinance_token)
    result = asyncio.run(
        ingest_finance_news(
            start_time=request.start_time,
            end_time=request.end_time,
        )
    )
    return result.to_dict()


@router.post("/news-bronze-to-silver", status_code=status.HTTP_200_OK)
def trigger_news_bronze_to_silver(
    request: BronzeToSilverRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, int]:
    """Run Bronze-to-Silver normalization inside the backend runtime."""

    from backend.pipelines.news_bronze_to_silver import NewsBronzeToSilverPipeline

    _require_internal_token(x_openfinance_token)
    pipeline = NewsBronzeToSilverPipeline()
    stats = pipeline.run(object_paths=request.object_paths)
    return stats.to_dict()


@router.post("/news-rag-index", status_code=status.HTTP_200_OK)
def trigger_news_rag_index(
    request: RagIndexRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Build or refresh the semantic news index."""

    from backend.rag.news.indexer import NewsRAGIndexer

    _require_internal_token(x_openfinance_token)
    indexer = NewsRAGIndexer()
    try:
        try:
            stats = indexer.run(rebuild_all=request.rebuild_all)
        except TypeError:
            stats = indexer.run()
    finally:
        close = getattr(indexer, "close", None)
        if callable(close):
            close()
    return stats.model_dump()
