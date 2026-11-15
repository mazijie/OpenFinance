"""News API routes."""

from __future__ import annotations

from functools import lru_cache

from backend.lakehouse.starrocks_daily_reader import StarRocksUnavailable

from fastapi import APIRouter, Depends, HTTPException, Query, status

from backend.schemas.news import NewsPage
from backend.services.news_service import NewsService

router = APIRouter(prefix="/api/news", tags=["news"])


@lru_cache(maxsize=1)
def get_news_service() -> NewsService:
    return NewsService()


@router.get("", response_model=NewsPage, status_code=status.HTTP_200_OK)
def list_news(
    page: int = Query(default=1, ge=1, description="Page number, starting from 1"),
    page_size: int = Query(default=20, ge=1, le=100, description="Items per page"),
    news_service: NewsService = Depends(get_news_service),
) -> NewsPage:
    """Return paginated news already stored in the lakehouse."""

    try:
        return news_service.list_news(page=page, page_size=page_size)
    except StarRocksUnavailable:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list news from lakehouse: {exc}",
        ) from exc
