"""Business service for lakehouse news lookup."""

from __future__ import annotations

from datetime import date

from backend.config import settings
from backend.repositories.news_repository import NewsRepository
from backend.schemas.news import NewsItem, NewsPage, NewsSearchResult


class NewsService:
    """Application service used by tools and agents."""

    def __init__(self, repository: NewsRepository | None = None) -> None:
        self.repository = repository or NewsRepository()

    def search_news(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        limit: int = 10,
    ) -> list[NewsItem]:
        """Return only news items for callers that expect list output."""

        return self.repository.search_news(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)

    def list_news(self, *, page: int = 1, page_size: int = 20) -> NewsPage:
        """Return paginated lakehouse news listing."""

        return self.repository.list_news(page=page, page_size=page_size)

    def search_news_with_message(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        limit: int = 10,
    ) -> NewsSearchResult:
        """Return items with empty-state message."""

        items = self.search_news(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)
        if not items:
            return NewsSearchResult(items=[], message=settings.NEWS_EMPTY_RESULT_MESSAGE)
        return NewsSearchResult(items=items, message=None)
