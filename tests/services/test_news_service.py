from __future__ import annotations

from datetime import UTC, date, datetime

from backend.schemas.news import NewsItem, NewsPage
from backend.services.news_service import NewsService


class FakeRepository:
    def __init__(self, items: list[NewsItem]) -> None:
        self.items = items
        self.pages: dict[tuple[int, int], NewsPage] = {}

    def search_news(self, symbol: str, start_date: date, end_date: date, limit: int = 10):
        return self.items[:limit]

    def list_news(self, *, page: int = 1, page_size: int = 20) -> NewsPage:
        return self.pages[(page, page_size)]


def test_news_service_empty_message() -> None:
    service = NewsService(repository=FakeRepository([]))
    result = service.search_news_with_message(symbol="NVDA", start_date=date(2026, 7, 1), end_date=date(2026, 7, 14))
    assert result.items == []
    assert result.message == "当前自有湖仓在指定时间范围内未检索到相关新闻。"


def test_news_service_list_news() -> None:
    repository = FakeRepository([])
    repository.pages[(1, 2)] = NewsPage(
        items=[
            NewsItem(
                news_id="n1",
                provider="juhe",
                title="财经快讯",
                summary="摘要",
                url="https://example.com/news-1",
                published_at=datetime(2026, 7, 17, 8, 0, tzinfo=UTC),
                language="zh-cn",
            )
        ],
        page=1,
        page_size=2,
        total=1,
        total_pages=1,
        has_next=False,
    )
    service = NewsService(repository=repository)

    result = service.list_news(page=1, page_size=2)

    assert result.total == 1
    assert result.page == 1
    assert len(result.items) == 1
    assert result.items[0].news_id == "n1"
