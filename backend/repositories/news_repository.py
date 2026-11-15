"""Repository layer for querying Silver news articles."""

from __future__ import annotations

from datetime import date

from backend.lakehouse.starrocks_readers import StarRocksReader
from backend.schemas.news import NewsItem, NewsPage
from backend.utils.time import day_end_utc, day_start_utc


class NewsRepository:
    """Read-only repository over the Iceberg Silver table."""

    def __init__(self, writer: StarRocksReader | None = None) -> None:
        self.writer = writer or StarRocksReader()

    def search_news(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        limit: int = 10,
    ) -> list[NewsItem]:
        """Search news articles from Silver storage."""

        normalized_symbol = symbol.strip().upper()
        start_dt = day_start_utc(start_date)
        end_dt = day_end_utc(end_date)

        records = [
            record
            for record in self.writer.scan_records(start_at=start_dt, end_at=end_dt)
            if normalized_symbol in record.symbols and start_dt <= record.published_at <= end_dt
        ]
        records.sort(key=lambda item: item.published_at, reverse=True)
        return [self._to_news_item(record) for record in records[:limit]]

    def list_news(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> NewsPage:
        """List paginated news articles already stored in Silver."""

        records, total = self.writer.news_page(page=page, page_size=page_size)
        items = [self._to_news_item(record) for record in records]
        return NewsPage.build(items=items, page=page, page_size=page_size, total=total)

    def _to_news_item(self, record) -> NewsItem:
        return NewsItem.model_validate(
            {
                **record.model_dump(),
                "url": str(record.url),
            }
        )
