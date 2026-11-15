from __future__ import annotations

from datetime import UTC, datetime

from backend.repositories.news_repository import NewsRepository
from backend.schemas.news import NewsArticleRecord


class FakeWriter:
    def __init__(self, records: list[NewsArticleRecord]) -> None:
        self._records = records

    def scan_records(self, *, start_at, end_at) -> list[NewsArticleRecord]:
        return [r for r in self._records if start_at <= r.published_at <= end_at]

    def news_page(self, *, page, page_size):
        records = sorted(self._records, key=lambda r: (r.published_at, r.ingested_at), reverse=True)
        return records[(page - 1) * page_size:page * page_size], len(records)


def test_search_news_by_symbol_and_date() -> None:
    repository = NewsRepository(
        writer=FakeWriter(
            [
                NewsArticleRecord(
                    news_id="1",
                    provider="juhe",
                    source_name="Example",
                    title="NVIDIA launches new GPU",
                    summary="Summary",
                    url="https://example.com/nvda",
                    published_at=datetime(2026, 7, 14, 1, 0, tzinfo=UTC),
                    ingested_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
                    language="en",
                    symbols=["NVDA"],
                    entities=["nvidia"],
                    content_hash="hash-1",
                    raw_object_path="path-1",
                ),
                NewsArticleRecord(
                    news_id="2",
                    provider="juhe",
                    source_name="Example",
                    title="Tesla update",
                    summary="Summary",
                    url="https://example.com/tsla",
                    published_at=datetime(2026, 7, 14, 1, 0, tzinfo=UTC),
                    ingested_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
                    language="en",
                    symbols=["TSLA"],
                    entities=["tesla"],
                    content_hash="hash-2",
                    raw_object_path="path-2",
                ),
            ]
        )
    )

    items = repository.search_news(symbol="nvda", start_date=datetime(2026, 7, 13).date(), end_date=datetime(2026, 7, 14).date(), limit=10)

    assert len(items) == 1
    assert items[0].news_id == "1"


def test_list_news_paginated() -> None:
    repository = NewsRepository(
        writer=FakeWriter(
            [
                NewsArticleRecord(
                    news_id="1",
                    provider="juhe",
                    source_name="Source A",
                    title="Older news",
                    summary="Summary A",
                    url="https://example.com/a",
                    published_at=datetime(2026, 7, 14, 1, 0, tzinfo=UTC),
                    ingested_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
                    language="zh-cn",
                    symbols=["000001.SZ"],
                    entities=["平安银行"],
                    content_hash="hash-a",
                    raw_object_path="path-a",
                ),
                NewsArticleRecord(
                    news_id="2",
                    provider="juhe",
                    source_name="Source B",
                    title="Newest news",
                    summary="Summary B",
                    url="https://example.com/b",
                    published_at=datetime(2026, 7, 15, 1, 0, tzinfo=UTC),
                    ingested_at=datetime(2026, 7, 15, 2, 0, tzinfo=UTC),
                    language="zh-cn",
                    symbols=["600519.SH"],
                    entities=["贵州茅台"],
                    content_hash="hash-b",
                    raw_object_path="path-b",
                ),
                NewsArticleRecord(
                    news_id="3",
                    provider="juhe",
                    source_name="Source C",
                    title="Middle news",
                    summary="Summary C",
                    url="https://example.com/c",
                    published_at=datetime(2026, 7, 14, 12, 0, tzinfo=UTC),
                    ingested_at=datetime(2026, 7, 14, 12, 30, tzinfo=UTC),
                    language="zh-cn",
                    symbols=["300750.SZ"],
                    entities=["宁德时代"],
                    content_hash="hash-c",
                    raw_object_path="path-c",
                ),
            ]
        )
    )

    page = repository.list_news(page=1, page_size=2)

    assert page.total == 3
    assert page.total_pages == 2
    assert page.has_next is True
    assert [item.news_id for item in page.items] == ["2", "3"]
