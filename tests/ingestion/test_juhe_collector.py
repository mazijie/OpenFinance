from __future__ import annotations

import asyncio

import httpx
import pytest

from backend.config import settings
from backend.ingestion.collectors.juhe import JuheCollector, JuheCollectorError


def test_juhe_collect_news_only_fetches_detail_for_new_is_content_one(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JUHE_NEWS_API_KEY", "test-key")

    detail_calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "toutiao/content" in url:
            detail_calls.append(request.url.params["uniquekey"])
            return httpx.Response(
                200,
                json={
                    "error_code": 0,
                    "result": {
                        "detail": {
                            "uniquekey": "new-1",
                            "content": "宁德时代发布了新的扩产计划。",
                            "category": "caijing",
                        }
                    },
                },
            )

        return httpx.Response(
            200,
            json={
                "error_code": 0,
                "result": {
                    "data": [
                        {
                            "uniquekey": "old-1",
                            "title": "旧新闻",
                            "url": "https://example.com/old",
                            "author_name": "旧来源",
                            "date": "2026-07-17 10:00:00",
                            "category": "caijing",
                            "is_content": "1",
                        },
                        {
                            "uniquekey": "new-1",
                            "title": "宁德时代扩大产能",
                            "url": "https://example.com/catl-news",
                            "author_name": "第一来源",
                            "date": "2026-07-17 10:30:00",
                            "category": "caijing",
                            "is_content": "1",
                        },
                        {
                            "uniquekey": "new-2",
                            "title": "市场快讯",
                            "url": "https://example.com/flash-news",
                            "author_name": "快讯源",
                            "date": "2026-07-17 10:35:00",
                            "category": "caijing",
                            "is_content": "0",
                        },
                    ]
                },
            },
        )

    transport = httpx.MockTransport(handler)

    async def run_test() -> None:
        async with httpx.AsyncClient(transport=transport) as client:
            collector = JuheCollector(client=client)
            result = await collector.collect_news(existing_external_ids={"old-1"}, limit=10)

        assert len(result.articles) == 1
        article_by_id = {article.external_news_id: article for article in result.articles}

        rich_article = article_by_id["new-1"]
        assert rich_article.title == "宁德时代扩大产能"
        assert rich_article.content == "宁德时代发布了新的扩产计划。"
        assert rich_article.category == "caijing"

        assert "new-2" not in article_by_id

        assert detail_calls == ["new-1"]

    asyncio.run(run_test())


def test_juhe_collect_news_skips_empty_detail_content(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JUHE_NEWS_API_KEY", "test-key")

    async def handler(request: httpx.Request) -> httpx.Response:
        if "toutiao/content" in str(request.url):
            return httpx.Response(200, json={"error_code": 0, "result": {"detail": {"content": "  "}}})
        return httpx.Response(
            200,
            json={"error_code": 0, "result": {"data": [{
                "uniquekey": "empty-detail", "title": "空正文", "url": "https://example.com/empty",
                "is_content": "1",
            }]}},
        )

    async def run_test() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await JuheCollector(client=client).collect_news(limit=1)
        assert result.articles == []
        assert result.raw_response["articles"] == []

    asyncio.run(run_test())


def test_juhe_collect_news_pages_until_target_valid_articles(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JUHE_NEWS_API_KEY", "test-key")
    monkeypatch.setattr(settings, "JUHE_NEWS_LIST_PAGE_SIZE", 2)
    monkeypatch.setattr(settings, "JUHE_NEWS_MAX_PAGES", 3)
    requested_pages: list[str] = []
    detail_calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if "toutiao/content" in str(request.url):
            uniquekey = request.url.params["uniquekey"]
            detail_calls.append(uniquekey)
            return httpx.Response(200, json={"error_code": 0, "result": {"detail": {"content": f"正文 {uniquekey}"}}})

        page = request.url.params["page"]
        requested_pages.append(page)
        rows_by_page = {
            "1": [
                {"uniquekey": "no-content", "title": "无正文", "url": "https://example.com/0", "is_content": "0"},
                {"uniquekey": "valid-1", "title": "正文一", "url": "https://example.com/1", "is_content": "1"},
            ],
            "2": [
                {"uniquekey": "valid-2", "title": "正文二", "url": "https://example.com/2", "is_content": "1"},
                {"uniquekey": "unused", "title": "不应读取", "url": "https://example.com/3", "is_content": "1"},
            ],
        }
        return httpx.Response(200, json={"error_code": 0, "result": {"data": rows_by_page.get(page, [])}})

    async def run_test() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await JuheCollector(client=client).collect_news(limit=2)
        assert [article.external_news_id for article in result.articles] == ["valid-1", "valid-2"]
        assert requested_pages == ["1", "2"]
        assert detail_calls == ["valid-1", "valid-2"]
        assert len(result.raw_response["list_responses"]) == 2

    asyncio.run(run_test())


def test_juhe_collect_news_skips_detail_for_known_news(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JUHE_NEWS_API_KEY", "test-key")

    detail_calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if "toutiao/content" in str(request.url):
            detail_calls.append(request.url.params["uniquekey"])
            return httpx.Response(200, json={"error_code": 0, "result": {"detail": {"content": "unused"}}})

        return httpx.Response(
            200,
            json={
                "error_code": 0,
                "result": {
                    "data": [
                        {
                            "uniquekey": "known-1",
                            "title": "已入库新闻",
                            "url": "https://example.com/known",
                            "author_name": "来源",
                            "date": "2026-07-17 09:00:00",
                            "category": "caijing",
                            "is_content": "1",
                        }
                    ]
                },
            },
        )

    transport = httpx.MockTransport(handler)

    async def run_test() -> None:
        async with httpx.AsyncClient(transport=transport) as client:
            collector = JuheCollector(client=client)
            result = await collector.collect_news(existing_external_ids={"known-1"}, limit=10)

        assert result.articles == []
        assert detail_calls == []

    asyncio.run(run_test())


def test_juhe_collect_news_empty_result(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JUHE_NEWS_API_KEY", "test-key")
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"error_code": 0, "result": {"data": []}}))

    async def run_test() -> None:
        async with httpx.AsyncClient(transport=transport) as client:
            collector = JuheCollector(client=client)
            result = await collector.collect_news(limit=1)

        assert result.articles == []

    asyncio.run(run_test())


def test_juhe_collect_news_timeout(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JUHE_NEWS_API_KEY", "test-key")

    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timeout", request=request)

    transport = httpx.MockTransport(handler)

    async def run_test() -> None:
        async with httpx.AsyncClient(transport=transport) as client:
            collector = JuheCollector(client=client)
            with pytest.raises(JuheCollectorError):
                await collector.collect_news(limit=1)

    asyncio.run(run_test())


def test_juhe_collect_news_api_error(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JUHE_NEWS_API_KEY", "test-key")
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"error_code": 10012, "reason": "rate limit"}))

    async def run_test() -> None:
        async with httpx.AsyncClient(transport=transport) as client:
            collector = JuheCollector(client=client)
            with pytest.raises(JuheCollectorError):
                await collector.collect_news(limit=1)

    asyncio.run(run_test())


def test_juhe_collect_news_requires_api_key(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JUHE_NEWS_API_KEY", "")

    async def run_test() -> None:
        collector = JuheCollector()
        with pytest.raises(JuheCollectorError):
            await collector.collect_news(limit=1)

    asyncio.run(run_test())


def test_juhe_collect_news_deduplicates_list_ids(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JUHE_NEWS_API_KEY", "test-key")
    detail_calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if "toutiao/content" in str(request.url):
            detail_calls.append(request.url.params["uniquekey"])
            return httpx.Response(200, json={"error_code": 0, "result": {"detail": {"content": "内容"}}})
        row = {"uniquekey": "same", "title": "重复新闻", "url": "https://example.com/news", "is_content": "1"}
        return httpx.Response(200, json={"error_code": 0, "result": {"data": [row, row]}})

    async def run_test() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await JuheCollector(client=client).collect_news(limit=10)
        assert len(result.articles) == 1
        assert detail_calls == ["same"]

    asyncio.run(run_test())
