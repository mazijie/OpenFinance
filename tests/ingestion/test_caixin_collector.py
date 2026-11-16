from datetime import UTC, datetime

import httpx
import pytest

from backend.ingestion.collectors.caixin import CaixinCollector, CaixinCollectorError
from backend.pipelines.news_bronze_to_silver import NewsBronzeToSilverPipeline
from backend.rag.news.chunking import build_source_text
from backend.rag.common.citations import build_citation_context
from backend.rag.common.retrieval import aggregate_articles
from backend.schemas.rag import RagSearchHit


def row(title: str, url: str, timestamp: int, summary: str = "摘要") -> dict:
    return {"title": title, "url": url, "time": timestamp, "summary": summary, "tag": "市场"}


@pytest.mark.asyncio
async def test_collection_deduplicates_and_normalizes_for_silver() -> None:
    rows = [row("新", "https://example.com/new?cxapp_link=true", 1800000100),
            row("旧", "https://example.com/old?utm_source=test", 1800000000),
            row("重复", "https://example.com/old", 1800000000),
            row("无摘要", "https://example.com/invalid", 1800000000, "")]
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"success": True, "data": {"data": rows}}))
    async with httpx.AsyncClient(transport=transport) as client:
        result = await CaixinCollector(client).collect_news()
    assert len(result.articles) == 2
    article = result.articles[0]
    assert article.title == "旧"
    assert article.external_news_id == "https://example.com/old"
    assert len(result.raw_response["list_response"]["data"]["data"]) == 4
    pipeline = NewsBronzeToSilverPipeline(bronze_writer=object(), news_writer=object())
    record = pipeline._normalize_article(provider="caixin", article=article.raw,
        raw_object_path="news/provider=caixin/example", ingested_at=datetime(2027, 1, 16, tzinfo=UTC))
    assert record.published_at == datetime.fromtimestamp(1800000000, UTC)
    assert record.content is None and record.summary == "摘要"
    assert build_source_text(record) == "标题：旧\n摘要：摘要"
    async with httpx.AsyncClient(transport=transport) as client:
        again = await CaixinCollector(client).collect_news(existing_external_ids={article.external_news_id})
    assert len(again.articles) == 1
    assert again.articles[0].title == "新"


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [{"success": False}, {"success": True, "data": {"data": {}}}])
async def test_bad_feed_response_fails(payload: dict) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(CaixinCollectorError):
            await CaixinCollector(client).collect_news()


@pytest.mark.asyncio
async def test_missing_time_does_not_fall_back_to_ingestion_time() -> None:
    value = row("新闻", "https://example.com/a", 0)
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"success": True, "data": {"data": [value]}}))
    async with httpx.AsyncClient(transport=transport) as client:
        result = await CaixinCollector(client).collect_news()
    assert not result.articles


def test_summary_evidence_is_labeled() -> None:
    published = datetime(2026, 1, 1, tzinfo=UTC)
    hit = RagSearchHit(chunk_id="chunk", news_id="news", provider="caixin", source_name="财新",
        title="标题", summary="摘要内容", content=None, url="https://example.com/a",
        published_at=published, ingested_at=published, content_hash="hash", raw_object_path="bronze",
        chunk_index=0, chunk_text="标题：标题 摘要：摘要内容", similarity=0.8, rerank_score=0.8)
    article = aggregate_articles([hit], now=published)[0].model_copy(update={"citation_id": 1})
    assert article.metadata["content_type"] == "summary"
    assert "内容类型：新闻摘要" in build_citation_context([article])
