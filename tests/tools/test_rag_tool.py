from __future__ import annotations

from datetime import date

from backend.schemas.rag import RagArticleResult, RagEvidence, RagSearchHit, RagSearchResult
from backend.tools.rag import RagTool


class FakeRagService:
    def __init__(self, result: RagSearchResult) -> None:
        self.result = result
        self.calls: list[dict[str, object]] = []

    def search(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


def test_rag_tool_returns_empty_message() -> None:
    service = FakeRagService(RagSearchResult(hits=[], message="当前自有湖仓在指定时间范围内未检索到相关新闻。"))
    tool = RagTool(service=service)

    result = tool.run(question="NVDA 最近发生了什么", symbol="NVDA", start_date=date(2026, 7, 1), end_date=date(2026, 7, 18))

    assert result["success"] is True
    assert result["data"]["message"] == "当前自有湖仓在指定时间范围内未检索到相关新闻。"
    assert service.calls[0]["question"] == "NVDA 最近发生了什么"


def test_rag_tool_returns_article_citations() -> None:
    hit = RagSearchHit(
        chunk_id="chunk-1",
        news_id="news-1",
        external_news_id="ext-1",
        provider="juhe",
        source_name="Source",
        title="NVIDIA news",
        summary="Summary",
        content="Content",
        url="https://example.com/news-1",
        published_at="2026-07-18T00:00:00+00:00",
        ingested_at="2026-07-18T01:00:00+00:00",
        language="zh-cn",
        symbols=["NVDA"],
        entities=["NVIDIA"],
        content_hash="hash-1",
        raw_object_path="news/provider=juhe/test.json",
        chunk_index=0,
        chunk_text="chunk text",
        similarity=0.91,
    )
    article = RagArticleResult(
        citation_id=1,
        news_id=hit.news_id,
        content_hash=hit.content_hash,
        title=hit.title,
        source_name=hit.source_name,
        provider=hit.provider,
        published_at=hit.published_at,
        url=hit.url,
        final_score=0.8,
        evidence_chunks=[RagEvidence(chunk_id=hit.chunk_id, chunk_text=hit.chunk_text, rerank_score=0.9, final_score=0.8)],
    )
    service = FakeRagService(RagSearchResult(hits=[hit], articles=[article], message=None))
    tool = RagTool(service=service)

    result = tool.run(question="NVDA 最近有啥新闻", symbol="NVDA", limit=3)

    assert result["success"] is True
    assert len(result["data"]["articles"]) == 1
    assert result["data"]["citations"][0]["citation_id"] == 1


def test_rag_tool_forwards_as_of_cutoff() -> None:
    service = FakeRagService(RagSearchResult(hits=[], articles=[]))
    tool = RagTool(service=service)
    cutoff = "2026-10-23T12:00:00+00:00"

    tool.run(question="最近事件", symbol="600519.SH", as_of=cutoff)

    assert service.calls[0]["as_of"].isoformat() == "2026-10-23T12:00:00+00:00"
