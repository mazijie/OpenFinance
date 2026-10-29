from __future__ import annotations

from datetime import UTC, datetime

from backend.harness.evidence import EvidenceService, assign_citations
from backend.harness.web import WebSearchResult


def test_evidence_service_merges_sources_and_deduplicates():
    def market_search(**kwargs):
        return {
            "success": True,
            "data": {
                "symbol": "000001.SZ",
                "start_date": "2026-10-01",
                "end_date": "2026-10-25",
                "first_close": 10,
                "last_close": 11,
                "interval_return": 0.1,
                "annualized_volatility": 0.2,
            },
            "error": None,
        }

    def news_search(**kwargs):
        article = {
            "news_id": "news-1",
            "title": "平安银行公告",
            "provider": "juhe",
            "source_name": "交易所",
            "url": "https://example.com/news",
            "published_at": "2026-10-20T00:00:00Z",
            "content_hash": "hash",
            "final_score": 0.8,
            "evidence_chunks": [{"chunk_text": "公告内容"}],
        }
        return {"success": True, "data": {"articles": [article]}, "error": None}

    service = EvidenceService(market_search=market_search, news_search=news_search, annual_search=lambda **kwargs: {"success": True, "data": {"articles": []}})
    pack = service.search(
        query="平安银行最近变化",
        sources=["market", "news", "annual_report"],
        symbol="000001.SZ",
        as_of=datetime(2026, 10, 25, tzinfo=UTC),
    )

    assert {item.source_type for item in pack.documents} == {"market", "news"}
    documents, citations = assign_citations(pack.documents)
    assert [citation.citation_id for citation in citations] == [1, 2]
    assert len(documents) == 2


def test_evidence_service_falls_back_to_web_when_internal_sources_are_empty():
    class FakeWebService:
        def search(self, query, *, symbol=None, limit, as_of):
            return [
                WebSearchResult(
                    rank=1,
                    title="公开财经报道",
                    url="https://finance.example.com/article",
                    domain="finance.example.com",
                    text="公开报道提供了补充信息。",
                    published_at=datetime(2026, 10, 25, tzinfo=UTC),
                    fetched_at=datetime(2026, 10, 25, tzinfo=UTC),
                    metadata={"excerpt_only": True, "provider": "so360"},
                )
            ]

    service = EvidenceService(
        market_search=lambda **kwargs: {"success": True, "data": None},
        news_search=lambda **kwargs: {"success": True, "data": {"articles": []}},
        annual_search=lambda **kwargs: {"success": True, "data": {"articles": []}},
        web_service=FakeWebService(),
    )
    pack = service.search(
        query="公司最新变化",
        sources=["market", "news", "annual_report"],
        symbol="000001.SZ",
        as_of=datetime(2026, 10, 25, tzinfo=UTC),
        web_fallback=True,
    )

    assert [document.source_type for document in pack.documents] == ["web"]
    assert pack.documents[0].url == "https://finance.example.com/article"
    assert "web" in pack.sources


def test_web_fallback_also_fills_partially_missing_internal_sources():
    class FakeWebService:
        def search(self, query, *, symbol=None, limit, as_of):
            return [
                WebSearchResult(
                    rank=1,
                    title="公开补充资料",
                    url="https://finance.example.com/follow-up",
                    domain="finance.example.com",
                    text="公开补充资料内容" * 30,
                    published_at=datetime(2026, 10, 25, tzinfo=UTC),
                    fetched_at=datetime(2026, 10, 25, tzinfo=UTC),
                    metadata={"excerpt_only": True},
                )
            ]

    def market_search(**kwargs):
        return {
            "success": True,
            "data": {
                "symbol": "000001.SZ",
                "start_date": "2026-10-01",
                "end_date": "2026-10-25",
                "first_close": 10,
                "last_close": 11,
                "interval_return": 0.1,
                "annualized_volatility": 0.2,
            },
        }

    service = EvidenceService(
        market_search=market_search,
        news_search=lambda **kwargs: {"success": True, "data": {"articles": []}},
        annual_search=lambda **kwargs: {"success": True, "data": {"articles": []}},
        web_service=FakeWebService(),
    )
    pack = service.search(
        query="000001.SZ 公司变化",
        sources=["market", "news", "annual_report"],
        symbol="000001.SZ",
        as_of=datetime(2026, 10, 25, tzinfo=UTC),
        web_fallback=True,
    )

    assert {document.source_type for document in pack.documents} == {"market", "web"}

