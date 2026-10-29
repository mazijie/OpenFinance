from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from backend.main import app
from backend.schemas.news import NewsItem, NewsPage


class FakeNewsService:
    def list_news(self, *, page: int = 1, page_size: int = 20) -> NewsPage:
        assert page == 1
        assert page_size == 2
        return NewsPage(
            items=[
                NewsItem(
                    news_id="news-1",
                    provider="juhe",
                    title="财经新闻",
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


def test_list_news_api() -> None:
    from backend.api.news import get_news_service

    app.dependency_overrides[get_news_service] = lambda: FakeNewsService()
    client = TestClient(app)

    try:
        response = client.get("/api/news", params={"page": 1, "page_size": 2})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["page"] == 1
    assert payload["page_size"] == 2
    assert payload["total"] == 1
    assert payload["items"][0]["news_id"] == "news-1"
    assert payload["items"][0]["title"] == "财经新闻"


def test_frontend_page_and_assets() -> None:
    client = TestClient(app)

    page_response = client.get("/ui")
    script_response = client.get("/static/app.js")

    assert page_response.status_code == 200
    assert "OpenFinance Agent" in page_response.text
    assert "/api/chat" in script_response.text
    assert "/api/news" in script_response.text
    assert "/api/market" in script_response.text
    assert "/api/watchlist" in script_response.text
    assert "/api/monitor/reports" in script_response.text
    assert "/api/agent/threads" in script_response.text
    assert "echarts" in page_response.text
    assert "marketChart" in page_response.text
    assert "monitorReportList" in page_response.text
    assert 'id="marketPage"' in page_response.text
    assert "行情、智能问答" in page_response.text
    assert 'id="modelSettingsForm"' in page_response.text
    assert 'id="chatThreadTabs"' in page_response.text
    assert 'id="newChatThreadButton"' in page_response.text
    assert 'id="agentPage"' not in page_response.text
    assert "湖仓" not in page_response.text
    assert "LAKEHOUSE" not in page_response.text
    assert "Silver/Gold" not in script_response.text
    assert "预算" not in page_response.text
    assert "预算" not in script_response.text
