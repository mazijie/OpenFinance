from __future__ import annotations

from backend.schemas.news import NewsSearchResult
from backend.tools.news import NewsTool


class FakeNewsService:
    def __init__(self, result: NewsSearchResult) -> None:
        self.result = result
        self.called = False

    def search_news_with_message(self, symbol, start_date, end_date, limit=10):
        self.called = True
        return self.result


def test_news_tool_returns_lakehouse_empty_message() -> None:
    message = "当前自有湖仓在指定时间范围内未检索到相关新闻。"
    service = FakeNewsService(result=NewsSearchResult(items=[], message=message))
    tool = NewsTool(service=service)

    result = tool.run(symbol="NVDA", start_date="2026-07-01", end_date="2026-07-14", limit=5)

    assert service.called is True
    assert result["success"] is True
    assert result["data"]["message"] == message


def test_news_tool_returns_empty_lakehouse_results() -> None:
    service = FakeNewsService(result=NewsSearchResult(items=[], message="当前自有湖仓在指定时间范围内未检索到相关新闻。"))
    tool = NewsTool(service=service)

    result = tool.run(symbol="NVDA", start_date="2026-07-01", end_date="2026-07-14", limit=5)

    assert result["success"] is True
    assert result["data"]["items"] == []
