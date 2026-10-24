"""Tool for querying news from the Silver lakehouse table."""

from __future__ import annotations

import logging

from datetime import UTC, date, datetime, timedelta
from typing import Any

from backend.services.news_service import NewsService
from backend.tools.base import BaseTool, ToolResult, build_error_result, build_success_result


logger = logging.getLogger(__name__)

class NewsTool(BaseTool):
    """Lakehouse-backed news lookup tool."""

    name = "news_tool"
    description = "Search recent company news from the internal lakehouse Silver table."

    def __init__(self, service: NewsService | None = None) -> None:
        self.service = service or NewsService()

    def run(self, **kwargs: Any) -> ToolResult:
        symbol = str(kwargs.get("symbol") or "").strip().upper()
        limit = int(kwargs.get("limit", 10))
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        if not symbol:
            return build_error_result("Stock symbol cannot be empty")

        resolved_start = self._coerce_date(start_date) or (datetime.now(UTC).date() - timedelta(days=7))
        resolved_end = self._coerce_date(end_date) or datetime.now(UTC).date()

        result = self.service.search_news_with_message(
            symbol=symbol,
            start_date=resolved_start,
            end_date=resolved_end,
            limit=limit,
        )

        logger.info("Tool结果: news_tool, symbol=%s, start_date=%s, end_date=%s, limit=%d, items=%d", symbol, resolved_start, resolved_end, limit, len(result.items))

        payload = {
            "symbol": symbol,
            "start_date": resolved_start.isoformat(),
            "end_date": resolved_end.isoformat(),
            "items": [item.model_dump(mode="json") for item in result.items],
            "message": result.message,
        }
        return build_success_result(payload)

    def _coerce_date(self, value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        return date.fromisoformat(str(value))


def get_news(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    limit: int = 10,
    service: NewsService | None = None,
) -> ToolResult:
    """Convenient function interface used by the agent layer."""

    tool = NewsTool(service=service)
    return tool.run(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)
