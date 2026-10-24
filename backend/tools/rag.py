"""Tool for semantic news retrieval from the pgvector index."""

from __future__ import annotations

import logging

from datetime import UTC, date, datetime, timedelta
from typing import Any

from backend.config import settings
from backend.services.rag_service import RagService
from backend.tools.base import BaseTool, ToolResult, build_error_result, build_success_result

logger = logging.getLogger(__name__)


class RagTool(BaseTool):
    """Semantic news retrieval tool."""

    name = "rag_tool"
    description = "Search internal news chunks semantically from the pgvector index."

    def __init__(self, service: RagService | None = None) -> None:
        self.service = service or RagService()

    def run(self, **kwargs: Any) -> ToolResult:
        question = str(kwargs.get("question") or "").strip()
        symbol = str(kwargs.get("symbol") or "").strip().upper() or None
        try: limit = int(kwargs.get("limit", settings.RAG_TOP_K))
        except (TypeError, ValueError): return build_error_result("limit must be an integer")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        as_of = self._coerce_datetime(kwargs.get("as_of"))

        if not question:
            return build_error_result("Question cannot be empty")
        if not 1 <= limit <= 50:
            return build_error_result("limit must be between 1 and 50")

        resolved_start = self._coerce_date(start_date) or (datetime.now(UTC).date() - timedelta(days=settings.RAG_DEFAULT_LOOKBACK_DAYS))
        resolved_end = self._coerce_date(end_date) or datetime.now(UTC).date()
        if resolved_start > resolved_end:
            return build_error_result("start_date must be before or equal to end_date")

        result = self.service.search(
            question=question,
            symbol=symbol,
            start_date=resolved_start,
            end_date=resolved_end,
            as_of=as_of,
            limit=limit,
        )

        logger.info(
            "Tool result: rag_tool, symbol=%s, start_date=%s, end_date=%s, limit=%d, hits=%d",
            symbol,
            resolved_start,
            resolved_end,
            limit,
            len(result.hits),
        )

        payload = {
            "question": question,
            "symbol": symbol,
            "start_date": resolved_start.isoformat(),
            "end_date": resolved_end.isoformat(),
            "as_of": as_of.isoformat() if as_of else None,
            "articles": [article.model_dump(mode="json") for article in result.articles],
            "citations": [article.model_dump(mode="json") for article in result.articles],
            "message": result.message,
            "error_type": result.error_type,
            "error_stage": result.error_stage,
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

    def _coerce_datetime(self, value: Any) -> datetime | None:
        if value is None:
            return None
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def search_rag(
    *,
    question: str,
    symbol: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    as_of: datetime | None = None,
    limit: int = settings.RAG_TOP_K,
    service: RagService | None = None,
) -> ToolResult:
    """Convenient function interface used by the agent layer."""

    tool = RagTool(service=service)
    return tool.run(
        question=question,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        as_of=as_of,
        limit=limit,
    )
