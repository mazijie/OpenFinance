"""Tool for semantic retrieval over internal annual reports."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from backend.services.annual_report_rag_service import AnnualReportRagService
from backend.tools.base import BaseTool, ToolResult, build_error_result, build_success_result


class AnnualReportTool(BaseTool):
    name = "annual_report_tool"
    description = "Search internal CNInfo annual-report sections with page-level citations."

    def __init__(self, service: AnnualReportRagService | None = None) -> None:
        self.service = service or AnnualReportRagService()

    def run(self, **kwargs: Any) -> ToolResult:
        question = str(kwargs.get("question") or "").strip()
        stock_code = str(kwargs.get("stock_code") or "").strip().upper() or None
        report_year = kwargs.get("report_year")
        as_of = kwargs.get("as_of")
        published_after = kwargs.get("published_after")
        as_of = self._coerce_datetime(as_of)
        published_after = self._coerce_datetime(published_after)
        if not question:
            return build_error_result("question cannot be empty")
        try:
            articles = self.service.search(
                question=question,
                stock_code=stock_code,
                report_year=int(report_year) if report_year else None,
                as_of=as_of,
                published_after=published_after,
                limit=int(kwargs.get("limit", 5)),
            )
        except Exception as exc:
            return build_error_result(f"annual-report retrieval failed: {exc}")
        return build_success_result({"articles": [article.model_dump(mode="json") for article in articles], "citations": [article.model_dump(mode="json") for article in articles]})

    def _coerce_datetime(self, value: Any) -> datetime | None:
        if value is None:
            return None
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def search_annual_reports(**kwargs: Any) -> ToolResult:
    return AnnualReportTool().run(**kwargs)
