from datetime import UTC, datetime

from backend.schemas.rag import RagArticleResult
from backend.tools.annual_reports import AnnualReportTool


class FakeAnnualReportService:
    def __init__(self):
        self.kwargs = None

    def search(self, **kwargs):
        self.kwargs = kwargs
        return [
            RagArticleResult(
                citation_id=1,
                news_id="report-1",
                content_hash="chunk-1",
                title="年报",
                provider="cninfo",
                published_at=datetime.now(UTC),
                url="/api/reports/report-1/download",
                final_score=0.9,
            )
        ]


def test_annual_report_tool_forwards_structured_filters() -> None:
    service = FakeAnnualReportService()
    result = AnnualReportTool(service=service).run(
        question="研发投入",
        stock_code="600519.SH",
        report_year=2024,
        section_path="管理层讨论与分析",
        content_types=["table"],
        page_start=10,
        page_end=20,
        limit=3,
    )
    assert result["success"] is True
    assert service.kwargs["section_path"] == "管理层讨论与分析"
    assert service.kwargs["content_types"] == ["table"]
    assert service.kwargs["page_start"] == 10
