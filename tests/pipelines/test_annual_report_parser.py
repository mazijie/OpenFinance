from backend.pipelines.annual_report_parser import build_report_sections
from backend.schemas.annual_report import AnnualReportPageRecord


def _page(number: int, text: str) -> AnnualReportPageRecord:
    return AnnualReportPageRecord(
        report_id="report-1", stock_code="600519", report_year=2024, page_number=number,
        text=text, text_sha256=str(number), char_count=len(text),
    )


def test_section_builder_preserves_page_ranges() -> None:
    sections = build_report_sections([
        _page(1, "封面和重要提示"),
        _page(2, "第一章 公司业务概要 业务描述"),
        _page(3, "第一章 公司业务概要 后续说明"),
    ])
    assert sections
    assert sections[0].start_page == 1
    assert sections[-1].end_page == 3
