from datetime import UTC, datetime

from backend.ingestion.collectors.cninfo import CNInfoCollector


def test_cninfo_parser_accepts_millisecond_timestamp_and_relative_pdf_url() -> None:
    report = CNInfoCollector._parse_announcement(
        {
            "announcementId": "12345",
            "announcementTitle": "贵州茅台2024年年度报告",
            "announcementTime": 1745798400000,
            "secName": "贵州茅台",
            "adjunctUrl": "finalpage/2025-04-28/12345.PDF",
        },
        code="600519",
        exchange="SH",
    )
    assert report is not None
    assert report.report_year == 2024
    assert report.pdf_url == "https://static.cninfo.com.cn/finalpage/2025-04-28/12345.PDF"
    assert report.announcement_time == datetime(2025, 4, 28, tzinfo=UTC)


def test_cninfo_full_report_filter_excludes_summary_and_english() -> None:
    assert CNInfoCollector._is_full_report("2024年年度报告") is True
    assert CNInfoCollector._is_full_report("2024年年度报告摘要") is False
    assert CNInfoCollector._is_full_report("2024年年度报告（英文版）") is False
