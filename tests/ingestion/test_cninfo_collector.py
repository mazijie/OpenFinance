from datetime import UTC, datetime
from urllib.parse import parse_qs

import httpx

from backend.providers.cninfo import CNInfoCollector


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


def test_annual_report_query_omits_column_across_pages() -> None:
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        params = parse_qs(request.content.decode())
        requests.append(params)
        page = int(params["pageNum"][0])
        return httpx.Response(200, json={
            "announcements": [{
                "announcementId": str(page),
                "announcementTitle": "天力锂能2024年年度报告",
                "announcementTime": 1745798400000,
                "secName": "天力锂能",
            }],
            "hasMore": page == 1,
        })

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        reports = CNInfoCollector(client).list_annual_reports(
            stock_code="301152.BJ:gfbj0833757", report_year=2024,
        )

    assert [report.announcement_id for report in reports] == ["1", "2"]
    assert all("column" not in params for params in requests)
    assert [params["pageNum"] for params in requests] == [["1"], ["2"]]
    assert all(params["stock"] == ["301152,gfbj0833757"] for params in requests)
    assert all(report.stock_code == "301152" and report.exchange == "BJ" for report in reports)


def test_manual_query_resolves_and_caches_authoritative_org_id() -> None:
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"stockList": [
                {"code": "000001", "orgId": "gssz0000001", "category": "A股"},
                {"code": "000002", "orgId": "gssz0000002", "category": "A股"},
            ]})
        return httpx.Response(200, json={"announcements": [], "hasMore": False})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        collector = CNInfoCollector(client)
        collector.list_annual_reports(stock_code="000001.SZ", report_year=2024)
        collector.list_annual_reports(stock_code="000002.SZ", report_year=2024)

    assert [request.method for request in requests] == ["GET", "POST", "POST"]
    assert parse_qs(requests[1].content.decode())["stock"] == ["000001,gssz0000001"]
    assert parse_qs(requests[2].content.decode())["stock"] == ["000002,gssz0000002"]


def test_manual_query_refuses_unknown_or_ambiguous_org_id() -> None:
    import pytest
    from backend.providers.cninfo import CNInfoCollectorError

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(200, json={"stockList": [
            {"code": "000001", "orgId": "first", "category": "A股"},
            {"code": "000001", "orgId": "second", "category": "A股"},
        ]})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        collector = CNInfoCollector(client)
        with pytest.raises(CNInfoCollectorError, match="2 organization IDs"):
            collector.list_annual_reports(stock_code="000001.SZ", report_year=2024)
        with pytest.raises(CNInfoCollectorError, match="0 organization IDs"):
            collector.list_annual_reports(stock_code="000002.SZ", report_year=2024)
