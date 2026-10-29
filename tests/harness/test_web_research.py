from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.harness.web import WebResearchError, WebResearchService, canonicalize_url
from backend.schemas.harness import WebDomain


class FakeResponse:
    def __init__(self, url: str, *, status_code: int = 200, content: bytes = b"", content_type: str = "text/html"):
        self.url = url
        self.status_code = status_code
        self.content = content
        self.headers = {"content-type": content_type}
        self.encoding = "utf-8"
        self.text = content.decode("utf-8", errors="replace")


class FakeClient:
    def __init__(self):
        self.post_urls = []

    def _search_response(self, url):
        return FakeResponse(
            url,
            content="""
            {
              "announcements": [
                {
                  "announcementId": "a1",
                  "announcementTitle": "<em>贵州茅台</em>：2025年年度报告",
                  "announcementTime": 1700000000000,
                  "adjunctUrl": "finalpage/2025-01-01/a1.PDF",
                  "secCode": "600519",
                  "secName": "贵州茅台"
                },
                {
                  "announcementId": "a2",
                  "announcementTitle": "贵州茅台2025年年度报告补充公告",
                  "announcementTime": 1700000000000,
                  "adjunctUrl": "https://finance.example.com/news.PDF",
                  "secCode": "600519",
                  "secName": "贵州茅台"
                }
              ]
            }
            """.encode(),
            content_type="application/json",
        )

    def get(self, url, **kwargs):
        if url.endswith("/robots.txt"):
            return FakeResponse(url, status_code=404)
        if "/new/fulltextSearch/full?" in url:
            return self._search_response(url)
        return FakeResponse(
            url,
            content=b"<html><head><title>Market Notice</title></head><body>Official notice content.</body></html>",
        )

    def post(self, url, **kwargs):
        self.post_urls.append(url)
        return self._search_response(url)


class FakeStore:
    def __init__(self, allowed: bool = True) -> None:
        self.allowed = allowed
        self.saved = None

    def is_allowed_web_host(self, host):
        return self.allowed and host in {"www.sse.com.cn", "static.cninfo.com.cn"}

    def latest_web_document(self, url, *, ttl_seconds):
        return None

    def save_web_document(self, document):
        self.saved = document
        return document


def _public_resolver(host, port):
    return [(2, 1, 6, "", ("93.184.216.34", 443))]


def test_canonicalize_url_removes_tracking_and_fragment():
    assert canonicalize_url("https://WWW.SSE.COM.CN/a/?utm_source=x&b=2#part") == "https://www.sse.com.cn/a?b=2"


def test_allowlisted_public_page_is_extracted_and_saved():
    store = FakeStore()
    service = WebResearchService(store=store, client=FakeClient(), resolver=_public_resolver)

    document = service.fetch("https://www.sse.com.cn/notice")

    assert document.domain == "www.sse.com.cn"
    assert "Official notice content" in document.text
    assert store.saved.content_hash


def test_rejects_domain_outside_allowlist():
    service = WebResearchService(store=FakeStore(allowed=False), client=FakeClient(), resolver=_public_resolver)
    with pytest.raises(WebResearchError, match="allowlist"):
        service.fetch("https://www.sse.com.cn/notice")


def test_rejects_private_resolution():
    def private_resolver(host, port):
        return [(2, 1, 6, "", ("127.0.0.1", 443))]

    service = WebResearchService(store=FakeStore(), client=FakeClient(), resolver=private_resolver)
    with pytest.raises(WebResearchError, match="non-public"):
        service.fetch("https://www.sse.com.cn/notice")


def test_web_search_enriches_allowlisted_pages_and_keeps_external_snippets():
    client = FakeClient()
    service = WebResearchService(
        store=FakeStore(),
        client=client,
        resolver=_public_resolver,
    )

    results = service.search(
        "贵州茅台 2025 年报",
        limit=5,
        as_of=datetime(2026, 10, 27, tzinfo=UTC),
    )

    assert len(results) == 2
    assert results[0].domain == "static.cninfo.com.cn"
    assert results[0].metadata["excerpt_only"] is False
    assert "Official notice content" in results[0].text
    assert results[1].domain == "finance.example.com"
    assert results[1].metadata["excerpt_only"] is True
    assert "年度报告补充公告" in results[1].text


def test_web_search_uses_security_query_before_general_fulltext():
    client = FakeClient()
    service = WebResearchService(
        store=FakeStore(),
        client=client,
        resolver=_public_resolver,
    )

    results = service.search(
        "2025 年报 净利润",
        symbol="600519.SH",
        limit=2,
        as_of=datetime(2026, 10, 27, tzinfo=UTC),
    )

    assert client.post_urls
    assert len(results) == 2

