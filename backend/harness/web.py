"""Allowlisted web fetching with SSRF protection and immutable snapshots."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser
from uuid import uuid4

import httpx
from bs4 import BeautifulSoup

from backend.config import settings
from backend.harness.store import HarnessStore
from backend.schemas.harness import WebDocument


class WebResearchError(Exception):
    """Raised when an allowlisted web page cannot be safely retrieved."""


@dataclass(frozen=True, slots=True)
class WebSearchResult:
    rank: int
    title: str
    url: str
    domain: str
    text: str
    published_at: datetime | None
    fetched_at: datetime
    metadata: dict[str, Any]

SEARCH_GENERIC_TERMS = {
    "查询",
    "分析",
    "研究",
    "帮我",
    "一下",
    "最近",
    "近期",
    "目前",
    "现在",
    "今天",
    "今年",
    "行情",
    "股价",
    "股票",
    "消息",
    "新闻",
    "公告",
    "年报",
    "财报",
    "情况",
    "怎么样",
    "如何",
    "为什么",
    "是否",
    "以及",
    "相关",
    "数据",
}


def canonicalize_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.hostname:
        raise WebResearchError("Only absolute HTTP(S) URLs are supported")
    host = parsed.hostname.casefold().rstrip(".")
    port = f":{parsed.port}" if parsed.port else ""
    ignored = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"}
    query = urlencode(
        sorted(
            (key, item)
            for key, item in parse_qsl(parsed.query, keep_blank_values=True)
            if key.casefold() not in ignored
        )
    )
    path = re.sub(r"/+", "/", parsed.path or "/").rstrip("/") or "/"
    return urlunsplit(("https", f"{host}{port}", path, query, ""))


class WebResearchService:
    def __init__(
        self,
        *,
        store: HarnessStore | None = None,
        client: httpx.Client | None = None,
        resolver: Callable[[str, int], list[tuple[Any, ...]]] | None = None,
    ) -> None:
        self.store = store or HarnessStore()
        self.client = client or httpx.Client(
            timeout=settings.AGENT_WEB_FETCH_TIMEOUT_SECONDS,
            follow_redirects=False,
            headers={"User-Agent": "OpenFinance-Agent/0.2 (+local research agent)"},
        )
        self.resolver = resolver or socket.getaddrinfo

    def fetch(self, url: str) -> WebDocument:
        canonical = canonicalize_url(url)
        cached = self.store.latest_web_document(
            canonical,
            ttl_seconds=settings.AGENT_WEB_CACHE_TTL_SECONDS,
        )
        if cached is not None:
            return cached
        final_url, response = self._fetch_with_redirects(canonical)
        body = response.content
        if len(body) > settings.AGENT_WEB_MAX_BYTES:
            raise WebResearchError("Web response exceeds configured size limit")
        content_type = response.headers.get("content-type", "").casefold()
        if "text/html" in content_type or "application/xhtml" in content_type:
            title, text = self._extract_html(body, final_url)
        elif "application/pdf" in content_type or body.startswith(b"%PDF"):
            title, text = self._extract_pdf(body, final_url)
        elif content_type.startswith("text/") or not content_type:
            text = body.decode(self._encoding(response), errors="replace").strip()
            title = urlsplit(final_url).path.rsplit("/", 1)[-1] or final_url
        else:
            raise WebResearchError(f"Unsupported web content type: {content_type}")
        if not text:
            raise WebResearchError("No extractable text found")
        fetched_at = datetime.now(UTC)
        document = WebDocument(
            document_id=str(uuid4()),
            url=final_url,
            canonical_url=canonical,
            domain=urlsplit(final_url).hostname or "",
            title=title[:500] or final_url,
            text=text[:200_000],
            content_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
            fetched_at=fetched_at,
            metadata={"content_type": content_type, "size": len(body)},
        )
        return self.store.save_web_document(document)

    def search(
        self,
        query: str,
        *,
        symbol: str | None = None,
        limit: int = 8,
        as_of: datetime | None = None,
    ) -> list[WebSearchResult]:
        """Discover public web results and enrich allowlisted pages with full text."""

        if not settings.AGENT_WEB_SEARCH_ENABLED:
            return []
        normalized_query = " ".join(f"{query} {symbol or ''}".split())[:300]
        if not normalized_query:
            return []
        search_key = _cninfo_search_key(normalized_query)
        if not search_key:
            return []
        requested_years = re.findall(r"(?<!\d)(20\d{2})(?!\d)", normalized_query)
        cutoff = as_of or datetime.now(UTC)
        requested_limit = min(limit, settings.AGENT_WEB_SEARCH_MAX_RESULTS)
        requested_title_terms = _requested_title_terms(normalized_query)
        candidates: list[WebSearchResult] = []
        if symbol:
            try:
                candidates = self._search_announcements_by_security(
                    symbol=symbol,
                    query=normalized_query,
                    cutoff=cutoff,
                    limit=requested_limit,
                    requested_years=requested_years,
                    requested_title_terms=requested_title_terms,
                )
            except Exception:
                candidates = []
        if not candidates:
            endpoint = settings.AGENT_WEB_SEARCH_ENDPOINT
            parsed = urlsplit(endpoint)
            if parsed.scheme.casefold() != "https" or not parsed.hostname:
                raise WebResearchError("Web search endpoint must use HTTPS")
            self._validate_public_host(endpoint, require_allowlist=False)
            params = {
                "searchkey": search_key,
                "sdate": "",
                "edate": "",
                "isfulltext": "false",
                "sortName": "pubdate",
                "sortType": "desc",
                "pageNum": 1,
                "pageSize": min(50, max(limit * 8, 30)),
            }
            search_url = f"{endpoint}?{urlencode(params)}"
            if not self._robots_allows(search_url):
                raise WebResearchError("robots.txt disallows web search")
            response = self.client.get(
                search_url,
                headers={"Referer": "https://www.cninfo.com.cn/"},
            )
            if response.status_code >= 400:
                raise WebResearchError(f"Web search failed with HTTP {response.status_code}")
            if len(response.content) > settings.AGENT_WEB_MAX_BYTES:
                raise WebResearchError("Web search response exceeds configured size limit")
            try:
                payload = json.loads(
                    response.content.decode(self._encoding(response), errors="replace")
                )
            except ValueError as exc:
                raise WebResearchError("Web search returned invalid JSON") from exc
            candidates = self._parse_cninfo_results(
                payload,
                query=search_key,
                cutoff=cutoff,
                limit=requested_limit,
                requested_years=requested_years,
                requested_title_terms=requested_title_terms,
            )
        enriched: list[WebSearchResult] = []
        fetched_count = 0
        for candidate in candidates:
            result = candidate
            if (
                fetched_count < settings.AGENT_WEB_SEARCH_FETCH_LIMIT
                and self.store.is_allowed_web_host(candidate.domain)
            ):
                try:
                    page = self.fetch(candidate.url)
                    fetched_count += 1
                    result = WebSearchResult(
                        rank=candidate.rank,
                        title=candidate.title,
                        url=page.url,
                        domain=page.domain,
                        text=_select_relevant_text(
                            page.text,
                            normalized_query,
                            max_chars=12_000,
                        ),
                        published_at=candidate.published_at,
                        fetched_at=page.fetched_at,
                        metadata={
                            **candidate.metadata,
                            **page.metadata,
                            "excerpt_only": False,
                            "document_id": page.document_id,
                        },
                    )
                except Exception:
                    pass
            enriched.append(result)
        return enriched

    def _search_announcements_by_security(
        self,
        *,
        symbol: str,
        query: str,
        cutoff: datetime,
        limit: int,
        requested_years: list[str],
        requested_title_terms: list[str],
    ) -> list[WebSearchResult]:
        code, exchange = _normalize_symbol(symbol)
        if exchange == "BJ":
            return []
        org_id = f"gssh0{code}" if exchange == "SH" else f"gssz{code}"
        endpoint = settings.AGENT_WEB_ANNOUNCEMENT_ENDPOINT
        self._validate_public_host(endpoint, require_allowlist=False)
        if not self._robots_allows(endpoint):
            return []
        params = {
            "stock": f"{code},{org_id}",
            "tabName": "fulltext",
            "pageNum": 1,
            "pageSize": min(50, max(limit * 8, 30)),
            "column": "sse" if exchange == "SH" else "szse",
            "category": "",
            "plate": "",
            "searchkey": _announcement_search_key(query),
            "secid": "",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }
        response = self.client.post(
            endpoint,
            data=params,
            headers={"Referer": "https://www.cninfo.com.cn/"},
        )
        if response.status_code >= 400:
            return []
        try:
            payload = json.loads(
                response.content.decode(self._encoding(response), errors="replace")
            )
        except ValueError:
            return []
        return self._parse_cninfo_results(
            payload,
            query=_announcement_search_key(query) or code,
            cutoff=cutoff,
            limit=limit,
            requested_years=requested_years,
            requested_title_terms=requested_title_terms,
        )

    def _parse_cninfo_results(
        self,
        payload: dict[str, Any],
        *,
        query: str,
        cutoff: datetime,
        limit: int,
        requested_years: list[str],
        requested_title_terms: list[str],
    ) -> list[WebSearchResult]:
        fetched_at = datetime.now(UTC)
        results: list[WebSearchResult] = []
        seen: set[str] = set()
        rows = payload.get("announcements") if isinstance(payload, dict) else None
        for item in rows if isinstance(rows, list) else []:
            if not isinstance(item, dict):
                continue
            announcement_id = str(item.get("announcementId") or "").strip()
            raw_title = str(item.get("announcementTitle") or "").strip()
            title = " ".join(
                BeautifulSoup(raw_title, "html.parser").get_text("", strip=True).split()
            )
            title = re.sub(r"\s*([：:，,。])\s*", r"\1", title)
            adjunct_url = str(item.get("adjunctUrl") or "").strip()
            if not title or not adjunct_url or announcement_id in seen:
                continue
            if requested_years and not any(year in title for year in requested_years):
                continue
            if requested_title_terms and not any(term in title for term in requested_title_terms):
                continue
            if requested_title_terms == ["年度报告", "年报"] and "半年度" in title:
                continue
            if "英文版" in title or "英文" in title:
                continue
            raw_url = (
                adjunct_url
                if adjunct_url.startswith("https://")
                else f"https://static.cninfo.com.cn/{adjunct_url.lstrip('/')}"
            )
            try:
                url = canonicalize_url(raw_url)
            except WebResearchError:
                continue
            timestamp = item.get("announcementTime")
            try:
                numeric_timestamp = float(timestamp)
                published_at = datetime.fromtimestamp(
                    numeric_timestamp / 1000
                    if numeric_timestamp > 10_000_000_000
                    else numeric_timestamp,
                    tz=UTC,
                )
            except (TypeError, ValueError, OSError):
                published_at = None
            if published_at is not None and published_at > cutoff:
                continue
            host = (urlsplit(url).hostname or "").casefold()
            if not host:
                continue
            sec_name = str(item.get("secName") or "").strip()
            snippet = f"{sec_name}公告：{title}" if sec_name else f"公告：{title}"
            seen.add(announcement_id)
            results.append(
                WebSearchResult(
                    rank=len(results) + 1,
                    title=title[:500],
                    url=url,
                    domain=host,
                    text=snippet[:2_000],
                    published_at=published_at,
                    fetched_at=fetched_at,
                    metadata={
                        "provider": "cninfo",
                        "query": query,
                        "excerpt_only": True,
                        "search_rank": len(results) + 1,
                        "announcement_id": announcement_id,
                        "security_code": str(item.get("secCode") or "").strip(),
                        "security_name": sec_name,
                    },
                )
            )
            if len(results) >= limit:
                break
        return results

    def _fetch_with_redirects(self, url: str) -> tuple[str, httpx.Response]:
        current = url
        for redirect_index in range(settings.AGENT_WEB_MAX_REDIRECTS + 1):
            self._validate_host(current)
            if not self._robots_allows(current):
                raise WebResearchError("robots.txt disallows fetching this URL")
            response = self.client.get(current)
            if response.status_code in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                if not location:
                    raise WebResearchError("Redirect response has no Location header")
                current = canonicalize_url(str(httpx.URL(current).join(location)))
                if redirect_index >= settings.AGENT_WEB_MAX_REDIRECTS:
                    raise WebResearchError("Too many redirects")
                continue
            if response.status_code >= 400:
                raise WebResearchError(f"Web request failed with HTTP {response.status_code}")
            return current, response
        raise WebResearchError("Too many redirects")

    def _validate_host(self, url: str) -> None:
        self._validate_public_host(url, require_allowlist=True)

    def _validate_public_host(self, url: str, *, require_allowlist: bool) -> None:
        parsed = urlsplit(url)
        if parsed.scheme.casefold() != "https":
            raise WebResearchError("Only HTTPS web retrieval is allowed")
        host = parsed.hostname or ""
        if require_allowlist and not self.store.is_allowed_web_host(host):
            raise WebResearchError(f"Host is not in the web allowlist: {host}")
        try:
            addresses = self.resolver(host, parsed.port or 443)
        except OSError as exc:
            raise WebResearchError(f"Failed to resolve web host: {host}") from exc
        for address in addresses:
            ip = ipaddress.ip_address(str(address[4][0]))
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                raise WebResearchError("Web host resolves to a non-public address")

    def _robots_allows(self, url: str) -> bool:
        parsed = urlsplit(url)
        robots_url = urlunsplit((parsed.scheme, parsed.netloc, "/robots.txt", "", ""))
        try:
            response = self.client.get(robots_url)
            if response.status_code >= 400:
                return True
            parser = RobotFileParser()
            parser.set_url(robots_url)
            parser.parse(response.text.splitlines())
            return parser.can_fetch("OpenFinance-Agent", url)
        except Exception:
            return True

    @staticmethod
    def _encoding(response: httpx.Response) -> str:
        return response.encoding or "utf-8"

    @staticmethod
    def _extract_html(body: bytes, url: str) -> tuple[str, str]:
        html = body.decode("utf-8", errors="replace")
        title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
        title = re.sub(r"\s+", " ", title_match.group(1)).strip() if title_match else url
        try:
            import trafilatura

            text = trafilatura.extract(
                html,
                url=url,
                include_links=False,
                include_tables=True,
                favor_recall=True,
            )
            metadata = trafilatura.extract_metadata(html)
            if metadata and metadata.title:
                title = metadata.title
        except Exception:
            text = re.sub(r"<script.*?</script>|<style.*?</style>", " ", html, flags=re.IGNORECASE | re.DOTALL)
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text).strip()
        return title, text or ""

    @staticmethod
    def _extract_pdf(body: bytes, url: str) -> tuple[str, str]:
        try:
            import fitz

            document = fitz.open(stream=body, filetype="pdf")
            text = "\n\n".join(page.get_text("text") for page in document)
            title = document.metadata.get("title") or url
            return title, text
        except Exception as exc:
            raise WebResearchError(f"Failed to extract PDF: {exc}") from exc


def _cninfo_search_key(query: str) -> str:
    cleaned = re.sub(
        r"请|帮我|查询|查一下|分析|研究|看看|一下|最近|近期|目前|现在|今天|今年|"
        r"怎么样|如何|为什么|是否|以及|相关|一个|这份|那只|这只|的|吗|呢|[，。！？,.!?]",
        " ",
        query,
    )
    tokens = [
        token
        for token in re.split(r"\s+", cleaned)
        if token
        and token not in SEARCH_GENERIC_TERMS
        and not re.fullmatch(r"(?:19|20)\d{2}年?", token)
    ]
    if not tokens:
        return ""
    cjk_tokens = [token for token in tokens if re.search(r"[\u4e00-\u9fff]", token) and len(token) >= 2]
    if cjk_tokens:
        base = max(cjk_tokens, key=len)[:40]
    else:
        symbol_tokens = [token for token in tokens if re.search(r"\d{6}", token)]
        base = (
            re.search(r"\d{6}", symbol_tokens[0]).group(0)  # type: ignore[union-attr]
            if symbol_tokens
            else tokens[0][:40]
        )
    years = "".join(dict.fromkeys(re.findall(r"(?<!\d)(20\d{2})(?!\d)", query)))
    intent = ""
    if "半年报" in query or "半年度报告" in query:
        intent = "半年报"
    elif "年报" in query or "年度报告" in query or "财报" in query:
        intent = "年报"
    elif "公告" in query:
        intent = "公告"
    return f"{base}{years}{intent}"[:60]


def _requested_title_terms(query: str) -> list[str]:
    if "半年度报告" in query or "半年报" in query:
        return ["半年度报告", "半年报"]
    if "年度报告" in query or "年报" in query:
        return ["年度报告", "年报"]
    if "公告" in query:
        return ["公告"]
    if "财报" in query or "财务" in query:
        return ["报告", "财务", "业绩"]
    return []


def _announcement_search_key(query: str) -> str:
    if "半年报" in query or "半年度报告" in query:
        return "半年报"
    if "年报" in query or "年度报告" in query or "财报" in query:
        return "年报"
    if "公告" in query:
        return "公告"
    for term in ("分红", "利润", "营收", "现金流", "业绩", "重组", "回购", "减持", "增持"):
        if term in query:
            return term
    return ""


def _normalize_symbol(value: str) -> tuple[str, str]:
    normalized = value.strip().upper()
    if "." in normalized:
        code, exchange = normalized.split(".", 1)
    else:
        code = normalized
        exchange = "SH" if code.startswith(("5", "6")) else "SZ"
    code = code.zfill(6)
    if len(code) != 6 or not code.isdigit() or exchange not in {"SH", "SZ", "BJ"}:
        raise WebResearchError(f"Invalid security symbol: {value}")
    return code, exchange


def _select_relevant_text(text: str, query: str, *, max_chars: int) -> str:
    terms = [
        token
        for token in re.split(r"\s+|[，。！？,.!?：:；;]", query)
        if len(token) >= 2
        and token not in SEARCH_GENERIC_TERMS
        and not re.fullmatch(r"(?:19|20)\d{2}年?", token)
        and not re.fullmatch(r"\d{6}(?:\.(?:SH|SZ|BJ))?", token.upper())
    ]
    windows: list[tuple[int, int]] = []
    for term in terms:
        start = 0
        found_for_term = 0
        while found_for_term < 2:
            position = text.find(term, start)
            if position < 0:
                break
            windows.append((max(0, position - 1_200), min(len(text), position + 2_800)))
            start = position + len(term)
            found_for_term += 1
    if not windows:
        return text[:max_chars]
    windows.sort()
    merged: list[tuple[int, int]] = []
    for start, end in windows:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    chunks: list[str] = []
    used = 0
    for start, end in merged:
        chunk = text[start:end].strip()
        if not chunk:
            continue
        remaining = max_chars - used
        if remaining <= 0:
            break
        chunks.append(chunk[:remaining])
        used += len(chunks[-1])
    return "\n\n".join(chunks) if chunks else text[:max_chars]

