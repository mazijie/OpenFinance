"""Collect the latest Caixin Data Plus news summaries."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from backend.config import settings
from backend.logging_utils import get_logger
from backend.schemas.news import ProviderNewsArticle, ProviderNewsResponse

logger = get_logger(__name__)


class CaixinCollectorError(Exception):
    """Raised when the Caixin feed cannot be read."""


def canonical_news_url(value: str) -> str:
    """Remove tracking parameters while retaining parameters that identify an article."""
    parsed = urlsplit(value.strip())
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValueError("invalid news URL")
    query = sorted((key, val) for key, val in parse_qsl(parsed.query, keep_blank_values=True)
                   if not key.lower().startswith("utm_") and key.lower() != "cxapp_link")
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, urlencode(query), ""))


class CaixinCollector:
    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._client = client

    async def collect_news(self, *, existing_external_ids: set[str] | None = None,
                           limit: int | None = None) -> ProviderNewsResponse:
        if self._client is None:
            async with httpx.AsyncClient(timeout=settings.NEWS_HTTP_TIMEOUT_SECONDS) as client:
                payload = await self._request(client)
        else:
            payload = await self._request(self._client)

        rows = payload["data"]["data"]
        known = existing_external_ids or set()
        seen: set[str] = set()
        articles: list[ProviderNewsArticle] = []
        invalid = duplicates = 0
        # ``limit`` remains accepted for callers of the previous interface.
        for row in rows:
            try:
                if not isinstance(row, dict):
                    raise ValueError("invalid row")
                title = str(row.get("title") or "").strip()
                summary = str(row.get("summary") or "").strip()
                url = canonical_news_url(str(row.get("url") or ""))
                timestamp = row.get("time")
                if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)) or timestamp <= 0:
                    raise ValueError("invalid publication time")
                published_at = datetime.fromtimestamp(timestamp, UTC)
                if not title or not summary:
                    raise ValueError("missing title or summary")
            except (ValueError, TypeError, OverflowError, OSError):
                invalid += 1
                continue
            if url in known or url in seen:
                duplicates += 1
                continue
            seen.add(url)
            normalized = {**row, "external_news_id": url, "url": url,
                          "published": published_at.isoformat(), "source_name": "财新",
                          "category": row.get("tag"), "cover_image_url": row.get("pic")}
            articles.append(ProviderNewsArticle(external_news_id=url, title=title, url=url,
                category=row.get("tag"), source_name="财新", language="zh-cn",
                published_at=published_at, summary=summary, raw=normalized))

        articles.sort(key=lambda item: item.published_at)
        logger.info("财新新闻采集完成", event="caixin_collect_completed", extra_fields={
            "fetched_count": len(rows), "invalid_count": invalid, "duplicate_count": duplicates,
            "accepted_count": len(articles),
            "latest_page_full": len(rows) >= 100,
        })
        return ProviderNewsResponse(articles=articles, raw_response={"provider": "caixin",
            "list_response": payload, "articles": [article.raw for article in articles]})

    async def _request(self, client: httpx.AsyncClient) -> dict[str, Any]:
        error: Exception | None = None
        for attempt in range(max(0, settings.NEWS_HTTP_MAX_RETRIES) + 1):
            try:
                response = await client.get(settings.CAIXIN_NEWS_URL,
                    params={"pageNum": "1", "pageSize": "100", "showLabels": "true"},
                    headers={"User-Agent": "Mozilla/5.0", "Referer": "https://cxdata.caixin.com/index/newsTab?tab=latest"},
                    timeout=settings.NEWS_HTTP_TIMEOUT_SECONDS)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or payload.get("success") is not True or not isinstance(payload.get("data"), dict) or not isinstance(payload["data"].get("data"), list):
                    raise CaixinCollectorError("Unexpected Caixin response structure")
                return payload
            except (httpx.HTTPError, ValueError, CaixinCollectorError) as exc:
                error = exc
                if attempt >= settings.NEWS_HTTP_MAX_RETRIES:
                    break
                logger.warning("财新新闻请求失败，准备重试", event="caixin_collect_retry",
                               extra_fields={"attempt": attempt + 1, "error": str(exc)})
                await asyncio.sleep(settings.NEWS_HTTP_RETRY_BACKOFF_SECONDS * (2 ** attempt))
        raise CaixinCollectorError(f"Failed to request Caixin news: {error}") from error
