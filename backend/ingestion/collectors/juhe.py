"""Juhe Chinese finance news collector."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from backend.config import settings
from backend.logging_utils import get_logger
from backend.schemas.news import ProviderNewsArticle, ProviderNewsResponse
from backend.utils.time import parse_possible_datetime

logger = get_logger(__name__)

FINANCE_CATEGORY = "caijing"


class JuheCollectorError(Exception):
    """Raised when Juhe collection fails."""


class JuheCollector:
    """Async client wrapper around the Juhe Toutiao APIs."""

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._client = client

    async def collect_news(
        self,
        *,
        existing_external_ids: set[str] | None = None,
        limit: int | None = None,
    ) -> ProviderNewsResponse:
        """Collect finance news list, then fetch details only for unseen items."""

        if not settings.JUHE_NEWS_API_KEY:
            raise JuheCollectorError("JUHE_NEWS_API_KEY is not configured")

        known_ids = existing_external_ids or set()
        target_count = max(1, limit or settings.NEWS_INGEST_LIMIT)
        page_size = max(1, min(target_count, settings.JUHE_NEWS_LIST_PAGE_SIZE))
        max_pages = max(1, settings.JUHE_NEWS_MAX_PAGES)

        logger.info(
            "开始采集聚合财经新闻列表",
            event="juhe_collect_started",
            extra_fields={
                "category": FINANCE_CATEGORY,
                "target_count": target_count,
                "page_size": page_size,
                "max_pages": max_pages,
                "known_ids": len(known_ids),
            },
        )

        timeout = httpx.Timeout(
            connect=settings.NEWS_HTTP_TIMEOUT_SECONDS,
            read=settings.NEWS_HTTP_TIMEOUT_SECONDS,
            write=settings.NEWS_HTTP_TIMEOUT_SECONDS,
            pool=settings.NEWS_HTTP_TIMEOUT_SECONDS,
        )

        if self._client is not None:
            list_payloads, detail_payloads, new_rows = await self._collect_pages(
                client=self._client,
                known_ids=known_ids,
                target_count=target_count,
                page_size=page_size,
                max_pages=max_pages,
            )
        else:
            async with httpx.AsyncClient(timeout=timeout) as client:
                list_payloads, detail_payloads, new_rows = await self._collect_pages(
                    client=client,
                    known_ids=known_ids,
                    target_count=target_count,
                    page_size=page_size,
                    max_pages=max_pages,
                )

        articles = [self._build_article(row, detail_payloads.get(str(row.get("uniquekey") or "").strip())) for row in new_rows]
        raw_response = {
            "provider": "juhe",
            "category": FINANCE_CATEGORY,
            "list_response": list_payloads[0] if list_payloads else {},
            "list_responses": list_payloads,
            "detail_responses": detail_payloads,
            "articles": [article.raw for article in articles],
        }

        logger.info(
            "聚合财经新闻采集完成",
            event="juhe_collect_completed",
            extra_fields={
                "category": FINANCE_CATEGORY,
                "list_count": sum(len(self._extract_list_rows(payload)) for payload in list_payloads),
                "new_count": len(articles),
                "pages_scanned": len(list_payloads),
                "target_count": target_count,
                "known_ids": len(known_ids),
            },
        )
        return ProviderNewsResponse(articles=articles, raw_response=raw_response)

    async def _collect_pages(
        self,
        *,
        client: httpx.AsyncClient,
        known_ids: set[str],
        target_count: int,
        page_size: int,
        max_pages: int,
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], list[dict[str, Any]]]:
        """Page through list results until the target number of valid articles is reached."""

        list_payloads: list[dict[str, Any]] = []
        detail_payloads: dict[str, dict[str, Any]] = {}
        accepted_rows: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for page_offset in range(max_pages):
            payload = await self._request_with_retry(
                client,
                settings.JUHE_NEWS_LIST_URL,
                params={
                    "key": settings.JUHE_NEWS_API_KEY,
                    "type": FINANCE_CATEGORY,
                    "page": settings.JUHE_NEWS_DEFAULT_PAGE + page_offset,
                    "page_size": page_size,
                    "is_filter": settings.JUHE_NEWS_DEFAULT_IS_FILTER,
                },
            )
            list_payloads.append(payload)
            rows = self._extract_list_rows(payload)
            if not rows:
                break

            page_details, page_rows = await self._fetch_new_details(
                client=client,
                rows=rows,
                known_ids=known_ids,
                batch_ids=seen_ids,
                max_articles=target_count - len(accepted_rows),
            )
            detail_payloads.update(page_details)
            accepted_rows.extend(page_rows)
            if len(accepted_rows) >= target_count or len(rows) < page_size:
                break

        return list_payloads, detail_payloads, accepted_rows

    async def _fetch_new_details(
        self,
        *,
        client: httpx.AsyncClient,
        rows: list[dict[str, Any]],
        known_ids: set[str],
        batch_ids: set[str],
        max_articles: int,
    ) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
        detail_payloads: dict[str, dict[str, Any]] = {}
        new_rows: list[dict[str, Any]] = []

        for row in rows:
            if len(new_rows) >= max_articles:
                break
            uniquekey = str(row.get("uniquekey") or "").strip()
            if not uniquekey:
                logger.warning(
                    "列表项缺少 uniquekey，跳过详情拉取",
                    event="juhe_missing_uniquekey",
                    extra_fields={"title": row.get("title")},
                )
                continue
            if uniquekey in known_ids:
                continue
            if uniquekey in batch_ids:
                logger.info(
                    "列表中发现重复新闻，跳过重复详情请求",
                    event="juhe_duplicate_list_item",
                    extra_fields={"uniquekey": uniquekey},
                )
                continue
            batch_ids.add(uniquekey)

            if not settings.JUHE_NEWS_FETCH_DETAILS or not self._has_content(row):
                logger.info(
                    "新闻列表项不含正文，跳过采集",
                    event="juhe_article_skipped_without_content",
                    extra_fields={"uniquekey": uniquekey},
                )
                continue

            try:
                detail = await self._fetch_detail(client, uniquekey)
            except Exception as exc:
                logger.warning(
                    "聚合新闻详情拉取失败，跳过该新闻",
                    event="juhe_detail_failed",
                    extra_fields={"uniquekey": uniquekey, "error": str(exc) or repr(exc)},
                )
                continue

            if not self._has_nonempty_content(detail):
                logger.info(
                    "新闻详情正文为空，跳过采集",
                    event="juhe_article_skipped_empty_content",
                    extra_fields={"uniquekey": uniquekey},
                )
                continue

            detail_payloads[uniquekey] = detail
            new_rows.append(row)

        return detail_payloads, new_rows

    def _has_content(self, row: dict[str, Any]) -> bool:
        """Return whether a list row declares that a detail body is available."""

        value = row.get("is_content", row.get("has_content"))
        if isinstance(value, bool):
            return value
        return str(value or "").strip().casefold() in {"1", "true", "yes"}

    def _has_nonempty_content(self, detail: dict[str, Any]) -> bool:
        return bool(str(detail.get("content") or "").strip())

    async def _fetch_detail(self, client: httpx.AsyncClient, uniquekey: str) -> dict[str, Any]:
        payload = await self._request_with_retry(
            client,
            settings.JUHE_NEWS_DETAIL_URL,
            params={"key": settings.JUHE_NEWS_API_KEY, "uniquekey": uniquekey},
        )
        result = payload.get("result")
        if isinstance(result, dict):
            if isinstance(result.get("detail"), dict):
                return result["detail"]
            return result
        return {}

    async def _request_with_retry(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        last_error: Exception | None = None

        for attempt in range(settings.NEWS_HTTP_MAX_RETRIES + 1):
            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise JuheCollectorError("Juhe returned unexpected payload type")
                if payload.get("error_code") not in (0, "0", None):
                    raise JuheCollectorError(f"Juhe API error: {payload.get('error_code')} {payload.get('reason')}")
                return payload
            except (httpx.TimeoutException, httpx.HTTPStatusError, httpx.RequestError, JuheCollectorError) as exc:
                last_error = exc
                sleep_seconds = settings.NEWS_HTTP_RETRY_BACKOFF_SECONDS * (2**attempt)
                logger.warning(
                    "聚合新闻请求失败，准备重试",
                    event="juhe_collect_retry",
                    extra_fields={
                        "attempt": attempt + 1,
                        "url": url,
                        "error_type": type(exc).__name__,
                        "error": str(exc) or repr(exc),
                        "sleep_seconds": sleep_seconds,
                    },
                )
                if attempt >= settings.NEWS_HTTP_MAX_RETRIES:
                    break
                await asyncio.sleep(sleep_seconds)

        message = "Failed to request Juhe news"
        if last_error is not None:
            message = f"{message}: {type(last_error).__name__}: {last_error!s}"
        raise JuheCollectorError(message) from last_error

    def _extract_list_rows(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        result = payload.get("result")
        if not isinstance(result, dict):
            return []
        data = result.get("data")
        if not isinstance(data, list):
            return []
        return [row for row in data if isinstance(row, dict)]

    def _build_article(self, row: dict[str, Any], detail: dict[str, Any] | None) -> ProviderNewsArticle:
        merged = dict(row)
        if isinstance(detail, dict):
            merged.update({key: value for key, value in detail.items() if value not in (None, "")})

        published_at = parse_possible_datetime(str(merged.get("date") or row.get("date") or ""))
        source_name = str(merged.get("author_name") or merged.get("author") or merged.get("category") or "聚合数据")
        content = merged.get("content")
        summary = content or merged.get("abstract") or merged.get("title")
        url = str(merged.get("url") or row.get("url") or "")

        return ProviderNewsArticle(
            external_news_id=str(merged.get("uniquekey") or row.get("uniquekey") or ""),
            category=str(merged.get("category") or row.get("category") or FINANCE_CATEGORY),
            title=str(merged.get("title") or row.get("title") or ""),
            url=url,
            source_name=source_name,
            language="zh-cn",
            published_at=published_at,
            summary=str(summary) if summary else None,
            content=str(content) if content else None,
            cover_image_url=str(merged.get("thumbnail_pic_s") or row.get("thumbnail_pic_s") or "") or None,
            raw=merged,
        )
