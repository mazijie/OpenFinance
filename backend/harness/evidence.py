"""Unified evidence retrieval across internal sources and allowlisted web pages."""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

from backend.config import settings
from backend.schemas.harness import (
    AgentCitation,
    EvidenceDocument,
    EvidencePack,
    EvidenceSourceType,
)
from backend.tools.annual_reports import search_annual_reports
from backend.tools.market import get_market_data
from backend.tools.rag import search_rag


class EvidenceService:
    """Turn heterogeneous tools into one citable evidence contract."""

    def __init__(
        self,
        *,
        market_search=None,
        news_search=None,
        annual_search=None,
        web_service=None,
    ) -> None:
        self.market_search = market_search or get_market_data
        self.news_search = news_search or search_rag
        self.annual_search = annual_search or search_annual_reports
        self.web_service = web_service

    def search(
        self,
        *,
        query: str,
        sources: Iterable[str],
        symbol: str | None = None,
        report_year: int | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        as_of: datetime | None = None,
        limit: int = 8,
        section_path: str | None = None,
        content_types: list[str] | None = None,
        page_start: int | None = None,
        page_end: int | None = None,
        web_urls: list[str] | None = None,
        web_fallback: bool = False,
    ) -> EvidencePack:
        cutoff = as_of or datetime.now(UTC)
        start = start_date or (cutoff - timedelta(days=180))
        end = end_date or cutoff
        selected = {str(source).strip().casefold() for source in sources if str(source).strip()}
        if not selected:
            selected = {"market", "news", "annual_report"} if symbol else {"news"}
        documents: list[EvidenceDocument] = []
        missing: list[str] = []
        jobs: dict[Any, str] = {}
        source_document_counts: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="evidence") as pool:
            if "market" in selected and symbol:
                jobs[pool.submit(self._market, symbol, start, end, cutoff, limit)] = "market"
            if "news" in selected:
                jobs[pool.submit(self._news, query, symbol, start, end, cutoff, limit)] = "news"
            if "annual_report" in selected:
                jobs[pool.submit(self._annual, query, symbol, report_year, cutoff, limit, section_path, content_types, page_start, page_end)] = "annual_report"
            if "web" in selected and web_urls and self.web_service is not None:
                jobs[pool.submit(self._web_urls, web_urls)] = "web"
            elif "web" in selected and self.web_service is not None:
                jobs[pool.submit(self._web_search, query, symbol, cutoff, limit)] = "web"
            elif "web" in selected:
                missing.append("web search service is unavailable")
            for future in as_completed(jobs):
                source = jobs[future]
                try:
                    source_documents = future.result()
                    if source == "news" and isinstance(source_documents, tuple):
                        source_documents, warnings = source_documents
                        missing.extend(f"news: {warning}" for warning in warnings)
                    source_document_counts[source] = len(source_documents)
                    documents.extend(source_documents)
                except Exception as exc:
                    source_document_counts[source] = 0
                    missing.append(f"{source}: {exc}")
            internal_sources = {
                source
                for source in selected
                if source in {"market", "news", "annual_report"}
            }
            incomplete_internal_sources = [
                source
                for source in internal_sources
                if source_document_counts.get(source, 0) == 0
            ]
            if (
                web_fallback
                and (not documents or incomplete_internal_sources)
                and "web" not in selected
                and self.web_service is not None
                and settings.AGENT_WEB_SEARCH_ENABLED
            ):
                selected.add("web")
                try:
                    documents.extend(self._web_search(query, symbol, cutoff, limit))
                except Exception as exc:
                    missing.append(f"web fallback: {exc}")
        deduplicated: list[EvidenceDocument] = []
        seen: set[str] = set()
        for document in documents:
            key = f"{document.source_type}|{document.source_id}|{document.locator or ''}"
            if key in seen:
                continue
            seen.add(key)
            deduplicated.append(document)
        return EvidencePack(
            documents=deduplicated[: max(1, limit * max(1, len(selected)))],
            missing=missing,
            sources=[source for source in ("market", "news", "annual_report", "web") if source in selected],
            as_of=cutoff,
        )

    def _market(self, symbol: str, start: datetime, end: datetime, cutoff: datetime, limit: int) -> list[EvidenceDocument]:
        result = self.market_search(
            symbol=symbol,
            start_date=start.date(),
            end_date=end.date(),
            as_of=cutoff,
        )
        if not result.get("success") or not result.get("data"):
            return []
        data = result["data"]
        source_id = f"{data.get('symbol') or symbol}:{data.get('end_date') or end.date().isoformat()}"
        text = (
            f"{data.get('symbol') or symbol} 行情摘要\n"
            f"区间：{data.get('start_date')} 至 {data.get('end_date')}\n"
            f"首收盘：{data.get('first_close')}；末收盘：{data.get('last_close')}\n"
            f"区间收益：{data.get('interval_return')}；年化波动率：{data.get('annualized_volatility')}\n"
            f"数据说明：{data.get('data_gap_note') or '无'}"
        )
        return [
            EvidenceDocument(
                evidence_id=_evidence_id("market", source_id, text),
                source_type="market",
                source_id=source_id,
                title=f"{data.get('symbol') or symbol} 行情证据",
                text=text,
                published_at=cutoff,
                locator=str(data.get("end_date") or ""),
                metadata={"summary": data},
            )
        ]

    def _news(self, query: str, symbol: str | None, start: datetime, end: datetime, cutoff: datetime, limit: int) -> tuple[list[EvidenceDocument], list[str]]:
        result = self.news_search(
            question=query,
            symbol=symbol,
            start_date=start.date(),
            end_date=end.date(),
            as_of=cutoff,
            limit=limit,
        )
        data = result.get("data") or {}
        if not result.get("success") or data.get("error_stage"):
            raise RuntimeError(data.get("message") or result.get("error") or "新闻检索不可用")
        documents: list[EvidenceDocument] = []
        for article in data.get("articles") or []:
            chunks = article.get("evidence_chunks") or []
            text = "\n".join(str(chunk.get("chunk_text") or "") for chunk in chunks)
            if not text:
                continue
            source_id = str(article.get("news_id") or "")
            documents.append(
                EvidenceDocument(
                    evidence_id=_evidence_id("news", source_id, text),
                    source_type="news",
                    source_id=source_id,
                    title=str(article.get("title") or "新闻"),
                    text=text[:12_000],
                    url=str(article.get("url") or "") or None,
                    published_at=article.get("published_at"),
                    locator=str(article.get("content_hash") or ""),
                    score=article.get("final_score"),
                    metadata={"provider": article.get("provider"), "source_name": article.get("source_name")},
                )
            )
        return documents, list(data.get("warnings") or [])

    def _annual(
        self,
        query: str,
        symbol: str | None,
        report_year: int | None,
        cutoff: datetime,
        limit: int,
        section_path: str | None = None,
        content_types: list[str] | None = None,
        page_start: int | None = None,
        page_end: int | None = None,
    ) -> list[EvidenceDocument]:
        result = self.annual_search(
            question=query,
            stock_code=symbol,
            report_year=report_year,
            as_of=cutoff,
            section_path=section_path,
            content_types=content_types,
            page_start=page_start,
            page_end=page_end,
            limit=limit,
        )
        data = result.get("data") or {}
        if not result.get("success"):
            raise RuntimeError(result.get("error", "annual-report retrieval failed"))
        documents: list[EvidenceDocument] = []
        for article in data.get("articles") or []:
            chunks = article.get("evidence_chunks") or []
            text = "\n".join(str(chunk.get("chunk_text") or "") for chunk in chunks)
            if not text:
                continue
            source_id = str(article.get("news_id") or "")
            locator = str(article.get("title") or "")
            documents.append(
                EvidenceDocument(
                    evidence_id=_evidence_id("annual_report", source_id, text),
                    source_type="annual_report",
                    source_id=source_id,
                    title=str(article.get("title") or "年报"),
                    text=text[:12_000],
                    url=str(article.get("url") or "") or None,
                    published_at=article.get("published_at"),
                    locator=locator,
                    score=article.get("final_score"),
                    metadata={
                        "provider": article.get("provider"),
                        "source_name": article.get("source_name"),
                        **dict(article.get("metadata") or {}),
                    },
                )
            )
        return documents

    def _web_urls(self, urls: list[str]) -> list[EvidenceDocument]:
        documents: list[EvidenceDocument] = []
        for url in urls:
            page = self.web_service.fetch(url)
            documents.append(
                EvidenceDocument(
                    evidence_id=_evidence_id("web", page.canonical_url, page.content_hash),
                    source_type="web",
                    source_id=page.document_id,
                    title=page.title,
                    text=page.text[:12_000],
                    url=page.url,
                    published_at=None,
                    fetched_at=page.fetched_at,
                    locator=page.canonical_url,
                    metadata=page.metadata,
                )
            )
        return documents

    def _web_search(
        self,
        query: str,
        symbol: str | None,
        cutoff: datetime,
        limit: int,
    ) -> list[EvidenceDocument]:
        search_query = f"{query} {symbol or ''}".strip()
        results = self.web_service.search(
            search_query,
            symbol=symbol,
            limit=limit,
            as_of=cutoff,
        )
        documents: list[EvidenceDocument] = []
        for result in results:
            documents.append(
                EvidenceDocument(
                    evidence_id=_evidence_id("web", result.url, result.text),
                    source_type="web",
                    source_id=result.metadata.get("document_id") or result.url,
                    title=result.title,
                    text=result.text[:12_000],
                    url=result.url,
                    published_at=result.published_at,
                    fetched_at=result.fetched_at,
                    locator=f"search result #{result.rank}",
                    metadata=result.metadata,
                )
            )
        return documents


def assign_citations(documents: Iterable[EvidenceDocument]) -> tuple[list[EvidenceDocument], list[AgentCitation]]:
    selected: list[EvidenceDocument] = []
    citations: list[AgentCitation] = []
    seen: set[str] = set()
    for document in documents:
        if document.evidence_id in seen:
            continue
        seen.add(document.evidence_id)
        selected.append(document)
        citations.append(
            AgentCitation(
                citation_id=len(citations) + 1,
                source_type=document.source_type,
                source_id=document.source_id,
                title=document.title,
                url=document.url,
                published_at=document.published_at,
                fetched_at=document.fetched_at,
                locator=document.locator,
                excerpt=document.text[:1200],
                report_id=document.metadata.get("report_id"),
                page_start=document.metadata.get("page_start"),
                page_end=document.metadata.get("page_end"),
                section_path=document.metadata.get("section_path"),
                region_ids=document.metadata.get("region_ids", []),
            )
        )
    return selected, citations


def _evidence_id(source_type: str, source_id: str, text: str) -> str:
    return hashlib.sha256(f"{source_type}|{source_id}|{text}".encode("utf-8")).hexdigest()

