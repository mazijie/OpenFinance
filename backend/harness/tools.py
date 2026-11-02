"""Tool registry and typed tools exposed to the research agent."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Literal

from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel, Field, model_validator

from backend.config import settings
from backend.harness.evidence import EvidenceService
from backend.schemas.harness import EvidenceDocument
from backend.services.realtime_quote_service import RealtimeQuoteService
from backend.harness.policy import PolicyGuard
from backend.harness.store import HarnessStore
from backend.harness.web import WebResearchService
from backend.monitoring.store import MonitorStore
from backend.schemas.monitor import MonitorRunRequest, WatchlistItemCreate, WatchlistItemPatch
from backend.services.finance_service import FinanceService
from backend.services.investment_monitor_service import InvestmentMonitorService
from backend.services.watchlist_service import WatchlistService


@dataclass(slots=True)
class ToolContext:
    thread_id: str
    turn_id: str
    run_kind: str
    as_of: datetime
    store: HarnessStore
    evidence_service: EvidenceService
    web_service: WebResearchService


class ResearchSearchInput(BaseModel):
    query: str
    sources: list[Literal["market", "news", "annual_report", "web"]] = Field(
        default_factory=lambda: ["market", "news", "annual_report"]
    )
    symbol: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    limit: int = Field(default=8, ge=1, le=20)
    report_year: int | None = Field(default=None, ge=2000, le=2100)
    section_path: str | None = None
    content_types: list[str] = Field(default_factory=list)
    page_start: int | None = Field(default=None, ge=1)
    page_end: int | None = Field(default=None, ge=1)
    web_urls: list[str] = Field(default_factory=list)


class AnnualReportSearchInput(BaseModel):
    query: str = Field(min_length=1)
    stock_code: str | None = None
    report_year: int | None = Field(default=None, ge=2000, le=2100)
    section_path: str | None = None
    content_types: list[str] = Field(default_factory=list)
    page_start: int | None = Field(default=None, ge=1)
    page_end: int | None = Field(default=None, ge=1)
    as_of: datetime | None = None
    limit: int = Field(default=8, ge=1, le=20)

    @model_validator(mode="after")
    def validate_page_range(self) -> "AnnualReportSearchInput":
        if self.page_start is not None and self.page_end is not None and self.page_start > self.page_end:
            raise ValueError("page_start must be <= page_end")
        return self


class ResolveSecurityInput(BaseModel):
    query: str


class RealtimeQuoteInput(BaseModel):
    symbol: str = Field(description="股票代码，例如 600519 或 600519.SH；优先包含交易所后缀")


class WebFetchInput(BaseModel):
    url: str


class AddWatchlistInput(BaseModel):
    symbol: str
    price_move: float = 0.03
    volume_ratio: float = 1.8
    note: str | None = None


class UpdateWatchlistInput(BaseModel):
    symbol: str
    enabled: bool | None = None
    price_move: float | None = None
    volume_ratio: float | None = None
    note: str | None = None


class DeleteWatchlistInput(BaseModel):
    symbol: str


class SubmitMonitorInput(BaseModel):
    trading_date: date | None = None
    cutoff_at: datetime | None = None


class MonitorLookupInput(BaseModel):
    run_id: str | None = None
    report_id: str | None = None
    symbol: str | None = None
    limit: int = Field(default=10, ge=1, le=50)


class ToolRegistry:
    """Build one allowlisted tool set for a harness run."""

    def __init__(
        self,
        *,
        context: ToolContext,
        watchlist_service: WatchlistService | None = None,
        monitor_store: MonitorStore | None = None,
        finance_service: FinanceService | None = None,
    ) -> None:
        self.context = context
        self.watchlist_service = watchlist_service or WatchlistService()
        self.monitor_store = monitor_store or MonitorStore()
        self.finance_service = finance_service or FinanceService()
        self._tools = self._build_tools()

    @property
    def tools(self) -> list[BaseTool]:
        return [tool for tool in self._tools if PolicyGuard.allows(self.context.run_kind, tool.name)]

    @property
    def tool_map(self) -> dict[str, BaseTool]:
        return {tool.name: tool for tool in self.tools}

    def _build_tools(self) -> list[BaseTool]:
        def research_search(**kwargs: Any) -> str:
            request = ResearchSearchInput.model_validate(kwargs)
            if self.context.run_kind == "monitor":
                request.sources = [source for source in request.sources if source != "web"]
                request.web_urls = []
            allowed = PolicyGuard.allowed_sources(self.context.run_kind)
            request.sources = [source for source in request.sources if source in allowed]
            pack = self.context.evidence_service.search(
                query=request.query,
                sources=request.sources,
                symbol=request.symbol,
                report_year=request.report_year,
                start_date=request.start_date,
                end_date=request.end_date,
                as_of=self.context.as_of,
                limit=request.limit,
                section_path=request.section_path,
                content_types=request.content_types or None,
                page_start=request.page_start,
                page_end=request.page_end,
                web_urls=request.web_urls,
                web_fallback=self.context.run_kind == "interactive",
            )
            return _dump(
                {
                    "documents": [doc.model_dump(mode="json") for doc in pack.documents],
                    "missing": pack.missing,
                    "sources": pack.sources,
                    "as_of": pack.as_of.isoformat() if pack.as_of else None,
                }
            )

        def annual_report_search(**kwargs: Any) -> str:
            request = AnnualReportSearchInput.model_validate(kwargs)
            requested_cutoff = request.as_of
            if requested_cutoff is not None and requested_cutoff.tzinfo is None:
                requested_cutoff = requested_cutoff.replace(tzinfo=UTC)
            cutoff = min(requested_cutoff, self.context.as_of) if requested_cutoff is not None else self.context.as_of
            pack = self.context.evidence_service.search(
                query=request.query,
                sources=["annual_report"],
                symbol=request.stock_code,
                report_year=request.report_year,
                as_of=cutoff,
                limit=request.limit,
                section_path=request.section_path,
                content_types=request.content_types or None,
                page_start=request.page_start,
                page_end=request.page_end,
            )
            return _dump(
                {
                    "results": [
                        {
                            "report_id": doc.source_id,
                            "page_start": doc.metadata["page_start"],
                            "page_end": doc.metadata["page_end"],
                            "section_path": doc.metadata["section_path"],
                            "region_ids": doc.metadata["region_ids"],
                            "content_type": doc.metadata["content_type"],
                            "chunk_text": doc.text,
                            "source_pdf_url": doc.url,
                        }
                        for doc in pack.documents
                    ],
                    "documents": [doc.model_dump(mode="json") for doc in pack.documents],
                    "missing": pack.missing,
                    "sources": pack.sources,
                    "as_of": pack.as_of.isoformat() if pack.as_of else None,
                }
            )

        def resolve_security(query: str) -> str:
            normalized = query.strip().upper()
            options = self.finance_service.list_security_options().items
            matches = [
                option.model_dump()
                for option in options
                if option.symbol.upper() == normalized or normalized in option.name.upper()
            ][:10]
            return _dump({"query": query, "matches": matches})

        def list_watchlist() -> str:
            return _dump({"items": [item.model_dump(mode="json") for item in self.watchlist_service.list()]})

        def realtime_quote(symbol: str) -> str:
            try:
                result = RealtimeQuoteService().lookup(symbol, as_of=self.context.as_of)
                quote = result["quote"]
                if not quote or quote["status"] != "fresh":
                    status = quote["status"] if quote else "unavailable"
                    document = EvidenceDocument(
                        evidence_id=f"realtime-status:{symbol}:{self.context.as_of.isoformat()}",
                        source_type="market", source_id=symbol,
                        title=f"{symbol} 实时行情状态", text=f"{symbol} 没有更新（状态：{status}）。",
                        published_at=self.context.as_of, metadata={"status": status})
                    return _dump({"documents": [document.model_dump(mode="json")], "message": "没有更新", "status": status})
                document = EvidenceDocument(
                    evidence_id=f"realtime:{quote['symbol']}:{quote['captured_at'].isoformat()}",
                    source_type="market", source_id=quote["symbol"],
                    title=f"{quote['symbol']} 实时行情快照",
                    text=f"{quote['name']} {quote['symbol']} 最新价 {quote['price']}；来源 {quote['source']}；行情时间 {quote['source_time'].isoformat()}；采集时间 {quote['captured_at'].isoformat()}。",
                    published_at=quote["source_time"], fetched_at=quote["captured_at"],
                    locator=quote["source_time"].isoformat(), metadata={"status": quote["status"]})
                return _dump({"documents": [document.model_dump(mode="json")], "message": "行情快照"})
            except ValueError as exc:
                return _dump({"documents": [], "message": str(exc), "status": "invalid_symbol"})
            except Exception:
                document = EvidenceDocument(
                    evidence_id=f"realtime-unavailable:{symbol}:{self.context.as_of.isoformat()}",
                    source_type="market", source_id=symbol, title=f"{symbol} 实时行情状态",
                    text=f"{symbol} 行情服务不可用，没有更新。", published_at=self.context.as_of,
                    metadata={"status": "unavailable"})
                return _dump({"documents": [document.model_dump(mode="json")], "message": "没有更新", "status": "unavailable"})

        def fetch_web_page(url: str) -> str:
            if self.context.run_kind != "interactive":
                return _dump({"error": "Web retrieval is disabled for monitor runs"})
            page = self.context.web_service.fetch(url)
            return _dump(page.model_dump(mode="json"))

        def add_watchlist_item(symbol: str, price_move: float = 0.03, volume_ratio: float = 1.8, note: str | None = None) -> str:
            item = self.watchlist_service.create(
                WatchlistItemCreate(
                    symbol=symbol,
                    thresholds={"price_move": price_move, "volume_ratio": volume_ratio},
                    note=note,
                )
            )
            return _dump({"item": item.model_dump(mode="json")})

        def update_watchlist_item(
            symbol: str,
            enabled: bool | None = None,
            price_move: float | None = None,
            volume_ratio: float | None = None,
            note: str | None = None,
        ) -> str:
            thresholds = None
            if price_move is not None or volume_ratio is not None:
                current = self.watchlist_service.get(symbol)
                if current is None:
                    raise ValueError("Watchlist item not found")
                thresholds = {
                    "price_move": price_move if price_move is not None else current.thresholds.price_move,
                    "volume_ratio": volume_ratio if volume_ratio is not None else current.thresholds.volume_ratio,
                }
            item = self.watchlist_service.update(
                symbol,
                WatchlistItemPatch(
                    enabled=enabled,
                    thresholds=thresholds,
                    note=note,
                ),
            )
            if item is None:
                raise ValueError("Watchlist item not found")
            return _dump({"item": item.model_dump(mode="json")})

        def delete_watchlist_item(symbol: str) -> str:
            return _dump({"deleted": self.watchlist_service.delete(symbol), "symbol": symbol.upper()})

        def submit_monitor_run(trading_date: date | None = None, cutoff_at: datetime | None = None) -> str:
            accepted = InvestmentMonitorService(
                store=self.monitor_store,
                harness_store=self.context.store,
            ).create_run(
                MonitorRunRequest(
                    trading_date=trading_date,
                    cutoff_at=cutoff_at,
                    thread_id=self.context.thread_id,
                )
            )
            return _dump(accepted.model_dump(mode="json"))

        def get_monitor_status(run_id: str | None = None) -> str:
            if not run_id:
                return _dump({"error": "run_id is required"})
            run = self.monitor_store.get_run(run_id)
            return _dump({"run": run.model_dump(mode="json") if run else None})

        def list_monitor_reports(symbol: str | None = None, limit: int = 10) -> str:
            reports, total = self.monitor_store.list_reports(symbol=symbol, limit=limit)
            return _dump(
                {
                    "total": total,
                    "items": [_public_monitor_report(report) for report in reports],
                }
            )

        def get_monitor_report(report_id: str | None = None) -> str:
            if not report_id:
                return _dump({"error": "report_id is required"})
            report = self.monitor_store.get_report(report_id)
            return _dump({"report": _public_monitor_report(report) if report else None})

        return [
            StructuredTool.from_function(
                func=realtime_quote,
                name="realtime_quote",
                description="查询指定股票的最新行情快照；没有更新时明确反馈，历史行情仍使用 research_search。",
                args_schema=RealtimeQuoteInput,
            ),
            StructuredTool.from_function(
                func=research_search,
                name="research_search",
                description=(
                    "Search unified internal evidence, automatically fall back to public web search, "
                    "or fetch explicitly supplied allowlisted web URLs. "
                    "Use this before making claims. Sources: market, news, annual_report, web."
                ),
                args_schema=ResearchSearchInput,
            ),
            StructuredTool.from_function(
                func=annual_report_search,
                name="annual_report_search",
                description="Search internal annual-report chunks with report, section, page and content-type filters.",
                args_schema=AnnualReportSearchInput,
            ),
            StructuredTool.from_function(
                func=resolve_security,
                name="resolve_security",
                description="Resolve a stock code or company name before querying financial evidence.",
                args_schema=ResolveSecurityInput,
            ),
            StructuredTool.from_function(
                func=list_watchlist,
                name="list_watchlist",
                description="List the current investment watchlist.",
            ),
            StructuredTool.from_function(
                func=fetch_web_page,
                name="fetch_web_page",
                description="Fetch one explicitly provided URL from the configured authoritative allowlist.",
                args_schema=WebFetchInput,
            ),
            StructuredTool.from_function(
                func=add_watchlist_item,
                name="add_watchlist_item",
                description="Propose adding an A-share security to monitoring. This is a write action requiring approval.",
                args_schema=AddWatchlistInput,
            ),
            StructuredTool.from_function(
                func=update_watchlist_item,
                name="update_watchlist_item",
                description="Propose updating a watchlist item. This is a write action requiring approval.",
                args_schema=UpdateWatchlistInput,
            ),
            StructuredTool.from_function(
                func=delete_watchlist_item,
                name="delete_watchlist_item",
                description="Propose deleting a watchlist item. This is a write action requiring approval.",
                args_schema=DeleteWatchlistInput,
            ),
            StructuredTool.from_function(
                func=submit_monitor_run,
                name="submit_monitor_run",
                description="Propose submitting a monitoring run for the enabled watchlist. This requires approval.",
                args_schema=SubmitMonitorInput,
            ),
            StructuredTool.from_function(
                func=get_monitor_status,
                name="get_monitor_status",
                description="Read monitor run status by run id.",
                args_schema=MonitorLookupInput,
            ),
            StructuredTool.from_function(
                func=list_monitor_reports,
                name="list_monitor_reports",
                description="List recent monitor reports, optionally filtered by symbol.",
            ),
            StructuredTool.from_function(
                func=get_monitor_report,
                name="get_monitor_report",
                description="Read one monitor report by id.",
            ),
        ]


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _public_monitor_report(report) -> dict[str, Any]:
    payload = report.model_dump(mode="json")
    if not settings.INVESTMENT_MONITOR_RECOMMENDATIONS_ENABLED:
        payload["recommendation_5d"] = None
        payload["recommendation_20d"] = None
        payload["model_metadata"] = {
            **dict(payload.get("model_metadata") or {}),
            "shadow_mode": True,
        }
    return payload

