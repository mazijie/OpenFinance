"""Tool registry and typed tools exposed to the research agent."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel, Field, model_validator

from backend.harness.evidence import EvidenceService
from backend.schemas.harness import EvidenceDocument
from backend.services.realtime_quote_service import RealtimeQuoteService
from backend.harness.policy import PolicyGuard
from backend.harness.store import HarnessStore
from backend.harness.web import WebResearchService
from backend.services.finance_service import FinanceService
from backend.services.market_gold_service import MarketGoldService


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


class MarketScreenInput(BaseModel):
    trading_date: str | None = Field(default=None, description="交易日 YYYY-MM-DD，默认最近交易日")
    sort_by: Literal['return_1d', 'return_5d', 'return_20d', 'amount', 'avg_amount_1d', 'avg_amount_5d', 'avg_amount_20d', 'ma_5d', 'ma_20d', 'price_ma_5d_deviation', 'price_ma_20d_deviation', 'volatility_20d_annualized', 'avg_volume_5d', 'volume_ratio_5d'] = 'return_1d'
    direction: Literal['asc', 'desc'] = 'desc'
    limit: int = Field(default=20, ge=1, le=100)
    min_value: float | None = None
    max_value: float | None = None


class StockOverviewInput(BaseModel):
    symbol: str = Field(min_length=1, max_length=16)
    query: str = Field(default='近期经营及重要新闻', min_length=1)
    limit: int = Field(default=5, ge=1, le=20)


class ResolveSecurityInput(BaseModel):
    query: str


class RealtimeQuoteInput(BaseModel):
    symbol: str = Field(description="股票代码，例如 600519 或 600519.SH；优先包含交易所后缀")


class WebFetchInput(BaseModel):
    url: str


class ToolRegistry:
    """Build one allowlisted tool set for a harness run."""

    def __init__(
        self,
        *,
        context: ToolContext,
        finance_service: FinanceService | None = None,
    ) -> None:
        self.context = context
        self.finance_service = finance_service or FinanceService()
        self._tools = self._build_tools()

    @property
    def tools(self) -> list[BaseTool]:
        return [tool for tool in self._tools if PolicyGuard.allows(self.context.run_kind, tool.name)]

    @property
    def tool_map(self) -> dict[str, BaseTool]:
        return {tool.name: tool for tool in self.tools}

    def _build_tools(self) -> list[BaseTool]:
        def market_screen(**kwargs: Any) -> str:
            from datetime import date
            request = MarketScreenInput.model_validate(kwargs)
            result = MarketGoldService().screen(as_of=self.context.as_of,
                trading_date=date.fromisoformat(request.trading_date) if request.trading_date else None,
                sort_by=request.sort_by, direction=request.direction, limit=request.limit,
                min_value=request.min_value, max_value=request.max_value)
            doc = EvidenceDocument(evidence_id=f"market-screen:{self.context.turn_id}:{request.model_dump_json()}",
                source_type="market", source_id=str(result['trading_date']), title="全市场行情筛选",
                text=_dump(result), locator=str(result['trading_date']), metadata={"screen": result})
            return _dump({**result, "documents": [doc.model_dump(mode="json")]})

        def stock_overview(**kwargs: Any) -> str:
            request = StockOverviewInput.model_validate(kwargs)
            result = MarketGoldService(evidence_service=self.context.evidence_service).overview(
                symbol=request.symbol, query=request.query, as_of=self.context.as_of, limit=request.limit)
            if result['market']:
                doc = EvidenceDocument(evidence_id=f"market-overview:{request.symbol}:{self.context.as_of.isoformat()}",
                    source_type="market", source_id=request.symbol, title=f"{request.symbol} 行情概览",
                    text=_dump(result['market']), metadata={"market": result['market']})
                result['documents'].insert(0, doc.model_dump(mode="json"))
            return _dump(result)

        def research_search(**kwargs: Any) -> str:
            request = ResearchSearchInput.model_validate(kwargs)
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
            page = self.context.web_service.fetch(url)
            return _dump(page.model_dump(mode="json"))

        return [
            StructuredTool.from_function(func=market_screen, name="market_screen", args_schema=MarketScreenInput,
                description="按收益、成交额、均线、波动率或成交量筛选和排序全市场股票。历史行情使用当前已知版本。"),
            StructuredTool.from_function(func=stock_overview, name="stock_overview", args_schema=StockOverviewInput,
                description="查询单股行情概览及相关新闻、年报证据。"),
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
                func=fetch_web_page,
                name="fetch_web_page",
                description="Fetch one explicitly provided URL from the configured authoritative allowlist.",
                args_schema=WebFetchInput,
            ),
        ]


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)
