"""Schemas for watchlists, scheduled research runs, and monitor reports."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


RecommendationStance = Literal["bullish", "neutral", "bearish", "avoid"]
RecommendationHorizon = Literal["5d", "20d"]
MonitorDataStatus = Literal["complete", "degraded"]
MonitorReportStatus = Literal["ready", "degraded", "failed"]
MonitorRunStatus = Literal["queued", "running", "completed", "degraded", "skipped", "failed"]
MonitorEventType = Literal["price_move", "volume_surge", "ma_cross", "news", "annual_report"]
CitationSourceType = Literal["market", "news", "annual_report"]


def _normalize_symbol(value: str) -> str:
    normalized = str(value).strip().upper()
    if not normalized:
        raise ValueError("symbol cannot be empty")
    return normalized


class WatchlistThresholds(BaseModel):
    """Per-security thresholds used by the deterministic event engine."""

    price_move: float = Field(default=0.03, ge=0.0, le=1.0)
    volume_ratio: float = Field(default=1.8, ge=1.0, le=20.0)


class WatchlistItemCreate(BaseModel):
    symbol: str = Field(min_length=4, max_length=16)
    enabled: bool = True
    thresholds: WatchlistThresholds = Field(default_factory=WatchlistThresholds)
    note: str | None = Field(default=None, max_length=500)

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return _normalize_symbol(value)


class WatchlistItemPatch(BaseModel):
    enabled: bool | None = None
    thresholds: WatchlistThresholds | None = None
    note: str | None = Field(default=None, max_length=500)


class WatchlistItem(BaseModel):
    symbol: str
    security_id: str
    name: str
    exchange: str
    enabled: bool = True
    thresholds: WatchlistThresholds = Field(default_factory=WatchlistThresholds)
    note: str | None = None
    version: int = 1
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def _utc(cls, value: datetime | str) -> datetime:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00")) if isinstance(value, str) else value
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


class MonitorEvent(BaseModel):
    event_id: str
    event_type: MonitorEventType
    severity: Literal["low", "medium", "high"]
    symbol: str
    trading_date: date
    observed_at: datetime
    metric_value: float | None = None
    threshold: float | None = None
    description: str
    dedupe_key: str


class Citation(BaseModel):
    citation_id: int = Field(ge=1)
    source_type: CitationSourceType
    source_id: str
    title: str
    published_at: datetime | None = None
    locator: str | None = None
    url: str | None = None
    excerpt: str | None = None


class HorizonRecommendation(BaseModel):
    horizon: RecommendationHorizon
    stance: RecommendationStance
    confidence: float = Field(ge=0.0, le=1.0)
    actionable: bool
    entry_condition: str | None = None
    invalidation_condition: str | None = None
    rationale: str
    citation_ids: list[int] = Field(min_length=1)


class MonitorJudgement(BaseModel):
    """The only free-form model output accepted by the monitor workflow."""

    summary: str
    recommendation_5d: HorizonRecommendation
    recommendation_20d: HorizonRecommendation
    risk_notes: list[str] = Field(default_factory=list)


class MonitorReport(BaseModel):
    report_id: str
    run_id: str
    symbol: str
    name: str
    trading_date: date
    data_cutoff_at: datetime
    data_status: MonitorDataStatus
    status: MonitorReportStatus
    events: list[MonitorEvent] = Field(default_factory=list)
    citations: list[Citation] = Field(default_factory=list)
    recommendation_5d: HorizonRecommendation | None = None
    recommendation_20d: HorizonRecommendation | None = None
    analysis: str = ""
    risk_notes: list[str] = Field(default_factory=list)
    missing_data: list[str] = Field(default_factory=list)
    model_metadata: dict[str, Any] = Field(default_factory=dict)
    generated_at: datetime


class MonitorReportPage(BaseModel):
    items: list[MonitorReport] = Field(default_factory=list)
    total: int = Field(ge=0)
    limit: int = Field(ge=1, le=100)
    offset: int = Field(ge=0)


class MonitorRunRequest(BaseModel):
    trading_date: date | None = None
    cutoff_at: datetime | None = None
    force: bool = False
    thread_id: str | None = None

    @field_validator("cutoff_at")
    @classmethod
    def _cutoff_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class MonitorRun(BaseModel):
    run_id: str
    run_key: str
    trading_date: date
    cutoff_at: datetime
    status: MonitorRunStatus
    watchlist_version: str
    origin_thread_id: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    stats: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class MonitorRunAccepted(BaseModel):
    run_id: str
    status: MonitorRunStatus
    thread_id: str | None = None
    deduplicated: bool = False
