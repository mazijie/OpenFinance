"""Schemas for finance lakehouse data."""

from __future__ import annotations

from datetime import UTC, date, datetime
from math import ceil
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


def _ensure_utc_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class FinanceMetadata(BaseModel):
    provider: str
    source_record_id: str | None = None
    source_version: str | None = None
    payload_hash: str | None = None
    raw_object_path: str
    event_time: datetime
    available_at: datetime
    ingested_at: datetime
    schema_version: int = 1

    @field_validator("event_time", "available_at", "ingested_at", mode="before")
    @classmethod
    def _normalize_datetime(cls, value: datetime | str) -> datetime:
        normalized = _ensure_utc_datetime(value)
        assert normalized is not None
        return normalized


class SecurityMasterRecord(FinanceMetadata):
    security_id: str
    symbol: str
    exchange: str
    security_type: str
    name: str
    list_date: date | None = None
    delist_date: date | None = None
    status: str = "active"


class SecurityOption(BaseModel):
    """A security available for selection in the market UI."""

    symbol: str
    name: str
    exchange: str


class SecurityOptionsResult(BaseModel):
    """Security directory response for the market UI."""

    items: list[SecurityOption] = Field(default_factory=list)


class TradingCalendarRecord(BaseModel):
    exchange: str
    trading_date: date
    is_open: bool
    session_open_at: datetime | None = None
    session_close_at: datetime | None = None

    @field_validator("session_open_at", "session_close_at", mode="before")
    @classmethod
    def _normalize_calendar_datetime(cls, value: datetime | str | None) -> datetime | None:
        return _ensure_utc_datetime(value)


class DailyBarRecord(FinanceMetadata):
    security_id: str
    symbol: str
    exchange: str
    trading_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float | None = None
    turnover: float | None = None
    is_suspended: bool = False
    is_st: bool = False
    adjustment_factor: float = 1.0
    adjustment_type: str = "none"


class FinanceBronzeObject(BaseModel):
    domain: str = "finance"
    provider: str
    ingested_at: datetime
    source_record_id: str | None = None
    source_version: str | None = None
    payload_hash: str | None = None
    request_params: dict[str, Any] = Field(default_factory=dict)
    raw_response: dict[str, Any]

    @field_validator("ingested_at", mode="before")
    @classmethod
    def _normalize_ingested_at(cls, value: datetime | str) -> datetime:
        normalized = _ensure_utc_datetime(value)
        assert normalized is not None
        return normalized


class MarketQueryParams(BaseModel):
    symbol: str | None = None
    security_id: str | None = None
    start_date: date
    end_date: date
    adjustment_type: str = "qfq"
    as_of: datetime | None = None
    limit: int = Field(default=500, ge=1, le=5000)

    @field_validator("symbol", "security_id")
    @classmethod
    def _normalize_optional_code(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    @field_validator("as_of", mode="before")
    @classmethod
    def _normalize_as_of(cls, value: datetime | str | None) -> datetime | None:
        return _ensure_utc_datetime(value)


class MarketSummary(BaseModel):
    symbol: str
    security_id: str
    exchange: str
    start_date: date
    end_date: date
    adjustment_type: str
    first_close: float | None = None
    last_close: float | None = None
    interval_return: float | None = None
    cumulative_return: float | None = None
    annualized_volatility: float | None = None
    recent_trading_date: date | None = None
    suspended_days: int = 0
    data_gap_note: str | None = None
    adjustment_note: str | None = None
    bars: list[dict[str, Any]] = Field(default_factory=list)


class MarketSearchResult(BaseModel):
    items: list[MarketSummary] = Field(default_factory=list)
    message: str | None = None


class GoldMetricRecord(BaseModel):
    """One point-in-time row from the Gold security metrics table."""

    security_id: str
    symbol: str
    exchange: str
    trading_date: date
    provider: str
    adjustment_type: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float | None = None
    turnover: float | None = None
    is_suspended: bool
    is_st: bool
    adjustment_factor: float
    daily_return_1d: float | None = None
    return_5d: float | None = None
    return_20d: float | None = None
    ma_5d: float | None = None
    ma_20d: float | None = None
    volume_ma_5d: float | None = None
    volatility_20d_annualized: float | None = None
    observed_days_20d: int = 0
    metric_status: str
    source_available_at: datetime
    source_ingested_at: datetime
    feature_available_at: datetime
    gold_built_at: datetime
    schema_version: int

    @field_validator(
        "source_available_at",
        "source_ingested_at",
        "feature_available_at",
        "gold_built_at",
        mode="before",
    )
    @classmethod
    def _normalize_gold_time(cls, value: datetime | str) -> datetime:
        normalized = _ensure_utc_datetime(value)
        assert normalized is not None
        return normalized


class Pagination(BaseModel):
    items: list[Any] = Field(default_factory=list)
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=100)
    total: int = Field(ge=0)
    total_pages: int = Field(ge=0)
    has_next: bool = False

    @classmethod
    def build(cls, *, items: list[Any], page: int, page_size: int, total: int) -> "Pagination":
        total_pages = ceil(total / page_size) if total else 0
        return cls(
            items=items,
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
            has_next=page < total_pages,
        )


AdjustmentType = Literal["none", "qfq", "hfq", "forward", "backward"]
