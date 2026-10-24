"""Schemas for internal job execution APIs."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field


class IngestFinanceNewsRequest(BaseModel):
    """Payload for triggering finance news ingestion."""

    start_time: datetime | None = None
    end_time: datetime | None = None
    limit: int | None = Field(default=None, ge=1, le=500)


class BronzeToSilverRequest(BaseModel):
    """Payload for Bronze-to-Silver normalization."""

    object_paths: list[str] = Field(default_factory=list)


class IngestAshareDailyBarsRequest(BaseModel):
    """Payload for triggering A-share daily bar ingestion."""

    symbols: list[str] = Field(default_factory=list, min_length=1)
    start_date: datetime
    end_date: datetime
    adjustment_type: str = Field(default="qfq")
    idempotency_key: str | None = Field(default=None, max_length=256)
    include_security_master: bool = True
    include_trading_calendar: bool = True


class AShareUniverseCoverageRequest(BaseModel):
    provider: str = Field(min_length=1)
    adjustment_type: str = Field(min_length=1)
    universe_as_of_date: date
    universe_symbol_count: int = Field(ge=1)
    processed_batches: int = Field(ge=1)
    written_daily_bars: int = Field(ge=0)
    dag_id: str = Field(min_length=1)
    dag_run_id: str = Field(min_length=1)


class AShareGapPlanRequest(BaseModel):
    adjustment_type: str = Field(default="qfq")
    lookback_days: int | None = Field(default=None, ge=1, le=3660)
    end_date: date | None = None


class RagIndexRequest(BaseModel):
    """Payload for building the news RAG index."""

    rebuild_all: bool = Field(default=False)


class IngestAnnualReportsRequest(BaseModel):
    """Payload for CNInfo annual-report ingestion."""

    stocks: list[str] = Field(default_factory=list)
    years: list[int] = Field(default_factory=list)
