"""Schemas for internal job execution APIs."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field

from backend.schemas.annual_report import AnnualReportParseResult


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
    start_date: date | None = None
    symbols: list[str] | None = None
    mode: str = "daily"
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


class AnnualReportBronzeManifestRequest(BaseModel):
    """Select existing Bronze annual-report manifests for replay."""

    stocks: list[str] = Field(default_factory=list)
    years: list[int] = Field(min_length=1)


class AnnualReportParseRequest(BaseModel):
    """Request one or more Bronze manifests for parser-service processing."""

    manifest_paths: list[str] = Field(min_length=1)
    force: bool = False


class AnnualReportRaySubmitRequest(BaseModel):
    """Submit an existing Bronze manifest batch to the shared Ray cluster."""

    manifest_paths: list[str] = Field(min_length=1)
    force: bool = False


class AnnualReportRayJobResponse(BaseModel):
    """Ray job identifier returned to Airflow."""

    job_id: str = Field(min_length=1)
    status: str = Field(min_length=1)


class AnnualReportIRToSilverRequest(BaseModel):
    """Parser responses to materialize into Iceberg Silver."""

    parse_results: list[AnnualReportParseResult] = Field(min_length=1)


class AnnualReportRecoveryPlanRequest(BaseModel):
    report_ids: list[str] = Field(default_factory=list)


class AnnualReportRecoverOneRequest(BaseModel):
    manifest_path: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    pdf_sha256: str = Field(min_length=64, max_length=64)
    pending_stages: list[str] = Field(min_length=1)
