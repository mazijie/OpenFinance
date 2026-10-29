"""Schemas for CNInfo annual-report ingestion and serving."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


AnnualReportPageType = Literal["text", "scan", "mixed", "complex"]
AnnualReportRegionType = Literal[
    "heading",
    "paragraph",
    "table",
    "figure",
    "formula",
    "footnote",
]


class AnnualReportAnnouncement(BaseModel):
    """Normalized announcement metadata returned by CNInfo."""

    provider: str = "cninfo"
    announcement_id: str
    stock_code: str
    exchange: str
    company_name: str | None = None
    org_id: str | None = None
    report_year: int
    title: str
    announcement_time: datetime
    pdf_url: str
    adjunct_size: int | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("stock_code", mode="before")
    @classmethod
    def normalize_stock_code(cls, value: str) -> str:
        return str(value).strip().upper()


class AnnualReportBronzeObject(BaseModel):
    """Manifest stored alongside a raw annual-report PDF."""

    provider: str = "cninfo"
    ingested_at: datetime
    announcement: AnnualReportAnnouncement
    query_params: dict[str, Any] = Field(default_factory=dict)
    pdf_sha256: str
    pdf_size: int
    pdf_object_path: str
    metadata_object_path: str | None = None
    extraction_status: str = "pending"


class AnnualReportParseResult(BaseModel):
    manifest_path: str = Field(min_length=1)
    parse_run_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    status: Literal["succeeded", "partial"]
    ir_object_path: str = Field(min_length=1)
    quality_object_path: str = Field(min_length=1)
    page_count: int = Field(ge=1)
    needs_review_pages: list[int]
    force: bool = False


class AnnualReportSilverRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AnnualReportRecord(AnnualReportSilverRecord):
    """Minimal Silver annual-report document record."""

    report_id: str = Field(min_length=1)
    stock_code: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    company_name: str | None = None
    report_year: int
    announcement_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    announcement_time: datetime
    pdf_sha256: str = Field(min_length=1)
    page_count: int
    extraction_status: str
    pdf_object_path: str = Field(min_length=1)
    parse_run_id: str = Field(min_length=1)
    ingested_at: datetime


class AnnualReportPageRecord(AnnualReportSilverRecord):
    """Minimal Silver page record."""

    report_id: str = Field(min_length=1)
    parse_run_id: str = Field(min_length=1)
    page_number: int = Field(ge=1)
    page_type: AnnualReportPageType
    text: str
    extraction_method: str = Field(min_length=1)
    extraction_status: str
    ir_page_path: str | None = None

    @model_validator(mode="after")
    def require_review_for_empty_page(self) -> "AnnualReportPageRecord":
        if not self.text.strip() and self.extraction_status == "ok":
            raise ValueError("An empty annual-report page cannot have extraction_status=ok")
        return self


class AnnualReportSectionRecord(AnnualReportSilverRecord):
    """Minimal Silver section record."""

    section_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    parse_run_id: str = Field(min_length=1)
    section_title: str = Field(min_length=1)
    section_path: str = Field(min_length=1)
    level: int
    parent_section_id: str | None = None
    start_page: int
    end_page: int
    section_order: int


class AnnualReportParseRunRecord(AnnualReportSilverRecord):
    """Minimal Silver record for one parser execution."""

    parse_run_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    pdf_sha256: str = Field(min_length=1)
    parser_version: str = Field(min_length=1)
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    ir_object_path: str = Field(min_length=1)


class AnnualReportRegionRecord(AnnualReportSilverRecord):
    """Minimal locator for a page region; detailed IR stays in MinIO."""

    region_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    parse_run_id: str = Field(min_length=1)
    page_number: int = Field(ge=1)
    region_type: AnnualReportRegionType
    bbox: list[float] = Field(min_length=4, max_length=4)
    reading_order: int
    text: str
    content_ref: str | None = None


class AnnualReportChunkRecord(AnnualReportSilverRecord):
    """Canonical chunk manifest before vector indexing."""

    chunk_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    parse_run_id: str = Field(min_length=1)
    section_path: str = Field(min_length=1)
    content_type: AnnualReportRegionType
    page_start: int = Field(ge=1)
    page_end: int = Field(ge=1)
    region_ids: list[str] = Field(min_length=1)
    chunk_text: str = Field(min_length=1)
    content_hash: str = Field(min_length=1)


class AnnualReportPage(BaseModel):
    """Page returned by the serving API."""

    report_id: str
    page_number: int = Field(ge=1)
    text: str
    extraction_status: str
    page_type: AnnualReportPageType
    ir_page_path: str | None = None


class AnnualReportRegion(BaseModel):
    """Region locator returned by the serving API."""

    region_id: str
    report_id: str
    page_number: int = Field(ge=1)
    region_type: str
    bbox: list[float] = Field(min_length=4, max_length=4)
    reading_order: int
    text: str
    content_ref: str | None = None


class AnnualReportItem(BaseModel):
    """Report metadata returned by the serving API."""

    report_id: str
    stock_code: str
    exchange: str
    company_name: str | None = None
    report_year: int
    announcement_id: str
    title: str
    announcement_time: datetime
    pdf_url: str | None = None
    page_count: int
    extraction_status: str


class AnnualReportPageResult(BaseModel):
    items: list[AnnualReportItem] = Field(default_factory=list)
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=100)
    total: int = Field(ge=0)
    has_next: bool = False


class AnnualReportSearchHit(BaseModel):
    """A citable annual-report retrieval hit."""

    chunk_id: str
    report_id: str
    stock_code: str
    report_year: int
    company_name: str | None = None
    section_path: str
    page_start: int = Field(ge=1)
    page_end: int = Field(ge=1)
    chunk_text: str
    parse_run_id: str
    content_type: AnnualReportRegionType
    region_ids: list[str] = Field(min_length=1)
    announcement_time: datetime
    ingested_at: datetime
    similarity: float = 0.0
    lexical_score: float | None = None
    rrf_score: float | None = None
    rerank_score: float | None = None
    final_score: float | None = None
    embedding: list[float] | None = Field(default=None, exclude=True)
