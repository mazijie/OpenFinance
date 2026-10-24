"""Schemas for CNInfo annual-report ingestion and serving."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


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


class AnnualReportRecord(BaseModel):
    """Silver annual-report document record."""

    report_id: str
    stock_code: str
    exchange: str
    company_name: str | None = None
    report_year: int
    announcement_id: str
    title: str
    announcement_time: datetime
    pdf_url: str
    is_canonical: bool = True
    version_type: str = "original"
    pdf_sha256: str
    pdf_size: int
    page_count: int = 0
    extraction_status: str = "pending"
    raw_object_path: str
    pdf_object_path: str
    ingested_at: datetime


class AnnualReportPageRecord(BaseModel):
    """Extracted text for one PDF page."""

    report_id: str
    stock_code: str
    report_year: int
    page_number: int
    text: str
    text_sha256: str
    char_count: int
    extraction_method: str = "pymupdf"
    extraction_status: str = "ok"


class AnnualReportSectionRecord(BaseModel):
    """A logical section spanning one or more report pages."""

    section_id: str
    report_id: str
    stock_code: str
    report_year: int
    section_title: str
    section_path: str
    start_page: int
    end_page: int
    text: str
    section_order: int


class AnnualReportPage(BaseModel):
    """Page returned by the serving API."""

    report_id: str
    page_number: int
    text: str
    extraction_status: str


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
    pdf_url: str
    is_canonical: bool
    version_type: str
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
    page_start: int
    page_end: int
    chunk_text: str
    announcement_time: datetime | None = None
    ingested_at: datetime | None = None
    similarity: float = 0.0
    lexical_score: float | None = None
    rrf_score: float | None = None
    rerank_score: float | None = None
    embedding: list[float] | None = Field(default=None, exclude=True)
