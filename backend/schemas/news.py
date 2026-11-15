"""Schemas for news ingestion, storage and retrieval."""

from __future__ import annotations

from datetime import date, datetime, timezone
from math import ceil
from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator


class ProviderNewsArticle(BaseModel):
    """Normalized subset of a provider article."""

    external_news_id: str | None = None
    category: str | None = None
    title: str = ""
    url: str
    source_name: str | None = None
    language: str | None = None
    published_at: datetime | None = None
    summary: str | None = None
    content: str | None = None
    cover_image_url: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class ProviderNewsResponse(BaseModel):
    """Structured provider response."""

    articles: list[ProviderNewsArticle] = Field(default_factory=list)
    raw_response: dict[str, Any] = Field(default_factory=dict)


class BronzeNewsObject(BaseModel):
    """Raw ingestion object persisted in Bronze."""

    provider: str
    ingested_at: datetime
    query: str
    start_time: datetime | None = None
    end_time: datetime | None = None
    limit: int | None
    raw_response: dict[str, Any]

    @field_validator("ingested_at", mode="before")
    @classmethod
    def _ensure_ingested_at(cls, value: datetime | str) -> datetime:
        if isinstance(value, datetime):
            dt = value
        else:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)


class NewsArticleRecord(BaseModel):
    """Silver record stored in Iceberg."""

    news_id: str
    external_news_id: str | None = None
    provider: str
    category: str | None = None
    source_name: str | None = None
    title: str
    summary: str | None = None
    content: str | None = None
    url: HttpUrl
    cover_image_url: str | None = None
    published_at: datetime
    ingested_at: datetime
    language: str | None = None
    symbols: list[str] = Field(default_factory=list)
    entities: list[str] = Field(default_factory=list)
    content_hash: str
    raw_object_path: str

    @field_validator("published_at", "ingested_at", mode="before")
    @classmethod
    def _normalize_datetime(cls, value: datetime | str) -> datetime:
        if isinstance(value, datetime):
            dt = value
        else:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)


class NewsItem(BaseModel):
    """News item returned to callers."""

    news_id: str
    external_news_id: str | None = None
    provider: str
    category: str | None = None
    source_name: str | None = None
    title: str
    summary: str | None = None
    content: str | None = None
    url: str
    cover_image_url: str | None = None
    published_at: datetime
    language: str | None = None
    symbols: list[str] = Field(default_factory=list)
    entities: list[str] = Field(default_factory=list)


class NewsPage(BaseModel):
    """Paginated lakehouse news listing."""

    items: list[NewsItem] = Field(default_factory=list)
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=100)
    total: int = Field(ge=0)
    total_pages: int = Field(ge=0)
    has_next: bool = False

    @classmethod
    def build(cls, *, items: list[NewsItem], page: int, page_size: int, total: int) -> "NewsPage":
        total_pages = ceil(total / page_size) if total else 0
        return cls(
            items=items,
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
            has_next=page < total_pages,
        )


class NewsSearchParams(BaseModel):
    """Search parameters for lakehouse lookup."""

    symbol: str
    start_date: date
    end_date: date
    limit: int = Field(default=10, ge=1, le=100)

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol cannot be empty")
        return normalized


class NewsSearchResult(BaseModel):
    """Service output for news lookup."""

    items: list[NewsItem] = Field(default_factory=list)
    message: str | None = None
