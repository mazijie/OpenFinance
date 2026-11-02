"""Schemas for RAG indexing and retrieval."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class RagChunkRecord(BaseModel):
    """A single text chunk stored in the pgvector index."""

    chunk_id: str
    news_id: str
    external_news_id: str | None = None
    provider: str
    source_name: str | None = None
    title: str
    summary: str | None = None
    content: str | None = None
    url: str
    published_at: datetime
    ingested_at: datetime
    language: str | None = None
    symbols: list[str] = Field(default_factory=list)
    entities: list[str] = Field(default_factory=list)
    content_hash: str
    raw_object_path: str
    chunk_index: int
    chunk_text: str
    embedding: list[float]


class RagSearchHit(BaseModel):
    """A retrieved chunk returned from vector search."""

    chunk_id: str
    news_id: str
    external_news_id: str | None = None
    provider: str
    source_name: str | None = None
    title: str
    summary: str | None = None
    content: str | None = None
    url: str
    published_at: datetime
    ingested_at: datetime
    language: str | None = None
    symbols: list[str] = Field(default_factory=list)
    entities: list[str] = Field(default_factory=list)
    content_hash: str
    raw_object_path: str
    chunk_index: int
    chunk_text: str
    similarity: float
    lexical_score: float | None = None
    rrf_score: float | None = None
    rerank_score: float | None = None
    final_score: float | None = None
    embedding: list[float] | None = Field(default=None, exclude=True)


class RagEvidence(BaseModel):
    """A citable chunk selected for an article-level result."""

    chunk_id: str
    chunk_text: str
    rerank_score: float
    final_score: float


class RagArticleResult(BaseModel):
    """A deduplicated, citable news article result."""

    citation_id: int
    news_id: str
    content_hash: str
    title: str
    source_name: str | None = None
    provider: str
    published_at: datetime
    url: str
    final_score: float
    metadata: dict[str, Any] = Field(default_factory=dict)
    evidence_chunks: list[RagEvidence] = Field(default_factory=list)


class RagSearchResult(BaseModel):
    """Semantic search result payload."""

    hits: list[RagSearchHit] = Field(default_factory=list)
    articles: list[RagArticleResult] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    message: str | None = None
    error_type: str | None = None
    error_stage: str | None = None


class CitationAssessment(BaseModel):
    """Evaluation contract; correctness requires an external entailment reviewer."""

    completeness: bool
    validity: bool
    correctness: bool | None = None
    correctness_notes: str | None = None


class RagIndexStats(BaseModel):
    """Statistics for one indexing run."""

    scanned_articles: int = 0
    skipped_articles: int = 0
    skipped_existing: int = 0
    chunk_count: int = 0
    written_chunks: int = 0
    embeddings_failed: int = 0
    errors: int = 0
    details: dict[str, Any] = Field(default_factory=dict)
