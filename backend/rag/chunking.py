"""Text preparation helpers for news RAG."""

from __future__ import annotations

import re
from collections.abc import Iterable

from backend.config import settings
from backend.schemas.news import NewsArticleRecord


_SENTENCE_SPLIT = re.compile(r"(?<=[。！？!?；;])\s*|\n+")
_WHITESPACE = re.compile(r"\s+")


def build_source_text(record: NewsArticleRecord) -> str | None:
    """Build the source text that can be chunked for embeddings."""

    parts = [record.title.strip(), (record.summary or "").strip(), (record.content or "").strip()]
    combined = "\n\n".join(part for part in parts if part)
    if record.provider == "caixin" and record.summary and not record.content:
        return f"标题：{record.title.strip()}\n摘要：{record.summary.strip()}"
    if len(combined) < settings.RAG_MIN_TEXT_LENGTH:
        return None
    return combined


def split_text(text: str, *, chunk_size: int | None = None, overlap: int | None = None) -> list[str]:
    """Split text into overlapping chunks using a lightweight recursive strategy."""

    resolved_chunk_size = max(1, chunk_size or settings.RAG_CHUNK_SIZE)
    resolved_overlap = max(0, overlap or settings.RAG_CHUNK_OVERLAP)
    normalized = _WHITESPACE.sub(" ", text).strip()
    if not normalized:
        return []
    if len(normalized) <= resolved_chunk_size:
        return [normalized]

    segments = [segment.strip() for segment in _SENTENCE_SPLIT.split(normalized) if segment and segment.strip()]
    if not segments:
        segments = [normalized]

    chunks: list[str] = []
    current = ""

    for segment in segments:
        if len(segment) > resolved_chunk_size:
            if current:
                chunks.append(current)
                current = ""
            chunks.extend(_split_long_segment(segment, resolved_chunk_size, resolved_overlap))
            continue

        if not current:
            current = segment
            continue

        candidate = f"{current} {segment}".strip()
        if len(candidate) <= resolved_chunk_size:
            current = candidate
            continue

        chunks.append(current)
        current = _with_overlap(current, segment, resolved_overlap)

    if current:
        chunks.append(current)

    return [chunk.strip() for chunk in chunks if chunk.strip()]


def _split_long_segment(segment: str, chunk_size: int, overlap: int) -> list[str]:
    """Split a single oversized segment into windows."""

    if chunk_size <= 0:
        return [segment]

    step = max(1, chunk_size - overlap)
    chunks: list[str] = []
    start = 0
    while start < len(segment):
        chunk = segment[start : start + chunk_size].strip()
        if chunk:
            chunks.append(chunk)
        start += step
    return chunks


def _with_overlap(previous: str, next_segment: str, overlap: int) -> str:
    """Build the next chunk seed while keeping some trailing context."""

    if overlap <= 0:
        return next_segment
    tail = previous[-overlap:].strip()
    return f"{tail} {next_segment}".strip() if tail else next_segment


def has_rag_value(record: NewsArticleRecord) -> bool:
    """Return whether the record has enough substance for vectorization."""

    return build_source_text(record) is not None
