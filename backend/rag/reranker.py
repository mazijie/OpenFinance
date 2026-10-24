"""Pluggable Cohere-compatible reranking client."""

from __future__ import annotations

from typing import Protocol

import httpx

from backend.config import settings
from backend.schemas.rag import RagSearchHit


class RerankerError(Exception):
    """Raised for an unavailable or invalid reranker response."""


class Reranker(Protocol):
    def rerank(self, *, question: str, candidates: list[RagSearchHit]) -> list[RagSearchHit]: ...


class CohereCompatibleReranker:
    """Adapt the Cohere v2 rerank request/response contract."""

    def __init__(self) -> None:
        self.enabled = settings.RAG_RERANK_ENABLED
        self.url = settings.RAG_RERANK_URL
        self.client = httpx.Client(timeout=settings.RAG_RERANK_TIMEOUT_SECONDS)

    def close(self) -> None:
        self.client.close()

    def rerank(self, *, question: str, candidates: list[RagSearchHit]) -> list[RagSearchHit]:
        if not candidates:
            return []
        if not self.enabled:
            return [candidate.model_copy(update={"rerank_score": candidate.rrf_score or candidate.similarity}) for candidate in candidates]
        if not self.url or not settings.RAG_RERANK_API_KEY:
            raise RerankerError("Reranker is enabled but URL or API key is missing")
        documents = [f"标题：{candidate.title}\n正文：{candidate.chunk_text}" for candidate in candidates]
        try:
            response = self.client.post(
                self.url,
                headers={"Authorization": f"Bearer {settings.RAG_RERANK_API_KEY}", "Content-Type": "application/json"},
                json={"model": settings.RAG_RERANK_MODEL, "query": question, "documents": documents, "top_n": len(documents)},
            )
            response.raise_for_status()
            results = response.json().get("results")
        except (httpx.HTTPError, ValueError) as exc:
            raise RerankerError(f"Reranker request failed: {exc}") from exc
        if not isinstance(results, list):
            raise RerankerError("Reranker response did not contain results")
        ranked: list[RagSearchHit] = []
        for item in results:
            index = item.get("index") if isinstance(item, dict) else None
            score = item.get("relevance_score") if isinstance(item, dict) else None
            if not isinstance(index, int) or index < 0 or index >= len(candidates) or score is None:
                continue
            ranked.append(candidates[index].model_copy(update={"rerank_score": float(score)}))
        if not ranked:
            raise RerankerError("Reranker response contained no usable results")
        return ranked
