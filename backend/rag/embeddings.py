"""Embedding service used by the news RAG pipeline."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

import httpx

from backend.config import settings

logger = logging.getLogger(__name__)


class RagEmbeddingError(Exception):
    """Raised when embeddings cannot be created."""


class EmbeddingService:
    """Wrapper around the configured Ark embedding endpoint."""

    def __init__(self) -> None:
        api_key = settings.resolved_embedding_api_key
        if not api_key:
            raise RagEmbeddingError("No embedding API key configured")

        base_url = settings.resolved_embedding_base_url.rstrip("/")
        if not base_url:
            raise RagEmbeddingError("LLM_BASE_URL is not configured")

        self._api_key = api_key
        self._url = f"{base_url}{settings.resolved_embedding_path}"
        self._timeout = httpx.Timeout(settings.NEWS_HTTP_TIMEOUT_SECONDS)
        self._client = httpx.Client(timeout=self._timeout)

    def close(self) -> None:
        self._client.close()

    def embed_documents(self, texts: Iterable[str]) -> list[list[float]]:
        """Embed a batch of documents."""

        payload = [text.strip() for text in texts if text and text.strip()]
        if not payload:
            return []

        inputs = [{"type": "text", "text": text} for text in payload]
        response = self._post({"model": settings.resolved_embedding_model, "input": inputs})
        return self._extract_embeddings(response)

    def embed_query(self, text: str) -> list[float]:
        """Embed a query string."""

        embeddings = self.embed_documents([text])
        if not embeddings:
            raise RagEmbeddingError("Empty embedding response for query")
        return embeddings[0]

    def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self._client.post(
                self._url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self._api_key}",
                },
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise RagEmbeddingError("Ark returned an unexpected payload")
            if data.get("error") or data.get("error_code") not in (None, 0, "0"):
                raise RagEmbeddingError(f"Ark embedding error: {data.get('error') or data.get('message') or data}")
            return data
        except (httpx.HTTPError, ValueError, RagEmbeddingError) as exc:
            logger.exception("Embedding request failed")
            raise RagEmbeddingError(f"Embedding generation failed: {exc}") from exc

    def _extract_embeddings(self, payload: dict[str, Any]) -> list[list[float]]:
        data = payload.get("data")
        if not isinstance(data, list):
            raise RagEmbeddingError("Ark embedding response missing data")

        embeddings: list[list[float]] = []
        for item in data:
            if not isinstance(item, dict):
                raise RagEmbeddingError("Ark embedding response item is invalid")
            vector = item.get("embedding")
            if not isinstance(vector, list):
                raise RagEmbeddingError("Ark embedding response missing embedding vector")
            normalized = [float(value) for value in vector]
            if len(normalized) != settings.resolved_embedding_dimension:
                raise RagEmbeddingError(f"Embedding dimension {len(normalized)} does not match configured {settings.resolved_embedding_dimension}")
            embeddings.append(normalized)
        return embeddings
