"""Client for the remote Ray Qwen embedding service."""

from __future__ import annotations

import logging
import math
from collections.abc import Iterable
from typing import Any

import httpx

from backend.config import settings

logger = logging.getLogger(__name__)


class RagEmbeddingError(Exception):
    """Raised when embeddings cannot be created."""


class EmbeddingService:
    """Client shared by indexing and retrieval."""

    def __init__(self, *, model_name: str | None = None, dimension: int | None = None) -> None:
        api_key = settings.resolved_embedding_api_key
        base_url = settings.resolved_embedding_base_url.rstrip("/")
        if not base_url:
            raise RagEmbeddingError("EMBEDDING_BASE_URL is not configured")

        self._api_key = api_key
        self._url = f"{base_url}{settings.resolved_embedding_path}"
        self._timeout = httpx.Timeout(settings.EMBEDDING_TIMEOUT_SECONDS)
        self._client = httpx.Client(timeout=self._timeout, trust_env=False)
        self._model_name = model_name or settings.resolved_embedding_model
        self._dimension = dimension or settings.resolved_embedding_dimension
        if self._model_name != settings.resolved_embedding_model or self._dimension != settings.resolved_embedding_dimension:
            raise RagEmbeddingError("Requested embedding model does not match the configured Ray model")

    def close(self) -> None:
        self._client.close()

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed_documents(self, texts: Iterable[str]) -> list[list[float]]:
        return self._embed(texts, "document")

    def _embed(self, texts: Iterable[str], input_type: str) -> list[list[float]]:

        payload = list(texts)
        if any(not isinstance(text, str) or not text.strip() for text in payload):
            raise RagEmbeddingError("Embedding inputs must be nonempty strings")
        payload = [text.strip() for text in payload]
        if not payload:
            return []

        embeddings: list[list[float]] = []
        batch_size = max(1, settings.EMBEDDING_BATCH_SIZE)
        for start in range(0, len(payload), batch_size):
            batch = payload[start : start + batch_size]
            response = self._post(
                {
                    "model": self.model_name,
                    "input": batch,
                    "input_type": input_type,
                }
            )
            entries = response.get("data")
            if not isinstance(entries, list) or any(not isinstance(item, dict) or type(item.get("index")) is not int for item in entries) or sorted(item["index"] for item in entries) != list(range(len(batch))):
                raise RagEmbeddingError("Embedding response indexes do not match inputs")
            batch_embeddings = self._extract_embeddings(response)
            if len(batch_embeddings) != len(batch):
                raise RagEmbeddingError(
                    f"Embedding response returned {len(batch_embeddings)} vectors for {len(batch)} inputs"
                )
            embeddings.extend(batch_embeddings)
        return embeddings

    def embed_query(self, text: str) -> list[float]:
        """Embed a query string."""

        embeddings = self._embed([text], "query")
        if not embeddings:
            raise RagEmbeddingError("Empty embedding response for query")
        return embeddings[0]

    def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            headers = {"Content-Type": "application/json"}
            if self._api_key:
                headers["Authorization"] = f"Bearer {self._api_key}"
            response = self._client.post(
                self._url,
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and data.get("model") != self.model_name:
                raise RagEmbeddingError("Embedding service returned a different model")
            if not isinstance(data, dict):
                raise RagEmbeddingError("Embedding provider returned an unexpected payload")
            if data.get("error") or data.get("error_code") not in (None, 0, "0"):
                raise RagEmbeddingError(
                    f"Embedding provider error: {data.get('error') or data.get('message') or data}"
                )
            return data
        except (httpx.HTTPError, ValueError, RagEmbeddingError) as exc:
            logger.exception("Embedding request failed")
            raise RagEmbeddingError(f"Embedding generation failed: {exc}") from exc

    def _extract_embeddings(self, payload: dict[str, Any]) -> list[list[float]]:
        data = payload.get("data")
        if not isinstance(data, list):
            raise RagEmbeddingError("Embedding response missing data")

        embeddings: list[list[float]] = []
        ordered = sorted(
            data,
            key=lambda item: item.get("index", 0) if isinstance(item, dict) else 0,
        )
        for item in ordered:
            if not isinstance(item, dict):
                raise RagEmbeddingError("Embedding response item is invalid")
            vector = item.get("embedding")
            if not isinstance(vector, list):
                raise RagEmbeddingError("Embedding response missing embedding vector")
            normalized = [float(value) for value in vector]
            if not all(math.isfinite(value) for value in normalized):
                raise RagEmbeddingError("Embedding contains a non-finite value")
            if len(normalized) != self.dimension:
                raise RagEmbeddingError(f"Embedding dimension {len(normalized)} does not match configured {self.dimension}")
            norm = math.sqrt(sum(value * value for value in normalized))
            if not 0.98 <= norm <= 1.02:
                raise RagEmbeddingError("Embedding is not normalized")
            embeddings.append(normalized)
        return embeddings
