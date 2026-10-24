"""OpenSearch-backed BM25 index for news chunks."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import httpx

from backend.config import settings
from backend.schemas.rag import RagChunkRecord, RagSearchHit
from backend.utils.time import day_end_utc, day_start_utc


class LexicalStoreError(Exception):
    """Raised when the BM25 index cannot be used."""


class OpenSearchNewsStore:
    """Maintain and query the OpenSearch BM25 chunk index."""

    def __init__(self) -> None:
        self.url = settings.OPENSEARCH_URL.rstrip("/")
        self.index = settings.OPENSEARCH_INDEX
        self.enabled = settings.OPENSEARCH_ENABLED
        auth = None
        if settings.OPENSEARCH_USERNAME:
            auth = (settings.OPENSEARCH_USERNAME, settings.OPENSEARCH_PASSWORD)
        self.client = httpx.Client(timeout=settings.OPENSEARCH_TIMEOUT_SECONDS, auth=auth)

    def ensure_index(self) -> None:
        if not self.enabled:
            return
        response = self.client.put(f"{self.url}/{self.index}", json=self._mapping())
        already_exists = response.status_code == 400 and "resource_already_exists_exception" in response.text
        if response.status_code not in (200, 201) and not already_exists:
            self._raise(response, "creating lexical index")

    def upsert_chunks(self, chunks: list[RagChunkRecord]) -> int:
        if not self.enabled or not chunks:
            return 0
        lines: list[str] = []
        for chunk in chunks:
            lines.append('{"index":{"_index":"%s","_id":"%s"}}' % (self.index, chunk.chunk_id))
            lines.append(self._document(chunk))
        response = self.client.post(
            f"{self.url}/_bulk", content="\n".join(lines) + "\n", headers={"Content-Type": "application/x-ndjson"}
        )
        if response.status_code >= 300:
            self._raise(response, "bulk indexing lexical chunks")
        payload = response.json()
        if payload.get("errors"):
            raise LexicalStoreError(f"OpenSearch bulk indexing reported errors: {payload}")
        return len(chunks)

    def existing_content_hashes(self, content_hashes: list[str]) -> set[str]:
        if not self.enabled or not content_hashes:
            return set()
        response = self.client.post(f"{self.url}/{self.index}/_search", json={"size": len(content_hashes), "_source": ["content_hash"], "query": {"terms": {"content_hash": content_hashes}}})
        if response.status_code >= 300:
            self._raise(response, "checking lexical index")
        return {str(item["_source"]["content_hash"]) for item in response.json().get("hits", {}).get("hits", [])}

    def clear(self) -> None:
        if self.enabled:
            response = self.client.delete(f"{self.url}/{self.index}")
            if response.status_code not in (200, 404): self._raise(response, "clearing lexical index")
            self.ensure_index()

    def close(self) -> None:
        self.client.close()

    def search_chunks(
        self,
        *,
        question: str,
        symbol: str | None,
        start_date: date,
        end_date: date,
        as_of: datetime | None = None,
        limit: int,
    ) -> list[RagSearchHit]:
        if not self.enabled:
            return []
        published_before = min(day_end_utc(end_date), as_of) if as_of is not None else day_end_utc(end_date)
        filters: list[dict[str, Any]] = [
            {"range": {"published_at": {"gte": day_start_utc(start_date).isoformat(), "lte": published_before.isoformat()}}}
        ]
        if as_of is not None:
            filters.append({"range": {"ingested_at": {"lte": as_of.isoformat()}}})
        if symbol:
            filters.append({"term": {"symbols": symbol.strip().upper()}})
        response = self.client.post(
            f"{self.url}/{self.index}/_search",
            json={
                "size": limit,
                "query": {
                    "bool": {
                        "filter": filters,
                        "must": [{"multi_match": {"query": question, "fields": ["title^3", "summary^2", "chunk_text"]}}],
                    }
                },
            },
        )
        if response.status_code >= 300:
            self._raise(response, "searching lexical chunks")
        hits: list[RagSearchHit] = []
        for item in response.json().get("hits", {}).get("hits", []):
            source = dict(item.get("_source") or {})
            source["chunk_id"] = item.get("_id")
            source["similarity"] = 0.0
            source["lexical_score"] = float(item.get("_score") or 0.0)
            hits.append(RagSearchHit.model_validate(source))
        return hits

    def _mapping(self) -> dict[str, Any]:
        return {
            "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
            "mappings": {
                "properties": {
                    "title": {"type": "text", "analyzer": "cjk"},
                    "summary": {"type": "text", "analyzer": "cjk"},
                    "chunk_text": {"type": "text", "analyzer": "cjk"},
                    "symbols": {"type": "keyword"},
                    "published_at": {"type": "date"},
                    "ingested_at": {"type": "date"},
                    "url": {"type": "keyword", "index": False},
                    "content_hash": {"type": "keyword"},
                }
            },
        }

    @staticmethod
    def _document(chunk: RagChunkRecord) -> str:
        import json

        return json.dumps(chunk.model_dump(mode="json", exclude={"embedding"}), ensure_ascii=False)

    @staticmethod
    def _raise(response: httpx.Response, action: str) -> None:
        raise LexicalStoreError(f"OpenSearch error while {action}: {response.status_code} {response.text}")
