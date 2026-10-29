"""OpenSearch BM25 index for annual-report chunks."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import httpx

from backend.config import settings
from backend.schemas.annual_report import AnnualReportSearchHit


class AnnualReportLexicalStoreError(Exception):
    """Raised when the annual-report lexical index is unavailable."""


def _json_default(value: Any) -> str:
    """Serialize temporal values using the ISO-8601 format expected by OpenSearch."""

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _normalize_temporal(value: Any) -> Any:
    """Normalize temporal strings loaded from Iceberg before indexing date fields."""

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
        except ValueError:
            return value
    return value


class AnnualReportLexicalStore:
    INDEX_SCHEMA_VERSION = "annual-report-v3"

    def __init__(self) -> None:
        self.url = settings.OPENSEARCH_URL.rstrip("/")
        self.index = settings.OPENSEARCH_ANNUAL_REPORT_INDEX
        self.enabled = settings.OPENSEARCH_ENABLED
        auth = (settings.OPENSEARCH_USERNAME, settings.OPENSEARCH_PASSWORD) if settings.OPENSEARCH_USERNAME else None
        self.client = httpx.Client(timeout=settings.OPENSEARCH_TIMEOUT_SECONDS, auth=auth)

    def ensure_index(self) -> None:
        if not self.enabled:
            return
        response = self.client.put(f"{self.url}/{self.index}", json=self._mapping())
        exists = response.status_code == 400 and "resource_already_exists_exception" in response.text
        if exists:
            mapping = self.client.get(f"{self.url}/{self.index}/_mapping")
            if mapping.status_code >= 300:
                raise AnnualReportLexicalStoreError(mapping.text)
            try:
                version = mapping.json()[self.index]["mappings"].get("_meta", {}).get("schema_version")
            except (KeyError, TypeError, ValueError) as exc:
                raise AnnualReportLexicalStoreError("OpenSearch returned an invalid annual-report mapping") from exc
            if version == self.INDEX_SCHEMA_VERSION:
                return

            # The annual-report lexical schema is disposable. Do not mutate an
            # older mapping in place: its documents may have fields and
            # analyzers that are incompatible with the current generation.
            response = self.client.delete(f"{self.url}/{self.index}")
            if response.status_code not in (200, 404):
                raise AnnualReportLexicalStoreError(response.text)
            response = self.client.put(f"{self.url}/{self.index}", json=self._mapping())
            if response.status_code not in (200, 201):
                raise AnnualReportLexicalStoreError(response.text)
            return
        if response.status_code not in (200, 201):
            raise AnnualReportLexicalStoreError(response.text)

    def upsert(self, chunks: list[dict[str, Any]], generation_id: str | None = None) -> int:
        if not self.enabled or not chunks:
            return 0
        lines: list[str] = []
        import json

        for chunk in chunks:
            active_generation = generation_id or chunk.get("generation_id") or "legacy"
            document_id = f"{active_generation}:{chunk['chunk_id']}"
            lines.append(json.dumps({"index": {"_index": self.index, "_id": document_id}}))
            payload = {key: value for key, value in chunk.items() if key != "embedding"}
            payload["generation_id"] = active_generation
            for key in ("announcement_time", "ingested_at"):
                if key in payload:
                    payload[key] = _normalize_temporal(payload[key])
            lines.append(json.dumps(
                payload,
                ensure_ascii=False,
                default=_json_default,
            ))
        response = self.client.post(f"{self.url}/_bulk?refresh=wait_for", content="\n".join(lines) + "\n", headers={"Content-Type": "application/x-ndjson"})
        if response.status_code >= 300 or response.json().get("errors"):
            raise AnnualReportLexicalStoreError(response.text)
        return len(chunks)

    def clear(self) -> None:
        if not self.enabled:
            return
        response = self.client.delete(f"{self.url}/{self.index}")
        if response.status_code not in (200, 404):
            raise AnnualReportLexicalStoreError(response.text)
        self.ensure_index()

    def retain_chunks(self, chunk_ids: list[str]) -> None:
        if not self.enabled:
            return
        allowed = [{"terms": {"_id": chunk_ids[start:start + 50000]}} for start in range(0, len(chunk_ids), 50000)]
        query = {"bool": {"must_not": [{"bool": {"should": allowed, "minimum_should_match": 1}}]}} if allowed else {"match_all": {}}
        response = self.client.post(f"{self.url}/{self.index}/_delete_by_query", json={"query": query})
        if response.status_code >= 300:
            raise AnnualReportLexicalStoreError(response.text)

    def count_generation(self, generation_id: str) -> int:
        if not self.enabled:
            return 0
        response = self.client.get(f"{self.url}/{self.index}/_count", params={"q": f"generation_id:{generation_id}"})
        if response.status_code >= 300:
            raise AnnualReportLexicalStoreError(response.text)
        return int(response.json().get("count", 0))

    def contains_chunk_ids(self, generation_id: str, chunk_ids: list[str]) -> bool:
        if not self.enabled or not chunk_ids:
            return True
        response = self.client.post(
            f"{self.url}/{self.index}/_mget",
            json={"ids": [f"{generation_id}:{chunk_id}" for chunk_id in chunk_ids]},
        )
        if response.status_code >= 300:
            raise AnnualReportLexicalStoreError(response.text)
        docs = response.json().get("docs", [])
        return len(docs) == len(chunk_ids) and all(
            item.get("found") and item.get("_source", {}).get("generation_id") == generation_id
            for item in docs
        )

    def delete_generation(self, generation_id: str) -> None:
        if not self.enabled:
            return
        response = self.client.post(
            f"{self.url}/{self.index}/_delete_by_query",
            json={"query": {"term": {"generation_id": generation_id}}},
        )
        if response.status_code >= 300:
            raise AnnualReportLexicalStoreError(response.text)

    def delete_unversioned_documents(self) -> None:
        if not self.enabled:
            return
        response = self.client.post(
            f"{self.url}/{self.index}/_delete_by_query",
            json={"query": {"bool": {"must_not": [{"exists": {"field": "generation_id"}}]}}},
        )
        if response.status_code >= 300:
            raise AnnualReportLexicalStoreError(response.text)

    def search(
        self,
        *,
        question: str,
        stock_code: str | None,
        report_year: int | None,
        section_path: str | None,
        content_types: list[str] | None,
        page_start: int | None,
        page_end: int | None,
        as_of: Any,
        published_after: Any = None,
        limit: int,
        generation_id: str | None = None,
        report_id: str | None = None,
    ) -> list[AnnualReportSearchHit]:
        if not self.enabled:
            return []
        filters: list[dict[str, Any]] = []
        if generation_id:
            filters.append({"term": {"generation_id": generation_id}})
        if report_id:
            filters.append({"term": {"report_id": report_id}})
        if stock_code:
            filters.append({"term": {"stock_code": stock_code.strip().upper()}})
        if report_year:
            filters.append({"term": {"report_year": report_year}})
        if section_path:
            filters.append({"term": {"section_path.keyword": section_path}})
        if content_types:
            filters.append({"terms": {"content_type": content_types}})
        if page_start is not None:
            filters.append({"range": {"page_end": {"gte": page_start}}})
        if page_end is not None:
            filters.append({"range": {"page_start": {"lte": page_end}}})
        if as_of is not None:
            filters.extend([
                {"range": {"ingested_at": {"lte": as_of.isoformat()}}},
                {"range": {"announcement_time": {"lte": as_of.isoformat()}}},
            ])
        if published_after is not None:
            filters.append({"range": {"announcement_time": {"gte": published_after.isoformat()}}})
        query = {"bool": {"filter": filters, "must": [{"multi_match": {"query": question, "fields": ["section_path^2", "chunk_text^3"]}}]}}
        response = self.client.post(f"{self.url}/{self.index}/_search", json={"size": limit, "query": query})
        if response.status_code >= 300:
            raise AnnualReportLexicalStoreError(response.text)
        hits: list[AnnualReportSearchHit] = []
        for item in response.json().get("hits", {}).get("hits", []):
            source = dict(item.get("_source") or {})
            source["chunk_id"] = source.get("chunk_id") or item.get("_id")
            source["similarity"] = 0.0
            source["lexical_score"] = float(item.get("_score") or 0.0)
            hits.append(AnnualReportSearchHit.model_validate(source))
        return hits

    def close(self) -> None:
        self.client.close()

    def _mapping(self) -> dict[str, Any]:
        return {
            "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
            "mappings": {
                "_meta": {"schema_version": self.INDEX_SCHEMA_VERSION},
                "properties": {
                    "report_id": {"type": "keyword"},
                    "generation_id": {"type": "keyword"},
                    "parse_run_id": {"type": "keyword"},
                    "region_ids": {"type": "keyword"},
                    "stock_code": {"type": "keyword"},
                    "report_year": {"type": "integer"},
                    "section_path": {"type": "text", "analyzer": "cjk", "fields": {"keyword": {"type": "keyword"}}},
                    "content_type": {"type": "keyword"},
                    "page_start": {"type": "integer"},
                    "page_end": {"type": "integer"},
                    "chunk_text": {"type": "text", "analyzer": "cjk"},
                    "announcement_time": {"type": "date"},
                    "ingested_at": {"type": "date"},
                }
            },
        }

