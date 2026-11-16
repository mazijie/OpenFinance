"""Build and maintain the RAG index from Silver news records."""

from __future__ import annotations

import json
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from backend.config import settings
from backend.lakehouse.news_writer import NewsWriter
from backend.logging_utils import get_logger
from backend.rag.news.chunking import build_source_text, has_rag_value, split_text
from backend.rag.common.embeddings import EmbeddingService, RagEmbeddingError
from backend.rag.news.lexical_store import OpenSearchNewsStore
from backend.rag.news.vector_store import PgVectorNewsStore, RagStoreError
from backend.schemas.news import NewsArticleRecord
from backend.schemas.rag import RagChunkRecord, RagIndexStats
from backend.services.security_alias_service import build_security_matcher
from backend.utils.hash import sha256_text

logger = get_logger(__name__)


@dataclass(slots=True)
class RagBuildStats:
    scanned_articles: int = 0
    skipped_articles: int = 0
    skipped_existing: int = 0
    chunk_count: int = 0
    written_chunks: int = 0
    embeddings_failed: int = 0
    errors: int = 0

    def to_model(self) -> RagIndexStats:
        return RagIndexStats.model_validate(asdict(self))


class NewsRAGIndexer:
    """Create pgvector chunks from Silver news records."""

    def __init__(
        self,
        *,
        news_writer: NewsWriter | None = None,
        vector_store: PgVectorNewsStore | None = None,
        embedding_service: EmbeddingService | None = None,
        lexical_store: OpenSearchNewsStore | None = None,
    ) -> None:
        self.news_writer = news_writer or NewsWriter()
        self.vector_store = vector_store or PgVectorNewsStore()
        self.embedding_service = embedding_service or EmbeddingService()
        self.lexical_store = lexical_store or OpenSearchNewsStore()
        self.symbol_matcher = None
        if news_writer is None:
            try:
                self.symbol_matcher = build_security_matcher()
            except Exception:
                self.symbol_matcher = None
        self.vector_store.ensure_schema()
        self.lexical_store.ensure_index()

    def run(self, records: Iterable[NewsArticleRecord] | None = None, *, rebuild_all: bool = False) -> RagIndexStats:
        """Index all eligible Silver records into pgvector."""
        with self._index_lock():
            self._reconciliation_details = {}
            self._record_status("running")
            try:
                result = self._run(records, rebuild_all=rebuild_all)
                if result.embeddings_failed or result.errors:
                    self._record_status("failed", {**result.model_dump(), "progress": self._reconciliation_details}, "Indexing did not complete")
                else:
                    self._record_status("success", result.model_dump())
                return result
            except Exception as exc:
                self._record_status("failed", self._reconciliation_details, error=f"{type(exc).__name__}: {exc}"[:1000])
                raise

    def _record_status(self, status: str, details: dict | None = None, error: str | None = None) -> None:
        if not isinstance(self.vector_store, PgVectorNewsStore):
            return
        with self.vector_store._connection() as conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO openfinance.news_index_reconciliation
                (id, status, updated_at, last_success_at, details, error)
                VALUES (1, %s, now(), CASE WHEN %s = 'success' THEN now() ELSE NULL END, %s::jsonb, %s)
                ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status, updated_at=EXCLUDED.updated_at,
                last_success_at=COALESCE(EXCLUDED.last_success_at, news_index_reconciliation.last_success_at),
                details=EXCLUDED.details, error=EXCLUDED.error""",
                (status, status, json.dumps(details or {}), error))

    @contextmanager
    def _index_lock(self):
        if not isinstance(self.vector_store, PgVectorNewsStore):
            yield
            return
        with self.vector_store._connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", ("openfinance_news_index",))
            if not cur.fetchone()[0]:
                raise RagStoreError("Another news indexing or reconciliation run is active")
            try:
                yield
            finally:
                cur.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", ("openfinance_news_index",))

    def _run(self, records: Iterable[NewsArticleRecord] | None = None, *, rebuild_all: bool = False) -> RagIndexStats:

        source_records = list(records) if records is not None else self.news_writer.scan_records()
        stats = RagBuildStats(scanned_articles=len(source_records))

        eligible_records = [record for record in source_records if has_rag_value(record)]
        stats.skipped_articles = len(source_records) - len(eligible_records)

        if not eligible_records:
            return stats.to_model()

        content_hashes = [record.content_hash for record in eligible_records]
        existing_chunks = self.vector_store.chunks_for_content_hashes(content_hashes)
        existing_by_id = {chunk.chunk_id: chunk for chunk in existing_chunks}
        expected: dict[str, tuple[NewsArticleRecord, int, str]] = {}
        for record in eligible_records:
            source_text = build_source_text(record)
            if not source_text:
                stats.skipped_articles += 1
                continue
            chunks = split_text(source_text)
            if not chunks:
                stats.skipped_articles += 1
                continue
            for chunk_index, chunk_text in enumerate(chunks):
                chunk_hash = sha256_text("|".join([record.content_hash, str(chunk_index), chunk_text.casefold()]))
                expected[str(uuid5(NAMESPACE_URL, chunk_hash))] = (record, chunk_index, chunk_text)
        missing_vectors = [value for key, value in expected.items() if rebuild_all or key not in existing_by_id]
        self._reconciliation_details.update(expected_chunks=len(expected), missing_vector_chunks=len(missing_vectors), repaired_vector_chunks=0)
        stats.chunk_count = len(expected)
        stats.skipped_existing = max(0, len(expected) - len(missing_vectors))

        try: embeddings = self.embedding_service.embed_documents([chunk_text for _, _, chunk_text in missing_vectors]) if missing_vectors else []
        except RagEmbeddingError:
            stats.embeddings_failed = len(missing_vectors); return stats.to_model()

        if len(embeddings) != len(missing_vectors):
            stats.errors += 1
            return stats.to_model()

        chunk_records = [
            self._build_chunk_record(record=record, chunk_index=chunk_index, chunk_text=chunk_text, embedding=embedding)
            for (record, chunk_index, chunk_text), embedding in zip(missing_vectors, embeddings, strict=True)
        ]
        if chunk_records: self.vector_store.upsert_chunks(chunk_records)
        verified = {chunk.chunk_id: chunk for chunk in self.vector_store.chunks_for_content_hashes(content_hashes)}
        self._reconciliation_details["repaired_vector_chunks"] = len(set(verified) - set(existing_by_id))
        if set(expected) - set(verified):
            raise RagStoreError("News vector index is incomplete after reconciliation")
        existing_by_id = verified
        if self.lexical_store.enabled:
            lexical_ids = self.lexical_store.existing_chunk_ids(list(expected))
            missing_ids = set(expected) - lexical_ids
            self._reconciliation_details.update(missing_lexical_chunks=len(missing_ids), repaired_lexical_chunks=0)
            try:
                self.lexical_store.upsert_chunks([existing_by_id[chunk_id] for chunk_id in missing_ids])
            except Exception:
                try:
                    remaining = self.lexical_store.existing_chunk_ids(list(missing_ids))
                    self._reconciliation_details["repaired_lexical_chunks"] = len(remaining)
                except Exception:
                    self._reconciliation_details["repaired_lexical_chunks"] = None
                raise
            if set(expected) - self.lexical_store.existing_chunk_ids(list(expected)):
                raise RagStoreError("News lexical index is incomplete after reconciliation")
            self._reconciliation_details["repaired_lexical_chunks"] = len(missing_ids)
        stats.written_chunks = len(chunk_records)
        result = stats.to_model()
        result.details = {"missing_vector_chunks": len(missing_vectors),
                          "repaired_vector_chunks": len(chunk_records),
                          "missing_lexical_chunks": len(missing_ids) if self.lexical_store.enabled else None,
                          "repaired_lexical_chunks": len(missing_ids) if self.lexical_store.enabled else None}
        logger.info("RAG indexing completed", extra=stats.to_model().model_dump())
        return result

    def close(self) -> None:
        self.lexical_store.close()

    def _build_chunk_record(
        self,
        *,
        record: NewsArticleRecord,
        chunk_index: int,
        chunk_text: str,
        embedding: list[float],
    ) -> RagChunkRecord:
        symbols = list(record.symbols)
        entities = list(record.entities)
        if self.symbol_matcher is not None:
            matched_symbols, matched_entities = self.symbol_matcher.extract(
                record.title,
                record.summary,
                record.content,
            )
            symbols = sorted(set(symbols) | set(matched_symbols))
            entities = sorted(set(entities) | set(matched_entities))
        chunk_hash = sha256_text(
            "|".join(
                [
                    record.content_hash,
                    str(chunk_index),
                    chunk_text.casefold(),
                ]
            )
        )
        return RagChunkRecord(
            chunk_id=str(uuid5(NAMESPACE_URL, chunk_hash)),
            news_id=record.news_id,
            external_news_id=record.external_news_id,
            provider=record.provider,
            source_name=record.source_name,
            title=record.title,
            summary=record.summary,
            content=record.content,
            url=str(record.url),
            published_at=record.published_at,
            ingested_at=record.ingested_at,
            language=record.language,
            symbols=symbols,
            entities=entities,
            content_hash=record.content_hash,
            raw_object_path=record.raw_object_path,
            chunk_index=chunk_index,
            chunk_text=chunk_text,
            embedding=embedding,
        )


def main() -> None:
    """CLI entry point for local indexing."""

    indexer = NewsRAGIndexer()
    stats = indexer.run()
    print(json.dumps(stats.model_dump(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
