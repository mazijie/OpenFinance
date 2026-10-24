"""Build and maintain the RAG index from Silver news records."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, asdict
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from backend.config import settings
from backend.lakehouse.news_writer import NewsWriter
from backend.logging_utils import get_logger
from backend.rag.chunking import build_source_text, has_rag_value, split_text
from backend.rag.embeddings import EmbeddingService, RagEmbeddingError
from backend.rag.lexical_store import OpenSearchNewsStore
from backend.rag.vector_store import PgVectorNewsStore, RagStoreError
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

        source_records = list(records) if records is not None else self.news_writer.scan_records()
        stats = RagBuildStats(scanned_articles=len(source_records))

        eligible_records = [record for record in source_records if has_rag_value(record)]
        stats.skipped_articles = len(source_records) - len(eligible_records)

        if not eligible_records:
            return stats.to_model()

        if rebuild_all:
            self.vector_store.clear()
            self.lexical_store.clear()
        content_hashes = [record.content_hash for record in eligible_records]
        existing_hashes = self.vector_store.existing_content_hashes(content_hashes)
        lexical_hashes = self.lexical_store.existing_content_hashes(content_hashes)
        records_to_embed = [record for record in eligible_records if record.content_hash not in existing_hashes]
        backfill_hashes = existing_hashes - lexical_hashes if self.lexical_store.enabled else set()
        stats.skipped_existing = len(eligible_records) - len(records_to_embed) - len(backfill_hashes)
        if not records_to_embed and not backfill_hashes:
            return stats.to_model()

        chunk_groups: list[tuple[NewsArticleRecord, list[str]]] = []
        for record in records_to_embed:
            source_text = build_source_text(record)
            if not source_text:
                stats.skipped_articles += 1
                continue
            chunks = split_text(source_text)
            if not chunks:
                stats.skipped_articles += 1
                continue
            chunk_groups.append((record, chunks))

        flattened_chunks: list[tuple[NewsArticleRecord, int, str]] = []
        for record, chunks in chunk_groups:
            for chunk_index, chunk_text in enumerate(chunks):
                flattened_chunks.append((record, chunk_index, chunk_text))

        stats.chunk_count = len(flattened_chunks) + len(self.vector_store.chunks_for_content_hashes(backfill_hashes))

        try: embeddings = self.embedding_service.embed_documents([chunk_text for _, _, chunk_text in flattened_chunks]) if flattened_chunks else []
        except RagEmbeddingError:
            stats.embeddings_failed = len(flattened_chunks); return stats.to_model()

        if len(embeddings) != len(flattened_chunks):
            stats.errors += 1
            return stats.to_model()

        chunk_records = [
            self._build_chunk_record(record=record, chunk_index=chunk_index, chunk_text=chunk_text, embedding=embedding)
            for (record, chunk_index, chunk_text), embedding in zip(flattened_chunks, embeddings, strict=True)
        ]

        chunk_records.extend(self.vector_store.chunks_for_content_hashes(backfill_hashes))
        # are allowed to fail the job instead of silently creating a stale hybrid index.
        if records_to_embed: self.vector_store.upsert_chunks(chunk_records[:len(flattened_chunks)])
        self.lexical_store.upsert_chunks(chunk_records)
        stats.written_chunks = len(chunk_records)
        logger.info("RAG indexing completed", extra=stats.to_model().model_dump())
        return stats.to_model()

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
