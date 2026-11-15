"""pgvector-backed news chunk store."""

from __future__ import annotations

import re
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Iterator

try:  # pragma: no cover - exercised in real environment
    import psycopg
    from pgvector import Vector
    from pgvector.psycopg import register_vector
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - test fallback
    psycopg = None  # type: ignore[assignment]
    Vector = list  # type: ignore[assignment]
    register_vector = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]

from backend.config import settings
from backend.logging_utils import get_logger
from backend.schemas.rag import RagChunkRecord, RagSearchHit
from backend.utils.time import day_end_utc, day_start_utc
from backend.rag.vector_schema import ensure_vector_dimension

logger = get_logger(__name__)


class RagStoreError(Exception):
    """Raised when the vector store cannot be accessed."""


class PgVectorNewsStore:
    """Persist and query RAG chunks in PostgreSQL using pgvector."""

    def __init__(self, dsn: str | None = None) -> None:
        if psycopg is None or register_vector is None or dict_row is None:
            raise RagStoreError("psycopg/pgvector dependencies are not installed")
        self.dsn = dsn or settings.DATABASE_URL
        if not self.dsn:
            raise RagStoreError("DATABASE_URL is not configured")
        self.schema = settings.rag_schema
        self.table = settings.RAG_VECTOR_TABLE
        self.dimension = settings.resolved_embedding_dimension
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.schema) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.table):
            raise RagStoreError("RAG schema/table names must be safe SQL identifiers")
        if self.dimension <= 0:
            raise RagStoreError("Embedding dimension must be positive")

    def ensure_schema(self) -> None:
        """Create schema, extension, table and index if they do not exist."""

        with self._connection(register_types=False) as conn, conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            ensure_vector_dimension(
                cur,
                schema=self.schema,
                table=self.table,
                dimension=self.dimension,
                create_table_sql=self._create_table_sql,
                error_type=RagStoreError,
            )
            cur.execute(self._create_table_sql())
            cur.execute(self._create_index_sql())
        logger.info("RAG vector schema ensured", extra={"schema": self.schema, "table": self.table})

    def existing_content_hashes(self, content_hashes: Iterable[str]) -> set[str]:
        """Return content hashes already stored in the vector table."""

        wanted = [value for value in content_hashes if value]
        if not wanted:
            return set()

        sql = f"SELECT DISTINCT content_hash FROM {self.fqtn} WHERE content_hash = ANY(%s)"
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (wanted,))
            return {str(row[0]) for row in cur.fetchall() if row and row[0]}

    def chunks_for_content_hashes(self, content_hashes: Iterable[str]) -> list[RagChunkRecord]:
        wanted = [value for value in content_hashes if value]
        if not wanted: return []
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT chunk_id, news_id, external_news_id, provider, source_name, title, summary, content, url, published_at, ingested_at, language, symbols, entities, content_hash, raw_object_path, chunk_index, chunk_text, embedding FROM {self.fqtn} WHERE content_hash = ANY(%s)", (wanted,))
            rows = cur.fetchall()
            for row in rows:
                if hasattr(row["embedding"], "to_list"):
                    row["embedding"] = row["embedding"].to_list()
                elif hasattr(row["embedding"], "tolist"):
                    row["embedding"] = row["embedding"].tolist()
            return [RagChunkRecord.model_validate(row) for row in rows]

    def clear(self) -> None:
        with self._connection() as conn, conn.cursor() as cur: cur.execute(f"TRUNCATE TABLE {self.fqtn}")

    def upsert_chunks(self, chunks: list[RagChunkRecord]) -> int:
        """Insert or update vector chunks."""

        if not chunks:
            return 0

        sql = f"""
            INSERT INTO {self.fqtn} (
                chunk_id,
                news_id,
                external_news_id,
                provider,
                source_name,
                title,
                summary,
                content,
                url,
                published_at,
                ingested_at,
                language,
                symbols,
                entities,
                content_hash,
                raw_object_path,
                chunk_index,
                chunk_text,
                embedding
            )
            VALUES (
                %(chunk_id)s,
                %(news_id)s,
                %(external_news_id)s,
                %(provider)s,
                %(source_name)s,
                %(title)s,
                %(summary)s,
                %(content)s,
                %(url)s,
                %(published_at)s,
                %(ingested_at)s,
                %(language)s,
                %(symbols)s,
                %(entities)s,
                %(content_hash)s,
                %(raw_object_path)s,
                %(chunk_index)s,
                %(chunk_text)s,
                %(embedding)s
            )
            ON CONFLICT (chunk_id) DO UPDATE SET
                news_id = EXCLUDED.news_id,
                external_news_id = EXCLUDED.external_news_id,
                provider = EXCLUDED.provider,
                source_name = EXCLUDED.source_name,
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                content = EXCLUDED.content,
                url = EXCLUDED.url,
                published_at = EXCLUDED.published_at,
                ingested_at = EXCLUDED.ingested_at,
                language = EXCLUDED.language,
                symbols = EXCLUDED.symbols,
                entities = EXCLUDED.entities,
                content_hash = EXCLUDED.content_hash,
                raw_object_path = EXCLUDED.raw_object_path,
                chunk_index = EXCLUDED.chunk_index,
                chunk_text = EXCLUDED.chunk_text,
                embedding = EXCLUDED.embedding
        """

        with self._connection() as conn, conn.cursor() as cur:
            for chunk in chunks:
                cur.execute(sql, self._chunk_to_params(chunk))
        logger.info("RAG chunks upserted", extra={"count": len(chunks)})
        return len(chunks)

    def search_chunks(
        self,
        *,
        query_embedding: list[float],
        symbol: str | None,
        start_date: date,
        end_date: date,
        as_of: datetime | None = None,
        limit: int = 5,
    ) -> list[RagSearchHit]:
        """Run semantic search against the vector table."""

        published_before = min(day_end_utc(end_date), as_of) if as_of is not None else day_end_utc(end_date)
        where_clauses = ["published_at >= %s", "published_at <= %s"]
        params: list[Any] = [day_start_utc(start_date), published_before]
        if as_of is not None:
            where_clauses.append("ingested_at <= %s")
            params.append(as_of)
        if symbol:
            where_clauses.append("symbols && %s")
            params.append([symbol.strip().upper()])

        query_vector = Vector(query_embedding)
        # The first vector placeholder appears in SELECT, before WHERE.
        params.insert(0, query_vector)
        params.append(query_vector)
        params.append(limit)
        sql = f"""
            SELECT
                chunk_id,
                news_id,
                external_news_id,
                provider,
                source_name,
                title,
                summary,
                content,
                url,
                published_at,
                ingested_at,
                language,
                symbols,
                entities,
                content_hash,
                raw_object_path,
                chunk_index,
                chunk_text,
                embedding,
                1 - (embedding <=> %s) AS similarity
            FROM {self.fqtn}
            WHERE {" AND ".join(where_clauses)}
            ORDER BY embedding <=> %s
            LIMIT %s
        """

        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, tuple(params))
            return [RagSearchHit.model_validate(row) for row in cur.fetchall()]

    @property
    def fqtn(self) -> str:
        """Return the fully qualified table name."""

        return f"{self.schema}.{self.table}"

    @contextmanager
    def _connection(self, *, register_types: bool = True) -> Iterator[Any]:
        if psycopg is None or register_vector is None:
            raise RagStoreError("psycopg/pgvector dependencies are not installed")
        conn = psycopg.connect(self.dsn, autocommit=True)
        if register_types:
            register_vector(conn)
        try:
            yield conn
        finally:
            conn.close()

    def _create_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.fqtn} (
                chunk_id text PRIMARY KEY,
                news_id text NOT NULL,
                external_news_id text,
                provider text NOT NULL,
                source_name text,
                title text NOT NULL,
                summary text,
                content text,
                url text NOT NULL,
                published_at timestamptz NOT NULL,
                ingested_at timestamptz NOT NULL,
                language text,
                symbols text[] NOT NULL DEFAULT '{{}}',
                entities text[] NOT NULL DEFAULT '{{}}',
                content_hash text NOT NULL,
                raw_object_path text NOT NULL,
                chunk_index integer NOT NULL,
                chunk_text text NOT NULL,
                embedding vector({self.dimension}) NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                UNIQUE (content_hash, chunk_index)
            )
        """

    def _create_index_sql(self) -> str:
        return f"""
            CREATE INDEX IF NOT EXISTS {self.table}_embedding_idx
            ON {self.fqtn}
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100)
        """

    def _chunk_to_params(self, chunk: RagChunkRecord) -> dict[str, Any]:
        return {
            "chunk_id": chunk.chunk_id,
            "news_id": chunk.news_id,
            "external_news_id": chunk.external_news_id,
            "provider": chunk.provider,
            "source_name": chunk.source_name,
            "title": chunk.title,
            "summary": chunk.summary,
            "content": chunk.content,
            "url": chunk.url,
            "published_at": chunk.published_at,
            "ingested_at": chunk.ingested_at,
            "language": chunk.language,
            "symbols": chunk.symbols,
            "entities": chunk.entities,
            "content_hash": chunk.content_hash,
            "raw_object_path": chunk.raw_object_path,
            "chunk_index": chunk.chunk_index,
            "chunk_text": chunk.chunk_text,
            "embedding": Vector(chunk.embedding),
        }
