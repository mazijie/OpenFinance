"""pgvector storage for annual-report chunks."""

from __future__ import annotations

import re
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

try:
    import psycopg
    from pgvector import Vector
    from pgvector.psycopg import register_vector
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None
    Vector = list  # type: ignore[assignment]
    register_vector = None
    dict_row = None

from backend.config import settings
from backend.schemas.annual_report import AnnualReportSearchHit


class AnnualReportStoreError(Exception):
    """Raised when the annual-report vector store is unavailable."""


class AnnualReportVectorStore:
    def __init__(self, dsn: str | None = None) -> None:
        if psycopg is None or register_vector is None or dict_row is None:
            raise AnnualReportStoreError("pgvector dependencies are not installed")
        self.dsn = dsn or settings.DATABASE_URL
        self.schema = settings.rag_schema
        self.table = settings.RAG_ANNUAL_REPORT_VECTOR_TABLE
        if not self.dsn or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.table):
            raise AnnualReportStoreError("Annual-report vector configuration is invalid")

    @property
    def fqtn(self) -> str:
        return f"{self.schema}.{self.table}"

    def ensure_schema(self) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.fqtn} (
                chunk_id text PRIMARY KEY, report_id text NOT NULL, stock_code text NOT NULL,
                report_year integer NOT NULL, company_name text, section_path text NOT NULL,
                page_start integer NOT NULL, page_end integer NOT NULL, chunk_text text NOT NULL,
                embedding vector({settings.resolved_embedding_dimension}) NOT NULL,
                content_hash text NOT NULL, UNIQUE(report_id, content_hash)
            )""")
            cur.execute(
                f"ALTER TABLE {self.fqtn} ADD COLUMN IF NOT EXISTS announcement_time timestamptz"
            )
            cur.execute(
                f"ALTER TABLE {self.fqtn} ADD COLUMN IF NOT EXISTS ingested_at timestamptz"
            )
            cur.execute(f"CREATE INDEX IF NOT EXISTS {self.table}_embedding_idx ON {self.fqtn} USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)")

    def existing_hashes(self, hashes: list[str]) -> set[str]:
        if not hashes:
            return set()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT content_hash FROM {self.fqtn} WHERE content_hash = ANY(%s)", (hashes,))
            return {str(row[0]) for row in cur.fetchall()}

    def upsert(self, chunks: list[dict[str, Any]]) -> int:
        sql = f"""INSERT INTO {self.fqtn} (chunk_id, report_id, stock_code, report_year, company_name, section_path, page_start, page_end, chunk_text, embedding, content_hash, announcement_time, ingested_at)
            VALUES (%(chunk_id)s, %(report_id)s, %(stock_code)s, %(report_year)s, %(company_name)s, %(section_path)s, %(page_start)s, %(page_end)s, %(chunk_text)s, %(embedding)s, %(content_hash)s, %(announcement_time)s, %(ingested_at)s)
            ON CONFLICT (chunk_id) DO UPDATE SET
                chunk_text=EXCLUDED.chunk_text,
                embedding=EXCLUDED.embedding,
                content_hash=EXCLUDED.content_hash,
                announcement_time=EXCLUDED.announcement_time,
                ingested_at=EXCLUDED.ingested_at"""
        with self._connection() as conn, conn.cursor() as cur:
            for chunk in chunks:
                values = dict(chunk)
                values["embedding"] = Vector(values["embedding"])
                cur.execute(sql, values)
        return len(chunks)

    def search(
        self,
        *,
        embedding: list[float],
        stock_code: str | None,
        report_year: int | None,
        as_of: datetime | None,
        published_after: datetime | None,
        limit: int,
    ) -> list[AnnualReportSearchHit]:
        where = ["TRUE"]
        params: list[Any] = []
        if stock_code:
            where.append("stock_code = %s"); params.append(stock_code.strip().upper())
        if report_year:
            where.append("report_year = %s"); params.append(report_year)
        if as_of is not None:
            where.append("ingested_at IS NOT NULL AND ingested_at <= %s")
            params.append(as_of)
            where.append("announcement_time IS NOT NULL AND announcement_time <= %s")
            params.append(as_of)
        if published_after is not None:
            where.append("announcement_time >= %s")
            params.append(published_after)
        query = Vector(embedding); params.extend([query, query, limit])
        sql = f"SELECT chunk_id, report_id, stock_code, report_year, company_name, section_path, page_start, page_end, chunk_text, announcement_time, ingested_at, 1 - (embedding <=> %s) AS similarity, embedding FROM {self.fqtn} WHERE {' AND '.join(where)} ORDER BY embedding <=> %s LIMIT %s"
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, tuple(params))
            return [AnnualReportSearchHit.model_validate(row) for row in cur.fetchall()]

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        conn = psycopg.connect(self.dsn, autocommit=True)
        register_vector(conn)
        try:
            yield conn
        finally:
            conn.close()
