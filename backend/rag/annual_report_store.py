"""pgvector storage for annual-report chunks."""

from __future__ import annotations

import hashlib
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
        expected_columns = {
            "generation_id", "chunk_id", "report_id", "parse_run_id", "stock_code", "report_year",
            "company_name", "section_path", "content_type", "region_ids",
            "page_start", "page_end", "chunk_text", "embedding", "content_hash",
            "embedding_model", "chunker_version", "announcement_time", "ingested_at",
        }
        with self._connection(register_types=False) as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (self.fqtn,))
            cur.execute(
                "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
                (self.schema, self.table),
            )
            column_rows = cur.fetchall()
            existing = {row[0] for row in column_rows}
            nullable_required = any(row[1] == "YES" and row[0] != "company_name" for row in column_rows)
            cur.execute(
                "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
                "JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=%s AND c.relname=%s AND a.attname='embedding' AND NOT a.attisdropped",
                (self.schema, self.table),
            )
            vector_type = cur.fetchone()
            wrong_dimension = vector_type is not None and vector_type[0] not in {"vector", f"vector({settings.resolved_embedding_dimension})"}
            if existing and (existing != expected_columns or wrong_dimension or nullable_required):
                raise AnnualReportStoreError("Annual-report vector schema mismatch; existing data was preserved")
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.fqtn} (
                generation_id text NOT NULL, chunk_id text NOT NULL, report_id text NOT NULL, parse_run_id text NOT NULL,
                stock_code text NOT NULL, report_year integer NOT NULL, company_name text,
                section_path text NOT NULL, content_type text NOT NULL,
                region_ids text[] NOT NULL, page_start integer NOT NULL,
                page_end integer NOT NULL, chunk_text text NOT NULL,
                embedding vector NOT NULL,
                content_hash text NOT NULL, embedding_model text NOT NULL, chunker_version text NOT NULL,
                announcement_time timestamptz NOT NULL, ingested_at timestamptz NOT NULL,
                PRIMARY KEY (generation_id, chunk_id), UNIQUE(generation_id, report_id, content_hash)
            )""")
            cur.execute(f"CREATE INDEX IF NOT EXISTS {self.table}_filters_idx ON {self.fqtn} (generation_id, stock_code, report_year)")

    def retain_chunks(self, chunk_ids: list[str]) -> None:
        """Keep only current chunks from pages that passed parsing checks."""
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self.fqtn} WHERE NOT (chunk_id = ANY(%s))",
                (chunk_ids,),
            )

    def existing_hashes(self, hashes: list[str], generation_id: str | None = None) -> set[str]:
        if not hashes:
            return set()
        with self._connection() as conn, conn.cursor() as cur:
            if generation_id:
                cur.execute(f"SELECT content_hash FROM {self.fqtn} WHERE generation_id=%s AND content_hash = ANY(%s)", (generation_id, hashes))
            else:
                cur.execute(f"SELECT content_hash FROM {self.fqtn} WHERE content_hash = ANY(%s)", (hashes,))
            return {str(row[0]) for row in cur.fetchall()}

    def existing_chunk_ids(self, chunk_ids: list[str], generation_id: str) -> set[str]:
        if not chunk_ids:
            return set()
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT chunk_id FROM {self.fqtn} WHERE generation_id=%s AND chunk_id=ANY(%s)",
                (generation_id, chunk_ids),
            )
            return {str(row[0]) for row in cur.fetchall()}

    def seed_generation(
        self,
        *,
        source_generation: str,
        target_generation: str,
        chunk_ids: list[str],
        embedding_model: str,
        chunker_version: str,
    ) -> int:
        if not chunk_ids:
            return 0
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""INSERT INTO {self.fqtn} (
                    generation_id, chunk_id, report_id, parse_run_id, stock_code, report_year,
                    company_name, section_path, content_type, region_ids, page_start, page_end,
                    chunk_text, embedding, content_hash, embedding_model, chunker_version,
                    announcement_time, ingested_at
                ) SELECT %s, chunk_id, report_id, parse_run_id, stock_code, report_year,
                    company_name, section_path, content_type, region_ids, page_start, page_end,
                    chunk_text, embedding, content_hash, embedding_model, chunker_version,
                    announcement_time, ingested_at
                FROM {self.fqtn}
                WHERE generation_id=%s AND chunk_id=ANY(%s)
                    AND embedding_model=%s AND chunker_version=%s
                ON CONFLICT (generation_id, chunk_id) DO NOTHING""",
                (target_generation, source_generation, chunk_ids, embedding_model, chunker_version),
            )
            return cur.rowcount

    def contains_chunk_ids(self, generation_id: str, chunk_ids: list[str]) -> bool:
        if not chunk_ids:
            return True
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT chunk_id FROM {self.fqtn} WHERE generation_id=%s AND chunk_id=ANY(%s)",
                (generation_id, chunk_ids),
            )
            return {str(row[0]) for row in cur.fetchall()} == set(chunk_ids)

    def embedding_for(self, generation_id: str, chunk_id: str) -> list[float]:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT embedding FROM {self.fqtn} WHERE generation_id=%s AND chunk_id=%s",
                (generation_id, chunk_id),
            )
            row = cur.fetchone()
            if row is None:
                raise AnnualReportStoreError(f"Missing vector for chunk {chunk_id} in generation {generation_id}")
            value = row[0]
            return [float(item) for item in (value.to_list() if hasattr(value, "to_list") else value)]

    def clear(self) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {self.fqtn}")

    def upsert(self, chunks: list[dict[str, Any]], generation_id: str | None = None) -> int:
        sql = f"""INSERT INTO {self.fqtn} (generation_id, chunk_id, report_id, parse_run_id, stock_code, report_year, company_name, section_path, content_type, region_ids, page_start, page_end, chunk_text, embedding, content_hash, embedding_model, chunker_version, announcement_time, ingested_at)
            VALUES (%(generation_id)s, %(chunk_id)s, %(report_id)s, %(parse_run_id)s, %(stock_code)s, %(report_year)s, %(company_name)s, %(section_path)s, %(content_type)s, %(region_ids)s, %(page_start)s, %(page_end)s, %(chunk_text)s, %(embedding)s, %(content_hash)s, %(embedding_model)s, %(chunker_version)s, %(announcement_time)s, %(ingested_at)s)
            ON CONFLICT (generation_id, chunk_id) DO UPDATE SET
                parse_run_id=EXCLUDED.parse_run_id,
                chunk_text=EXCLUDED.chunk_text,
                content_type=EXCLUDED.content_type,
                region_ids=EXCLUDED.region_ids,
                embedding=EXCLUDED.embedding,
                content_hash=EXCLUDED.content_hash,
                embedding_model=EXCLUDED.embedding_model,
                chunker_version=EXCLUDED.chunker_version,
                announcement_time=EXCLUDED.announcement_time,
                ingested_at=EXCLUDED.ingested_at"""
        with self._connection() as conn, conn.cursor() as cur:
            for chunk in chunks:
                values = dict(chunk)
                values["generation_id"] = generation_id or values.get("generation_id") or "legacy"
                values.setdefault("embedding_model", settings.resolved_embedding_model)
                values.setdefault("chunker_version", "annual-report-chunker-v1")
                values["embedding"] = Vector(values["embedding"])
                cur.execute(sql, values)
        return len(chunks)

    def count_generation(self, generation_id: str) -> int:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {self.fqtn} WHERE generation_id=%s", (generation_id,))
            return int(cur.fetchone()[0])

    def delete_generation(self, generation_id: str) -> None:
        self._validate_generation_id(generation_id)
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(f"DROP INDEX IF EXISTS {self.schema}.{self._generation_index_name(generation_id)}")
            cur.execute(f"DELETE FROM {self.fqtn} WHERE generation_id=%s", (generation_id,))

    def create_generation_index(self, generation_id: str) -> None:
        self._validate_generation_id(generation_id)
        index_name = self._generation_index_name(generation_id)
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {index_name} ON {self.fqtn} "
                f"USING hnsw ((embedding::vector({int(settings.resolved_embedding_dimension)})) vector_cosine_ops) WHERE generation_id='{generation_id}'"
            )

    @staticmethod
    def _validate_generation_id(generation_id: str) -> None:
        if not re.fullmatch(r"[a-f0-9]{32}(?:-(?:rebuild|repair))?", generation_id):
            raise AnnualReportStoreError("Invalid annual-report index generation id")

    def _generation_index_name(self, generation_id: str) -> str:
        self._validate_generation_id(generation_id)
        suffix = hashlib.sha256(generation_id.encode("ascii")).hexdigest()[:16]
        return f"{self.table[:38]}_emb_{suffix}"

    def search(
        self,
        *,
        embedding: list[float],
        stock_code: str | None,
        report_year: int | None,
        section_path: str | None = None,
        content_types: list[str] | None = None,
        page_start: int | None = None,
        page_end: int | None = None,
        as_of: datetime | None,
        published_after: datetime | None,
        limit: int,
        generation_id: str | None = None,
        report_id: str | None = None,
    ) -> list[AnnualReportSearchHit]:
        where = ["TRUE"]
        params: list[Any] = []
        if generation_id:
            self._validate_generation_id(generation_id)
            where.append(f"generation_id = '{generation_id}'")
        if report_id:
            where.append("report_id = %s"); params.append(report_id)
        if stock_code:
            where.append("stock_code = %s"); params.append(stock_code.strip().upper())
        if report_year:
            where.append("report_year = %s"); params.append(report_year)
        if section_path:
            where.append("section_path = %s"); params.append(section_path)
        if content_types:
            where.append("content_type = ANY(%s)"); params.append(content_types)
        if page_start is not None:
            where.append("page_end >= %s"); params.append(page_start)
        if page_end is not None:
            where.append("page_start <= %s"); params.append(page_end)
        if as_of is not None:
            where.append("ingested_at IS NOT NULL AND ingested_at <= %s")
            params.append(as_of)
            where.append("announcement_time IS NOT NULL AND announcement_time <= %s")
            params.append(as_of)
        if published_after is not None:
            where.append("announcement_time >= %s")
            params.append(published_after)
        query = Vector(embedding)
        params = [query, *params, query, limit]
        dimension = int(settings.resolved_embedding_dimension)
        sql = f"SELECT chunk_id, report_id, parse_run_id, stock_code, report_year, company_name, section_path, content_type, region_ids, page_start, page_end, chunk_text, announcement_time, ingested_at, 1 - (embedding <=> %s) AS similarity, embedding FROM {self.fqtn} WHERE {' AND '.join(where)} ORDER BY embedding::vector({dimension}) <=> %s LIMIT %s"
        with self._connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            for row in rows:
                if row.get("embedding") is not None:
                    value = row["embedding"]
                    row["embedding"] = [float(item) for item in (value.to_list() if hasattr(value, "to_list") else value)]
            return [AnnualReportSearchHit.model_validate(row) for row in rows]

    @contextmanager
    def _connection(self, *, register_types: bool = True) -> Iterator[Any]:
        conn = psycopg.connect(self.dsn, autocommit=True)
        if register_types:
            register_vector(conn)
        try:
            yield conn
        finally:
            conn.close()
