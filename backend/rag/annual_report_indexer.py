"""Build the annual-report vector index from Silver sections."""

from __future__ import annotations

import hashlib

from backend.config import settings
from backend.lakehouse.annual_report_writer import AnnualReportWriter
from backend.pipelines.annual_report_pipeline_state import AnnualReportPipelineStateStore, StageLease
from backend.rag.annual_report_store import AnnualReportVectorStore
from backend.rag.annual_report_lexical_store import AnnualReportLexicalStore
from backend.rag.embeddings import EmbeddingService


class AnnualReportRAGIndexer:
    CHUNKER_VERSION = "annual-report-chunker-v1"

    def __init__(self, *, writer: AnnualReportWriter | None = None, store: AnnualReportVectorStore | None = None, embedding_service: EmbeddingService | None = None, lexical_store: AnnualReportLexicalStore | None = None, state_store: AnnualReportPipelineStateStore | None = None) -> None:
        self._versioned = state_store is not None or all(item is None for item in (writer, store, embedding_service, lexical_store))
        self.writer = writer or AnnualReportWriter()
        self.store = store or AnnualReportVectorStore()
        self.embedding_service = embedding_service or EmbeddingService()
        self.lexical_store = lexical_store or AnnualReportLexicalStore()
        self.state_store = state_store
        if self._versioned and self.state_store is None:
            self.state_store = AnnualReportPipelineStateStore()
        self.store.ensure_schema()
        self.lexical_store.ensure_index()
        if self.state_store is not None:
            self.state_store.ensure_schema()

    def close(self) -> None:
        self.embedding_service.close()
        self.lexical_store.close()

    def run(self, *, rebuild_all: bool = False) -> dict[str, int | str]:
        if self._versioned:
            return self._run_versioned(rebuild_all=rebuild_all)
        if rebuild_all:
            self.store.clear()
            self.lexical_store.clear()
        chunks_from_silver = self.writer.scan_chunks()
        reports = {report.report_id: report for report in self.writer.scan_reports()}
        pages = self.writer.scan_pages()
        page_status = {(page.report_id, page.parse_run_id, page.page_number): page.extraction_status for page in pages}
        chunks: list[dict] = []
        skipped_review_pages: set[tuple[str, int]] = {(page.report_id, page.page_number) for page in pages if page.report_id in reports and page.parse_run_id == reports[page.report_id].parse_run_id and page.extraction_status != "ok"}
        for chunk in chunks_from_silver:
            report = reports.get(chunk.report_id)
            if report is None:
                raise ValueError(f"Annual-report chunk {chunk.chunk_id} has no Silver report")
            if chunk.parse_run_id != report.parse_run_id:
                continue
            source_pages = range(chunk.page_start, chunk.page_end + 1)
            statuses = [page_status[(chunk.report_id, chunk.parse_run_id, number)] for number in source_pages]
            if any(status != "ok" for status in statuses):
                skipped_review_pages.update((chunk.report_id, number) for number in source_pages if page_status[(chunk.report_id, chunk.parse_run_id, number)] != "ok")
                continue
            chunks.append({
                "chunk_id": chunk.chunk_id,
                "report_id": chunk.report_id,
                "parse_run_id": chunk.parse_run_id,
                "stock_code": report.stock_code,
                "report_year": report.report_year,
                "company_name": report.company_name,
                "section_path": chunk.section_path,
                "content_type": chunk.content_type,
                "region_ids": chunk.region_ids,
                "page_start": chunk.page_start,
                "page_end": chunk.page_end,
                "chunk_text": chunk.chunk_text,
                "content_hash": chunk.content_hash,
                "announcement_time": report.announcement_time,
                "ingested_at": report.ingested_at,
            })
        current_ids = [item["chunk_id"] for item in chunks]
        self.store.retain_chunks(current_ids)
        self.lexical_store.retain_chunks(current_ids)
        existing = self.store.existing_hashes([item["content_hash"] for item in chunks])
        pending = [item for item in chunks if item["content_hash"] not in existing]
        embeddings = self.embedding_service.embed_documents([item["chunk_text"] for item in pending])
        for item, embedding in zip(pending, embeddings, strict=True):
            item["embedding"] = embedding
        written = self.store.upsert(pending) if pending else 0
        # Re-upsert all active lexical documents: a failed OpenSearch bulk must
        # recover even if the preceding pgvector write already succeeded.
        lexical_written = self.lexical_store.upsert(chunks) if chunks else 0
        return {
            "scanned_sections": len(chunks_from_silver),
            "chunk_count": len(chunks),
            "skipped_review_pages": len(skipped_review_pages),
            "skipped_existing": len(existing),
            "written_chunks": written,
            "written_lexical_chunks": lexical_written,
        }

    def _run_versioned(self, *, rebuild_all: bool) -> dict[str, int]:
        chunks_from_silver = self.writer.scan_chunks()
        committed_run_ids = {item.parse_run_id for item in self.writer.scan_parse_runs()}
        reports = {
            report.report_id: report
            for report in self.writer.scan_reports()
            if report.parse_run_id in committed_run_ids
        }
        pages = self.writer.scan_pages()
        page_status = {(page.report_id, page.parse_run_id, page.page_number): page.extraction_status for page in pages}
        chunks: list[dict] = []
        skipped_review_pages: set[tuple[str, int]] = {
            (page.report_id, page.page_number)
            for page in pages
            if page.report_id in reports
            and page.parse_run_id == reports[page.report_id].parse_run_id
            and page.extraction_status != "ok"
        }
        for chunk in chunks_from_silver:
            report = reports.get(chunk.report_id)
            if report is None or chunk.parse_run_id != report.parse_run_id:
                continue
            source_pages = range(chunk.page_start, chunk.page_end + 1)
            statuses = [page_status.get((chunk.report_id, chunk.parse_run_id, number), "needs_review") for number in source_pages]
            if any(status != "ok" for status in statuses):
                skipped_review_pages.update(
                    (chunk.report_id, number)
                    for number in source_pages
                    if page_status.get((chunk.report_id, chunk.parse_run_id, number), "needs_review") != "ok"
                )
                continue
            chunks.append({
                "chunk_id": chunk.chunk_id,
                "report_id": chunk.report_id,
                "parse_run_id": chunk.parse_run_id,
                "stock_code": report.stock_code,
                "report_year": report.report_year,
                "company_name": report.company_name,
                "section_path": chunk.section_path,
                "content_type": chunk.content_type,
                "region_ids": chunk.region_ids,
                "page_start": chunk.page_start,
                "page_end": chunk.page_end,
                "chunk_text": chunk.chunk_text,
                "content_hash": chunk.content_hash,
                "announcement_time": report.announcement_time,
                "ingested_at": report.ingested_at,
                "embedding_model": self.embedding_service.model_name,
                "chunker_version": self.CHUNKER_VERSION,
            })
        snapshot = "|".join(sorted(f"{item['chunk_id']}:{item['content_hash']}" for item in chunks))
        generation_id = hashlib.sha256(
            f"{self.embedding_service.model_name}|{self.embedding_service.dimension}|{self.CHUNKER_VERSION}|{snapshot}".encode("utf-8")
        ).hexdigest()[:32]
        if rebuild_all:
            generation_id = f"{generation_id}-rebuild"
        assert self.state_store is not None
        active = self.state_store.get_active_generation()
        leases: list[StageLease] = []
        stage_version = f"{self.embedding_service.model_name}:{self.embedding_service.dimension}:{self.CHUNKER_VERSION}"
        try:
            for report_id in sorted({item["report_id"] for item in chunks}):
                report = reports[report_id]
                leases.append(self.state_store.claim_stage(
                    report_id=report_id,
                    pdf_sha256=report.pdf_sha256,
                    stage="index",
                    stage_version=stage_version,
                ))
            if active and active["generation_id"] == generation_id:
                count = self.store.count_generation(generation_id)
                lexical_count = len(chunks) if not self.lexical_store.enabled else self.lexical_store.count_generation(generation_id)
                if count == len(chunks) and lexical_count == len(chunks):
                    for lease in leases:
                        self.state_store.complete_stage(lease, output_ref=generation_id)
                    return {
                        "scanned_sections": len(chunks_from_silver),
                        "chunk_count": len(chunks),
                        "skipped_review_pages": len(skipped_review_pages),
                        "skipped_existing": len(chunks),
                        "written_chunks": 0,
                        "written_lexical_chunks": 0,
                        "generation_id": generation_id,
                    }
                generation_id = f"{generation_id}-repair"
            self.state_store.prepare_generation(
                generation_id=generation_id,
                embedding_model=self.embedding_service.model_name,
                embedding_dimension=self.embedding_service.dimension,
                chunker_version=self.CHUNKER_VERSION,
                expected_chunks=len(chunks),
            )
            if active and active["generation_id"] != generation_id and active["embedding_model"] == self.embedding_service.model_name and active["chunker_version"] == self.CHUNKER_VERSION:
                self.store.seed_generation(
                    source_generation=str(active["generation_id"]),
                    target_generation=generation_id,
                    chunk_ids=[item["chunk_id"] for item in chunks],
                    embedding_model=self.embedding_service.model_name,
                    chunker_version=self.CHUNKER_VERSION,
                )
            chunk_ids = [item["chunk_id"] for item in chunks]
            existing_ids = self.store.existing_chunk_ids(chunk_ids, generation_id=generation_id)
            pending = [item for item in chunks if item["chunk_id"] not in existing_ids]
            batch_size = max(1, settings.EMBEDDING_BATCH_SIZE)
            written = 0
            for start in range(0, len(pending), batch_size):
                batch = pending[start:start + batch_size]
                vectors = self.embedding_service.embed_documents([item["chunk_text"] for item in batch])
                for item, embedding in zip(batch, vectors, strict=True):
                    item["embedding"] = embedding
                    item["generation_id"] = generation_id
                written += self.store.upsert(batch, generation_id=generation_id)
            lexical_written = self.lexical_store.upsert(chunks, generation_id=generation_id)
            vector_count = self.store.count_generation(generation_id)
            lexical_count = len(chunks) if not self.lexical_store.enabled else self.lexical_store.count_generation(generation_id)
            if vector_count != len(chunks) or lexical_count != len(chunks):
                raise RuntimeError(
                    f"Index generation incomplete: expected={len(chunks)}, vector={vector_count}, lexical={lexical_count}"
                )
            self.store.create_generation_index(generation_id)
            sample_chunks = sorted(chunks, key=lambda item: item["chunk_id"])[: min(20, len(chunks))]
            sample_ids = [item["chunk_id"] for item in sample_chunks]
            if not self.store.contains_chunk_ids(generation_id, sample_ids):
                raise RuntimeError("Index generation is missing expected pgvector chunk IDs")
            if self.lexical_store.enabled and not self.lexical_store.contains_chunk_ids(generation_id, sample_ids):
                raise RuntimeError("Index generation is missing expected OpenSearch chunk IDs")
            if sample_chunks:
                sample = sample_chunks[0]
                report_chunk_count = sum(1 for item in chunks if item["report_id"] == sample["report_id"])
                vector_hits = self.store.search(
                    embedding=self.store.embedding_for(generation_id, sample["chunk_id"]),
                    stock_code=sample["stock_code"],
                    report_year=sample["report_year"],
                    section_path=sample["section_path"],
                    content_types=[sample["content_type"]],
                    page_start=sample["page_start"],
                    page_end=sample["page_end"],
                    as_of=None,
                    published_after=None,
                    limit=report_chunk_count,
                    generation_id=generation_id,
                    report_id=sample["report_id"],
                )
                if sample["chunk_id"] not in {hit.chunk_id for hit in vector_hits}:
                    raise RuntimeError("Index generation sample vector retrieval failed")
                if self.lexical_store.enabled:
                    lexical_hits = self.lexical_store.search(
                        question=sample["chunk_text"],
                        stock_code=sample["stock_code"],
                        report_year=sample["report_year"],
                        section_path=sample["section_path"],
                        content_types=[sample["content_type"]],
                        page_start=sample["page_start"],
                        page_end=sample["page_end"],
                        as_of=None,
                        published_after=None,
                        limit=report_chunk_count,
                        generation_id=generation_id,
                        report_id=sample["report_id"],
                    )
                    if sample["chunk_id"] not in {hit.chunk_id for hit in lexical_hits}:
                        raise RuntimeError("Index generation sample lexical retrieval failed")
            self.state_store.mark_generation_ready(generation_id, vector_chunks=vector_count, lexical_chunks=lexical_count)
            self.state_store.activate_generation(generation_id)
            self.lexical_store.delete_unversioned_documents()
            for lease in leases:
                self.state_store.complete_stage(lease, output_ref=generation_id)
            for expired in self.state_store.expired_generations():
                self.store.delete_generation(expired)
                self.lexical_store.delete_generation(expired)
                self.state_store.delete_generation_record(expired)
            return {
                "scanned_sections": len(chunks_from_silver),
                "chunk_count": len(chunks),
                "skipped_review_pages": len(skipped_review_pages),
                "skipped_existing": len(existing_ids),
                "written_chunks": written,
                "written_lexical_chunks": lexical_written,
                "generation_id": generation_id,
            }
        except Exception as exc:
            self.state_store.fail_generation(generation_id, exc)
            for lease in leases:
                try:
                    self.state_store.fail_stage(lease, exc)
                except Exception:
                    pass
            raise
