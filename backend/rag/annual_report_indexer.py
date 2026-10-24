"""Build the annual-report vector index from Silver sections."""

from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from backend.lakehouse.annual_report_writer import AnnualReportWriter
from backend.rag.annual_report_store import AnnualReportVectorStore
from backend.rag.chunking import split_text
from backend.rag.embeddings import EmbeddingService
from backend.schemas.annual_report import AnnualReportSectionRecord
from backend.utils.hash import sha256_text


class AnnualReportRAGIndexer:
    def __init__(self, *, writer: AnnualReportWriter | None = None, store: AnnualReportVectorStore | None = None, embedding_service: EmbeddingService | None = None) -> None:
        self.writer = writer or AnnualReportWriter()
        self.store = store or AnnualReportVectorStore()
        self.embedding_service = embedding_service or EmbeddingService()
        self.store.ensure_schema()

    def run(self) -> dict[str, int]:
        sections = self.writer.scan_sections()
        reports = {report.report_id: report for report in self.writer.scan_reports()}
        chunks: list[dict] = []
        for section in sections:
            report = reports.get(section.report_id)
            for index, text in enumerate(split_text(section.text)):
                content_hash = sha256_text(f"{section.section_id}|{index}|{text}")
                chunks.append({
                    "chunk_id": str(uuid5(NAMESPACE_URL, content_hash)), "report_id": section.report_id,
                    "stock_code": section.stock_code, "report_year": section.report_year, "company_name": None,
                    "section_path": section.section_path, "page_start": section.start_page, "page_end": section.end_page,
                    "chunk_text": text, "content_hash": content_hash,
                    "announcement_time": report.announcement_time if report else None,
                    "ingested_at": report.ingested_at if report else None,
                })
        existing = self.store.existing_hashes([item["content_hash"] for item in chunks])
        pending = [item for item in chunks if item["content_hash"] not in existing]
        embeddings = self.embedding_service.embed_documents([item["chunk_text"] for item in pending])
        for item, embedding in zip(pending, embeddings, strict=True):
            item["embedding"] = embedding
        written = self.store.upsert(pending) if pending else 0
        return {"scanned_sections": len(sections), "chunk_count": len(chunks), "skipped_existing": len(existing), "written_chunks": written}
