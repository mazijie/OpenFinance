"""Normalize annual-report Bronze manifests and PDFs into Silver."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.lakehouse.annual_report_writer import AnnualReportWriter
from backend.pipelines.annual_report_parser import AnnualReportParseError, build_report_sections, extract_report_pages
from backend.schemas.annual_report import AnnualReportBronzeObject, AnnualReportRecord


@dataclass(slots=True)
class AnnualReportPipelineStats:
    processed_objects: int = 0
    written_reports: int = 0
    written_pages: int = 0
    written_sections: int = 0
    failed_count: int = 0

    def to_dict(self) -> dict[str, int]:
        return {"processed_objects": self.processed_objects, "written_reports": self.written_reports, "written_pages": self.written_pages, "written_sections": self.written_sections, "failed_count": self.failed_count}


class AnnualReportsBronzeToSilverPipeline:
    def __init__(self, bronze_writer: BronzeWriter | None = None, report_writer: AnnualReportWriter | None = None) -> None:
        self.bronze_writer = bronze_writer or BronzeWriter()
        self.report_writer = report_writer or AnnualReportWriter()

    def run(self, object_paths: list[str] | None = None) -> AnnualReportPipelineStats:
        paths = object_paths or self.bronze_writer.list_objects(prefix="reports/provider=cninfo/")
        stats = AnnualReportPipelineStats()
        for path in paths:
            stats.processed_objects += 1
            try:
                self.process_object(path, stats)
            except Exception:
                stats.failed_count += 1
        return stats

    def process_object(self, object_path: str, stats: AnnualReportPipelineStats | None = None) -> AnnualReportPipelineStats:
        stats = stats or AnnualReportPipelineStats(processed_objects=1)
        manifest = AnnualReportBronzeObject.model_validate(self.bronze_writer.read_json(object_path))
        report_id = str(uuid5(NAMESPACE_URL, f"cninfo:{manifest.announcement.announcement_id}:{manifest.pdf_sha256}"))
        pdf = self.bronze_writer.read_bytes(manifest.pdf_object_path)
        pages = extract_report_pages(pdf, report_id=report_id, stock_code=manifest.announcement.stock_code, report_year=manifest.announcement.report_year)
        sections = build_report_sections(pages)
        extraction_status = "ok" if any(page.text for page in pages) else "needs_ocr"
        record = AnnualReportRecord(
            report_id=report_id,
            stock_code=manifest.announcement.stock_code,
            exchange=manifest.announcement.exchange,
            company_name=manifest.announcement.company_name,
            report_year=manifest.announcement.report_year,
            announcement_id=manifest.announcement.announcement_id,
            title=manifest.announcement.title,
            announcement_time=manifest.announcement.announcement_time,
            pdf_url=manifest.announcement.pdf_url,
            is_canonical=True,
            version_type="correction" if any(word in manifest.announcement.title for word in ("更正", "修订", "补充")) else "original",
            pdf_sha256=manifest.pdf_sha256,
            pdf_size=manifest.pdf_size,
            page_count=len(pages),
            extraction_status=extraction_status,
            raw_object_path=object_path,
            pdf_object_path=manifest.pdf_object_path,
            ingested_at=manifest.ingested_at,
        )
        stats.written_reports += self.report_writer.append_report(record)
        stats.written_pages += self.report_writer.append_pages(pages)
        stats.written_sections += self.report_writer.append_sections(sections)
        return stats
