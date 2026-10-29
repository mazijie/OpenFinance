"""Write parser-derived annual-report IR into minimal Iceberg Silver tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import NAMESPACE_URL, uuid5
import logging

from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.ingestion.writers.derived_writer import DerivedWriter
from backend.lakehouse.annual_report_writer import AnnualReportWriter
from backend.pipelines.annual_report_pipeline_state import AnnualReportPipelineStateStore, StageLease
from backend.rag.annual_report_chunking import build_ir_chunks
from backend.schemas.annual_report import (
    AnnualReportPageRecord,
    AnnualReportParseRunRecord,
    AnnualReportRecord,
    AnnualReportRegionRecord,
    AnnualReportSectionRecord,
)
from backend.schemas.annual_report import AnnualReportBronzeObject

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AnnualReportIRPipelineStats:
    processed_runs: int = 0
    written_reports: int = 0
    written_pages: int = 0
    written_regions: int = 0
    written_sections: int = 0
    written_chunks: int = 0
    failed_count: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "processed_runs": self.processed_runs,
            "written_reports": self.written_reports,
            "written_pages": self.written_pages,
            "written_regions": self.written_regions,
            "written_sections": self.written_sections,
            "written_chunks": self.written_chunks,
            "failed_count": self.failed_count,
        }


class AnnualReportsIRToSilverPipeline:
    def __init__(
        self,
        *,
        bronze_writer: BronzeWriter | None = None,
        derived_writer: DerivedWriter | None = None,
        report_writer: AnnualReportWriter | None = None,
        state_store: AnnualReportPipelineStateStore | None = None,
    ) -> None:
        self._state_enabled = state_store is not None or all(item is None for item in (bronze_writer, derived_writer, report_writer))
        self.bronze_writer = bronze_writer or BronzeWriter()
        self.derived_writer = derived_writer or DerivedWriter()
        self.report_writer = report_writer or AnnualReportWriter()
        self.state_store = state_store
        if self._state_enabled and self.state_store is None:
            self.state_store = AnnualReportPipelineStateStore()
            self.state_store.ensure_schema()
        self._existing_parse_runs = {item.parse_run_id for item in self.report_writer.scan_parse_runs()}

    def run(self, parse_results: list[dict[str, object]]) -> AnnualReportIRPipelineStats:
        stats = AnnualReportIRPipelineStats()
        for result in parse_results:
            stats.processed_runs += 1
            try:
                self.process_result(result, stats)
            except Exception:
                logger.exception("Annual-report IR materialization failed for %s", result.get("parse_run_id"))
                stats.failed_count += 1
        return stats

    def process_result(self, result: dict[str, object], stats: AnnualReportIRPipelineStats | None = None) -> AnnualReportIRPipelineStats:
        stats = stats or AnnualReportIRPipelineStats(processed_runs=1)
        ir_path = _required_string(result, "ir_object_path", "parse result")
        ir = self.derived_writer.read_json(ir_path)
        parse_run_id = _required_string(result, "parse_run_id", "parse result")
        report_id = _required_string(result, "report_id", "parse result")
        if _required_string(ir, "parse_run_id", "Document IR") != parse_run_id:
            raise ValueError("parse result and Document IR parse_run_id do not match")
        if _required_string(ir, "report_id", "Document IR") != report_id:
            raise ValueError("parse result and Document IR report_id do not match")
        quality_path = _required_string(result, "quality_object_path", "parse result")
        quality = self.derived_writer.read_json(quality_path)
        page_count = int(_required_value(result, "page_count", "parse result"))
        needs_review_pages = [int(page) for page in _required_value(result, "needs_review_pages", "parse result")]
        if page_count != len(ir.get("pages") or []):
            raise ValueError("parse result page_count does not match Document IR")
        if needs_review_pages != [int(page) for page in quality.get("needs_review_pages", [])]:
            raise ValueError("parse result quality does not match quality.json")
        manifest_path = _required_string(result, "manifest_path", "parse result")
        manifest = AnnualReportBronzeObject.model_validate(self.bronze_writer.read_json(manifest_path))
        announcement = manifest.announcement
        expected_report_id = str(uuid5(NAMESPACE_URL, f"cninfo:{announcement.announcement_id}:{manifest.pdf_sha256}"))
        if report_id != expected_report_id or ir.get("pdf_sha256") != manifest.pdf_sha256:
            raise ValueError("Document IR does not match the Bronze PDF identity")
        parser_version = _required_string(ir, "parser_version", "Document IR")
        parse_status = _required_string(result, "status", "parse result")
        pages_payload = ir.get("pages")
        if not isinstance(pages_payload, list) or not pages_payload:
            raise ValueError("Document IR must contain at least one page")
        parse_run = AnnualReportParseRunRecord(
            parse_run_id=parse_run_id,
            report_id=report_id,
            pdf_sha256=manifest.pdf_sha256,
            parser_version=parser_version,
            status=parse_status,
            started_at=datetime.fromisoformat(_required_string(ir, "started_at", "Document IR")),
            finished_at=datetime.fromisoformat(_required_string(ir, "finished_at", "Document IR")),
            ir_object_path=ir_path,
        )
        report = AnnualReportRecord(
            report_id=report_id,
            stock_code=announcement.stock_code,
            exchange=announcement.exchange,
            company_name=announcement.company_name,
            report_year=announcement.report_year,
            announcement_id=announcement.announcement_id,
            title=announcement.title,
            announcement_time=announcement.announcement_time,
            pdf_sha256=manifest.pdf_sha256,
            page_count=len(pages_payload),
            extraction_status="needs_review" if parse_status == "partial" or any(
                str(page.get("extraction_status")) != "ok" for page in pages_payload if isinstance(page, dict)
            ) else "ok",
            pdf_object_path=manifest.pdf_object_path,
            ingested_at=manifest.ingested_at,
            parse_run_id=parse_run_id,
        )
        pages: list[AnnualReportPageRecord] = []
        regions: list[AnnualReportRegionRecord] = []
        for page in pages_payload:
            if not isinstance(page, dict):
                raise ValueError("Document IR page must be an object")
            page_number = int(_required_value(page, "page_number", "Document IR page"))
            page_regions = page.get("regions")
            if not isinstance(page_regions, list):
                raise ValueError(f"Document IR page {page_number} regions must be a list")
            pages.append(
                AnnualReportPageRecord(
                    report_id=report_id,
                    parse_run_id=parse_run_id,
                    page_number=page_number,
                    page_type=_required_value(page, "page_type", f"Document IR page {page_number}"),
                    text=_required_value(page, "text", f"Document IR page {page_number}"),
                    extraction_method=_required_value(page, "extraction_method", f"Document IR page {page_number}"),
                    extraction_status=_required_value(page, "extraction_status", f"Document IR page {page_number}"),
                    ir_page_path=page.get("ir_page_path"),
                )
            )
            for region in page_regions:
                if not isinstance(region, dict):
                    raise ValueError(f"Document IR page {page_number} region must be an object")
                regions.append(
                    AnnualReportRegionRecord(
                        region_id=_required_string(region, "region_id", f"Document IR page {page_number} region"),
                        report_id=report_id,
                        parse_run_id=parse_run_id,
                        page_number=page_number,
                        region_type=_required_value(region, "region_type", f"Document IR page {page_number} region"),
                        bbox=list(_required_value(region, "bbox", f"Document IR page {page_number} region")),
                        reading_order=int(_required_value(region, "reading_order", f"Document IR page {page_number} region")),
                        text=_region_text(region, f"Document IR page {page_number} region"),
                        content_ref=region.get("content_ref"),
                    )
                )
        sections_payload = ir.get("section_tree")
        if not isinstance(sections_payload, list):
            raise ValueError("Document IR section_tree must be a list")
        sections = [AnnualReportSectionRecord.model_validate(section) for section in sections_payload]
        chunks = build_ir_chunks(ir, report_id=report_id, parse_run_id=parse_run_id)
        projection = {
            "report_id": report_id,
            "parse_run_id": parse_run_id,
            "page_numbers": {item.page_number for item in pages},
            "region_ids": {item.region_id for item in regions},
            "section_ids": {item.section_id for item in sections},
            "chunk_ids": {item.chunk_id for item in chunks},
        }
        # Claim the lease only after all input validation and deterministic
        # projection construction has succeeded. Once claimed, every exit
        # path below is paired with complete_stage or fail_stage.
        lease: StageLease | None = None
        if self.state_store is not None:
            lease = self.state_store.claim_stage(
                report_id=report_id,
                pdf_sha256=manifest.pdf_sha256,
                stage="silver",
                stage_version="annual-report-silver-v1",
            )
        already_committed = parse_run_id in self._existing_parse_runs and result.get("force") is not True
        try:
            if already_committed and self.report_writer.verify_projection(**projection):
                if lease is not None:
                    self.state_store.complete_stage(lease, output_ref=ir_path)
                return stats
            if result.get("force") is True:
                self.report_writer.remove_parse_run(parse_run_id)
                self._existing_parse_runs.discard(parse_run_id)
            self.report_writer.append_pages(pages)
            self.report_writer.append_regions(regions)
            self.report_writer.append_sections(sections)
            self.report_writer.append_chunks(chunks)
            self.report_writer.append_report(report)
            if not self.report_writer.verify_projection(**projection):
                raise RuntimeError("Silver annual-report projection did not pass read-back verification")
            # A parse run is committed only after every Silver projection succeeds.
            self.report_writer.append_parse_runs([parse_run])
            if not self.report_writer.has_parse_run(report_id, parse_run_id):
                raise RuntimeError("Silver annual-report parse-run commit marker was not found after write")
            self._existing_parse_runs.add(parse_run_id)
            if lease is not None:
                self.state_store.complete_stage(lease, output_ref=parse_run_id)
        except Exception as exc:
            if lease is not None:
                self.state_store.fail_stage(lease, exc)
            raise
        stats.written_reports += 1
        stats.written_pages += len(pages)
        stats.written_regions += len(regions)
        stats.written_sections += len(sections)
        stats.written_chunks += len(chunks)
        return stats



def _required_value(payload: dict[str, object], key: str, context: str) -> object:
    if key not in payload or payload[key] is None:
        raise ValueError(f"{context} is missing required field {key}")
    return payload[key]


def _required_string(payload: dict[str, object], key: str, context: str) -> str:
    value = _required_value(payload, key, context)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}.{key} must be a non-empty string")
    return value


def _region_text(payload: dict[str, object], context: str) -> str:
    if "clean_text" in payload and payload["clean_text"] is not None:
        return str(payload["clean_text"])
    if "text" in payload and payload["text"] is not None:
        return str(payload["text"])
    raise ValueError(f"{context} is missing text")

