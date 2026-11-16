"""PDF parser service shared by Ray workers."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from typing import Any
from uuid import NAMESPACE_URL, uuid5


from backend.config import settings
from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.ingestion.writers.derived_writer import DerivedObjectNotFound, DerivedWriter, DerivedWriterError
from backend.pipelines.annual_report_parser import AnnualReportParseError, extract_document_ir
from backend.schemas.annual_report import AnnualReportBronzeObject, AnnualReportParseResult


UTC = timezone.utc

try:  # pragma: no cover - parser image dependency
    import fitz
except ImportError:  # pragma: no cover
    fitz = None


class PdfParserService:
    """Read Bronze manifests, create Document IR, and write derived artifacts."""

    def __init__(self, bronze: BronzeWriter | None = None, derived: DerivedWriter | None = None) -> None:
        self.bronze = bronze or BronzeWriter()
        self.derived = derived or DerivedWriter()

    def parse(self, manifest_path: str, *, force: bool = False) -> AnnualReportParseResult:
        if settings.PDF_LAYOUT_ENGINE == "mineru" and f"mineru-{settings.PDF_MINERU_TIER.lower()}-4.0.12" not in settings.PDF_PARSER_VERSION.lower():
            raise AnnualReportParseError("MinerU requires a PDF_PARSER_VERSION containing mineru-<tier>-4.0.12 to prevent reusing incompatible checkpoints")
        manifest = AnnualReportBronzeObject.model_validate(self.bronze.read_json(manifest_path))
        announcement = manifest.announcement
        report_id = str(uuid5(NAMESPACE_URL, f"cninfo:{announcement.announcement_id}:{manifest.pdf_sha256}"))
        parse_run_id = str(uuid5(NAMESPACE_URL, f"{report_id}:{settings.PDF_PARSER_VERSION}:{manifest.pdf_sha256}"))
        prefix = (
            f"reports/provider={announcement.provider}/stock={announcement.stock_code}/"
            f"report_year={announcement.report_year}/report_id={report_id}/"
            f"parser_version={settings.PDF_PARSER_VERSION}/run={parse_run_id}"
        )
        ir_path = f"{prefix}/document.json"
        quality_path = f"{prefix}/quality.json"
        started_at = datetime.now(UTC)
        pdf_bytes = self.bronze.read_bytes(manifest.pdf_object_path)
        if hashlib.sha256(pdf_bytes).hexdigest() != manifest.pdf_sha256:
            raise AnnualReportParseError("Bronze PDF sha256 does not match its manifest")
        if not force:
            try:
                existing = self.derived.read_json(ir_path)
                quality = self.derived.read_json(quality_path)
                if self._valid_committed_ir(existing, quality, report_id, parse_run_id, manifest.pdf_sha256):
                    return self._response_from_ir(existing, ir_path, quality_path, parse_run_id, report_id, manifest_path)
            except (DerivedObjectNotFound, DerivedWriterError):
                pass

        if fitz is None:
            raise AnnualReportParseError("PyMuPDF is not installed")
        source_pdf = fitz.open(stream=pdf_bytes, filetype="pdf")
        page_count = len(source_pdf)
        source_pdf.close()
        cached_raw_pages: dict[int, dict[str, Any]] = {}
        if not force:
            for page_number in range(1, page_count + 1):
                checkpoint_path = f"{prefix}/raw_pages/page={page_number:04d}.json"
                try:
                    checkpoint = self.derived.read_json(checkpoint_path)
                except DerivedObjectNotFound:
                    continue
                page = checkpoint.get("page")
                if (
                    checkpoint.get("report_id") == report_id
                    and checkpoint.get("parse_run_id") == parse_run_id
                    and checkpoint.get("pdf_sha256") == manifest.pdf_sha256
                    and checkpoint.get("parser_version") == settings.PDF_PARSER_VERSION
                    and checkpoint.get("page_number") == page_number
                    and isinstance(page, dict)
                    and page.get("page_number") == page_number
                    and isinstance(page.get("width"), (int, float))
                    and isinstance(page.get("height"), (int, float))
                    and page.get("page_type") in {"text", "scan", "mixed", "complex"}
                    and isinstance(page.get("extraction_method"), str)
                    and page.get("extraction_status") in {"ok", "needs_review"}
                    and isinstance(page.get("metrics"), dict)
                    and isinstance(page.get("diagnostics"), list)
                    and isinstance(page.get("regions"), list)
                ):
                    cached_raw_pages[page_number] = page

        def persist_raw_page(page: dict[str, Any]) -> None:
            page_number = int(page["page_number"])
            self.derived.write_json(
                f"{prefix}/raw_pages/page={page_number:04d}.json",
                {
                    "report_id": report_id,
                    "parse_run_id": parse_run_id,
                    "pdf_sha256": manifest.pdf_sha256,
                    "parser_version": settings.PDF_PARSER_VERSION,
                    "page_number": page_number,
                    "page": page,
                },
            )

        ir = extract_document_ir(
            pdf_bytes,
            report_id=report_id,
            stock_code=announcement.stock_code,
            report_year=announcement.report_year,
            parse_run_id=parse_run_id,
            parser_version=settings.PDF_PARSER_VERSION,
            cached_raw_pages=cached_raw_pages,
            on_raw_page=persist_raw_page,
        )
        raw_page_manifest_path = f"{prefix}/raw_pages/manifest.json"
        self.derived.write_json(
            raw_page_manifest_path,
            {
                "report_id": report_id,
                "parse_run_id": parse_run_id,
                "pdf_sha256": manifest.pdf_sha256,
                "parser_version": settings.PDF_PARSER_VERSION,
                "page_count": len(ir.get("pages") or []),
                "complete": True,
            },
        )
        ir["raw_page_manifest_path"] = raw_page_manifest_path
        ir["pdf_sha256"] = manifest.pdf_sha256
        ir["started_at"] = started_at.isoformat()
        ir["finished_at"] = datetime.now(UTC).isoformat()
        pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf") if fitz is not None else None
        for page in ir.get("pages", []):
            page_path = f"{prefix}/pages/page={int(page['page_number']):04d}.json"
            page["ir_page_path"] = page_path
            for region_index, region in enumerate(page.get("regions") or [], start=1):
                if region.get("region_type") not in {"table", "figure", "formula"}:
                    continue
                region_path = f"{prefix}/regions/page={int(page['page_number']):04d}-region={region_index:03d}.json"
                # The region JSON is always a valid fallback reference. Rendering a
                # crop is best effort because malformed OCR boxes must not discard
                # the rest of the report.
                region["content_ref"] = region_path
                if region.get("region_type") in {"figure", "formula"} and pdf_document is not None and len(region.get("bbox") or []) == 4:
                    try:
                        source_page = pdf_document[int(page["page_number"]) - 1]
                        rect = fitz.Rect(*region["bbox"]) & source_page.rect
                        if rect.is_empty or rect.width <= 0 or rect.height <= 0:
                            raise ValueError("region crop is outside the PDF page")
                        scale = max(1.0, float(settings.PDF_RENDER_DPI) / 72.0)
                        pixmap = source_page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=rect, alpha=False)
                        asset_path = f"{prefix}/assets/page={int(page['page_number']):04d}-region={region_index:03d}.png"
                        self.derived.write_bytes(asset_path, pixmap.tobytes("png"), content_type="image/png")
                        region["content_ref"] = asset_path
                    except Exception as exc:
                        page["extraction_status"] = "needs_review"
                        diagnostic = {
                            "code": "region_asset_render_failed",
                            "region_index": region_index,
                            "error": str(exc)[:500],
                        }
                        page.setdefault("diagnostics", []).append(diagnostic)
                        quality_diagnostics = ir.setdefault("quality", {}).setdefault("page_diagnostics", [])
                        page_diagnostic = next(
                            (item for item in quality_diagnostics if item.get("page_number") == int(page["page_number"])),
                            None,
                        )
                        if page_diagnostic is None:
                            quality_diagnostics.append(
                                {"page_number": int(page["page_number"]), "diagnostics": [diagnostic]}
                            )
                        else:
                            page_diagnostic.setdefault("diagnostics", []).append(diagnostic)
                        needs_review_pages = ir.setdefault("quality", {}).setdefault("needs_review_pages", [])
                        if int(page["page_number"]) not in needs_review_pages:
                            needs_review_pages.append(int(page["page_number"]))
                self.derived.write_json(region_path, region)
            self.derived.write_json(page_path, page)
        if pdf_document is not None:
            pdf_document.close()
        ir["finished_at"] = datetime.now(UTC).isoformat()
        self.derived.write_json(quality_path, ir["quality"])
        # document.json is the commit marker: all referenced artifacts exist.
        self.derived.write_json(ir_path, ir)
        return self._response_from_ir(ir, ir_path, quality_path, parse_run_id, report_id, manifest_path).model_copy(update={"force": force})

    @staticmethod
    def _response_from_ir(ir: dict[str, Any], ir_path: str, quality_path: str, parse_run_id: str, report_id: str, manifest_path: str) -> AnnualReportParseResult:
        quality = ir["quality"]
        needs_review = [int(page) for page in quality["needs_review_pages"]]
        return AnnualReportParseResult(
            manifest_path=manifest_path,
            parse_run_id=parse_run_id,
            report_id=report_id,
            status="partial" if needs_review else "succeeded",
            ir_object_path=ir_path,
            quality_object_path=quality_path,
            page_count=len(ir["pages"]),
            needs_review_pages=needs_review,
        )

    def _valid_committed_ir(
        self,
        ir: dict[str, Any],
        quality: dict[str, Any],
        report_id: str,
        parse_run_id: str,
        pdf_sha256: str,
    ) -> bool:
        pages = ir.get("pages")
        if (
            ir.get("report_id") != report_id
            or ir.get("parse_run_id") != parse_run_id
            or ir.get("pdf_sha256") != pdf_sha256
            or ir.get("parser_version") != settings.PDF_PARSER_VERSION
            or not isinstance(pages, list)
            or not pages
            or quality != ir.get("quality")
        ):
            return False
        raw_manifest_path = ir.get("raw_page_manifest_path")
        if not isinstance(raw_manifest_path, str) or not raw_manifest_path:
            return False
        raw_manifest = self.derived.read_json(raw_manifest_path)
        if not isinstance(raw_manifest, dict):
            return False
        if (
            raw_manifest.get("report_id") != report_id
            or raw_manifest.get("parse_run_id") != parse_run_id
            or raw_manifest.get("pdf_sha256") != pdf_sha256
            or raw_manifest.get("parser_version") != settings.PDF_PARSER_VERSION
            or raw_manifest.get("page_count") != len(pages)
            or raw_manifest.get("complete") is not True
        ):
            return False
        page_numbers: set[int] = set()
        for page in pages:
            if not isinstance(page, dict) or not page.get("ir_page_path"):
                return False
            page_number = page.get("page_number")
            if not isinstance(page_number, int) or page_number < 1 or page_number in page_numbers:
                return False
            page_numbers.add(page_number)
            try:
                page_artifact = self.derived.read_json(str(page["ir_page_path"]))
            except DerivedObjectNotFound:
                return False
            if not isinstance(page_artifact, dict):
                return False
            if page_artifact.get("page_number") != page_number:
                return False
            if page.get("extraction_status") not in {"ok", "needs_review"}:
                return False
            for region in page.get("regions") or []:
                if not isinstance(region, dict):
                    return False
                content_ref = region.get("content_ref")
                if content_ref and not self.derived.object_exists(str(content_ref)):
                    return False
        return True

