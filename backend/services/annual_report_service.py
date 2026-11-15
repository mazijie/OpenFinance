"""Application service for annual-report catalog and page access."""

from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.lakehouse.starrocks_readers import StarRocksReader
from backend.lakehouse.annual_report_visibility import committed_reports
from backend.schemas.annual_report import (
    AnnualReportBronzeObject,
    AnnualReportItem,
    AnnualReportPage,
    AnnualReportPageResult,
    AnnualReportRecord,
    AnnualReportRegion,
)


class AnnualReportService:
    def __init__(self, writer: StarRocksReader | None = None, bronze_writer: BronzeWriter | None = None) -> None:
        if writer is None:
            writer = StarRocksReader()
        self.writer = writer
        self.bronze_writer = bronze_writer or BronzeWriter()

    def list_reports(self, *, stock_code: str | None, report_year: int | None, page: int, page_size: int) -> AnnualReportPageResult:
        # The catalog is intentionally sourced from Bronze. A PDF is a usable
        # asset as soon as its manifest and object are committed; extraction is
        # an independent capability whose status is merged when Silver exists.
        silver_rows = committed_reports(self.writer.scan_reports(), self.writer.scan_parse_runs())
        rows = []
        for report_id, manifest in self._bronze_reports():
            silver = silver_rows.get(report_id)
            rows.append(self._catalog_item(report_id, manifest, silver))
        if stock_code:
            normalized_stock = stock_code.strip().upper()
            rows = [row for row in rows if row.stock_code == normalized_stock]
        if report_year:
            rows = [row for row in rows if row.report_year == report_year]
        rows.sort(key=lambda row: (row.report_year, row.announcement_time), reverse=True)
        total = len(rows)
        start = (page - 1) * page_size
        items = rows[start : start + page_size]
        return AnnualReportPageResult(items=items, page=page, page_size=page_size, total=total, has_next=start + page_size < total)

    def get_page(self, report_id: str, page_number: int) -> AnnualReportPage:
        reports = committed_reports(self.writer.scan_reports(report_id), self.writer.scan_parse_runs(report_id))
        active_run = reports[report_id].parse_run_id if report_id in reports else None
        pages = [page for page in self.writer.scan_pages(report_id) if page.page_number == page_number and page.parse_run_id == active_run]
        if not pages:
            raise LookupError("Annual report page not found")
        page = pages[0]
        return AnnualReportPage(report_id=page.report_id, page_number=page.page_number, text=page.text, extraction_status=page.extraction_status, page_type=page.page_type, ir_page_path=page.ir_page_path)

    def get_region(self, report_id: str, region_id: str) -> AnnualReportRegion:
        reports = committed_reports(self.writer.scan_reports(report_id), self.writer.scan_parse_runs(report_id))
        active_run = reports[report_id].parse_run_id if report_id in reports else None
        if active_run is None:
            raise LookupError("Annual report region not found")
        regions = [region for region in self.writer.scan_regions(report_id=report_id, parse_run_id=active_run) if region.region_id == region_id]
        if not regions:
            raise LookupError("Annual report region not found")
        region = regions[0]
        return AnnualReportRegion(
            region_id=region.region_id,
            report_id=region.report_id,
            page_number=region.page_number,
            region_type=region.region_type,
            bbox=region.bbox,
            reading_order=region.reading_order,
            text=region.text,
            content_ref=region.content_ref,
        )

    def read_pdf(self, report_id: str) -> bytes:
        """Read an annual-report PDF through the backend's MinIO connection."""

        for candidate_report_id, manifest in self._bronze_reports():
            if candidate_report_id == report_id:
                return self.bronze_writer.read_bytes(manifest.pdf_object_path)
        # Keep a Silver fallback for historical rows whose manifest catalog is
        # temporarily unavailable, while the normal path remains Bronze-first.
        reports = committed_reports(self.writer.scan_reports(report_id), self.writer.scan_parse_runs(report_id))
        if report_id in reports:
            return self.bronze_writer.read_bytes(reports[report_id].pdf_object_path)
        raise LookupError("Annual report not found")

    def _bronze_reports(self) -> list[tuple[str, AnnualReportBronzeObject]]:
        reports: list[tuple[str, AnnualReportBronzeObject]] = []
        for object_path in self.bronze_writer.list_objects(prefix="reports/provider=cninfo/"):
            if not object_path.endswith("/metadata.json"):
                continue
            try:
                manifest = AnnualReportBronzeObject.model_validate(self.bronze_writer.read_json(object_path))
                if not self.bronze_writer.object_exists(manifest.pdf_object_path):
                    continue
                report_id = str(
                    uuid5(
                        NAMESPACE_URL,
                        f"cninfo:{manifest.announcement.announcement_id}:{manifest.pdf_sha256}",
                    )
                )
            except Exception:
                # A malformed or incomplete manifest is not a displayable
                # report and should not make the catalog endpoint fail.
                continue
            reports.append((report_id, manifest))
        return reports

    @staticmethod
    def _catalog_item(
        report_id: str,
        manifest: AnnualReportBronzeObject,
        silver: AnnualReportRecord | None,
    ) -> AnnualReportItem:
        if silver is not None:
            return AnnualReportItem.model_validate(
                {
                    **silver.model_dump(),
                    "pdf_url": f"/api/reports/{report_id}/download",
                }
            )
        announcement = manifest.announcement
        return AnnualReportItem(
            report_id=report_id,
            stock_code=announcement.stock_code,
            exchange=announcement.exchange,
            company_name=announcement.company_name,
            report_year=announcement.report_year,
            announcement_id=announcement.announcement_id,
            title=announcement.title,
            announcement_time=announcement.announcement_time,
            pdf_url=f"/api/reports/{report_id}/download",
            # Page count is known after parsing. Keeping it at zero prevents
            # the UI from offering a text action before Silver is available.
            page_count=0,
            extraction_status="pending_parse",
        )
