"""Application service for annual-report catalog and page access."""

from __future__ import annotations

from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.lakehouse.annual_report_writer import AnnualReportWriter
from backend.schemas.annual_report import AnnualReportItem, AnnualReportPage, AnnualReportPageResult


class AnnualReportService:
    def __init__(self, writer: AnnualReportWriter | None = None, bronze_writer: BronzeWriter | None = None) -> None:
        self.writer = writer or AnnualReportWriter()
        self.bronze_writer = bronze_writer or BronzeWriter()

    def list_reports(self, *, stock_code: str | None, report_year: int | None, page: int, page_size: int) -> AnnualReportPageResult:
        rows = self.writer.scan_reports()
        if stock_code:
            rows = [row for row in rows if row.stock_code == stock_code.strip().upper()]
        if report_year:
            rows = [row for row in rows if row.report_year == report_year]
        rows.sort(key=lambda row: (row.report_year, row.announcement_time), reverse=True)
        total = len(rows)
        start = (page - 1) * page_size
        items = [AnnualReportItem.model_validate(row.model_dump()) for row in rows[start : start + page_size]]
        return AnnualReportPageResult(items=items, page=page, page_size=page_size, total=total, has_next=start + page_size < total)

    def get_page(self, report_id: str, page_number: int) -> AnnualReportPage:
        pages = [page for page in self.writer.scan_pages(report_id) if page.page_number == page_number]
        if not pages:
            raise LookupError("Annual report page not found")
        page = pages[0]
        return AnnualReportPage(report_id=page.report_id, page_number=page.page_number, text=page.text, extraction_status=page.extraction_status)

    def read_pdf(self, report_id: str) -> bytes:
        """Read an annual-report PDF through the backend's MinIO connection."""

        reports = [row for row in self.writer.scan_reports() if row.report_id == report_id]
        if not reports:
            raise LookupError("Annual report not found")
        return self.bronze_writer.read_bytes(reports[0].pdf_object_path)
