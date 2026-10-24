"""Silver writer and reader for annual-report documents."""

from __future__ import annotations

from collections.abc import Iterable

from backend.lakehouse.annual_report_tables import PAGE_ARROW_SCHEMA, REPORT_ARROW_SCHEMA, SECTION_ARROW_SCHEMA, ensure_annual_report_tables
from backend.schemas.annual_report import AnnualReportPageRecord, AnnualReportRecord, AnnualReportSectionRecord
from backend.lakehouse.write_coordination import append_with_retry


class AnnualReportWriter:
    def __init__(self) -> None:
        self.report_table, self.page_table, self.section_table = ensure_annual_report_tables()

    def append_report(self, record: AnnualReportRecord) -> int:
        return self._append(self.report_table, [record.model_dump()], REPORT_ARROW_SCHEMA)

    def append_pages(self, records: list[AnnualReportPageRecord]) -> int:
        return self._append(self.page_table, [record.model_dump() for record in records], PAGE_ARROW_SCHEMA)

    def append_sections(self, records: list[AnnualReportSectionRecord]) -> int:
        return self._append(self.section_table, [record.model_dump() for record in records], SECTION_ARROW_SCHEMA)

    def scan_reports(self) -> list[AnnualReportRecord]:
        return [AnnualReportRecord.model_validate(row) for row in self.report_table.scan().to_arrow().to_pylist()]

    def scan_pages(self, report_id: str | None = None) -> list[AnnualReportPageRecord]:
        rows = self.page_table.scan().to_arrow().to_pylist()
        return [AnnualReportPageRecord.model_validate(row) for row in rows if report_id is None or row.get("report_id") == report_id]

    def scan_sections(self, report_id: str | None = None) -> list[AnnualReportSectionRecord]:
        rows = self.section_table.scan().to_arrow().to_pylist()
        return [AnnualReportSectionRecord.model_validate(row) for row in rows if report_id is None or row.get("report_id") == report_id]

    def _append(self, table, rows: list[dict], schema) -> int:
        if not rows:
            return 0
        try:
            import pyarrow as pa
            arrow_table = pa.Table.from_pylist(rows, schema=schema)
        except ImportError:  # pragma: no cover
            arrow_table = type("Table", (), {"to_pylist": lambda self: rows, "num_rows": len(rows)})()
        table_name = "annual_reports"
        if table is self.page_table:
            table_name = "annual_report_pages"
        elif table is self.section_table:
            table_name = "annual_report_sections"

        def reload_table():
            self.report_table, self.page_table, self.section_table = ensure_annual_report_tables()
            return self.report_table if table_name == "annual_reports" else self.page_table if table_name == "annual_report_pages" else self.section_table

        append_with_retry(reload_table, arrow_table, table_name=table_name)
        return len(rows)
