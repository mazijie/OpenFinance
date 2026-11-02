"""Silver writer and reader for annual-report documents."""

from __future__ import annotations

import json

from backend.lakehouse.annual_report_tables import (
    CHUNK_ARROW_SCHEMA,
    PAGE_ARROW_SCHEMA,
    PARSE_RUN_ARROW_SCHEMA,
    REGION_ARROW_SCHEMA,
    REPORT_ARROW_SCHEMA,
    SECTION_ARROW_SCHEMA,
    ensure_annual_report_extended_tables,
    ensure_annual_report_tables,
)
from backend.schemas.annual_report import (
    AnnualReportChunkRecord,
    AnnualReportPageRecord,
    AnnualReportParseRunRecord,
    AnnualReportRecord,
    AnnualReportRegionRecord,
    AnnualReportSectionRecord,
)
from backend.lakehouse.write_coordination import iceberg_write_lock, upsert_with_retry


class AnnualReportWriter:
    def __init__(self) -> None:
        self.report_table, self.page_table, self.section_table = ensure_annual_report_tables()
        self.parse_run_table, self.region_table, self.chunk_table = ensure_annual_report_extended_tables()

    def append_report(self, record: AnnualReportRecord) -> int:
        return self._append(self.report_table, [record.model_dump()], REPORT_ARROW_SCHEMA)

    def append_pages(self, records: list[AnnualReportPageRecord]) -> int:
        return self._append(self.page_table, [record.model_dump() for record in records], PAGE_ARROW_SCHEMA)

    def append_sections(self, records: list[AnnualReportSectionRecord]) -> int:
        return self._append(self.section_table, [record.model_dump() for record in records], SECTION_ARROW_SCHEMA)

    def append_parse_runs(self, records: list[AnnualReportParseRunRecord]) -> int:
        return self._append(self.parse_run_table, [record.model_dump() for record in records], PARSE_RUN_ARROW_SCHEMA)

    def append_regions(self, records: list[AnnualReportRegionRecord]) -> int:
        rows = []
        for record in records:
            row = record.model_dump()
            row["bbox"] = json.dumps(row["bbox"], ensure_ascii=False)
            rows.append(row)
        return self._append(self.region_table, rows, REGION_ARROW_SCHEMA)

    def append_chunks(self, records: list[AnnualReportChunkRecord]) -> int:
        rows = []
        for record in records:
            row = record.model_dump()
            row["region_ids"] = json.dumps(row["region_ids"], ensure_ascii=False)
            rows.append(row)
        return self._append(self.chunk_table, rows, CHUNK_ARROW_SCHEMA)

    def scan_reports(self) -> list[AnnualReportRecord]:
        return [AnnualReportRecord.model_validate(row) for row in self.report_table.scan().to_arrow().to_pylist()]

    def existing_report_ids(self) -> set[str]:
        """Return stable report IDs already present in Silver for idempotent retries."""

        return {str(row["report_id"]) for row in self.report_table.scan().to_arrow().to_pylist()}

    def scan_pages(self, report_id: str | None = None) -> list[AnnualReportPageRecord]:
        rows = self.page_table.scan().to_arrow().to_pylist()
        return [AnnualReportPageRecord.model_validate(row) for row in rows if report_id is None or row.get("report_id") == report_id]

    def scan_sections(self, report_id: str | None = None) -> list[AnnualReportSectionRecord]:
        rows = self.section_table.scan().to_arrow().to_pylist()
        return [AnnualReportSectionRecord.model_validate(row) for row in rows if report_id is None or row.get("report_id") == report_id]

    def scan_parse_runs(self, report_id: str | None = None) -> list[AnnualReportParseRunRecord]:
        rows = self.parse_run_table.scan().to_arrow().to_pylist()
        return [AnnualReportParseRunRecord.model_validate(row) for row in rows if report_id is None or row.get("report_id") == report_id]

    def verify_projection(
        self,
        *,
        report_id: str,
        parse_run_id: str,
        page_numbers: set[int],
        region_ids: set[str],
        section_ids: set[str],
        chunk_ids: set[str],
    ) -> bool:
        reports = [item for item in self.scan_reports() if item.report_id == report_id and item.parse_run_id == parse_run_id]
        pages = [item for item in self.scan_pages(report_id) if item.parse_run_id == parse_run_id]
        regions = self.scan_regions(report_id, parse_run_id)
        sections = [item for item in self.scan_sections(report_id) if item.parse_run_id == parse_run_id]
        chunks = self.scan_chunks(report_id, parse_run_id)
        return (
            len(reports) == 1
            and {item.page_number for item in pages} == page_numbers
            and {item.region_id for item in regions} == region_ids
            and {item.section_id for item in sections} == section_ids
            and {item.chunk_id for item in chunks} == chunk_ids
        )

    def has_parse_run(self, report_id: str, parse_run_id: str) -> bool:
        return any(
            item.report_id == report_id and item.parse_run_id == parse_run_id
            for item in self.scan_parse_runs(report_id)
        )

    def projection_snapshot(self) -> dict[str, set[tuple[str, ...]]]:
        """Read current Silver keys once for batch reconciliation."""
        return {
            "reports": {(item.report_id, item.parse_run_id) for item in self.scan_reports()},
            "pages": {(item.report_id, item.parse_run_id, str(item.page_number)) for item in self.scan_pages()},
            "regions": {(item.report_id, item.parse_run_id, item.region_id) for item in self.scan_regions()},
            "sections": {(item.report_id, item.parse_run_id, item.section_id) for item in self.scan_sections()},
            "chunks": {(item.report_id, item.parse_run_id, item.chunk_id) for item in self.scan_chunks()},
            "parse_runs": {(item.report_id, item.parse_run_id) for item in self.scan_parse_runs()},
        }

    def scan_regions(self, report_id: str | None = None, parse_run_id: str | None = None) -> list[AnnualReportRegionRecord]:
        rows = self.region_table.scan().to_arrow().to_pylist()
        result: list[AnnualReportRegionRecord] = []
        for row in rows:
            if report_id is not None and row.get("report_id") != report_id:
                continue
            if parse_run_id is not None and row.get("parse_run_id") != parse_run_id:
                continue
            if isinstance(row.get("bbox"), str):
                row["bbox"] = json.loads(row["bbox"])
            result.append(AnnualReportRegionRecord.model_validate(row))
        return result

    def scan_chunks(self, report_id: str | None = None, parse_run_id: str | None = None) -> list[AnnualReportChunkRecord]:
        rows = self.chunk_table.scan().to_arrow().to_pylist()
        result: list[AnnualReportChunkRecord] = []
        for row in rows:
            if report_id is not None and row.get("report_id") != report_id:
                continue
            if parse_run_id is not None and row.get("parse_run_id") != parse_run_id:
                continue
            if isinstance(row.get("region_ids"), str):
                row["region_ids"] = json.loads(row["region_ids"])
            result.append(AnnualReportChunkRecord.model_validate(row))
        return result

    def remove_parse_run(self, parse_run_id: str) -> None:
        """Explicit force rebuild removes the previous projections of this run."""
        from pyiceberg.expressions import EqualTo

        with iceberg_write_lock():
            self.report_table, self.page_table, self.section_table = ensure_annual_report_tables()
            self.parse_run_table, self.region_table, self.chunk_table = ensure_annual_report_extended_tables()
            for table in (self.parse_run_table, self.page_table, self.section_table, self.region_table, self.chunk_table):
                table.delete(EqualTo("parse_run_id", parse_run_id))

    def _append(self, table, rows: list[dict], schema) -> int:
        if not rows:
            return 0
        try:
            import pyarrow as pa
            field_names = [field.name for field in schema]
            projected_rows = [{name: row.get(name) for name in field_names} for row in rows]
            for row_index, row in enumerate(projected_rows):
                missing_required = [
                    field.name
                    for field in schema
                    if not field.nullable and row.get(field.name) is None
                ]
                if missing_required:
                    raise ValueError(
                        f"Missing required Silver fields for row {row_index}: "
                        f"{missing_required}"
                    )
            arrow_table = pa.Table.from_pylist(projected_rows, schema=schema)
        except ImportError:  # pragma: no cover
            arrow_table = type("Table", (), {"to_pylist": lambda self: rows, "num_rows": len(rows)})()
        table_name = "annual_reports"
        if table is self.page_table:
            table_name = "annual_report_pages"
        elif table is self.section_table:
            table_name = "annual_report_sections"
        elif table is self.parse_run_table:
            table_name = "annual_report_parse_runs"
        elif table is self.region_table:
            table_name = "annual_report_regions"
        elif table is self.chunk_table:
            table_name = "annual_report_chunks"

        def reload_table():
            self.report_table, self.page_table, self.section_table = ensure_annual_report_tables()
            self.parse_run_table, self.region_table, self.chunk_table = ensure_annual_report_extended_tables()
            return {
                "annual_reports": self.report_table,
                "annual_report_pages": self.page_table,
                "annual_report_sections": self.section_table,
                "annual_report_parse_runs": self.parse_run_table,
                "annual_report_regions": self.region_table,
                "annual_report_chunks": self.chunk_table,
            }[table_name]

        keys = {
            "annual_reports": ["report_id", "parse_run_id"],
            "annual_report_pages": ["report_id", "parse_run_id", "page_number"],
            "annual_report_sections": ["report_id", "parse_run_id", "section_id"],
            "annual_report_parse_runs": ["parse_run_id"],
            "annual_report_regions": ["report_id", "parse_run_id", "region_id"],
            "annual_report_chunks": ["chunk_id"],
        }
        upsert_with_retry(reload_table, arrow_table, table_name=table_name, join_cols=keys[table_name])
        return len(rows)
