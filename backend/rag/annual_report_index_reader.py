"""Snapshot-pinned, read-only StarRocks source for complete index generations."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace

from backend.config import settings
from backend.lakehouse.catalog import get_catalog
from backend.lakehouse.starrocks_daily_reader import StarRocksDailyReader, _identifier
from backend.logging_utils import get_logger

logger = get_logger(__name__)

# The indexer consumes these fields only; in particular it does not need page text.
FIELDS = {
    "reports": ("report_id", "parse_run_id", "pdf_sha256", "stock_code", "report_year", "company_name", "announcement_time", "ingested_at"),
    "parse_runs": ("report_id", "parse_run_id", "started_at", "finished_at"),
    "pages": ("report_id", "parse_run_id", "page_number", "extraction_status"),
    "chunks": ("chunk_id", "report_id", "parse_run_id", "section_path", "content_type", "region_ids", "page_start", "page_end", "chunk_text", "content_hash"),
}


def _table_names():
    return {"reports": settings.ICEBERG_ANNUAL_REPORT_TABLE,
            "parse_runs": settings.ICEBERG_ANNUAL_REPORT_PARSE_RUN_TABLE,
            "pages": settings.ICEBERG_ANNUAL_REPORT_PAGE_TABLE,
            "chunks": settings.ICEBERG_ANNUAL_REPORT_CHUNK_TABLE}


def normalize_row(row):
    row = dict(row)
    for name, value in row.items():
        if isinstance(value, datetime):
            row[name] = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(row.get("region_ids"), str):
        row["region_ids"] = json.loads(row["region_ids"])
    return row


class StarRocksAnnualReportIndexReader:
    def __init__(self, *, catalog=None, sql_reader=None):
        self.catalog = catalog or get_catalog()
        self.sql_reader = sql_reader or StarRocksDailyReader()
        self.tables = {}
        self.snapshots = {}
        # Capture commit markers before projections: newer, uncommitted runs are
        # not made visible merely because their projection was read later.
        for kind in ("parse_runs", "reports", "pages", "chunks"):
            table = self.catalog.load_table(f"{settings.ICEBERG_NAMESPACE}.{_table_names()[kind]}")
            snapshot = table.current_snapshot()
            self.tables[kind] = table
            self.snapshots[kind] = snapshot.snapshot_id if snapshot else None
        logger.info("Annual report index source snapshots captured", event="annual_report_index_snapshots",
                    extra_fields={"snapshots": self.snapshots,
                                  "table_uuids": {k: str(t.metadata.table_uuid) for k, t in self.tables.items()}})

    def read_rows(self, kind, *, engine="starrocks"):
        snapshot_id = self.snapshots[kind]
        if snapshot_id is None:
            return []
        fields = FIELDS[kind]
        if engine == "starrocks":
            table = self.sql_reader._table(snapshot_id, _table_names()[kind])
            columns = ", ".join(_identifier(field) for field in fields)
            rows = self.sql_reader._query(f"SELECT {columns} FROM {table}", ())
        elif engine == "pyiceberg":
            rows = self.tables[kind].scan(snapshot_id=snapshot_id).select(*fields).to_arrow().to_pylist()
        else:
            raise ValueError("Unknown annual report index reader")
        return [normalize_row(row) for row in rows]

    def _scan(self, kind):
        return [SimpleNamespace(**row) for row in self.read_rows(kind)]

    def scan_reports(self):
        return self._scan("reports")

    def scan_parse_runs(self):
        return self._scan("parse_runs")

    def scan_pages(self):
        return self._scan("pages")

    def scan_chunks(self):
        return self._scan("chunks")
