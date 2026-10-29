"""Recreate only configured annual-report Silver and Gold projections.

Run inside the backend container with:
    python -m backend.maintenance.reset_annual_reports
Bronze PDFs/manifests and Derived IR are preserved for replay.
"""

from __future__ import annotations

import json

from backend.config import settings
from backend.lakehouse.catalog import get_catalog
from backend.lakehouse.annual_report_tables import ensure_annual_report_extended_tables, ensure_annual_report_tables
from backend.lakehouse.write_coordination import iceberg_write_lock
from backend.rag.annual_report_store import AnnualReportVectorStore
from backend.rag.annual_report_lexical_store import AnnualReportLexicalStore


def reset_annual_reports() -> dict[str, object]:
    from pyiceberg.exceptions import NoSuchTableError

    names = [settings.ICEBERG_ANNUAL_REPORT_TABLE, settings.ICEBERG_ANNUAL_REPORT_PAGE_TABLE,
             settings.ICEBERG_ANNUAL_REPORT_SECTION_TABLE, settings.ICEBERG_ANNUAL_REPORT_PARSE_RUN_TABLE,
             settings.ICEBERG_ANNUAL_REPORT_REGION_TABLE, settings.ICEBERG_ANNUAL_REPORT_CHUNK_TABLE]
    catalog = get_catalog()
    removed: list[str] = []
    with iceberg_write_lock():
        for name in names:
            identifier = f'{settings.ICEBERG_NAMESPACE}.{name}'
            try:
                catalog.purge_table(identifier)
                removed.append(identifier)
            except NoSuchTableError:
                pass
        ensure_annual_report_tables()
        ensure_annual_report_extended_tables()
    vector = AnnualReportVectorStore()
    vector.ensure_schema()
    vector.clear()
    lexical = AnnualReportLexicalStore()
    try:
        lexical.clear()
    finally:
        lexical.close()
    return {'reset_silver_tables': removed, 'reset_vector_table': vector.fqtn, 'reset_lexical_index': settings.OPENSEARCH_ANNUAL_REPORT_INDEX}


if __name__ == '__main__':
    print(json.dumps(reset_annual_reports(), ensure_ascii=False))
