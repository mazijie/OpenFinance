"""Rebuild clean annual-report Silver tables from the current Iceberg snapshot."""

from __future__ import annotations

import argparse
from collections.abc import Iterable

import pyarrow as pa

from backend.lakehouse.annual_report_tables import (
    PAGE_ARROW_SCHEMA,
    PAGE_SCHEMA,
    REPORT_ARROW_SCHEMA,
    REPORT_SCHEMA,
    SECTION_ARROW_SCHEMA,
    SECTION_SCHEMA,
)
from backend.lakehouse.catalog import get_catalog
from backend.lakehouse.write_coordination import append_with_retry
from backend.config import settings


def _dedupe(rows: Iterable[dict], key_columns: tuple[str, ...]) -> list[dict]:
    unique: dict[tuple[object, ...], dict] = {}
    for row in rows:
        key = tuple(row[column] for column in key_columns)
        unique.setdefault(key, row)
    return list(unique.values())


def _create_table(catalog, name: str, schema):
    identifier = f"{settings.ICEBERG_NAMESPACE}.{name}"
    try:
        return catalog.load_table(identifier)
    except Exception:
        return catalog.create_table(identifier=identifier, schema=schema)


def _append(table, rows: list[dict], arrow_schema, table_name: str) -> None:
    if not rows:
        return
    arrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)
    append_with_retry(lambda: table, arrow_table, table_name=table_name)


def rebuild(*, report_table_name: str, page_table_name: str, section_table_name: str) -> dict[str, int]:
    catalog = get_catalog()
    namespace = settings.ICEBERG_NAMESPACE
    source_reports = catalog.load_table(f"{namespace}.{settings.ICEBERG_ANNUAL_REPORT_TABLE}")
    source_pages = catalog.load_table(f"{namespace}.{settings.ICEBERG_ANNUAL_REPORT_PAGE_TABLE}")
    source_sections = catalog.load_table(f"{namespace}.{settings.ICEBERG_ANNUAL_REPORT_SECTION_TABLE}")

    reports = _dedupe(source_reports.scan().to_arrow().to_pylist(), ("report_id",))
    pages = _dedupe(source_pages.scan().to_arrow().to_pylist(), ("report_id", "page_number"))
    sections = _dedupe(source_sections.scan().to_arrow().to_pylist(), ("section_id",))

    target_reports = _create_table(catalog, report_table_name, REPORT_SCHEMA)
    target_pages = _create_table(catalog, page_table_name, PAGE_SCHEMA)
    target_sections = _create_table(catalog, section_table_name, SECTION_SCHEMA)
    _append(target_reports, reports, REPORT_ARROW_SCHEMA, report_table_name)
    _append(target_pages, pages, PAGE_ARROW_SCHEMA, page_table_name)
    _append(target_sections, sections, SECTION_ARROW_SCHEMA, section_table_name)
    return {
        "reports": len(reports),
        "pages": len(pages),
        "sections": len(sections),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-table", default="annual_reports")
    parser.add_argument("--page-table", default="annual_report_pages")
    parser.add_argument("--section-table", default="annual_report_sections")
    args = parser.parse_args()
    print(rebuild(report_table_name=args.report_table, page_table_name=args.page_table, section_table_name=args.section_table))


if __name__ == "__main__":
    main()
