"""Move current annual-report catalog entries to canonical names without purging files."""

from __future__ import annotations

import psycopg

from backend.config import settings
from backend.lakehouse.annual_report_tables import PAGE_SCHEMA, REPORT_SCHEMA, SECTION_SCHEMA
from backend.lakehouse.catalog import get_catalog


def main() -> None:
    namespace = settings.ICEBERG_NAMESPACE
    catalog = get_catalog()
    pairs = (
        ("annual_reports_clean", "annual_reports", REPORT_SCHEMA),
        ("annual_report_pages_clean", "annual_report_pages", PAGE_SCHEMA),
        ("annual_report_sections_clean", "annual_report_sections", SECTION_SCHEMA),
    )
    locations = {}
    for source, target, schema in pairs:
        table = catalog.load_table(f"{namespace}.{source}")
        if table.schema().fields != schema.fields:
            raise RuntimeError(f"Source schema mismatch: {source}")
        locations[source] = table.metadata_location
        if catalog.table_exists(f"{namespace}.{target}"):
            existing = catalog.load_table(f"{namespace}.{target}")
            if existing.current_snapshot() is not None:
                raise RuntimeError(f"Target is not empty: {target}")
            locations[target] = existing.metadata_location

    with psycopg.connect(settings.DATABASE_URL) as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", ("openfinance_iceberg_write",))
        cur.execute("LOCK TABLE iceberg_tables IN SHARE ROW EXCLUSIVE MODE")
        for source, target, _ in pairs:
            cur.execute("SELECT table_name, metadata_location FROM iceberg_tables WHERE catalog_name=%s AND table_namespace=%s AND table_name IN (%s,%s)", (settings.ICEBERG_CATALOG_NAME, namespace, source, target))
            actual = dict(cur.fetchall())
            expected = {n: locations[n] for n in (source, target) if n in locations}
            if actual != expected:
                raise RuntimeError(f"Catalog changed during validation: {source}, {target}")
        for source, target, _ in pairs:
            if target in locations:
                cur.execute("DELETE FROM iceberg_tables WHERE catalog_name=%s AND table_namespace=%s AND table_name=%s", (settings.ICEBERG_CATALOG_NAME, namespace, target))
            cur.execute("UPDATE iceberg_tables SET table_name=%s WHERE catalog_name=%s AND table_namespace=%s AND table_name=%s", (target, settings.ICEBERG_CATALOG_NAME, namespace, source))
            if cur.rowcount != 1:
                raise RuntimeError(f"Failed to rename {source}")
    print("Catalog entries unified; no object files were deleted.")


if __name__ == "__main__":
    main()
