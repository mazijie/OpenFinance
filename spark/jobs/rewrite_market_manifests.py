"""Rewrite only daily-bars manifests while preserving the table schema."""
from __future__ import annotations

import argparse
import json

from daily_metrics import create_spark


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-snapshot", type=int, required=True)
    args = parser.parse_args()
    spark = create_spark()
    try:
        table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, "openfinance.openfinance.daily_bars")
        table.refresh()
        before = table.currentSnapshot()
        if before is None or before.snapshotId() != args.expected_snapshot:
            raise RuntimeError("Production snapshot changed since the preflight; no rewrite performed")
        schema_before = table.schema().asStruct().toString()
        uuid_before = str(table.uuid())
        manifests_before = before.allManifests(table.io()).size()
        result = [r.asDict(recursive=True) for r in spark.sql(
            "CALL openfinance.system.rewrite_manifests(table => 'openfinance.openfinance.daily_bars')").collect()]
        table.refresh()
        after = table.currentSnapshot()
        schema_after = table.schema().asStruct().toString()
        if schema_before != schema_after or uuid_before != str(table.uuid()):
            raise RuntimeError("Table identity or schema changed during manifest rewrite")
        print("MANIFEST_REWRITE_RESULT=" + json.dumps({
            "uuid": uuid_before, "schema": schema_after,
            "snapshot_before": str(before.snapshotId()), "snapshot_after": str(after.snapshotId()),
            "manifests_before": manifests_before, "manifests_after": after.allManifests(table.io()).size(),
            "procedure": result,
        }))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
