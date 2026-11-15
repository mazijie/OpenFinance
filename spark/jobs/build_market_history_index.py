"""Build the PostgreSQL history index from one immutable Iceberg snapshot."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import functions as F

from daily_metrics import create_spark


TABLE = "openfinance.openfinance.daily_bars"


def summarize(frame, now):
    effective = F.greatest("available_at", "ingested_at")
    return frame.groupBy("symbol", "adjustment_type").agg(
        F.min(F.when(effective <= F.lit(now), F.col("trading_date"))).alias("earliest_date"),
        F.min(F.when(effective > F.lit(now), effective)).alias("next_visible"),
    )


def main() -> None:
    dsn = os.environ.get("MARKET_HISTORY_INDEX_DSN")
    conn = (psycopg2.connect(dsn) if dsn else
            psycopg2.connect(host="postgres", dbname="openfinance",
                             user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"]))
    conn.autocommit = True
    spark = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", ("openfinance_market_history_index",))
            if not cur.fetchone()[0]:
                print("History index rebuild already running")
                return
        spark = create_spark()
        table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, TABLE)
        table.refresh()
        snapshot = table.currentSnapshot()
        if snapshot is None:
            print("Daily bars has no snapshot")
            return
        table_uuid, snapshot_id = str(table.uuid()), int(snapshot.snapshotId())
        summary = snapshot.summary()
        def metric(name):
            value = summary.get(name)
            return int(value) if value is not None else None
        file_count = metric("total-data-files")
        file_bytes = metric("total-files-size")
        print(json.dumps({"event": "market_history_snapshot_metrics", "snapshot_id": str(snapshot_id),
                          "data_files": file_count, "data_bytes": file_bytes,
                          "mean_file_bytes": round(file_bytes / file_count) if file_count and file_bytes is not None else None,
                          "manifest_count": snapshot.allManifests(table.io()).size(),
                          "added_data_files": metric("added-data-files"),
                          "removed_data_files": metric("deleted-data-files")}))
        now = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            cur.execute("""SELECT 1 FROM openfinance.market_history_index_state
                WHERE singleton AND table_uuid = %s AND snapshot_id = %s
                AND valid_from <= %s AND (valid_until IS NULL OR %s < valid_until)""",
                (table_uuid, snapshot_id, now, now))
            if cur.fetchone():
                print("History index is current")
                return
        frame = spark.read.format("iceberg").option("versionAsOf", snapshot_id).load(TABLE)
        groups = summarize(frame, now).collect()
        rows = [(r.symbol, r.adjustment_type, r.earliest_date) for r in groups]
        future = [r.next_visible.replace(tzinfo=timezone.utc) for r in groups if r.next_visible is not None]
        valid_until = min(future) if future else None
        table.refresh()
        latest = table.currentSnapshot()
        if latest is None or int(latest.snapshotId()) != snapshot_id:
            print("Snapshot changed during history-index build; the next run will retry")
            return
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("DELETE FROM openfinance.market_history_index")
            if rows:
                execute_values(cur, "INSERT INTO openfinance.market_history_index (symbol, adjustment_type, earliest_date) VALUES %s", rows, page_size=1000)
            cur.execute("""INSERT INTO openfinance.market_history_index_state
                (singleton, table_uuid, snapshot_id, valid_from, valid_until, published_at)
                VALUES (true, %s, %s, %s, %s, now())
                ON CONFLICT (singleton) DO UPDATE SET table_uuid=excluded.table_uuid,
                  snapshot_id=excluded.snapshot_id, valid_from=excluded.valid_from,
                  valid_until=excluded.valid_until, published_at=excluded.published_at""",
                (table_uuid, snapshot_id, now, valid_until))
        conn.commit()
        print(json.dumps({"event": "history_index_published", "snapshot_id": str(snapshot_id),
                          "symbols": len(rows), "valid_until": str(valid_until) if valid_until else None}))
    finally:
        if spark is not None:
            spark.stop()
        conn.close()


if __name__ == "__main__":
    main()
