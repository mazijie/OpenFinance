"""Inspect or rewrite only an isolated market-history benchmark table."""
from __future__ import annotations

import argparse
from datetime import date, timedelta
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
from daily_metrics import create_spark
from pyspark.sql import functions as F


SAFE_TABLE = re.compile(r"^openfinance\.bench_[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("inspect", "rewrite-manifests", "rewrite-data", "compare"))
    parser.add_argument("--table", required=True)
    parser.add_argument("--reference")
    parser.add_argument("--reference-snapshot", type=int)
    parser.add_argument("--symbol", default="600519.SH")
    parser.add_argument("--adjustment-type", default="none")
    parser.add_argument("--end-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--reference-start", type=date.fromisoformat)
    parser.add_argument("--reference-end", type=date.fromisoformat)
    parser.add_argument("--reference-exchange", choices=("SH", "SZ", "BJ"))
    parser.add_argument("--reference-provider")
    args = parser.parse_args()
    if not SAFE_TABLE.fullmatch(args.table):
        parser.error("--table must be an isolated openfinance.bench_<id>.<table> table")
    if args.reference and not re.fullmatch(r"openfinance\.[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", args.reference):
        parser.error("Invalid reference identifier")
    if args.action == "compare" and (not args.reference or args.reference_snapshot is None):
        parser.error("compare requires --reference and --reference-snapshot")
    spark = create_spark()
    try:
        if args.action.startswith("rewrite-"):
            proc = "rewrite_manifests" if args.action == "rewrite-manifests" else "rewrite_data_files"
            options = ", options => map('min-input-files', '2')" if args.action == "rewrite-data" else ""
            result = spark.sql(f"CALL openfinance.system.{proc}(table => '{args.table.split('.', 1)[1]}'{options})").collect()
            print(json.dumps({"action": args.action, "result": [r.asDict() for r in result]}, default=str))
            return
        if args.action == "compare":
            left = spark.read.format("iceberg").option("versionAsOf", args.reference_snapshot).load(args.reference)
            right = spark.table(args.table)
            if args.reference_start:
                left = left.filter(F.col("trading_date") >= args.reference_start)
            if args.reference_end:
                left = left.filter(F.col("trading_date") <= args.reference_end)
            if args.reference_exchange:
                left = left.filter(F.col("exchange") == args.reference_exchange)
            if args.reference_provider:
                left = left.filter(F.col("provider") == args.reference_provider)
            if left.schema != right.schema:
                raise ValueError("Reference and test schemas differ")
            missing = left.exceptAll(right).count()
            extra = right.exceptAll(left).count()
            print(json.dumps({"missing_rows": missing, "extra_rows": extra}))
            if missing or extra:
                raise SystemExit(1)
            return
        files = spark.table(f"{args.table}.files").agg(F.count("*").alias("files"),
            F.sum("file_size_in_bytes").alias("bytes"),
            F.expr("percentile_approx(file_size_in_bytes, 0.5)").alias("median_bytes")).first().asDict()
        manifests = spark.table(f"{args.table}.manifests").count()
        durations = {}
        for name, days in (("daily", 365), ("weekly", 1095), ("monthly", 1826)):
            start = args.end_date - timedelta(days=days - 1)
            t = time.monotonic()
            count = (spark.table(args.table).filter((F.col("symbol") == args.symbol)
                & (F.col("adjustment_type") == args.adjustment_type) & F.col("trading_date").between(start, args.end_date)).count())
            durations[name] = {"seconds": round(time.monotonic() - t, 3), "rows": count}
        print(json.dumps({"table": args.table, "files": files, "manifests": manifests,
                          "spark_scan": durations}, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
