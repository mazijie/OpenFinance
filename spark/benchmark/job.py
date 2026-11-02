"""Isolated Iceberg benchmark job."""
import argparse
import json
import time
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from pyspark.sql import functions as F
from daily_metrics import create_spark, calculate_metrics, load_daily_bars
from build_gold_security_daily_metrics import build_gold_frame, validate_gold_frame, write_gold_table
from rebuild_silver_finance import MigrationTable, build_frames, build_compacted_tables, table_exists

NAMES = ("daily_bars", "security_master", "trading_calendar")
BASE = "openfinance.openfinance"

def identifier(value):
    if not value.replace("_", "").isalnum() or not value[0].isalpha() or not value.isascii():
        raise ValueError("Identifiers must start with a letter and contain only ASCII letters, numbers or underscores")
    return value.lower()

def compare_tables(spark, target, reference, keys):
    """Compare every row, allowing a small absolute and relative float tolerance."""
    left, right = spark.table(target), spark.table(reference)
    if left.dtypes != right.dtypes:
        raise ValueError(f"Schemas differ: {target} and {reference}")
    l, r = left.withColumn("_bench_present", F.lit(True)).alias("l"), right.withColumn("_bench_present", F.lit(True)).alias("r")
    condition = None
    for key in keys:
        equal = F.col(f"l.{key}").eqNullSafe(F.col(f"r.{key}"))
        condition = equal if condition is None else condition & equal
    mismatch = F.lit(False)
    for name, typ in left.dtypes:
        if name == "gold_built_at":
            continue
        a, b = F.col(f"l.{name}"), F.col(f"r.{name}")
        same = a.eqNullSafe(b)
        if typ in {"double", "float"}:
            same = same | (a.isNotNull() & b.isNotNull() & (F.abs(a - b) <= F.lit(1e-9) + F.lit(1e-9) * F.abs(b)))
        mismatch = mismatch | ~same
    missing = F.col("l._bench_present").isNull() | F.col("r._bench_present").isNull()
    return l.join(r, condition, "full_outer").filter(missing | mismatch).count()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=["prepare", "run"])
    p.add_argument("--dataset", required=True)
    p.add_argument("--kind", choices=["real", "small", "medium"], default="real")
    p.add_argument("--workload", choices=["gold", "silver"], default="gold")
    p.add_argument("--run-id", default="prepare")
    p.add_argument("--partitions", type=int, default=4)
    p.add_argument("--distribution", choices=["hash", "none"], default="hash")
    p.add_argument("--result", required=True)
    p.add_argument("--manifest")
    p.add_argument("--reference")
    a = p.parse_args()
    dataset, run_id = identifier(a.dataset), identifier(a.run_id)
    if len(dataset) > 40:
        raise ValueError("Dataset ID must not exceed 40 characters")
    if a.partitions < 1:
        raise ValueError("partitions must be positive")
    spark = create_spark()
    namespace = "openfinance.bench_" + dataset
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    out = {"application_id": spark.sparkContext.applicationId, "spark_version": spark.version, "action": a.action,
           "dataset": dataset, "workload": a.workload, "run_id": run_id,
           "output_partitions": a.partitions, "write_distribution": a.distribution,
           "configuration": {k: spark.conf.get(k, None) for k in ["spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled", "spark.executor.cores", "spark.executor.memory", "spark.driver.memory", "spark.dynamicAllocation.enabled"]}}
    started = time.monotonic()
    try:
        if a.action == "prepare":
            out["sources"] = {}
            if a.kind == "real":
                for name in NAMES:
                    source, target = f"{BASE}.{name}", f"{namespace}.{name}"
                    if table_exists(spark, target):
                        raise ValueError(f"Input exists: {target}; choose a new dataset ID")
                    row = spark.sql(f"SELECT snapshot_id FROM {source}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
                    if row is None:
                        raise ValueError(f"No snapshot for {source}")
                    snapshot = int(row[0])
                    frame = spark.read.format("iceberg").option("versionAsOf", snapshot).load(source)
                    frame.writeTo(target).using("iceberg").create()
                    copy_snapshot = int(spark.sql(f"SELECT snapshot_id FROM {target}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0])
                    out["sources"][name] = {"table": source, "snapshot_id": snapshot, "rows": frame.count(), "copy": target, "copy_snapshot_id": copy_snapshot}
            else:
                count, days = (100, 252) if a.kind == "small" else (1000, 756)
                symbols = spark.range(count).selectExpr("id", "lpad(cast(id as string), 6, '0') as symbol")
                symbols = symbols.withColumn("security_id", F.concat(F.col("symbol"), F.lit(".SH")))
                dates = spark.range(days).selectExpr("id as day", "date_add(date'2020-01-01', cast(id as int)) as trading_date")
                pairs = symbols.crossJoin(dates).withColumn("close", F.lit(10.0) + F.col("id") / 1000 + F.col("day") / 100)
                bars = pairs.selectExpr("security_id", "symbol", "'SH' as exchange", "trading_date", "close as open", "close + 1 as high", "close - 1 as low", "close", "cast(1000 + day as double) as volume", "cast(close * (1000 + day) as double) as amount", "cast(0.1 as double) as turnover", "false as is_suspended", "false as is_st", "cast(1 as double) as adjustment_factor", "'qfq' as adjustment_type", "'synthetic' as provider", "concat(security_id, cast(trading_date as string)) as source_record_id", "'v1' as source_version", "'hash1' as payload_hash", "'synthetic' as raw_object_path", "cast(trading_date as timestamp) as event_time", "cast(trading_date as timestamp) as available_at", "cast(trading_date as timestamp) as ingested_at", "1 as schema_version")
                corrected = bars.filter("security_id = '000000.SH' AND trading_date = date'2020-01-01'").withColumn("source_version", F.lit("v2")).withColumn("payload_hash", F.lit("hash2")).withColumn("available_at", F.expr("available_at + INTERVAL 1 DAY"))
                bars = bars.unionByName(corrected)
                master = symbols.selectExpr("security_id", "symbol", "'SH' as exchange", "'stock' as security_type", "symbol as name", "date'2020-01-01' as list_date", "cast(null as date) as delist_date", "'active' as status", "'synthetic' as provider", "security_id as source_record_id", "'v1' as source_version", "'hash1' as payload_hash", "'synthetic' as raw_object_path", "timestamp'2020-01-01 00:00:00' as event_time", "timestamp'2020-01-01 00:00:00' as available_at", "timestamp'2020-01-01 00:00:00' as ingested_at", "1 as schema_version")
                calendar = dates.selectExpr("'SH' as exchange", "trading_date", "true as is_open", "cast(trading_date as timestamp) as session_open_at", "cast(trading_date as timestamp) as session_close_at")
                for name, frame in zip(NAMES, (bars, master, calendar)):
                    target = f"{namespace}.{name}"
                    frame.writeTo(target).using("iceberg").create()
                    out["sources"][name] = {"table": target, "rows": frame.count(), "snapshot_id": int(spark.sql(f"SELECT snapshot_id FROM {target}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0])}
                out["generator"] = {"seed": 0, "symbols": count, "days": days, "expected_daily_bars": count * days}
        else:
            if not a.manifest:
                raise ValueError("Input manifest is required")
            manifest = json.loads(Path(a.manifest).read_text(encoding="utf-8"))
            out["input_snapshots"] = {}
            for name in NAMES:
                current = spark.sql(f"SELECT snapshot_id FROM {namespace}.{name}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
                expected = manifest["sources"][name].get("copy_snapshot_id", manifest["sources"][name]["snapshot_id"])
                if current is None or int(current[0]) != expected:
                    raise ValueError(f"Input snapshot changed for {name}")
                out["input_snapshots"][name] = expected
            if a.workload == "gold":
                source = f"{namespace}.daily_bars"
                bars = load_daily_bars(spark, input_path=None, table=source, start_date=None, end_date=None)
                frame = build_gold_frame(calculate_metrics(bars), start_date=date(1900, 1, 1), end_date=None).repartition(a.partitions)
                out["plan"] = frame._jdf.queryExecution().toString()
                out["validation"] = validate_gold_frame(frame)
                target = f"{namespace}.gold_{run_id}"
                if table_exists(spark, target):
                    raise ValueError(f"Target already exists: {target}")
                write_gold_table(frame, target, a.distribution)
                targets = [target]
            else:
                tables = [MigrationTable(f"{namespace}.{name}", f"{namespace}.{name}_{run_id}", f"{namespace}.{name}_backup_{run_id}", partition) for name, partition in zip(NAMES, ("PARTITIONED BY (months(trading_date), exchange, provider)", "", ""))]
                frames = build_frames(spark, tables, a.partitions)
                out["plan"] = {name: frame._jdf.queryExecution().toString() for name, frame in frames.items()}
                out["validation"] = build_compacted_tables(spark, tables, frames, a.distribution)
                targets = [t.staging for t in tables]
            if manifest.get("generator"):
                expected_rows = manifest["generator"]["expected_daily_bars"]
                if a.workload == "gold":
                    if out["validation"]["row_count"] != expected_rows:
                        raise ValueError("Synthetic Gold row count differs from expected")
                    sample = spark.table(targets[0]).filter("security_id = '000000.SH' AND trading_date = date'2020-01-21'").first()
                    if sample is None or abs(sample["return_20d"] - 0.02) > 1e-9 or abs(sample["ma_5d"] - 10.18) > 1e-9:
                        raise ValueError("Synthetic Gold window metrics differ from expected")
                else:
                    if out["validation"][tables[0].name]["deduplicated_rows"] != expected_rows:
                        raise ValueError("Synthetic Silver row count differs from expected")
                    corrected = spark.table(targets[0]).filter("security_id = '000000.SH' AND trading_date = date'2020-01-01'").first()
                    if corrected is None or corrected["source_version"] != "v2":
                        raise ValueError("Synthetic Silver correction was not selected")
            out["targets"] = {}
            for target in targets:
                snap = spark.sql(f"SELECT snapshot_id FROM {target}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
                files = spark.sql(f"SELECT file_size_in_bytes FROM {target}.files").collect()
                sizes = sorted(int(row[0]) for row in files)
                result = spark.table(target)
                columns = [F.round(F.col(c), 9) if t in {"double", "float"} else F.col(c) for c, t in result.dtypes if c != "gold_built_at"]
                fingerprint = result.select(F.xxhash64(*columns).alias("h")).agg(F.count("*").alias("rows"), F.sum(F.col("h").cast("decimal(38,0)")).alias("hash_sum")).first()
                out["targets"][target] = {"snapshot_id": int(snap[0]), "file_count": len(sizes), "total_bytes": sum(sizes), "file_sizes": sizes, "rows": fingerprint.rows, "hash_sum": str(fingerprint.hash_sum)}
            out["elapsed_seconds"] = time.monotonic() - started
            if a.reference:
                reference = identifier(a.reference)
                keys = [["provider", "adjustment_type", "security_id", "trading_date"]] if a.workload == "gold" else [["provider", "adjustment_type", "security_id", "trading_date"], ["provider", "security_id"], ["exchange", "trading_date"]]
                refs = [f"{namespace}.gold_{reference}"] if a.workload == "gold" else [f"{namespace}.{name}_{reference}" for name in NAMES]
                out["comparison"] = {}
                for target, ref, key in zip(targets, refs, keys):
                    mismatches = compare_tables(spark, target, ref, key)
                    out["comparison"][target] = {"reference": ref, "mismatched_rows": mismatches, "float_absolute_tolerance": 1e-9, "float_relative_tolerance": 1e-9}
                    if mismatches:
                        raise ValueError(f"{target} differs from {ref}: {mismatches} rows")
        out["status"] = "success"
    except Exception as exc:
        out["status"], out["error"] = "failed", str(exc)
        raise
    finally:
        out.setdefault("elapsed_seconds", time.monotonic() - started)
        out["total_job_seconds"] = time.monotonic() - started
        Path(a.result).parent.mkdir(parents=True, exist_ok=True)
        Path(a.result).write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        spark.stop()

if __name__ == "__main__":
    main()
