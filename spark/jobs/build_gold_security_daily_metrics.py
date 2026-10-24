"""Build the Gold security daily metrics table from Iceberg Silver bars."""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from daily_metrics import calculate_metrics, create_spark, load_daily_bars


GOLD_TABLE = "openfinance.openfinance.gold_security_daily_metrics"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-table", default="openfinance.openfinance.daily_bars")
    parser.add_argument("--target-table", default=GOLD_TABLE)
    parser.add_argument("--start-date", help="Inclusive output start date.")
    parser.add_argument("--end-date", help="Optional inclusive target end date.")
    parser.add_argument("--mode", choices=("full", "incremental"), default="incremental")
    parser.add_argument("--lookback-days", type=int, default=45)
    parser.add_argument(
        "--warmup-days",
        type=int,
        default=45,
        help="Calendar-day lookback included in calculations but excluded from Gold output.",
    )
    return parser.parse_args()


def build_gold_frame(
    metrics: DataFrame,
    *,
    start_date: date,
    end_date: date | None,
) -> DataFrame:
    """Select a stable Gold contract and calculate feature availability."""

    security_window = Window.partitionBy(
        "provider",
        "adjustment_type",
        "security_id",
    ).orderBy("trading_date")
    # return_20d needs lag(20), and 20 returns need 21 closes, so feature
    # availability covers current row plus 20 preceding rows.
    feature_window = security_window.rowsBetween(-20, 0)

    enriched = (
        metrics.withColumn(
            "feature_available_at",
            F.max("available_at").over(feature_window),
        )
        .withColumn(
            "metric_status",
            F.when(
                (F.col("observed_days_20d") == 20)
                & F.col("daily_return_1d").isNotNull()
                & F.col("return_5d").isNotNull()
                & F.col("return_20d").isNotNull()
                & F.col("ma_5d").isNotNull()
                & F.col("ma_20d").isNotNull()
                & F.col("volume_ma_5d").isNotNull()
                & F.col("volatility_20d_annualized").isNotNull(),
                F.lit("complete"),
            ).otherwise(F.lit("partial")),
        )
        .withColumn("source_available_at", F.col("available_at"))
        .withColumn("source_ingested_at", F.col("ingested_at"))
        .withColumn("gold_built_at", F.current_timestamp())
        .withColumn("schema_version", F.lit(1).cast("int"))
    )

    result = enriched.filter(F.col("trading_date") >= F.lit(start_date))
    if end_date is not None:
        result = result.filter(F.col("trading_date") <= F.lit(end_date))

    return result.select(
        "security_id",
        "symbol",
        "exchange",
        "trading_date",
        "provider",
        "adjustment_type",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "turnover",
        "is_suspended",
        "is_st",
        "adjustment_factor",
        "daily_return_1d",
        "return_5d",
        "return_20d",
        "ma_5d",
        "ma_20d",
        "volume_ma_5d",
        "volatility_20d_annualized",
        "observed_days_20d",
        "metric_status",
        "source_available_at",
        "source_ingested_at",
        "feature_available_at",
        "gold_built_at",
        "schema_version",
    )


def validate_gold_frame(gold: DataFrame) -> dict[str, Any]:
    key_columns = ["provider", "adjustment_type", "security_id", "trading_date"]
    row_count = gold.count()
    if row_count == 0:
        raise ValueError("Gold frame is empty")

    duplicate_count = gold.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
    if duplicate_count:
        raise ValueError(f"Gold frame contains {duplicate_count} duplicated business keys")

    invalid_price_count = gold.filter(
        (F.col("high") < F.col("low"))
        | (F.col("high") < F.col("close"))
        | (F.col("low") > F.col("close"))
    ).count()
    if invalid_price_count:
        raise ValueError(f"Gold frame contains {invalid_price_count} invalid OHLC rows")

    summary = gold.agg(
        F.count("*").alias("row_count"),
        F.countDistinct("security_id").alias("security_count"),
        F.min("trading_date").alias("min_date"),
        F.max("trading_date").alias("max_date"),
        F.sum(F.when(F.col("metric_status") == "complete", 1).otherwise(0)).alias(
            "complete_metric_rows"
        ),
    ).first()

    return {
        "row_count": int(summary["row_count"]),
        "security_count": int(summary["security_count"]),
        "min_date": str(summary["min_date"]),
        "max_date": str(summary["max_date"]),
        "complete_metric_rows": int(summary["complete_metric_rows"]),
        "partial_metric_rows": int(summary["row_count"] - summary["complete_metric_rows"]),
        "duplicate_business_keys": 0,
        "invalid_price_rows": 0,
    }


def write_gold_table(gold: DataFrame, table_name: str) -> None:
    """Rebuild the learning Gold table atomically through a CTAS commit."""

    view_name = "_gold_security_daily_metrics_source"
    gold.createOrReplaceTempView(view_name)
    spark = gold.sparkSession
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {table_name}
        USING iceberg
        PARTITIONED BY (months(trading_date))
        TBLPROPERTIES (
          'format-version' = '2',
          'write.target-file-size-bytes' = '134217728'
        )
        AS SELECT * FROM {view_name}
        """
    )
    spark.catalog.dropTempView(view_name)


def merge_gold_table(
    gold: DataFrame,
    table_name: str,
    *,
    start_date: date,
    end_date: date,
) -> None:
    """Atomically replace only the monthly partitions recomputed by this run.

    Spark 4.1 currently crashes while planning an Iceberg MERGE whose source is
    an Iceberg-backed DataFrame. Iceberg dynamic partition overwrite provides the
    same bounded incremental semantics without that planner path.
    """

    spark = gold.sparkSession
    target = spark.table(table_name)
    month_end = (end_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    keys = ["provider", "adjustment_type", "security_id", "trading_date"]
    affected_existing = target.filter(
        (F.col("trading_date") >= F.lit(start_date))
        & (F.col("trading_date") <= F.lit(month_end))
    )
    unaffected_existing = affected_existing.join(gold.select(*keys), on=keys, how="left_anti")
    combined = unaffected_existing.unionByName(gold, allowMissingColumns=True)
    combined.writeTo(table_name).overwritePartitions()


def run(
    *,
    source_table: str,
    target_table: str,
    start_date: date,
    end_date: date | None,
    warmup_days: int,
    mode: str = "incremental",
) -> dict[str, Any]:
    if warmup_days < 0:
        raise ValueError("warmup_days cannot be negative")
    if mode == "incremental":
        # Dynamic partition overwrite replaces entire monthly partitions. Expand
        # the requested lookback to month boundaries so older rows in the same
        # partition cannot be dropped accidentally.
        start_date = start_date.replace(day=1)

    spark = create_spark()
    try:
        source_start = start_date - timedelta(days=warmup_days)
        daily_bars = load_daily_bars(
            spark,
            input_path=None,
            table=source_table,
            start_date=source_start.isoformat(),
            end_date=end_date.isoformat() if end_date else None,
        )
        metrics = calculate_metrics(daily_bars)
        gold = build_gold_frame(metrics, start_date=start_date, end_date=end_date)
        validation = validate_gold_frame(gold)
        target_exists = spark.catalog.tableExists(target_table)
        effective_mode = "full" if mode == "full" or not target_exists else "incremental"
        if effective_mode == "full":
            write_gold_table(gold, target_table)
        else:
            merge_gold_table(
                gold,
                target_table,
                start_date=start_date,
                end_date=end_date or gold.agg(F.max("trading_date")).first()[0],
            )

        persisted = spark.table(target_table)
        persisted_count = persisted.filter(F.col("trading_date") >= F.lit(start_date))
        if end_date is not None:
            persisted_count = persisted_count.filter(F.col("trading_date") <= F.lit(end_date))
        persisted_rows = persisted_count.count()
        if persisted_rows != validation["row_count"]:
            raise ValueError(
                f"Persisted Gold row count {persisted_rows} does not match "
                f"staged count {validation['row_count']}"
            )

        current_snapshot = spark.sql(
            f"SELECT snapshot_id FROM {target_table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()
        return {
            "table": target_table,
            "source": source_table,
            "source_start_with_warmup": source_start.isoformat(),
            "target_start": start_date.isoformat(),
            "target_end": end_date.isoformat() if end_date else validation["max_date"],
            "warmup_days": warmup_days,
            "mode": effective_mode,
            "snapshot_id": int(current_snapshot["snapshot_id"]) if current_snapshot else None,
            "validation": validation,
        }
    finally:
        spark.stop()


def main() -> None:
    args = parse_args()
    lookback_days = args.lookback_days
    if lookback_days < 0:
        raise SystemExit("--lookback-days cannot be negative")
    start_date = (
        date.fromisoformat(args.start_date)
        if args.start_date
        else date(2025, 1, 1)
        if args.mode == "full"
        else date.today() - timedelta(days=lookback_days)
    )
    try:
        summary = run(
            source_table=args.source_table,
            target_table=args.target_table,
            start_date=start_date,
            end_date=date.fromisoformat(args.end_date) if args.end_date else None,
            warmup_days=args.warmup_days,
            mode=args.mode,
        )
    except Exception as exc:
        raise SystemExit(f"Gold build failed: {exc}") from exc
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
