"""Second Spark learning job: calculate daily price and risk metrics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", help="Normalized daily-bars Parquet path.")
    source.add_argument("--table", help="Iceberg table, for example openfinance.openfinance.daily_bars.")
    parser.add_argument("--output", help="Optional metrics Parquet output path.")
    parser.add_argument("--start-date", help="Optional inclusive trading-date lower bound.")
    parser.add_argument("--end-date", help="Optional inclusive trading-date upper bound.")
    parser.add_argument("--write-mode", choices=("overwrite", "error"), default="overwrite")
    return parser.parse_args()


def create_spark(master: str | None = None) -> SparkSession:
    builder = SparkSession.builder
    if master:
        builder = builder.master(master)
    return (
        builder.config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def calculate_metrics(daily_bars: DataFrame) -> DataFrame:
    """Add point-in-time metrics without reading future observations."""

    security_window = Window.partitionBy(
        "provider",
        "adjustment_type",
        "security_id",
    ).orderBy("trading_date")
    close_5 = security_window.rowsBetween(-4, 0)
    close_20 = security_window.rowsBetween(-19, 0)

    valid_close = F.when(~F.col("is_suspended"), F.col("close"))
    valid_volume = F.when(~F.col("is_suspended"), F.col("volume"))

    with_lags = (
        daily_bars.withColumn("close_lag_1", F.lag(valid_close, 1).over(security_window))
        .withColumn("close_lag_5", F.lag(valid_close, 5).over(security_window))
        .withColumn("close_lag_20", F.lag(valid_close, 20).over(security_window))
        .withColumn(
            "daily_return_1d",
            F.when(
                (~F.col("is_suspended"))
                & F.col("close_lag_1").isNotNull()
                & (F.col("close_lag_1") > 0),
                F.col("close") / F.col("close_lag_1") - 1,
            ),
        )
        .withColumn(
            "return_5d",
            F.when(
                (~F.col("is_suspended"))
                & F.col("close_lag_5").isNotNull()
                & (F.col("close_lag_5") > 0),
                F.col("close") / F.col("close_lag_5") - 1,
            ),
        )
        .withColumn(
            "return_20d",
            F.when(
                (~F.col("is_suspended"))
                & F.col("close_lag_20").isNotNull()
                & (F.col("close_lag_20") > 0),
                F.col("close") / F.col("close_lag_20") - 1,
            ),
        )
    )

    return (
        with_lags.withColumn(
            "ma_5d",
            F.avg(valid_close).over(close_5),
        )
        .withColumn(
            "ma_20d",
            F.avg(valid_close).over(close_20),
        )
        .withColumn(
            "volume_ma_5d",
            F.avg(valid_volume).over(close_5),
        )
        .withColumn(
            "volatility_20d_annualized",
            F.stddev_samp("daily_return_1d").over(close_20) * F.sqrt(F.lit(252.0)),
        )
        .withColumn(
            "observed_days_20d",
            F.count(valid_close).over(close_20),
        )
    )


def load_daily_bars(
    spark: SparkSession,
    *,
    input_path: str | None,
    table: str | None,
    start_date: str | None,
    end_date: str | None,
) -> DataFrame:
    if input_path:
        daily_bars = spark.read.parquet(input_path)
    elif table:
        daily_bars = spark.table(table)
    else:
        raise ValueError("Either input_path or table is required")

    if start_date:
        daily_bars = daily_bars.filter(F.col("trading_date") >= F.to_date(F.lit(start_date)))
    if end_date:
        daily_bars = daily_bars.filter(F.col("trading_date") <= F.to_date(F.lit(end_date)))
    return deduplicate_daily_bars(daily_bars)


def deduplicate_daily_bars(daily_bars: DataFrame) -> DataFrame:
    """Keep the latest visible row for each business key before windowing."""

    business_key = Window.partitionBy(
        "provider",
        "adjustment_type",
        "security_id",
        "trading_date",
    ).orderBy(
        F.col("available_at").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
        F.col("source_version").desc_nulls_last(),
        F.col("payload_hash").desc_nulls_last(),
    )
    return (
        daily_bars.withColumn("_source_row_number", F.row_number().over(business_key))
        .filter(F.col("_source_row_number") == 1)
        .drop("_source_row_number")
    )


def run(
    *,
    input_path: str | None,
    table: str | None,
    output_path: str | None,
    start_date: str | None,
    end_date: str | None,
    write_mode: str = "overwrite",
) -> dict[str, Any]:
    spark = create_spark()
    try:
        daily_bars = load_daily_bars(
            spark,
            input_path=input_path,
            table=table,
            start_date=start_date,
            end_date=end_date,
        )
        metrics = calculate_metrics(daily_bars)

        if output_path:
            output = Path(output_path)
            output.parent.mkdir(parents=True, exist_ok=True)
            (
                metrics.repartition("trading_date")
                .write.mode(write_mode)
                .partitionBy("trading_date")
                .parquet(str(output))
            )
            result = spark.read.parquet(str(output))
        else:
            result = metrics

        latest_date = result.agg(F.max("trading_date")).first()[0]
        latest_rows = (
            result.filter(F.col("trading_date") == latest_date)
            .orderBy("security_id")
            .select(
                "security_id",
                "trading_date",
                "close",
                "daily_return_1d",
                "return_5d",
                "return_20d",
                "ma_5d",
                "ma_20d",
                "volume_ma_5d",
                "volatility_20d_annualized",
                "observed_days_20d",
            )
            .toJSON()
            .collect()
        )
        return {
            "input_rows": daily_bars.count(),
            "result_rows": result.count(),
            "security_count": result.select("security_id").distinct().count(),
            "date_range": [
                str(result.agg(F.min("trading_date")).first()[0]),
                str(latest_date),
            ],
            "latest_rows": [json.loads(row) for row in latest_rows],
            "source": input_path or table,
            "output": output_path,
        }
    finally:
        spark.stop()


def main() -> None:
    args = parse_args()
    try:
        summary = run(
            input_path=args.input,
            table=args.table,
            output_path=args.output,
            start_date=args.start_date,
            end_date=args.end_date,
            write_mode=args.write_mode,
        )
    except Exception as exc:
        raise SystemExit(f"Spark job failed: {exc}") from exc
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
