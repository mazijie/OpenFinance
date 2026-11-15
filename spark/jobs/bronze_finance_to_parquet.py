"""First Spark learning job: normalize finance Bronze JSON into Parquet.

This job intentionally keeps the transformation small enough to understand in one
sitting while preserving the important parts of the existing Python pipeline:
nested Bronze data, validation, deduplication and partitioned output.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


BRONZE_SCHEMA = StructType(
    [
        StructField("provider", StringType(), False),
        StructField("ingested_at", StringType(), False),
        StructField("source_version", StringType(), True),
        StructField("payload_hash", StringType(), True),
        StructField(
            "request_params",
            StructType(
                [
                    StructField("adjustment_type", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "raw_response",
            StructType(
                [
                    StructField("daily_bars", ArrayType(StructType([])), True),
                ]
            ),
            False,
        ),
    ]
)


BAR_SCHEMA = StructType(
    [
        StructField("security_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("trading_date", StringType(), True),
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("amount", DoubleType(), True),
        StructField("turnover", DoubleType(), True),
        StructField("is_suspended", BooleanType(), True),
        StructField("is_st", BooleanType(), True),
        StructField("adjustment_factor", DoubleType(), True),
        StructField("adjustment_type", StringType(), True),
        StructField("source_record_id", StringType(), True),
        StructField("source_version", StringType(), True),
        StructField("payload_hash", StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Bronze JSON file or directory.")
    parser.add_argument("--output", required=True, help="Parquet output directory.")
    parser.add_argument("--write-mode", choices=("overwrite", "error"), default="overwrite")
    return parser.parse_args()


def create_spark(master: str | None = None) -> SparkSession:
    builder = SparkSession.builder.appName("openfinance-bronze-finance")
    if master:
        builder = builder.master(master)
    return (
        builder.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, input_path: str) -> DataFrame:
    """Read one or more pretty-printed Bronze JSON objects."""

    nested_schema = StructType(
        [
            *BRONZE_SCHEMA.fields[:4],
            StructField(
                "request_params",
                BRONZE_SCHEMA["request_params"].dataType,
                True,
            ),
            StructField(
                "raw_response",
                StructType(
                    [
                        StructField("daily_bars", ArrayType(BAR_SCHEMA), True),
                    ]
                ),
                False,
            ),
        ]
    )
    return (
        spark.read.schema(nested_schema)
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(input_path)
        .withColumn("raw_object_path", F.input_file_name())
    )


def normalize_daily_bars(bronze: DataFrame) -> DataFrame:
    """Explode, validate and deduplicate Bronze daily bars."""

    bronze = bronze.filter(F.lower(F.col("provider")) != F.lit("baostock"))
    exploded = bronze.select(
        F.col("provider").alias("bronze_provider"),
        F.to_timestamp("ingested_at").alias("bronze_ingested_at"),
        F.col("source_version").alias("bronze_source_version"),
        F.col("payload_hash").alias("bronze_payload_hash"),
        F.col("request_params.adjustment_type").alias("request_adjustment_type"),
        F.col("raw_object_path"),
        F.explode("raw_response.daily_bars").alias("bar"),
    ).select("bronze_provider", "bronze_ingested_at", "bronze_source_version", "bronze_payload_hash", "request_adjustment_type", "raw_object_path", "bar.*")

    normalized = (
        exploded.withColumn("symbol", F.upper(F.trim("symbol")))
        .withColumn("exchange", F.upper(F.trim("exchange")))
        .withColumn(
            "security_id",
            F.coalesce(
                F.nullif(F.trim("security_id"), F.lit("")),
                F.concat_ws(".", F.upper(F.trim("symbol")), F.upper(F.trim("exchange"))),
            ),
        )
        .withColumn(
            "trading_date",
            F.to_date(F.coalesce(F.col("trading_date"), F.col("date"))),
        )
        .withColumn(
            "adjustment_type",
            F.lower(
                F.coalesce(
                    F.nullif(F.trim("adjustment_type"), F.lit("")),
                    F.col("request_adjustment_type"),
                )
            ),
        )
        .withColumn(
            "provider",
            F.coalesce(
                F.nullif(F.trim("bronze_provider"), F.lit("")),
                F.lit("unknown"),
            ),
        )
        .withColumn(
            "source_version",
            F.coalesce(
                F.nullif(F.trim("source_version"), F.lit("")),
                F.col("bronze_source_version"),
            ),
        )
        .withColumn(
            "payload_hash",
            F.coalesce(
                F.nullif(F.trim("payload_hash"), F.lit("")),
                F.col("bronze_payload_hash"),
            ),
        )
        .withColumn(
            "source_record_id",
            F.coalesce(
                F.nullif(F.trim("source_record_id"), F.lit("")),
                F.concat_ws(
                    "-",
                    F.upper(F.trim("symbol")),
                    F.date_format(F.col("trading_date"), "yyyy-MM-dd"),
                    F.col("adjustment_type"),
                ),
            ),
        )
        .withColumn(
            "is_suspended",
            F.when(
                (~F.coalesce(F.col("is_suspended"), F.lit(False)))
                & (F.col("volume") == 0)
                & (F.col("open") == 0)
                & (F.col("high") == 0)
                & (F.col("low") == 0)
                & (F.col("close") == 0),
                F.lit(True),
            ).otherwise(F.col("is_suspended")),
        )
        .withColumn("event_time", F.col("trading_date").cast("timestamp"))
        .withColumn("available_at", F.col("bronze_ingested_at"))
        .withColumn("ingested_at", F.col("bronze_ingested_at"))
        .withColumn("schema_version", F.lit(1).cast("int"))
    )

    required_values_present = (
        F.col("security_id").isNotNull()
        & F.col("symbol").isNotNull()
        & F.col("exchange").isNotNull()
        & F.col("trading_date").isNotNull()
        & F.col("open").isNotNull()
        & F.col("high").isNotNull()
        & F.col("low").isNotNull()
        & F.col("close").isNotNull()
        & F.col("volume").isNotNull()
    )
    finite_prices = F.lit(True)
    for name in ("open", "high", "low", "close"):
        finite_prices = finite_prices & ~F.isnan(name) & (F.abs(F.col(name)) < F.lit(float("inf")))
    allowed_prices = F.col("adjustment_type").isin("qfq", "forward") | (F.least("open", "high", "low", "close") >= 0)
    valid_numbers = finite_prices & allowed_prices & (F.col("volume") >= 0) & (F.col("volume") < F.lit(float("inf")))
    valid_ohlc = (
        (F.col("high") >= F.greatest("open", "low", "close"))
        & (F.col("low") <= F.least("open", "high", "close"))
    )

    valid = normalized.filter(required_values_present & valid_numbers & valid_ohlc & (F.col("request_adjustment_type") == "none") & (F.col("adjustment_type") == "none"))
    business_key = [
        "provider",
        "adjustment_type",
        "security_id",
        "trading_date",
    ]
    latest_record = (
        Window.partitionBy(*business_key)
        .orderBy(
            F.col("available_at").desc_nulls_last(),
            F.col("source_version").desc_nulls_last(),
            F.col("payload_hash").desc_nulls_last(),
        )
    )
    return valid.withColumn("_row_number", F.row_number().over(latest_record)).filter(
        F.col("_row_number") == 1
    ).drop("_row_number", "date", "bronze_provider", "bronze_ingested_at", "bronze_source_version", "bronze_payload_hash", "request_adjustment_type")


def run(input_path: str, output_path: str, write_mode: str = "overwrite") -> dict[str, Any]:
    spark = create_spark()
    try:
        bronze = read_bronze(spark, input_path)
        raw_count = bronze.select(
            F.sum(F.size("raw_response.daily_bars"))
        ).first()[0] or 0

        daily_bars = normalize_daily_bars(bronze)
        valid_count = daily_bars.count()

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        (
            daily_bars.repartition("trading_date", "exchange")
            .write.mode(write_mode)
            .partitionBy("trading_date", "exchange")
            .parquet(str(output))
        )
        written_count = spark.read.parquet(str(output)).count()
        return {
            "raw_daily_bar_rows": int(raw_count),
            "filtered_or_duplicate_rows": int(raw_count) - int(valid_count),
            "normalized_daily_bar_rows": int(valid_count),
            "written_daily_bar_rows": int(written_count),
            "output": str(output),
        }
    finally:
        spark.stop()


def main() -> None:
    args = parse_args()
    try:
        summary = run(args.input, args.output, args.write_mode)
    except Exception as exc:
        raise SystemExit(f"Spark job failed: {exc}") from exc
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
