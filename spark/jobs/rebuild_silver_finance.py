"""Deduplicate and repartition finance Silver tables with a rollback backup."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from daily_metrics import create_spark


@dataclass(frozen=True)
class MigrationTable:
    name: str
    staging: str
    backup: str
    partition_sql: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("build", "activate"), default="build")
    parser.add_argument("--suffix", default="20261025")
    return parser.parse_args()


def migration_tables(suffix: str) -> list[MigrationTable]:
    namespace = "openfinance.openfinance"
    return [
        MigrationTable(
            name=f"{namespace}.daily_bars",
            staging=f"{namespace}.daily_bars_compacted_{suffix}",
            backup=f"{namespace}.daily_bars_backup_{suffix}",
            partition_sql="PARTITIONED BY (months(trading_date), exchange, provider)",
        ),
        MigrationTable(
            name=f"{namespace}.security_master",
            staging=f"{namespace}.security_master_compacted_{suffix}",
            backup=f"{namespace}.security_master_backup_{suffix}",
            partition_sql="",
        ),
        MigrationTable(
            name=f"{namespace}.trading_calendar",
            staging=f"{namespace}.trading_calendar_compacted_{suffix}",
            backup=f"{namespace}.trading_calendar_backup_{suffix}",
            partition_sql="",
        ),
    ]


def deduplicate_daily_bars(df: DataFrame, partitions: int = 4) -> DataFrame:
    key_window = Window.partitionBy(
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
        df.withColumn("_row_number", F.row_number().over(key_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .repartition(
            partitions,
            "exchange",
            "provider",
            F.date_trunc("month", F.col("trading_date")),
        )
    )


def deduplicate_security_master(df: DataFrame, partitions: int = 4) -> DataFrame:
    key_window = Window.partitionBy("provider", "security_id").orderBy(
        F.col("available_at").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
        F.col("source_version").desc_nulls_last(),
        F.col("payload_hash").desc_nulls_last(),
    )
    return (
        df.withColumn("_row_number", F.row_number().over(key_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .repartition(partitions)
    )


def deduplicate_trading_calendar(df: DataFrame, partitions: int = 4) -> DataFrame:
    return (
        df.groupBy("exchange", "trading_date")
        .agg(
            F.max("is_open").alias("is_open"),
            F.max("session_open_at").alias("session_open_at"),
            F.max("session_close_at").alias("session_close_at"),
        )
        .repartition(partitions)
    )


def build_frames(spark: SparkSession, tables: list[MigrationTable], partitions: int = 4) -> dict[str, DataFrame]:
    daily_bars = deduplicate_daily_bars(spark.table(tables[0].name), partitions)
    security_master = deduplicate_security_master(spark.table(tables[1].name), partitions)
    trading_calendar = deduplicate_trading_calendar(spark.table(tables[2].name), partitions)
    return {
        tables[0].name: daily_bars,
        tables[1].name: security_master,
        tables[2].name: trading_calendar,
    }


def validate_unique(df: DataFrame, key_columns: list[str]) -> int:
    duplicate_count = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_count:
        raise ValueError(
            f"Compacted table still has {duplicate_count} duplicate keys: {key_columns}"
        )
    return int(df.count())


def table_exists(spark: SparkSession, identifier: str) -> bool:
    """Check Iceberg tables without enabling JDBC catalog view support."""

    parts = identifier.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected catalog.namespace.table, got {identifier}")
    _, namespace, table_name = parts
    return any(
        row["tableName"] == table_name
        for row in spark.sql(f"SHOW TABLES IN {parts[0]}.{namespace}").collect()
    )


def build_compacted_tables(
    spark: SparkSession,
    tables: list[MigrationTable],
    frames: dict[str, DataFrame],
    distribution: str = "hash",
) -> dict[str, Any]:
    if distribution not in {"hash", "none"}:
        raise ValueError("Unsupported Iceberg write distribution")
    results: dict[str, Any] = {}
    key_columns = {
        tables[0].name: ["provider", "adjustment_type", "security_id", "trading_date"],
        tables[1].name: ["provider", "security_id"],
        tables[2].name: ["exchange", "trading_date"],
    }

    for table in tables:
        if table_exists(spark, table.staging):
            raise ValueError(f"Staging table already exists: {table.staging}")
        if table_exists(spark, table.backup):
            raise ValueError(f"Backup table already exists: {table.backup}")

        source_count = spark.table(table.name).count()
        compacted = frames[table.name]
        view_name = f"_migration_{table.name.rsplit('.', 1)[-1]}"
        compacted.createOrReplaceTempView(view_name)
        spark.sql(
            f"""
            CREATE TABLE {table.staging}
            USING iceberg
            {table.partition_sql}
            TBLPROPERTIES (
              'format-version' = '2',
              'write.distribution-mode' = '{distribution}',
              'write.target-file-size-bytes' = '134217728'
            )
            AS SELECT * FROM {view_name}
            """
        )
        spark.catalog.dropTempView(view_name)

        staged = spark.table(table.staging)
        staged_count = validate_unique(staged, key_columns[table.name])
        snapshot = spark.sql(
            f"SELECT snapshot_id FROM {table.staging}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()
        results[table.name] = {
            "source_rows": int(source_count),
            "deduplicated_rows": int(staged_count),
            "removed_duplicate_rows": int(source_count) - int(staged_count),
            "staging": table.staging,
            "backup": table.backup,
            "snapshot_id": int(snapshot["snapshot_id"]) if snapshot else None,
        }
    return results


def activate_tables(
    spark: SparkSession,
    tables: list[MigrationTable],
    key_columns: dict[str, list[str]],
) -> None:
    for table in tables:
        for identifier in (table.name, table.staging):
            if not table_exists(spark, identifier):
                raise ValueError(f"Required table does not exist: {identifier}")
        if table_exists(spark, table.backup):
            raise ValueError(f"Backup table already exists: {table.backup}")
        validate_unique(spark.table(table.staging), key_columns[table.name])

    for table in tables:
        _, namespace, current_name = table.name.split(".")
        backup_name = table.backup.rsplit(".", 1)[-1]
        staging_table = table.staging
        spark.sql(
            f"ALTER TABLE {table.name} RENAME TO "
            f"{namespace}.{backup_name}"
        )
        spark.sql(
            f"ALTER TABLE {staging_table} RENAME TO "
            f"{namespace}.{current_name}"
        )


def run(mode: str, suffix: str) -> dict[str, Any]:
    tables = migration_tables(suffix)
    spark = create_spark()
    try:
        key_columns = {
            tables[0].name: ["provider", "adjustment_type", "security_id", "trading_date"],
            tables[1].name: ["provider", "security_id"],
            tables[2].name: ["exchange", "trading_date"],
        }
        if mode == "build":
            frames = build_frames(spark, tables)
            return {
                "mode": mode,
                "tables": build_compacted_tables(spark, tables, frames),
            }

        activate_tables(spark, tables, key_columns)
        return {
            "mode": mode,
            "activated": {
                table.name: {
                    "backup": table.backup,
                    "rows": spark.table(table.name).count(),
                }
                for table in tables
            },
        }
    finally:
        spark.stop()


def main() -> None:
    args = parse_args()
    try:
        result = run(args.mode, args.suffix)
    except Exception as exc:
        raise SystemExit(f"Silver migration failed: {exc}") from exc
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
