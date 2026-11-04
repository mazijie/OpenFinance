"""Standalone Spark assertions; uses only in-memory frames and a local session."""

from datetime import date, datetime, timedelta
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark" / "jobs"))
from pyspark.sql import SparkSession, functions as F
from bronze_finance_to_parquet import BRONZE_SCHEMA, BAR_SCHEMA, normalize_daily_bars
from daily_metrics import calculate_metrics
from build_gold_security_daily_metrics import build_gold_frame, validate_gold_frame
from pyspark.sql.types import ArrayType, StructField, StructType


def main():
    spark = (SparkSession.builder.master("local[2]").appName("tencent-daily-check")
             .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        schema = StructType([*BRONZE_SCHEMA.fields[:5], StructField("raw_response", StructType([
            StructField("daily_bars", ArrayType(BAR_SCHEMA), True)
        ]), False)])
        raw = {"provider": "tencent", "ingested_at": "2026-11-04T09:00:00Z", "source_version": "test",
               "payload_hash": "raw", "request_params": {"adjustment_type": "qfq"}, "raw_response": {"daily_bars": [{
                   "security_id": "600519.SH", "symbol": "600519.SH", "exchange": "SH", "trading_date": "2026-11-04",
                   "open": -10.0, "close": -9.0, "high": -8.0, "low": -11.0, "volume": 100.0,
                   "is_st": None, "is_suspended": None, "adjustment_factor": None
               }]}}
        bronze = spark.read.schema(schema).json(spark.sparkContext.parallelize([json.dumps(raw)])).withColumn("raw_object_path", F.lit("test://raw"))
        normalized = normalize_daily_bars(bronze).collect()
        assert len(normalized) == 1 and normalized[0].close == -9
        assert normalized[0].is_st is None and normalized[0].is_suspended is None and normalized[0].adjustment_factor is None

        rows = [{"security_id": "600519.SH", "symbol": "600519", "exchange": "SH", "provider": "tencent",
                 "adjustment_type": "qfq", "trading_date": date(2026, 10, 1) + timedelta(days=i),
                 "open": 100.0 + i, "close": 100.0 + i, "high": 101.0 + i, "low": 99.0 + i,
                 "volume": 1000.0, "available_at": datetime(2026, 10, 1) + timedelta(days=i),
                 "ingested_at": datetime(2026, 10, 1) + timedelta(days=i), "raw_object_path": "test://bar",
                 "payload_hash": str(i), "source_version": "test", "source_record_id": str(i)} for i in range(21)]
        frame = spark.createDataFrame(rows)
        for name, kind in [("is_suspended", "boolean"), ("is_st", "boolean"), ("adjustment_factor", "double"), ("amount", "double"), ("turnover", "double")]:
            frame = frame.withColumn(name, F.lit(None).cast(kind))
        metrics = calculate_metrics(frame)
        last = metrics.orderBy(F.col("trading_date").desc()).first()
        assert last.observed_days_20d == 20 and last.ma_20d == 110.5
        assert abs(last.return_20d - .2) < 1e-10
        gold = build_gold_frame(metrics, start_date=date(2026, 10, 21), end_date=None)
        assert validate_gold_frame(gold)["row_count"] == 1
        result = gold.first()
        assert result.metric_status == "complete" and result.adjustment_factor is None
        assert result.feature_available_at == datetime(2026, 10, 21)
        print("PASS: Tencent nulls, negative adjusted prices, rolling metrics and Gold warmup")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
