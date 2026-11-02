"""Run with spark-submit to exercise keyed comparisons."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark" / "benchmark"))
from job import compare_tables
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("benchmark-comparison-test").getOrCreate()
try:
    schema = "id int, value double"
    spark.createDataFrame([(1, 10.0), (2, None)], schema).createOrReplaceTempView("reference")
    spark.createDataFrame([(1, 10.0000000001), (2, None)], schema).createOrReplaceTempView("within_tolerance")
    spark.createDataFrame([(1, 10.01), (3, None)], schema).createOrReplaceTempView("different")
    assert compare_tables(spark, "within_tolerance", "reference", ["id"]) == 0
    assert compare_tables(spark, "different", "reference", ["id"]) == 3
    print("comparison tests passed")
finally:
    spark.stop()
