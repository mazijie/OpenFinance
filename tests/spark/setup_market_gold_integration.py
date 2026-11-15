"""Create a small isolated Iceberg input and old Gold table for publication tests."""
from datetime import date, datetime, timedelta
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('market-gold-integration-setup').getOrCreate()
try:
    spark.sql('CREATE NAMESPACE IF NOT EXISTS openfinance.market_gold_test')
    columns = ['security_id', 'symbol', 'exchange', 'trading_date', 'close', 'volume', 'amount', 'turnover',
               'is_suspended', 'adjustment_type', 'available_at', 'ingested_at', 'source_version', 'payload_hash']
    now = datetime(2026, 1, 1)
    rows = [('TEST', 'TEST', 'SH', date(2025, 1, 1) + timedelta(days=i), float(i + 1), float(i + 1),
             100.0, 0.1, False, 'none', now, now, '1', str(i)) for i in range(24)]
    spark.createDataFrame(rows, columns).createOrReplaceTempView('gold_test_source')
    spark.sql('CREATE OR REPLACE TABLE openfinance.market_gold_test.bars USING iceberg AS SELECT * FROM gold_test_source')
    spark.sql('CREATE OR REPLACE TABLE openfinance.market_gold_test.gold USING iceberg AS '
              'SELECT security_id, trading_date FROM gold_test_source')
finally:
    spark.stop()
