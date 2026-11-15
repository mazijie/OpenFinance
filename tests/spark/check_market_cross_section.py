"""Run with spark-submit; verifies metric windows and missing amounts."""
import os
import sys
from datetime import date, datetime, timedelta
from statistics import stdev
from math import sqrt
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../spark/jobs'))
from pyspark.sql import SparkSession
from build_market_cross_section import build_cross_section

spark = SparkSession.builder.master('local[2]').appName('test-market-cross-section').getOrCreate()
try:
    start = date(2026, 1, 1)
    now = datetime(2026, 2, 1)
    rows = [('A', 'A', 'SH', start + timedelta(days=i), float(i + 1), 100.0, float(i + 1) * 10, 0.1,
             i == 10, 'none', now, now, '1', str(i)) for i in range(23)]
    columns = ['security_id', 'symbol', 'exchange', 'trading_date', 'close', 'volume', 'amount', 'turnover',
               'is_suspended', 'adjustment_type', 'available_at', 'ingested_at', 'source_version', 'payload_hash']
    source = spark.createDataFrame(rows, columns)
    schema = source.schema
    data = build_cross_section(source).orderBy('trading_date').collect()
    assert data[0].return_1d is None and data[0].avg_amount_5d is None
    assert abs(data[5].return_5d - 5.0) < 1e-9 and data[5].avg_amount_5d == 40.0
    assert data[10].return_1d is None and data[11].return_1d is None
    assert data[20].return_20d is None
    assert abs(data[22].return_5d - (23 / 18 - 1)) < 1e-9
    assert data[10].ma_5d is None and data[10].data_status == 'partial'
    assert 'is_suspended' not in data[11].asDict() and 'suspension_status' not in data[11].asDict()
    assert data[20].volatility_20d_annualized is None
    rows = [('B', 'B', 'SH', start + timedelta(days=i), float(i + 1), float(i + 1), 10.0, 0.1,
             None, 'none', now, now, '1', str(i)) for i in range(23)]
    bars = build_cross_section(spark.createDataFrame(rows, schema)).orderBy('trading_date').collect()
    last = bars[20]
    assert last.ma_5d == 19.0 and last.ma_20d == 11.5
    assert abs(last.price_ma_5d_deviation - (21 / 19 - 1)) < 1e-9
    assert abs(last.avg_volume_5d - 19.0) < 1e-9
    assert abs(last.volume_ratio_5d - (21 / 18)) < 1e-9
    assert abs(last.volatility_20d_annualized - stdev([1 / i for i in range(1, 21)]) * sqrt(252)) < 1e-9
    assert last.data_status == 'complete'
    assert 'is_suspended' not in last.asDict() and 'suspension_status' not in last.asDict()
    assert bars[19].volatility_20d_annualized is None
    zero_rows = [('C', 'C', 'SH', start + timedelta(days=i), 10.0, 0.0 if i < 5 else 1.0, 10.0, 0.1,
                  False, 'none', now, now, '1', str(i)) for i in range(6)]
    assert build_cross_section(spark.createDataFrame(zero_rows, columns)).orderBy('trading_date').collect()[-1].volume_ratio_5d is None
    print('PASS market cross section')
finally:
    spark.stop()
