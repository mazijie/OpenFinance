"""Create a small, disposable Iceberg source for the layout benchmark smoke test."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'spark' / 'jobs'))
from daily_metrics import create_spark

spark = create_spark()
name = 'openfinance.bench_layoutsmoke.source'
try:
    spark.sql('CREATE NAMESPACE IF NOT EXISTS openfinance.bench_layoutsmoke')
    if spark.catalog.tableExists(name):
        raise ValueError('Fixture already exists; use a clean benchmark namespace')
    spark.sql(f'''CREATE TABLE {name} (symbol STRING, trading_date DATE, exchange STRING,
              provider STRING, price DOUBLE) USING iceberg
              PARTITIONED BY (months(trading_date), exchange, provider)''')
    spark.sql(f'''INSERT INTO {name} VALUES
      ('000001.SZ', DATE '2026-01-01', 'SZ', 'tencent', 10.0),
      ('000001.SZ', DATE '2026-01-02', 'SZ', 'tencent', 10.5),
      ('600519.SH', DATE '2026-02-01', 'SH', 'tencent', 100.0),
      ('600519.SH', DATE '2026-02-02', 'SH', 'tencent', 101.0)''')
    snapshot = spark.sql(f'SELECT snapshot_id FROM {name}.snapshots ORDER BY committed_at DESC LIMIT 1').first()[0]
    print('FIXTURE_RESULT=' + json.dumps({'table': name, 'snapshot': str(snapshot)}), flush=True)
finally:
    spark.stop()
