"""Build a business-facing daily market cross section from Silver bars."""
from __future__ import annotations

import argparse
import json
import re
import time
import uuid
import os

from pyspark.sql import Window, functions as F
from daily_metrics import create_spark


def build_cross_section(bars):
    # The active product uses unadjusted bars. Pick one latest record per key
    # so historical provider versions cannot multiply the business rows.
    bars = bars.filter(F.col('adjustment_type') == 'none')
    versions = Window.partitionBy('security_id', 'trading_date').orderBy(
        F.col('available_at').desc_nulls_last(), F.col('ingested_at').desc_nulls_last(),
        F.col('source_version').desc_nulls_last(), F.col('payload_hash').desc_nulls_last())
    bars = bars.withColumn('_rank', F.row_number().over(versions)).filter(F.col('_rank') == 1).drop('_rank')
    order = Window.partitionBy('security_id').orderBy('trading_date')
    valid = ~F.coalesce(F.col('is_suspended'), F.lit(False))
    result = bars
    for n in (1, 5, 20):
        window = order.rowsBetween(-(n - 1), 0)
        lag = F.lag('close', n).over(order)
        lag_valid = F.lag(valid, n).over(order)
        # All observations in the interval must be present and unsuspended.
        complete = (F.count(F.when(valid, 1)).over(window) == n) & lag_valid
        result = result.withColumn(f'return_{n}d', F.when(complete & (lag > 0), F.col('close') / lag - 1))
        result = result.withColumn(f'avg_amount_{n}d', F.when(
            (F.count(F.when(valid & F.col('amount').isNotNull(), 1)).over(window) == n),
            F.avg(F.when(valid, F.col('amount'))).over(window)))
    for n in (5, 20):
        window = order.rowsBetween(-(n - 1), 0)
        close_ok = valid & F.col('close').isNotNull() & (F.col('close') > 0)
        mean = F.avg(F.when(close_ok, F.col('close'))).over(window)
        result = result.withColumn(f'ma_{n}d', F.when(F.count(F.when(close_ok, 1)).over(window) == n, mean))
        result = result.withColumn(f'price_ma_{n}d_deviation', F.when(
            F.col(f'ma_{n}d') > 0, F.col('close') / F.col(f'ma_{n}d') - 1))
    window5 = order.rowsBetween(-4, 0)
    prior5 = order.rowsBetween(-5, -1)
    volume_ok = valid & F.col('volume').isNotNull() & (F.col('volume') >= 0)
    result = result.withColumn('avg_volume_5d', F.when(
        F.count(F.when(volume_ok, 1)).over(window5) == 5, F.avg(F.when(volume_ok, F.col('volume'))).over(window5)))
    prior_mean = F.avg(F.when(volume_ok, F.col('volume'))).over(prior5)
    result = result.withColumn('volume_ratio_5d', F.when(
        volume_ok & (F.count(F.when(volume_ok, 1)).over(prior5) == 5) & (prior_mean > 0), F.col('volume') / prior_mean))
    returns20 = order.rowsBetween(-19, 0)
    result = result.withColumn('volatility_20d_annualized', F.when(
        F.count('return_1d').over(returns20) == 20, F.stddev_samp('return_1d').over(returns20) * F.sqrt(F.lit(252.0))))
    for n in (60, 120):
        window = order.rowsBetween(-(n - 1), 0)
        lag = F.lag('close', n).over(order)
        lag_valid = F.lag(valid, n).over(order)
        close_ok = valid & F.col('close').isNotNull() & (F.col('close') > 0)
        complete = F.count(F.when(close_ok, 1)).over(window) == n
        result = result.withColumn(f'return_{n}d', F.when(complete & lag_valid & (lag > 0), F.col('close') / lag - 1))
        result = result.withColumn(f'ma_{n}d', F.when(complete, F.avg(F.when(close_ok, F.col('close'))).over(window)))
    window60 = order.rowsBetween(-59, 0)
    window20 = order.rowsBetween(-19, 0)
    for n, window in ((20, window20), (60, window60)):
        turnover_ok = valid & F.col('turnover').isNotNull() & (F.col('turnover') >= 0)
        result = result.withColumn(f'avg_turnover_{n}d', F.when(
            F.count(F.when(turnover_ok, 1)).over(window) == n, F.avg(F.when(turnover_ok, F.col('turnover'))).over(window)))
    result = result.withColumn('volatility_60d_annualized', F.when(
        F.count('return_1d').over(window60) == 60, F.stddev_samp('return_1d').over(window60) * F.sqrt(F.lit(252.0))))
    prior_amount = F.avg(F.when(valid, F.col('amount'))).over(order.rowsBetween(-40, -21))
    prior_count = F.count(F.when(valid & F.col('amount').isNotNull() & (F.col('amount') >= 0), 1)).over(order.rowsBetween(-40, -21))
    result = result.withColumn('amount_change_20d', F.when((prior_count == 20) & (prior_amount > 0), F.col('avg_amount_20d') / prior_amount - 1))
    close_ok = valid & F.col('close').isNotNull() & (F.col('close') > 0)
    result = result.withColumn('_closes_60d', F.collect_list(F.when(close_ok, F.col('close'))).over(window60))
    # Aggregate in chronological order so each trough is compared only to earlier peaks.
    result = result.withColumn('max_drawdown_60d', F.when(F.size('_closes_60d') == 60, F.expr("""aggregate(
        _closes_60d, named_struct('peak', cast(0 as double), 'drawdown', cast(0 as double)),
        (acc, x) -> named_struct('peak', greatest(acc.peak, cast(x as double)),
          'drawdown', greatest(acc.drawdown, 1D - cast(x as double) / greatest(acc.peak, cast(x as double)))),
        acc -> acc.drawdown)"""))).drop('_closes_60d')
    percentile_fields = ('return_1d', 'return_5d', 'return_20d', 'return_60d', 'return_120d',
                         'volatility_20d_annualized', 'volatility_60d_annualized', 'max_drawdown_60d',
                         'amount', 'avg_amount_20d', 'amount_change_20d', 'turnover', 'avg_turnover_20d', 'avg_turnover_60d')
    for field in percentile_fields:
        rank_window = Window.partitionBy('trading_date', F.col(field).isNull()).orderBy(F.col(field))
        result = result.withColumn(f'{field}_percentile', F.when(F.col(field).isNotNull(), F.percent_rank().over(rank_window)))
    metrics = ('close', 'volume', 'amount', 'turnover', 'return_1d', 'return_5d', 'return_20d',
               'avg_amount_5d', 'avg_amount_20d', 'ma_5d', 'ma_20d', 'price_ma_5d_deviation',
               'price_ma_20d_deviation', 'volatility_20d_annualized', 'avg_volume_5d', 'volume_ratio_5d')
    incomplete = F.lit(False)
    for name in metrics:
        incomplete = incomplete | F.col(name).isNull()
    result = result.withColumn('data_status', F.when(incomplete, F.lit('partial')).otherwise(F.lit('complete')))
    return result.select('security_id', 'symbol', 'exchange', 'trading_date', 'close', 'volume', 'amount', 'turnover',
                         'return_1d', 'return_5d', 'return_20d',
                         'avg_amount_1d', 'avg_amount_5d', 'avg_amount_20d',
                         'ma_5d', 'ma_20d', 'price_ma_5d_deviation', 'price_ma_20d_deviation',
                         'volatility_20d_annualized', 'avg_volume_5d', 'volume_ratio_5d', 'data_status',
                         'return_60d', 'return_120d', 'ma_60d', 'ma_120d', 'volatility_60d_annualized',
                         'max_drawdown_60d', 'avg_turnover_20d', 'avg_turnover_60d', 'amount_change_20d',
                         *[f'{field}_percentile' for field in percentile_fields],
                         F.current_timestamp().alias('built_at'))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-table', default='openfinance.openfinance.daily_bars')
    p.add_argument('--target-table', default='openfinance.openfinance.gold_market_cross_section')
    p.add_argument('--stage-only', action='store_true', help='Build and validate a staging table without publishing it')
    p.add_argument('--promote-stage', help='Validate and publish a previously built staging table')
    args = p.parse_args()
    if args.stage_only and args.promote_stage:
        p.error('--stage-only and --promote-stage cannot be used together')
    for identifier in (args.source_table, args.target_table):
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2}', identifier):
            p.error('table names must be catalog.namespace.table identifiers')
    spark = create_spark()
    lock = None
    started = time.monotonic()
    try:
        # The PostgreSQL session lock covers manual runs as well as Airflow runs.
        jdbc = spark.conf.get('spark.sql.catalog.openfinance.uri')
        lock = spark._jvm.java.sql.DriverManager.getConnection(
            jdbc, os.environ['POSTGRES_USER'], os.environ['POSTGRES_PASSWORD'])
        statement = lock.createStatement()
        lock_result = statement.executeQuery("SELECT pg_try_advisory_lock(hashtextextended('openfinance_market_cross_section', 0))")
        if not lock_result.next() or not lock_result.getBoolean(1):
            raise RuntimeError('Another market cross-section build is running')
        lock_result.close()
        statement.close()
        refs = spark.sql(f"SELECT snapshot_id FROM {args.source_table}.refs WHERE name = 'main'").collect()
        if len(refs) != 1:
            raise ValueError('Silver source has no current snapshot')
        snapshot_id = int(refs[0].snapshot_id)
        if args.promote_stage:
            if not re.fullmatch(re.escape(args.target_table) + r'_stage_[0-9a-f]{12}', args.promote_stage):
                raise ValueError('Staging table does not belong to the target')
            stage = args.promote_stage
            props = {row.key: row.value for row in spark.sql(f'SHOW TBLPROPERTIES {stage}').collect()}
            if props.get('openfinance.source-table') != args.source_table or props.get('openfinance.source-snapshot-id') != str(snapshot_id):
                raise ValueError('Staging source or snapshot does not match the current Silver table')
        else:
            source = spark.read.option('versionAsOf', str(snapshot_id)).table(args.source_table)
            frame = build_cross_section(source)
            stage = f'{args.target_table}_stage_{uuid.uuid4().hex[:12]}'
            # Group each month before Iceberg writes to limit open writers.
            ordered = frame.repartition(32, F.year('trading_date'), F.month('trading_date')).sortWithinPartitions('trading_date')
            ordered.createOrReplaceTempView('_market_cross_section')
            spark.sql(f'''CREATE TABLE {stage} USING iceberg
                PARTITIONED BY (months(trading_date)) TBLPROPERTIES ('format-version'='2',
                'openfinance.source-table'='{args.source_table}', 'openfinance.source-snapshot-id'='{snapshot_id}')
                AS SELECT * FROM _market_cross_section''')
        staged = spark.table(stage)
        summary = staged.agg(F.count('*').alias('rows'), F.countDistinct('security_id').alias('securities'),
                             F.min('trading_date').alias('first_date'), F.max('trading_date').alias('last_date'),
                             *[F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f'{c}_nulls')
                               for c in ('ma_5d', 'ma_20d', 'volatility_20d_annualized', 'volume_ratio_5d')]).first().asDict()
        if not summary['rows']:
            raise ValueError(f'Cross section is empty; existing table preserved; stage={stage}')
        if staged.groupBy('security_id', 'trading_date').count().filter('count > 1').limit(1).count():
            raise ValueError(f'Duplicate cross-section business keys; stage={stage}')
        invalid = (F.col('max_drawdown_60d') < 0) | (F.col('max_drawdown_60d') > 1)
        percentile_fields = [name for name in staged.columns if name.endswith('_percentile')]
        for name in percentile_fields:
            invalid = invalid | (F.col(name) < 0) | (F.col(name) > 1)
        if staged.filter(invalid).limit(1).count():
            raise ValueError(f'Invalid factor values; stage={stage}')
        if spark.catalog.tableExists(args.target_table):
            old = spark.table(args.target_table)
            old_columns = ('close', 'volume', 'amount', 'turnover', 'return_1d', 'return_5d',
                           'return_20d', 'avg_amount_1d', 'avg_amount_5d', 'avg_amount_20d')
            common = [c for c in old_columns if c in old.columns]
            previous = old.select('security_id', 'trading_date', *common).alias('previous')
            current = staged.alias('current')
            comparison = previous.join(current, ['security_id', 'trading_date'], 'left')
            changed = F.lit(False)
            for name in common:
                changed = changed | ~F.col(f'previous.{name}').eqNullSafe(F.col(f'current.{name}'))
            changes = comparison.agg(
                F.sum(F.when(F.col('current.security_id').isNull(), 1).otherwise(0)).alias('missing_keys'),
                F.sum(F.when(changed, 1).otherwise(0)).alias('changed_rows')).first().asDict()
            summary['previous_comparison'] = changes
            if changes['missing_keys']:
                raise ValueError(f'Existing business keys missing from staging; stage={stage}')
        spark.catalog.refreshTable(args.source_table)
        latest = spark.sql(f"SELECT snapshot_id FROM {args.source_table}.refs WHERE name = 'main'").collect()
        if len(latest) != 1 or int(latest[0].snapshot_id) != snapshot_id:
            raise RuntimeError(f'Silver changed during build; existing table preserved; stage={stage}')
        print(json.dumps({'event': 'market_cross_section_validated', 'source_snapshot_id': str(snapshot_id),
                          'stage': stage, 'summary': summary, 'seconds': round(time.monotonic() - started, 3)}, default=str), flush=True)
        if not args.stage_only:
            # Iceberg RTAS publishes the replacement in one commit.
            spark.sql(f'''CREATE OR REPLACE TABLE {args.target_table} USING iceberg
                PARTITIONED BY (months(trading_date)) TBLPROPERTIES ('format-version'='2',
                'openfinance.source-snapshot-id'='{snapshot_id}', 'openfinance.validated-stage'='{stage}')
                AS SELECT * FROM {stage}''')
            print(json.dumps({'event': 'market_cross_section_published', 'target': args.target_table,
                              'source_snapshot_id': str(snapshot_id), 'stage': stage,
                              'seconds': round(time.monotonic() - started, 3)}), flush=True)
            # This uniquely named table was created by this invocation only.
            # Retain failed and stage-only builds for inspection.
            try:
                spark.sql(f'DROP TABLE {stage} PURGE')
            except Exception as exc:
                print(json.dumps({'event': 'market_cross_section_stage_cleanup_failed', 'stage': stage,
                                  'error': str(exc)}), flush=True)
    finally:
        if lock is not None:
            lock.close()
        spark.stop()


if __name__ == '__main__':
    main()
