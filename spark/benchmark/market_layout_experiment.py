"""Create and verify isolated market-history layout candidates.

All output tables must be under a bench_ namespace. Source snapshot reads are
immutable; the live table is never altered. Benchmark tables are retained for
inspection and reproducibility.
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta
import json
import re
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs'))
from daily_metrics import create_spark
from pyspark.sql import functions as F


TARGET = re.compile(r'^openfinance\.bench_[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$')
DEFAULT_SOURCE = 'openfinance.openfinance.daily_bars'


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('create', 'rewrite-manifests', 'sort', 'compare', 'append-day', 'compact-day'))
    parser.add_argument('--target', required=True)
    parser.add_argument('--source', default=DEFAULT_SOURCE)
    parser.add_argument('--source-snapshot', required=True, type=int)
    parser.add_argument('--target-snapshot', type=int)
    parser.add_argument('--layout', choices=('month', 'bucket16', 'bucket32'), default='month')
    parser.add_argument('--day', type=date.fromisoformat)
    parser.add_argument('--min-input-files', type=int, default=8)
    parser.add_argument('--result', required=True)
    args = parser.parse_args()
    if not TARGET.fullmatch(args.target):
        parser.error('target must be under an isolated bench_ namespace')
    if args.source != DEFAULT_SOURCE and not TARGET.fullmatch(args.source):
        parser.error('source must be the production daily table or an isolated benchmark table')
    if args.source == args.target:
        parser.error('source and target must differ')
    if args.action == 'compact-day' and (args.day is None or args.target_snapshot is None or args.min_input_files < 2):
        parser.error('compact-day requires --day, --target-snapshot, and --min-input-files >= 2')
    target_identifier = args.target.split('.', 1)[1]
    spark = create_spark()
    start = time.monotonic()
    result = {'action': args.action, 'target': args.target, 'source_snapshot': str(args.source_snapshot),
              'application_id': spark.sparkContext.applicationId}
    try:
        if args.action == 'create':
            if spark.catalog.tableExists(args.target):
                raise ValueError('Target exists; use a new name')
            spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {args.target.rsplit(".", 1)[0]}')
            bucket = {'month': '', 'bucket16': ', bucket(16, symbol)', 'bucket32': ', bucket(32, symbol)'}[args.layout]
            spark.sql(f'''CREATE TABLE {args.target} USING iceberg
                PARTITIONED BY (months(trading_date), exchange, provider{bucket})
                AS SELECT * FROM {args.source} VERSION AS OF {args.source_snapshot}''')
            result['layout'] = args.layout
        elif args.action == 'rewrite-manifests':
            if not args.target.endswith('.manifest_copy'):
                raise ValueError('Manifest rewrite is restricted to manifest_copy')
            result['procedure'] = [row.asDict(recursive=True) for row in spark.sql(
                f"CALL openfinance.system.rewrite_manifests(table => '{target_identifier}')").collect()]
        elif args.action == 'sort':
            if not args.target.endswith('.sorted'):
                raise ValueError('Data rewrite is restricted to the independent sorted table')
            result['procedure'] = [row.asDict(recursive=True) for row in spark.sql(
                f"CALL openfinance.system.rewrite_data_files(table => '{target_identifier}', "
                "strategy => 'sort', sort_order => 'symbol ASC NULLS FIRST, trading_date ASC NULLS FIRST', "
                "options => map('rewrite-all', 'true'))").collect()]
        elif args.action == 'compare':
            original = spark.read.format('iceberg').option('versionAsOf', args.source_snapshot).load(args.source)
            candidate = spark.table(args.target)
            if original.dtypes != candidate.dtypes:
                raise ValueError('Source and candidate column names or types differ')
            # CTAS may relax Iceberg requiredness; flag this separately from
            # row equality so an experimental layout cannot imply schema parity.
            result['schema_exact_match'] = original.schema == candidate.schema
            missing = original.exceptAll(candidate).count()
            extra = candidate.exceptAll(original).count()
            result.update(missing_rows=missing, extra_rows=extra)
            if missing or extra:
                raise ValueError(f'Candidate data differs: missing={missing}, extra={extra}')
        elif args.action == 'append-day':
            if args.target.endswith('.manifest_copy'):
                raise ValueError('Cannot write data to a table sharing source files')
            frame = spark.table(args.target)
            latest = frame.agg(F.max('trading_date')).first()[0]
            if latest is None:
                raise ValueError('Table is empty')
            original = frame.filter(F.col('trading_date') == F.lit(latest))
            # Model one day of input without claiming it is real market data.
            additions = original.withColumn('trading_date', F.date_add(F.col('trading_date'), 1))
            count = additions.count()
            before = spark.sql(f'SELECT count(*) FROM {args.target}.files').first()[0]
            additions.writeTo(args.target).append()
            after = spark.sql(f'SELECT count(*) FROM {args.target}.files').first()[0]
            result.update(synthetic_rows=count, source_day=str(latest), files_before=before,
                          files_after=after, net_added_files=after-before)
        elif args.action == 'compact-day':
            if not args.target.endswith(('.sorted', '.bucket16', '.bucket32')):
                raise ValueError('Compaction requires an independent layout candidate')
            before_snapshot = spark.sql(f'SELECT snapshot_id FROM {args.target}.snapshots ORDER BY committed_at DESC LIMIT 1').first()[0]
            if int(before_snapshot) != args.target_snapshot:
                raise ValueError('Candidate snapshot changed after the append benchmark')
            before_files = spark.sql(f'SELECT count(*) FROM {args.target}.files').first()[0]
            month_start = args.day.replace(day=1)
            next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
            condition = (f"trading_date >= DATE ''{month_start.isoformat()}'' AND "
                         f"trading_date < DATE ''{next_month.isoformat()}''")
            result['procedure'] = [row.asDict(recursive=True) for row in spark.sql(
                f"CALL openfinance.system.rewrite_data_files(table => '{target_identifier}', "
                f"where => '{condition}', options => map('min-input-files', '{args.min_input_files}', "
                "'target-file-size-bytes', '134217728'))").collect()]
            if any(row.get('failed_data_files_count', 0) for row in result['procedure']):
                raise RuntimeError('Some data files could not be compacted')
            after_snapshot = spark.sql(f'SELECT snapshot_id FROM {args.target}.snapshots ORDER BY committed_at DESC LIMIT 1').first()[0]
            before = spark.read.format('iceberg').option('versionAsOf', int(before_snapshot)).load(args.target).filter(F.col('trading_date').between(F.lit(month_start), F.lit(next_month - timedelta(days=1))))
            after = spark.read.format('iceberg').option('versionAsOf', int(after_snapshot)).load(args.target).filter(F.col('trading_date').between(F.lit(month_start), F.lit(next_month - timedelta(days=1))))
            row_count = before.count()
            if not row_count:
                raise ValueError('No rows found in the requested month')
            missing, extra = before.exceptAll(after).count(), after.exceptAll(before).count()
            result.update(day=str(args.day), partition_month=str(month_start), min_input_files=args.min_input_files,
                          before_snapshot=str(before_snapshot), before_files=before_files,
                          checked_rows=row_count, missing_rows=missing, extra_rows=extra)
            if missing or extra:
                raise ValueError(f'Compacted rows differ: missing={missing}, extra={extra}')
        snapshots = spark.sql(f'SELECT snapshot_id FROM {args.target}.snapshots ORDER BY committed_at DESC LIMIT 1').first()
        result['target_snapshot'] = str(snapshots[0]) if snapshots else None
        files = spark.sql(f'SELECT count(*) AS files, sum(file_size_in_bytes) AS bytes FROM {args.target}.files').first()
        result.update(files=files.files, bytes=files.bytes,
                      manifests=spark.sql(f'SELECT count(*) FROM {args.target}.manifests').first()[0], status='success')
    except Exception as exc:
        result.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        result['elapsed_seconds'] = round(time.monotonic() - start, 3)
        path = Path(args.result)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n')
        print(json.dumps(result, ensure_ascii=False, default=str), flush=True)
        spark.stop()


if __name__ == '__main__':
    main()
