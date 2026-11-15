"""Append a synthetic day in small batches using the production PyIceberg writer.

Run only after measuring the original candidate snapshot. The benchmark tables
are changed permanently; the live source snapshot is read-only.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
import re
import time

from pyiceberg.expressions import And, EqualTo

from backend.lakehouse.catalog import get_catalog
from backend.lakehouse.finance_writer import FinanceWriter
from backend.schemas.finance import DailyBarRecord


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target', required=True)
    parser.add_argument('--target-snapshot', required=True, type=int)
    parser.add_argument('--source-snapshot', required=True, type=int)
    parser.add_argument('--source-day', required=True, type=date.fromisoformat)
    parser.add_argument('--synthetic-day', required=True, type=date.fromisoformat)
    parser.add_argument('--batch-size', type=int, default=200)
    args = parser.parse_args()
    if not re.fullmatch(r'bench_[a-z][a-z0-9_]*\.(?:sorted|bucket16|bucket32)', args.target):
        parser.error('target must be an independent layout candidate under bench_')
    if args.batch_size < 1 or args.synthetic_day <= args.source_day:
        parser.error('batch size must be positive and synthetic day must follow source day')
    catalog = get_catalog()
    source = catalog.load_table('openfinance.daily_bars')
    target = catalog.load_table(args.target)
    if not target.current_snapshot() or target.current_snapshot().snapshot_id != args.target_snapshot:
        raise ValueError('Candidate snapshot differs from the benchmarked snapshot')
    if target.scan(row_filter=EqualTo('trading_date', args.synthetic_day), limit=1).to_arrow().num_rows:
        raise ValueError('Synthetic day already exists in candidate')
    started = time.monotonic()
    rows = source.scan(snapshot_id=args.source_snapshot, row_filter=And(
        EqualTo('trading_date', args.source_day), EqualTo('provider', 'tencent'),
        EqualTo('adjustment_type', 'none'))).to_arrow().to_pylist()
    if not rows:
        raise ValueError('No source rows for the requested date')
    keys = set()
    records = []
    for row in rows:
        row['trading_date'] = args.synthetic_day
        record = DailyBarRecord.model_validate(row)
        key = (record.provider, record.adjustment_type, record.security_id, record.trading_date)
        if key in keys:
            raise ValueError('Source day contains duplicate business keys')
        keys.add(key)
        records.append(record)
    records.sort(key=lambda row: row.symbol)
    writer = object.__new__(FinanceWriter)
    writer._reload_daily_bars = lambda: catalog.load_table(args.target)
    before = int(target.current_snapshot().summary.additional_properties['total-data-files'])
    counts = []
    for offset in range(0, len(records), args.batch_size):
        batch = records[offset:offset + args.batch_size]
        writer._append_new_daily_bars(batch)
        latest = catalog.load_table(args.target).current_snapshot()
        counts.append(int(latest.summary.additional_properties.get('added-data-files', '0')))
        print(json.dumps({'batch': len(counts), 'rows': len(batch), 'added_files': counts[-1]}), flush=True)
    after = int(latest.summary.additional_properties['total-data-files'])
    print('APPEND_RESULT=' + json.dumps({'target': args.target, 'source_day': str(args.source_day),
        'synthetic_day': str(args.synthetic_day), 'rows': len(records), 'batch_size': args.batch_size,
        'batches': len(counts), 'added_files_by_batch': counts, 'files_before': before,
        'files_after': after, 'net_added_files': after - before, 'snapshot': str(latest.snapshot_id),
        'seconds_including_source_scan': round(time.monotonic() - started, 3)}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
