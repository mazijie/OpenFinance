"""Measure the actual history endpoint against a pinned Iceberg table snapshot.

Runs in a separate backend process with an in-process HTTP client; it does not
modify the running API or the target table. Output is append-only JSON Lines so
long experiments retain their completed samples if interrupted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

from fastapi.testclient import TestClient
from pyiceberg.table import DataScan

from backend.api.market import get_finance_service
from backend.lakehouse.catalog import get_catalog
from backend.lakehouse.finance_writer import FinanceWriter
from backend.main import app
from backend.repositories.finance_repository import FinanceRepository
from backend.services.finance_service import FinanceService


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--table', required=True, help='Two-part Iceberg table identifier')
    parser.add_argument('--snapshot', required=True, type=int)
    parser.add_argument('--symbols', nargs='+', default=['000001.SZ', '600519.SH', '300750.SZ'])
    parser.add_argument('--iterations', type=int, default=10)
    parser.add_argument('--availability', choices=('once', 'always'), default='once',
                        help='Measure fallback availability once per group or on every sample')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error('iterations must be positive')

    table = get_catalog().load_table(args.table)
    if table.snapshot_by_id(args.snapshot) is None:
        raise ValueError('Pinned snapshot is no longer available')
    # Pin only this in-process reader, without publishing a table change.
    table.metadata = table.metadata.model_copy(update={'current_snapshot_id': args.snapshot})
    stats: dict[str, float | int] = {}
    plan_files, to_arrow = DataScan.plan_files, DataScan.to_arrow

    def timed_plan(scan):
        start = time.monotonic()
        tasks = list(plan_files(scan))
        stats['plan_seconds'] = float(stats.get('plan_seconds', 0)) + time.monotonic() - start
        stats['planned_files'] = int(stats.get('planned_files', 0)) + len(tasks)
        stats['planned_bytes'] = int(stats.get('planned_bytes', 0)) + sum(t.file.file_size_in_bytes for t in tasks)
        return tasks

    def timed_arrow(scan, *pos, **kwargs):
        start = time.monotonic()
        result = to_arrow(scan, *pos, **kwargs)
        stats['scan_seconds'] = float(stats.get('scan_seconds', 0)) + time.monotonic() - start
        stats['arrow_bytes'] = int(stats.get('arrow_bytes', 0)) + result.nbytes
        return result

    DataScan.plan_files = timed_plan
    DataScan.to_arrow = timed_arrow
    writer = object.__new__(FinanceWriter)
    service = FinanceService(FinanceRepository(writer=writer))
    service.history_table = lambda: table
    app.dependency_overrides[get_finance_service] = lambda: service
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('x', encoding='utf-8') as stream, TestClient(app) as client:
        for iteration in range(args.iterations):
            for symbol in args.symbols:
                for period in ('daily', 'weekly', 'monthly'):
                    stats.clear()
                    start = time.monotonic()
                    response = client.get('/api/market/history', params={
                        'symbol': symbol, 'period': period, 'adjustment_type': 'none',
                    })
                    elapsed = time.monotonic() - start
                    payload = response.json()
                    bars = payload['items'][0]['bars'] if response.is_success and payload.get('items') else []
                    digest = hashlib.sha256(json.dumps(bars, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
                    record = {'table': args.table, 'snapshot_id': str(args.snapshot),
                              'iteration': iteration + 1, 'symbol': symbol, 'period': period,
                              'status': response.status_code, 'endpoint_seconds': round(elapsed, 4),
                              'plan_seconds': round(float(stats.get('plan_seconds', 0)), 4),
                              'read_seconds': round(max(0, float(stats.get('scan_seconds', 0)) - float(stats.get('plan_seconds', 0))), 4),
                              'planned_files': stats.get('planned_files', 0), 'planned_bytes': stats.get('planned_bytes', 0),
                              'arrow_bytes': stats.get('arrow_bytes', 0), 'bars': len(bars), 'bars_sha256': digest,
                              'has_more': payload.get('has_more') if response.is_success else None}
                    if (response.is_success and payload.get('has_more') is None
                            and (args.availability == 'always' or iteration == 0)):
                        availability_start = time.monotonic()
                        availability = client.get('/api/market/history/availability', params={
                            'symbol': symbol, 'before': payload['next_before'], 'adjustment_type': 'none',
                            'snapshot_id': payload['snapshot_id'], 'table_uuid': payload['table_uuid'],
                            'query_time': payload['query_time'],
                        })
                        record['availability_seconds'] = round(time.monotonic() - availability_start, 4)
                        if availability.is_success:
                            record['has_more'] = availability.json()['has_more']
                        else:
                            record['availability_error'] = availability.status_code
                    stream.write(json.dumps(record, ensure_ascii=False) + '\n')
                    stream.flush()
                    print(json.dumps(record, ensure_ascii=False), flush=True)
                    if not response.is_success:
                        raise RuntimeError(f'History endpoint failed: {response.status_code}: {payload}')


if __name__ == '__main__':
    main()
