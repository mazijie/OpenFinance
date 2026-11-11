"""Summarize layout experiment samples and verify returned bar digests."""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import statistics


def percentile(values: list[float], q: float) -> float | None:
    return sorted(values)[max(0, math.ceil(len(values) * q) - 1)] if values else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('samples', nargs='+', type=Path)
    parser.add_argument('--min-samples', type=int, default=10)
    args = parser.parse_args()
    grouped = defaultdict(list)
    for path in args.samples:
        for line in path.read_text(encoding='utf-8').splitlines():
            sample = json.loads(line)
            grouped[(sample['table'], sample['symbol'], sample['period'])].append(sample)
    if not grouped:
        raise ValueError('No samples')
    digests = defaultdict(set)
    answer_sets = defaultdict(set)
    table_groups = defaultdict(set)
    results = []
    problems = []
    for (table, symbol, period), items in sorted(grouped.items()):
        key = (symbol, period)
        table_groups[table].add(key)
        digests[key].update((item['bars'], item['bars_sha256']) for item in items)
        answer_sets[key].update(item['has_more'] for item in items if item['has_more'] is not None)
        if len(items) < args.min_samples:
            problems.append(f'{table} {symbol} {period}: only {len(items)} samples')
        failures = [item for item in items if item['status'] != 200 or item.get('availability_error')]
        if failures:
            problems.append(f'{table} {symbol} {period}: {len(failures)} request failures')
        if not any(item['has_more'] is not None for item in items):
            problems.append(f'{table} {symbol} {period}: availability was never resolved')
        row = {'table': table, 'symbol': symbol, 'period': period, 'samples': len(items),
               'bars': items[0]['bars'], 'has_more': sorted({str(item['has_more']) for item in items})}
        for field in ('endpoint_seconds', 'plan_seconds', 'read_seconds', 'availability_seconds',
                      'planned_files', 'planned_bytes'):
            values = [item[field] for item in items if field in item]
            row[field] = {'median': round(statistics.median(values), 4), 'p95': round(percentile(values, .95), 4)} if values else None
        results.append(row)
    for (symbol, period), values in digests.items():
        if len(values) != 1:
            problems.append(f'{symbol} {period}: query result mismatch')
        if len(answer_sets[(symbol, period)]) != 1:
            problems.append(f'{symbol} {period}: history availability mismatch')
    all_groups = set().union(*table_groups.values())
    for table, keys in table_groups.items():
        if keys != all_groups:
            problems.append(f'{table}: missing symbol/period groups: {sorted(all_groups - keys)}')
    print(json.dumps({'results': results, 'problems': problems,
                      'notes': ['In-process HTTP; cache state uncontrolled; independent client processes bypass the production worker gate.',
                                'planned_bytes is file size, not bytes transferred.']}, indent=2, ensure_ascii=False))
    if problems:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
