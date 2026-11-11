"""Verify a manifest-only clone references precisely the pinned source files."""
from __future__ import annotations

import argparse
from collections import Counter
import json
from time import monotonic

from backend.lakehouse.catalog import get_catalog


def files(table, snapshot_id):
    tasks = list(table.scan(snapshot_id=snapshot_id).plan_files())
    if any(task.delete_files for task in tasks):
        raise ValueError('Delete-file snapshots are not supported')
    return Counter((task.file.file_path, task.file.file_size_in_bytes, task.file.record_count)
                   for task in tasks)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--source-snapshot', required=True, type=int)
    parser.add_argument('--target', required=True)
    parser.add_argument('--target-snapshot', required=True, type=int)
    args = parser.parse_args()
    catalog = get_catalog()
    start = monotonic()
    source = files(catalog.load_table(args.source), args.source_snapshot)
    target = files(catalog.load_table(args.target), args.target_snapshot)
    result = {'source': args.source, 'source_snapshot': str(args.source_snapshot),
              'target': args.target, 'target_snapshot': str(args.target_snapshot),
              'source_files': sum(source.values()), 'target_files': sum(target.values()),
              'missing_files': sum((source-target).values()), 'extra_files': sum((target-source).values()),
              'seconds': round(monotonic() - start, 3),
              'note': 'Identical immutable data-file paths, sizes, and row counts; no delete files.'}
    print(json.dumps(result, indent=2))
    if source != target:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
