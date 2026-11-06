"""Expire old security snapshots and remove verified BaoStock-only data files.

Requires stopped writers and freshly generated reference/classification reports.
The report and journal remain in output/. Other-provider data files are retained.
"""
from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse

from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.lakehouse.catalog import get_catalog
from backend.lakehouse.write_coordination import iceberg_write_lock

AUDIT = Path('output/baostock-security-history-inventory.json')
CLASSIFICATION = Path('output/baostock-security-history-classification.json')
JOURNAL = Path('output/baostock-security-history-purge.json')


def run():
    audit = json.loads(AUDIT.read_text())
    classified = json.loads(CLASSIFICATION.read_text())
    if classified['mixed']:
        raise RuntimeError('Mixed-provider historical files require individual rewriting')
    old_files = set(audit['exclusive_old_candidates'])
    classified_paths = {entry['path'] for group in classified.values() for entry in group}
    if old_files != classified_paths:
        raise RuntimeError('Classification does not match reference inventory')
    warehouse = BronzeWriter()
    catalog = get_catalog()
    target = catalog.load_table(audit['target'])
    if target.metadata_location != audit['metadata_location']:
        raise RuntimeError('Target metadata changed since inventory')
    tables = {'.'.join(n): catalog.load_table(n) for n in catalog.list_tables(('openfinance',))}
    if {n:t.metadata_location for n,t in tables.items()} != audit['table_metadata_locations']:
        raise RuntimeError('Catalog tables changed since inventory')
    refs = target.metadata.refs
    snapshot = target.current_snapshot().snapshot_id
    if snapshot != audit['current_snapshot_id'] or set(refs) != {'main'} or refs['main'].snapshot_id != snapshot:
        raise RuntimeError('Unexpected snapshot or protected branch/tag')
    base = urlparse(target.location())
    if base.scheme != 's3' or warehouse.client.get_bucket_versioning(base.netloc).status is not None:
        raise RuntimeError('Expected an unversioned S3 warehouse')
    prefix = target.location().rstrip('/') + '/'
    paths = [entry['path'] for entry in classified['baostock_only']]
    paths += audit['exclusive_old_manifest_candidates'] + audit['exclusive_old_manifest_lists']
    if not all(path.startswith(prefix) for path in paths):
        raise RuntimeError('Candidate outside target table location')
    if len(paths) != len(set(paths)):
        raise RuntimeError('Duplicate purge candidate')
    preserved = len(classified['other_only'])
    journal = {'target': audit['target'], 'current_snapshot_id': snapshot, 'candidate_count': len(paths),
               'baostock_data_files': len(classified['baostock_only']), 'preserved_other_files': preserved,
               'snapshot_count_before': len(target.metadata.snapshots), 'paths': paths, 'status': 'prepared'}
    JOURNAL.write_text(json.dumps(journal, ensure_ascii=False, indent=2))
    with iceberg_write_lock():
        target = get_catalog().load_table(audit['target'])
        if target.metadata_location != audit['metadata_location']:
            raise RuntimeError('Target changed while acquiring lock')
        expired = [s.snapshot_id for s in target.metadata.snapshots if s.snapshot_id != snapshot]
        if expired:
            target.maintenance.expire_snapshots().by_ids(expired).commit()
        target = get_catalog().load_table(audit['target'])
        if len(target.metadata.snapshots) != 1 or target.current_snapshot().snapshot_id != snapshot:
            raise RuntimeError('Snapshot expiration verification failed')
        journal['status'] = 'snapshots_expired'
        JOURNAL.write_text(json.dumps(journal, ensure_ascii=False, indent=2))
        for index, path in enumerate(paths, 1):
            parsed = urlparse(path)
            warehouse.client.remove_object(parsed.netloc, parsed.path.lstrip('/'))
            if index % 100 == 0:
                print(f'Deleted {index}/{len(paths)} historical objects', flush=True)
        journal['status'] = 'deleted'
        JOURNAL.write_text(json.dumps(journal, ensure_ascii=False, indent=2))
    return {k:v for k,v in journal.items() if k != 'paths'}


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False), flush=True)
