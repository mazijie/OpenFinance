"""Read-only inventory for planning removal of legacy BaoStock data.

Run inside the backend container: python -m backend.maintenance.inventory_baostock
No data is written or deleted. Historical files and external backups require
separate reference analysis before physical deletion.
"""
from __future__ import annotations

import json
from collections import Counter

from backend.config import settings
from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.lakehouse.catalog import get_catalog


def classify_bronze(payload: dict) -> dict:
    raw = payload.get('raw_response')
    if not isinstance(raw, dict):
        raise ValueError('missing or invalid raw_response')
    counts = {}
    for key in ('trading_calendar', 'security_master', 'daily_bars'):
        value = raw.get(key, [])
        if not isinstance(value, list):
            raise ValueError(f'invalid {key}')
        counts[key] = len(value)
    other = [key for key, value in raw.items() if key not in counts and value]
    calendar = counts['trading_calendar'] > 0
    market = counts['security_master'] > 0 or counts['daily_bars'] > 0
    kind = 'mixed' if calendar and market else 'calendar' if calendar else 'market' if market else 'empty_or_other'
    return {'kind': kind, 'counts': counts, 'other_nonempty_fields': sorted(other)}


def inventory() -> dict:
    writer = BronzeWriter()
    objects, errors = [], []
    prefix = 'finance/provider=baostock/'
    for obj in writer.client.list_objects(writer.bucket_name, prefix=prefix, recursive=True):
        try:
            result = classify_bronze(writer.read_json(obj.object_name))
            objects.append({'path': obj.object_name, 'size_bytes': obj.size, 'etag': obj.etag, **result})
        except Exception as exc:
            errors.append({'path': obj.object_name, 'error': f'{type(exc).__name__}: {exc}'})
    catalog = get_catalog()
    tables = []
    for identifier in catalog.list_tables((settings.ICEBERG_NAMESPACE,)):
        table = catalog.load_table(identifier)
        snapshot = table.current_snapshot()
        tables.append({'name': '.'.join(identifier), 'snapshot_count': len(table.metadata.snapshots),
                       'current_snapshot_id': snapshot.snapshot_id if snapshot else None,
                       'location': table.location()})
    return {'bronze_bucket': writer.bucket_name, 'bronze_prefix': prefix,
            'bronze_counts': dict(Counter(item['kind'] for item in objects)),
            'bronze_objects': objects, 'errors': errors, 'iceberg_tables': tables,
            'note': 'Table metadata only; provider rows, file references, unregistered objects and external backups are not inventoried.'}


if __name__ == '__main__':
    result = inventory()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result['errors']:
        raise SystemExit(1)
