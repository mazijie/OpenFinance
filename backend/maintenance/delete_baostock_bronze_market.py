"""Delete mixed BaoStock Bronze inputs only after verifying calendar-only copies.

Run with finance ingestion and normalization stopped. Does not delete Iceberg
files or pure-calendar inputs. Bucket versioning must be disabled.
"""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json

from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.maintenance.inventory_baostock import classify_bronze
from backend.maintenance.preserve_baostock_calendars import calendar_copy


def original_copy_path(source, payload):
    old = deepcopy(payload)
    old['raw_response']['security_master'] = []
    old['raw_response']['daily_bars'] = []
    old['payload_hash'] = None
    encoded = json.dumps(old, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')
    key = sha256(source.encode('utf-8') + b'\0' + encoded).hexdigest()
    return f'finance/provider=baostock/calendar-preserved/{key}.json', old


def run():
    writer = BronzeWriter()
    if writer.client.get_bucket_versioning(writer.bucket_name).status is not None:
        raise RuntimeError('Bucket versioning is enabled or suspended; refusing incomplete deletion')
    items = list(writer.client.list_objects(writer.bucket_name, prefix='finance/provider=baostock/requests/', recursive=True))
    plan = []
    for item in items:
        payload = writer.read_json(item.object_name)
        details = classify_bronze(payload)
        if payload.get('provider') != 'baostock' or details['kind'] != 'mixed' or details['other_nonempty_fields']:
            raise ValueError(f'Unexpected object; refusing deletion: {item.object_name}')
        for field in ('daily_bars', 'security_master'):
            if any(row.get('provider') not in (None, '', 'baostock') for row in payload['raw_response'].get(field, [])):
                raise ValueError(f'Non-BaoStock records present: {item.object_name}')
        dest, encoded = calendar_copy(item.object_name, payload)
        if writer.read_json(dest) != json.loads(encoded):
            raise RuntimeError(f'Calendar-only copy verification failed: {dest}')
        old_dest, old = original_copy_path(item.object_name, payload)
        if writer.read_json(old_dest) != old:
            raise RuntimeError(f'Legacy calendar copy is unexpected: {old_dest}')
        if writer.client.stat_object(writer.bucket_name, item.object_name).etag != item.etag:
            raise RuntimeError(f'Source changed: {item.object_name}')
        plan.append((item.object_name, item.etag, old_dest))
    for source, etag, old_dest in plan:
        if writer.client.stat_object(writer.bucket_name, source).etag != etag:
            raise RuntimeError(f'Source changed before deletion: {source}')
        writer.client.remove_object(writer.bucket_name, old_dest)
        writer.client.remove_object(writer.bucket_name, source)
        print(json.dumps({'deleted_source': source, 'deleted_legacy_copy': old_dest}, ensure_ascii=False), flush=True)
    print(json.dumps({'sources_deleted': len(plan), 'bucket_versioning': 'disabled'}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    run()
