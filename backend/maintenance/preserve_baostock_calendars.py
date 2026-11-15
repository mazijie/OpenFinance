"""Create and verify calendar-only copies of mixed BaoStock Bronze objects.

This operation never removes or modifies a source object. Re-running it is
idempotent; physical deletion requires separate table and snapshot analysis.
"""
from __future__ import annotations

import hashlib
import io
import json
from copy import deepcopy
from typing import Any

from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.maintenance.inventory_baostock import classify_bronze


def calendar_copy(source_path: str, payload: dict[str, Any]) -> tuple[str, bytes]:
    details = classify_bronze(payload)
    if details['kind'] != 'mixed' or details['other_nonempty_fields']:
        raise ValueError('expected mixed object without unclassified fields')
    result = deepcopy(payload)
    result['raw_response']['security_master'] = []
    result['raw_response']['daily_bars'] = []
    params = result.get('request_params') or {}
    result['request_params'] = {'kind': 'trading_calendar', **{key: params[key] for key in ('start_date', 'end_date') if key in params}}
    result['source_record_id'] = None
    result['payload_hash'] = None
    encoded = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')
    key = hashlib.sha256(source_path.encode('utf-8') + b'\0' + encoded).hexdigest()
    return f'finance/provider=baostock/calendar-preserved/{key}.json', encoded


def preserve() -> dict:
    writer = BronzeWriter()
    copies = []
    prefix = 'finance/provider=baostock/'
    objects = list(writer.client.list_objects(writer.bucket_name, prefix=prefix, recursive=True))
    for item in objects:
        payload = writer.read_json(item.object_name)
        if classify_bronze(payload)['kind'] != 'mixed':
            continue
        destination, contents = calendar_copy(item.object_name, payload)
        # Deterministic content-addressed destination makes retries safe.
        writer.client.put_object(writer.bucket_name, destination, io.BytesIO(contents), len(contents), content_type='application/json')
        verified = writer.read_json(destination)
        if verified != json.loads(contents):
            raise RuntimeError(f'calendar copy verification failed: {destination}')
        source = writer.client.stat_object(writer.bucket_name, item.object_name)
        if source.etag != item.etag or source.size != item.size:
            raise RuntimeError(f'source changed during copy: {item.object_name}')
        copies.append({'source': item.object_name, 'source_etag': item.etag, 'destination': destination,
                       'calendar_records': len(verified['raw_response']['trading_calendar'])})
    return {'copies': copies, 'source_objects_deleted': 0}


if __name__ == '__main__':
    print(json.dumps(preserve(), ensure_ascii=False, indent=2))
