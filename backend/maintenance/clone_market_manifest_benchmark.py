"""Create a metadata-isolated manifest benchmark referencing source data read-only.

Never purge or expire this table: its old manifests and data files are shared with
the pinned source snapshot. Only rewriting its manifests is permitted.
"""
from __future__ import annotations

import argparse
import json
import re
from uuid import uuid4

from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.serializers import ToOutputFile

from backend.config import settings
from backend.lakehouse.catalog import get_catalog


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--source-snapshot', required=True, type=int)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,39}', args.dataset):
        parser.error('dataset must be a lowercase identifier')
    catalog = get_catalog()
    source = catalog.load_table(f'{settings.ICEBERG_NAMESPACE}.{settings.ICEBERG_DAILY_BARS_TABLE}')
    snapshot = source.current_snapshot()
    if not snapshot or snapshot.snapshot_id != args.source_snapshot:
        raise ValueError('Source snapshot changed; choose a new experiment snapshot')
    summary = snapshot.summary.additional_properties
    if int(summary.get('total-delete-files', '0')):
        raise ValueError('Manifest benchmark requires a snapshot without delete files')
    namespace = f'bench_{args.dataset}'
    identifier = f'{namespace}.manifest_copy'
    try:
        catalog.create_namespace((namespace,))
    except NamespaceAlreadyExistsError:
        pass
    if catalog.table_exists(identifier):
        raise ValueError('Benchmark table already exists')
    location = f'{settings.ICEBERG_WAREHOUSE.rstrip("/")}/{namespace}/manifest_copy'
    properties = dict(source.metadata.properties)
    properties.update({'gc.enabled': 'false', 'write.metadata.delete-after-commit.enabled': 'false'})
    metadata = source.metadata.model_copy(deep=True, update={
        'location': location, 'table_uuid': uuid4(), 'properties': properties,
        'metadata_log': [],
    })
    metadata_location = f'{location}/metadata/00000-{uuid4()}.metadata.json'
    ToOutputFile.table_metadata(metadata, source.io.new_output(metadata_location))
    table = catalog.register_table(identifier, metadata_location)
    print(json.dumps({'table': identifier, 'source_snapshot': str(args.source_snapshot),
                      'table_uuid': str(table.metadata.table_uuid), 'metadata_location': metadata_location,
                      'warning': 'Read-only shared source files; never purge, expire, or rewrite data files.'}))


if __name__ == '__main__':
    main()
