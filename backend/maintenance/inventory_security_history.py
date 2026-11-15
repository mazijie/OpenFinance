"""Audit snapshot manifest references under the security-master location.

Read-only: report files referenced by old security snapshots, the current
snapshot, and all snapshots of other registered tables. This does not discover
external catalogs, removed tables, unregistered backups, or object versions.
"""
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys

from backend.config import settings
from backend.lakehouse.catalog import get_catalog


def run():
    catalog = get_catalog()
    names = catalog.list_tables((settings.ICEBERG_NAMESPACE,))
    tables = {'.'.join(name): catalog.load_table(name) for name in names}
    target = f'{settings.ICEBERG_NAMESPACE}.{settings.ICEBERG_SECURITY_MASTER_TABLE}'
    table = tables[target]
    current = table.current_snapshot().snapshot_id
    prefix = table.location().rstrip('/') + '/'
    snapshots = []
    snapshot_lists = defaultdict(set)
    for name, t in tables.items():
        for snap in t.metadata.snapshots:
            is_current = name == target and snap.snapshot_id == current
            snapshots.append((name, t, snap, is_current))
            if snap.manifest_list:
                snapshot_lists[snap.manifest_list].add('target_current' if is_current else 'target_old' if name == target else f'other:{name}')
    print(f'Inspecting {len(snapshots)} snapshot references', file=sys.stderr, flush=True)
    manifests = {}
    def read_snapshot(value):
        name, t, snap, is_current = value
        return name, t, is_current, snap.manifests(t.io)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for index, (name, t, is_current, entries) in enumerate(pool.map(read_snapshot, snapshots), 1):
            for manifest in entries:
                key = manifest.manifest_path
                if key not in manifests:
                    manifests[key] = [t, manifest, set()]
                manifests[key][2].add('target_current' if is_current else 'target_old' if name == target else f'other:{name}')
            if index % 250 == 0:
                print(f'Inspected {index}/{len(snapshots)} snapshots', file=sys.stderr, flush=True)
    print(f'Inspecting {len(manifests)} distinct manifests', file=sys.stderr, flush=True)
    roles = defaultdict(set)
    def read_manifest(value):
        t, manifest, refs = value
        return refs, manifest.fetch_manifest_entry(t.io)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for index, (refs, entries) in enumerate(pool.map(read_manifest, manifests.values()), 1):
            for entry in entries:
                path = entry.data_file.file_path
                if path.startswith(prefix):
                    roles[path].update(refs)
            if index % 250 == 0:
                print(f'Inspected {index}/{len(manifests)} manifests', file=sys.stderr, flush=True)
    current_files = {p for p,r in roles.items() if 'target_current' in r}
    foreign_files = {p for p,r in roles.items() if any(v.startswith('other:') for v in r)}
    old_files = {p for p,r in roles.items() if 'target_old' in r}
    candidates = sorted(old_files - current_files - foreign_files)
    def old_only(refs):
        return 'target_old' in refs and 'target_current' not in refs and not any(v.startswith('other:') for v in refs)
    old_manifests = sorted(path for path, (_, _, refs) in manifests.items() if old_only(refs))
    old_lists = sorted(path for path, refs in snapshot_lists.items() if old_only(refs))
    return {'target': target, 'metadata_location': table.metadata_location, 'current_snapshot_id': current,
            'table_metadata_locations': {n: t.metadata_location for n,t in tables.items()},
            'all_snapshots_by_table': {n: len(t.metadata.snapshots) for n,t in tables.items()},
            'old_security_files': len(old_files), 'current_security_files': len(current_files),
            'security_files_referenced_by_other_tables': len(foreign_files),
            'exclusive_old_candidates_count': len(candidates), 'exclusive_old_candidates': candidates,
            'exclusive_old_manifest_candidates': old_manifests, 'exclusive_old_manifest_lists': old_lists,
            'limitations': 'No physical deletion is authorized by this inventory; external catalogs, unregistered tables, metadata and object versions are not included.'}


if __name__ == '__main__':
    result = run()
    output = Path('output/baostock-security-history-inventory.json')
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps({k:v for k,v in result.items() if k not in ('exclusive_old_candidates', 'exclusive_old_manifest_candidates', 'exclusive_old_manifest_lists')}, ensure_ascii=False))
