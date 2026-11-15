"""Report current-snapshot data-file overlap and provider counts, read-only."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
from backend.lakehouse.catalog import get_catalog


def files(table):
    snapshot = table.current_snapshot()
    if not snapshot:
        return {}, Counter(), Counter()
    manifests = snapshot.manifests(table.io)
    result, counts, rows = {}, Counter(), Counter()
    with ThreadPoolExecutor(max_workers=8) as pool:
        for group in pool.map(lambda manifest: manifest.fetch_manifest_entry(table.io), manifests):
            for entry in group:
                data = entry.data_file
                provider = str(data.partition[2])
                result[data.file_path] = provider
                counts[provider] += 1
                rows[provider] += data.record_count
    return result, counts, rows


def main():
    catalog = get_catalog()
    current = catalog.load_table('openfinance.daily_bars')
    archive = catalog.load_table('openfinance.daily_bars_archive_qfq_20261106')
    print('Reading current table manifests...', flush=True)
    a, ac, ar = files(current)
    print('Reading archive table manifests...', flush=True)
    b, bc, br = files(archive)
    shared_snapshots = len({s.snapshot_id for s in current.metadata.snapshots} & {s.snapshot_id for s in archive.metadata.snapshots})
    print(json.dumps({'current_files': len(a), 'archive_files': len(b), 'shared_files': len(a.keys() & b.keys()),
                      'current_files_by_provider': ac, 'archive_files_by_provider': bc,
                      'current_record_counts': ar, 'archive_record_counts': br,
                      'shared_snapshot_ids': shared_snapshots}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
