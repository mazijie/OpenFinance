"""Inspect provider values in unreferenced historical security files; read only."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import pyarrow.parquet as pq
from backend.lakehouse.catalog import get_catalog
from backend.config import settings


def main():
    audit = json.loads(Path('output/baostock-security-history-inventory.json').read_text())
    table = get_catalog().load_table(audit['target'])
    if table.metadata_location != audit['metadata_location']:
        raise RuntimeError('Security table changed since reference inventory')
    def classify(path):
        with table.io.new_input(path).open() as source:
            counts = Counter(str(x) for x in pq.read_table(source, columns=['provider']).column('provider').to_pylist())
        return path, counts
    result = {'baostock_only': [], 'other_only': [], 'mixed': [], 'empty': []}
    with ThreadPoolExecutor(max_workers=8) as pool:
        for n, (path, counts) in enumerate(pool.map(classify, audit['exclusive_old_candidates']), 1):
            kind = 'empty' if not counts else 'baostock_only' if set(counts) == {'baostock'} else 'other_only' if 'baostock' not in counts else 'mixed'
            result[kind].append({'path': path, 'providers': dict(counts)})
            if n % 100 == 0:
                print(f'Inspected {n}/{len(audit["exclusive_old_candidates"])} files', file=sys.stderr, flush=True)
    output = Path('output/baostock-security-history-classification.json')
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps({key: len(value) for key, value in result.items()}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
