"""Remove BaoStock rows from the current security-master snapshot.

Historical snapshots and their files are deliberately retained pending a
cross-table reference analysis. Run only with all finance writers stopped.
"""
from collections import Counter
from hashlib import sha256
import json

from pyiceberg.expressions import EqualTo
from backend.config import settings
from backend.lakehouse.catalog import get_catalog
from backend.lakehouse.write_coordination import iceberg_write_lock


def fingerprints(rows):
    return Counter(sha256(json.dumps(row, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest() for row in rows)


def run():
    name = f'{settings.ICEBERG_NAMESPACE}.{settings.ICEBERG_SECURITY_MASTER_TABLE}'
    with iceberg_write_lock():
        table = get_catalog().load_table(name)
        before = table.scan().to_arrow().to_pylist()
        kept = [r for r in before if str(r.get('provider', '')).lower() != 'baostock']
        removed = len(before) - len(kept)
        if not removed:
            return {'table': name, 'removed': 0, 'preserved': len(kept), 'already_clean': True}
        if any(r.get('provider') != 'baostock' and str(r.get('provider', '')).lower() == 'baostock' for r in before):
            raise ValueError('Unexpected provider casing: refusing partial deletion')
        table.delete(EqualTo('provider', 'baostock'))
        after = get_catalog().load_table(name).scan().to_arrow().to_pylist()
        if fingerprints(kept) != fingerprints(after):
            raise RuntimeError('Post-delete verification failed; historical snapshots are retained')
        return {'table': name, 'removed': removed, 'preserved': len(after), 'verified': True,
                'historical_snapshots_cleaned': False}


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False), flush=True)
