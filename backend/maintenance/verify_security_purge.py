"""Verify the recorded security history purge and preserved live data."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
from urllib.parse import urlparse
from minio.error import S3Error
from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.lakehouse.catalog import get_catalog


def main():
    journal=json.loads(Path('output/baostock-security-history-purge.json').read_text())
    classification=json.loads(Path('output/baostock-security-history-classification.json').read_text())
    if journal['status'] != 'deleted':
        raise RuntimeError('Purge journal does not indicate completion')
    writer=BronzeWriter()
    def exists(path):
        p=urlparse(path)
        try:
            writer.client.stat_object(p.netloc,p.path.lstrip('/'))
            return True
        except S3Error as exc:
            if exc.code in {'NoSuchKey','NoSuchObject','NoSuchFile'}:
                return False
            raise
    with ThreadPoolExecutor(max_workers=8) as pool:
        remaining=sum(pool.map(exists,journal['paths']))
        preserved=list(pool.map(exists,[x['path'] for x in classification['other_only']]))
    table=get_catalog().load_table(journal['target'])
    counts=Counter(x['provider'] for x in table.scan().select('provider').to_arrow().to_pylist())
    result={'remaining_deleted_objects':remaining,'other_files_preserved':all(preserved),
            'snapshot_count':len(table.metadata.snapshots),'current_snapshot_id':table.current_snapshot().snapshot_id,
            'provider_counts':dict(counts)}
    print(json.dumps(result,ensure_ascii=False),flush=True)
    if remaining or not all(preserved) or len(table.metadata.snapshots)!=1 or counts.get('baostock',0):
        raise RuntimeError('Verification failed')


if __name__=='__main__':
    main()
