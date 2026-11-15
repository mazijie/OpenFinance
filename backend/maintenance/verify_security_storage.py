"""Check all security Parquet objects are either current or preserved Tencent history."""
import json
from pathlib import Path
from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.lakehouse.catalog import get_catalog


def main():
    table=get_catalog().load_table('openfinance.security_master')
    writer=BronzeWriter()
    current={task.file.file_path for task in table.scan().plan_files()}
    actual={'s3://openfinance-warehouse/'+obj.object_name for obj in writer.client.list_objects('openfinance-warehouse',prefix='openfinance/security_master/data/',recursive=True)}
    prior=json.loads(Path('output/baostock-security-history-classification.json').read_text())
    kept={row['path'] for row in prior['other_only']}
    missing=current-actual
    unexpected=actual-current-kept
    print(json.dumps({'current_files':len(current),'total_objects':len(actual),'preserved_tencent_history':len(kept),
                      'missing_current_files':len(missing),'unexpected_objects':len(unexpected)},ensure_ascii=False),flush=True)
    if missing or unexpected or not kept <= actual:
        raise RuntimeError('Security data object audit failed')

if __name__=='__main__':
    main()
