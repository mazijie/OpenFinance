"""Find BaoStock partitions in daily-bar object storage, read-only."""
from backend.ingestion.writers.bronze_writer import BronzeWriter
import json


def main():
    c=BronzeWriter().client
    bucket='openfinance-warehouse'
    root='openfinance/daily_bars/data/'
    found=[]
    for month in c.list_objects(bucket,prefix=root,recursive=False):
        if not month.is_dir: continue
        for exchange in c.list_objects(bucket,prefix=month.object_name,recursive=False):
            if not exchange.is_dir: continue
            for provider in c.list_objects(bucket,prefix=exchange.object_name,recursive=False):
                if provider.is_dir and provider.object_name.rstrip('/').endswith('provider=baostock'):
                    count=sum(1 for _ in c.list_objects(bucket,prefix=provider.object_name,recursive=True))
                    found.append({'prefix':provider.object_name,'objects':count})
    print(json.dumps({'baostock_partitions':found,'total_objects':sum(x['objects'] for x in found)},ensure_ascii=False),flush=True)

if __name__=='__main__':
    main()
