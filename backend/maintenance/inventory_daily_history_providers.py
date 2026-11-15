"""Count providers referenced in all registered daily-bar snapshots, read-only."""
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
import sys
from backend.lakehouse.catalog import get_catalog


def main():
    c=get_catalog()
    tables={name:c.load_table(f'openfinance.{name}') for name in ('daily_bars','daily_bars_archive_qfq_20261106')}
    work=[(name,t,s) for name,t in tables.items() for s in t.metadata.snapshots]
    manifests={}
    def snapshot(item):
        name,t,s=item
        return name,t,s.manifests(t.io)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for name,t,items in pool.map(snapshot,work):
            for m in items:
                if m.manifest_path not in manifests:
                    manifests[m.manifest_path]=[t,m,set()]
                manifests[m.manifest_path][2].add(name)
    print(f'Inspecting {len(manifests)} distinct manifests',file=sys.stderr,flush=True)
    by_path=defaultdict(set)
    def manifest(item):
        t,m,names=item
        spec=t.specs()[m.partition_spec_id]
        fields=[i for i,f in enumerate(spec.fields) if f.source_id==16 and str(f.transform)=='identity']
        if len(fields)!=1:
            raise ValueError('Cannot identify provider partition')
        return names,fields[0],m.fetch_manifest_entry(t.io)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for n,(names,index,entries) in enumerate(pool.map(manifest,manifests.values()),1):
            for e in entries:
                by_path[(e.data_file.file_path,str(e.data_file.partition[index]))].update(names)
            if n%250==0:
                print(f'Inspected {n}/{len(manifests)} manifests',file=sys.stderr,flush=True)
    totals={name:Counter() for name in tables}
    for (_,provider),names in by_path.items():
        for name in names: totals[name][provider]+=1
    print(json.dumps({'unique_files_by_provider':totals},ensure_ascii=False),flush=True)


if __name__=='__main__':
    main()
