"""Test threshold-based compaction on isolated synthetic daily updates."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True, type=Path)
    parser.add_argument('--day', required=True, type=date.fromisoformat)
    parser.add_argument('--min-input-files', default=8, type=int)
    parser.add_argument('--wait', action='store_true')
    parser.add_argument('--wait-timeout', type=int, default=7200)
    args = parser.parse_args()
    if args.min_input_files < 2 or args.wait_timeout < 1:
        parser.error('min-input-files must be >= 2 and wait timeout positive')
    folder = args.output_dir.resolve()
    try:
        relative = folder.relative_to(ROOT)
    except ValueError as exc:
        raise ValueError('Output directory must be within the shared project mount') from exc
    if relative.parts[0] not in ('output', '.cache'):
        raise ValueError('Output directory must be under output/ or .cache/')
    source = json.loads((folder / 'source.json').read_text())
    deadline = time.monotonic() + args.wait_timeout
    while True:
        try:
            appends = json.loads((folder / 'append-report.json').read_text())
            break
        except (FileNotFoundError, json.JSONDecodeError):
            if not args.wait:
                raise
            if time.monotonic() >= deadline:
                raise TimeoutError('Timed out waiting for append benchmark report')
            print('Waiting for append benchmark report', flush=True)
            time.sleep(30)
    names = ('sorted', 'bucket16', 'bucket32')
    targets = {item['target'].rsplit('.', 1)[-1]: item for item in appends}
    if set(targets) != set(names) or any(item['synthetic_day'] != str(args.day) for item in appends):
        raise ValueError('Unexpected append benchmark targets or day')

    def run(name):
        item = targets[name]
        result = folder / f'{name}-compact.json'
        log_path = folder / f'{name}-compact.log'
        if result.exists() or log_path.exists():
            raise ValueError(f'Refusing to overwrite {name} compaction result')
        cmd = ['docker', 'exec', '-u', 'root', 'openfinance-spark-master', '/opt/spark/bin/spark-submit',
               '--master', 'spark://spark-master:7077', '--executor-cores', '4', '--executor-memory', '8g',
               '--total-executor-cores', '8', '--driver-memory', '3g', '--properties-file',
               '/opt/openfinance/spark/conf/iceberg.properties', '--conf', 'spark.sql.shuffle.partitions=32',
               '/opt/openfinance/spark/benchmark/market_layout_experiment.py', 'compact-day',
               '--source-snapshot', source['snapshot_id'], '--target', 'openfinance.' + item['target'],
               '--target-snapshot', item['snapshot'],
               '--day', str(args.day), '--min-input-files', str(args.min_input_files),
               '--result', f'/opt/openfinance/{relative.as_posix()}/{name}-compact.json']
        with log_path.open('x') as log:
            subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, check=True)
        data = json.loads(result.read_text())
        if data['status'] != 'success' or data['missing_rows'] or data['extra_rows']:
            raise ValueError(f'Compaction validation failed: {name}')
        return data

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(run, names))
    path = folder / 'compaction-report.json'
    with path.open('x') as output:
        json.dump(results, output, ensure_ascii=False, indent=2)
        output.write('\n')
    print(f'Compaction report: {path}', flush=True)


if __name__ == '__main__':
    main()
