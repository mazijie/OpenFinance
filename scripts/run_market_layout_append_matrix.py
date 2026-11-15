"""Run the production-writer append simulation after read benchmarks finish."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
from pathlib import Path
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True, type=Path)
    parser.add_argument('--source-day', required=True, type=date.fromisoformat)
    parser.add_argument('--synthetic-day', required=True, type=date.fromisoformat)
    parser.add_argument('--batch-size', type=int, default=200)
    parser.add_argument('--wait', action='store_true')
    parser.add_argument('--wait-timeout', type=int, default=7200)
    args = parser.parse_args()
    folder = args.output_dir.resolve()
    source = json.loads((folder / 'source.json').read_text())
    report = folder / 'read-report.json'
    deadline = time.monotonic() + args.wait_timeout
    while True:
        try:
            data = json.loads(report.read_text())
            if data['problems']:
                raise ValueError(f'Read benchmark failed: {data["problems"]}')
            break
        except (FileNotFoundError, json.JSONDecodeError):
            if not args.wait:
                raise
            if time.monotonic() >= deadline:
                raise TimeoutError('Timed out waiting for read benchmark report')
            print('Waiting for read benchmark report', flush=True)
            time.sleep(30)

    def run(name):
        comparison = json.loads((folder / f'{name}-compare.json').read_text())
        path = folder / f'{name}-append.log'
        if path.exists():
            raise ValueError(f'Refusing to overwrite {path}')
        cmd = ['docker', 'exec', 'openfinance-backend', 'python', '-m',
               'scripts.simulate_market_layout_append', '--target', comparison['target'].removeprefix('openfinance.'),
               '--target-snapshot', comparison['target_snapshot'], '--source-snapshot', source['snapshot_id'],
               '--source-day', str(args.source_day), '--synthetic-day', str(args.synthetic_day),
               '--batch-size', str(args.batch_size)]
        with path.open('x') as log:
            subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, check=True)
        for line in path.read_text().splitlines():
            if line.startswith('APPEND_RESULT='):
                return json.loads(line.removeprefix('APPEND_RESULT='))
        raise ValueError(f'Missing append result: {path}')

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(run, ('sorted', 'bucket16', 'bucket32')))
    result_path = folder / 'append-report.json'
    with result_path.open('x') as out:
        json.dump(results, out, ensure_ascii=False, indent=2)
        out.write('\n')
    print(f'Append report: {result_path}', flush=True)


if __name__ == '__main__':
    main()
