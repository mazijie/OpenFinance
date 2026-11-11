"""Run three concurrent, isolated-client history benchmark streams per layout.

The test bypasses the production server's process-wide query gate: each stream
has its own in-process FastAPI app. Compare all layouts under the same load;
do not interpret the results as production single-worker concurrent latency.
"""
from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys
import time


SYMBOLS = ('000001.SZ', '600519.SH', '300750.SZ')
ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True, type=Path)
    parser.add_argument('--iterations', type=int, default=10)
    parser.add_argument('--wait', action='store_true', help='Wait for all comparison result files')
    parser.add_argument('--wait-timeout', type=int, default=7200)
    args = parser.parse_args()
    if args.iterations < 1 or args.wait_timeout < 1:
        parser.error('iterations and wait timeout must be positive')
    folder = args.output_dir.resolve()
    try:
        relative = folder.relative_to(ROOT)
    except ValueError as exc:
        raise ValueError('Output directory must be within the shared project mount') from exc
    if relative.parts[0] not in ('output', '.cache'):
        raise ValueError('Output directory must be under output/ or .cache/')
    source = json.loads((folder / 'source.json').read_text())
    identity = json.loads((folder / 'manifest-identity.json').read_text())
    if identity['missing_files'] or identity['extra_files']:
        raise ValueError('Manifest file identity verification failed')
    required = [folder / f'{x}-compare.json' for x in ('sorted', 'bucket16', 'bucket32')]
    deadline = time.monotonic() + args.wait_timeout
    while args.wait and not all(path.exists() for path in required):
        if time.monotonic() >= deadline:
            raise TimeoutError('Timed out waiting for candidate comparisons')
        print('Waiting for candidate comparisons', flush=True)
        time.sleep(30)
    for path in required:
        comparison = json.loads(path.read_text())
        if comparison['status'] != 'success' or comparison['missing_rows'] or comparison['extra_rows']:
            raise ValueError(f'Candidate comparison failed: {path}')
        if comparison['source_snapshot'] != source['snapshot_id']:
            raise ValueError(f'Candidate compared against the wrong source snapshot: {path}')
    candidates = [('source', source['source'], source['snapshot_id']),
                  ('manifest', identity['target'], identity['target_snapshot'])]
    for name, path in zip(('sorted', 'bucket16', 'bucket32'), required):
        comparison = json.loads(path.read_text())
        candidates.append((name, comparison['target'].removeprefix('openfinance.'), comparison['target_snapshot']))
    outputs = []
    for name, table, snapshot in candidates:
        print(f'Benchmarking {name}: table={table} snapshot={snapshot}', flush=True)
        def run_symbol(symbol):
            filename = f'{name}-{symbol.replace(".", "_")}.jsonl'
            path = folder / filename
            if path.exists():
                raise ValueError(f'Refusing to overwrite {path}')
            command = ['docker', 'exec', 'openfinance-backend', 'python', '-m',
                       'scripts.benchmark_market_layout', '--table', table, '--snapshot', snapshot,
                       '--symbols', symbol, '--iterations', str(args.iterations),
                       '--output', f'/app/{relative.as_posix()}/{filename}']
            with (folder / f'{name}-{symbol.replace(".", "_")}.log').open('w') as log:
                subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
            return path
        with ThreadPoolExecutor(max_workers=len(SYMBOLS)) as pool:
            outputs.extend(pool.map(run_symbol, SYMBOLS))
        print(f'Completed {name}', flush=True)
    report = folder / 'read-report.json'
    with report.open('x') as stream:
        subprocess.run([sys.executable, 'scripts/report_market_layout.py', '--min-samples', str(args.iterations),
                        *map(str, outputs)], stdout=stream, check=True)
    print(f'Report: {report}', flush=True)


if __name__ == '__main__':
    main()
