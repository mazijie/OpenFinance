import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[2] / 'scripts' / 'report_market_layout.py'


def sample(table, digest='same', available=True):
    return {'table': table, 'symbol': '000001.SZ', 'period': 'daily', 'bars': 1,
            'bars_sha256': digest, 'has_more': available, 'status': 200,
            'endpoint_seconds': 1.0, 'plan_seconds': 0.5, 'read_seconds': 0.1,
            'planned_files': 2, 'planned_bytes': 300}


def test_report_detects_mismatched_query_results(tmp_path):
    path = tmp_path / 'samples.jsonl'
    path.write_text('\n'.join(json.dumps(x) for x in (sample('source'), sample('candidate', 'different'))))
    result = subprocess.run([sys.executable, str(SCRIPT), '--min-samples', '1', str(path)],
                            text=True, capture_output=True)
    assert result.returncode == 1
    assert 'query result mismatch' in json.loads(result.stdout)['problems'][0]


def test_report_requires_resolved_availability_and_all_groups(tmp_path):
    path = tmp_path / 'samples.jsonl'
    missing = sample('candidate', available=None)
    other = sample('source') | {'symbol': '600519.SH'}
    path.write_text('\n'.join(json.dumps(x) for x in (sample('source'), missing, other)))
    result = subprocess.run([sys.executable, str(SCRIPT), '--min-samples', '1', str(path)],
                            text=True, capture_output=True)
    assert result.returncode == 1
    problems = json.loads(result.stdout)['problems']
    assert any('availability was never resolved' in item for item in problems)
    assert any('missing symbol/period groups' in item for item in problems)
