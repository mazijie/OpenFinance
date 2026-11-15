from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


def load_helpers():
    path = Path(__file__).parents[2] / "airflow" / "dags" / "annual_report_ray_helpers.py"
    spec = importlib.util.spec_from_file_location("annual_report_ray_helpers", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_final_status_reports_upstream_failure_after_index_success():
    module = load_helpers()
    tasks = [SimpleNamespace(task_id="ray_replay", map_index=-1, state="failed"),
             SimpleNamespace(task_id="chunk_index", map_index=-1, state="success")]
    context = {"dag_run": SimpleNamespace(get_task_instances=lambda: tasks),
               "task_instance": SimpleNamespace(task_id="final_status")}
    with pytest.raises(RuntimeError, match="ray_replay"):
        module.assert_annual_report_run_succeeded(context)


def test_manifest_collection_deduplicates_batches():
    module = load_helpers()
    assert module.collect_manifest_paths([[{"manifest_paths": ["a", "b"]}], [{"manifest_paths": ["a"]}]]) == ["a", "b"]


def test_replay_retries_terminal_failures_and_records_only_unresolved(monkeypatch):
    module = load_helpers()
    calls = []
    def submit(paths, *, force):
        path = paths[0]
        calls.append(path)
        if path == 'bad' or (path == 'transient' and calls.count(path) == 1):
            raise module.TerminalRayJobError(f'{path}-{calls.count(path)}', 'failed')
        return {'status': 'succeeded'}
    monkeypatch.setattr(module, 'submit_and_wait_annual_report_ray', submit)
    monkeypatch.setattr(module.time, 'sleep', lambda seconds: None)
    result = module.replay_reports_individually(['transient', 'bad', 'good'], max_attempts=2)
    assert calls == ['transient', 'transient', 'bad', 'bad', 'good']
    assert result['processed_reports'] == 2
    assert result['failed_reports'] == 1
    assert result['failures'][0]['manifest_path'] == 'bad'
    assert result['failures'][0]['job_ids'] == ['bad-1', 'bad-2']


def test_replay_does_not_resubmit_unconfirmed_job(monkeypatch):
    module = load_helpers()
    calls = []
    def submit(paths, *, force):
        calls.append(paths[0])
        raise TimeoutError('remote status unknown')
    monkeypatch.setattr(module, 'submit_and_wait_annual_report_ray', submit)
    with pytest.raises(TimeoutError, match='unknown'):
        module.replay_reports_individually(['a'], max_attempts=3, retry_delay=0)
    assert calls == ['a']


def test_stopping_is_not_treated_as_terminal(monkeypatch):
    module = load_helpers()
    statuses = iter(['stopping', 'stopped'])
    def request(method, path, payload=None, **kwargs):
        if method == 'POST':
            return {'job_id': 'job-1'}
        return {'status': next(statuses)}
    monkeypatch.setattr(module, '_request', request)
    monkeypatch.setattr(module.time, 'sleep', lambda seconds: None)
    with pytest.raises(module.TerminalRayJobError) as exc:
        module.submit_and_wait_annual_report_ray(['a'])
    assert exc.value.status == 'stopped'
