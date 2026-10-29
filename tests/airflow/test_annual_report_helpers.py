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
