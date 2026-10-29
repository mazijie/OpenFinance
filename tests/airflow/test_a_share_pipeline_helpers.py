from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from urllib.error import URLError

import pytest


def _load_helpers_module():
    path = Path(__file__).parents[2] / "airflow" / "dags" / "a_share_pipeline_helpers.py"
    spec = importlib.util.spec_from_file_location("a_share_pipeline_helpers", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load A-share helper module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_split_batches_handles_partial_batch() -> None:
    module = _load_helpers_module()
    assert module._split_batches(["a", "b", "c", "d", "e"], 2) == [["a", "b"], ["c", "d"], ["e"]]


def test_split_batches_rejects_non_positive_size() -> None:
    module = _load_helpers_module()
    with pytest.raises(ValueError):
        module._split_batches(["a"], 0)


def test_plan_preserves_available_universe_date() -> None:
    module = _load_helpers_module()
    plan = module.build_universe_plan({"status": "available", "as_of_date": "2026-08-20", "symbols": ["000001.SZ", "600519.SH", "300001.SZ"]}, 2)
    assert plan == {"status": "available", "universe_as_of_date": "2026-08-20", "symbol_count": 3, "batches": [["000001.SZ", "600519.SH"], ["300001.SZ"]]}


def test_plan_handles_already_covered_without_batches() -> None:
    module = _load_helpers_module()
    plan = module.build_universe_plan({"status": "already_covered", "as_of_date": "2026-08-20", "symbols": []}, 100)
    assert plan["batches"] == []


def test_build_gap_ingest_jobs_groups_equal_windows() -> None:
    module = _load_helpers_module()
    plan = {
        "adjustment_type": "qfq",
        "plans": [
            {"symbol": "000001.SZ", "start_date": "2026-09-04", "end_date": "2026-10-23", "missing_date_count": 30, "needs_metadata": False},
            {"symbol": "000002.SZ", "start_date": "2026-09-04", "end_date": "2026-10-23", "missing_date_count": 30, "needs_metadata": False},
            {"symbol": "000006.SZ", "start_date": "2026-09-05", "end_date": "2026-10-23", "missing_date_count": 29, "needs_metadata": False},
        ],
    }

    jobs = module.build_gap_ingest_jobs(plan, batch_size=2)

    assert jobs == [
        {
            "symbols": ["000001.SZ", "000002.SZ"],
            "window": {
                "start_date": "2026-09-04",
                "end_date": "2026-10-23",
                "adjustment_type": "qfq",
            },
            "missing_date_count": 60,
            "include_security_master": False,
        },
        {
            "symbols": ["000006.SZ"],
            "window": {
                "start_date": "2026-09-05",
                "end_date": "2026-10-23",
                "adjustment_type": "qfq",
            },
            "missing_date_count": 29,
            "include_security_master": False,
        },
    ]


def test_ingestion_status_poll_retries_transient_connection_error(monkeypatch) -> None:
    module = _load_helpers_module()
    calls = 0

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return json.dumps({"status": "success", "result": {"daily_bar_count": 1}}).encode()

    def fake_urlopen(request, timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise URLError("connection refused")
        return Response()

    monkeypatch.setenv("OPENFINANCE_INTERNAL_API_TOKEN", "test-token")
    monkeypatch.setenv("A_SHARE_JOB_POLL_SECONDS", "1")
    monkeypatch.setattr(module, "urlopen", fake_urlopen)
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    result = module._wait_for_a_share_ingestion("job-1")

    assert result == {"daily_bar_count": 1}
    assert calls == 2


def test_build_normalization_batches_groups_unique_bronze_objects() -> None:
    module = _load_helpers_module()
    results = [
        {"object_path": "finance/requests/a.json"},
        {"object_path": "finance/requests/b.json"},
        {"object_path": "finance/requests/c.json"},
    ]

    assert module.build_normalization_batches(results, 2) == [
        ["finance/requests/a.json", "finance/requests/b.json"],
        ["finance/requests/c.json"],
    ]


def test_build_normalization_batches_rejects_missing_object_path() -> None:
    module = _load_helpers_module()

    with pytest.raises(ValueError, match="one unique Bronze object"):
        module.build_normalization_batches(
            [{"object_path": "finance/requests/a.json"}, {}],
            10,
        )


def test_normalization_status_poll_retries_connection_error(monkeypatch) -> None:
    module = _load_helpers_module()
    calls = 0

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return json.dumps(
                {"status": "success", "result": {"processed_objects": 1}}
            ).encode()

    def fake_urlopen(request, timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise URLError("connection refused")
        return Response()

    monkeypatch.setenv("OPENFINANCE_INTERNAL_API_TOKEN", "test-token")
    monkeypatch.setenv("A_SHARE_JOB_POLL_SECONDS", "1")
    monkeypatch.setattr(module, "urlopen", fake_urlopen)
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    result = module._wait_for_finance_normalization("job-1")

    assert result == {"processed_objects": 1}
    assert calls == 2
