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


def test_partial_backfill_retries_only_failed_symbols_and_keeps_original(monkeypatch) -> None:
    module = _load_helpers_module()
    submitted = []
    def submit(payload):
        submitted.append(payload)
        return payload["symbols"][0]
    def wait(job):
        if job == "bad":
            raise RuntimeError("upstream failed")
        return {"object_path": "recovered", "daily_bar_count": 3, "processed_symbol_count": 1, "failed_symbols": [], "no_data_symbols": []}
    monkeypatch.setattr(module, "_submit_a_share_ingestion", submit)
    monkeypatch.setattr(module, "_wait_for_a_share_ingestion", wait)
    initial = {"object_path": "original", "daily_bar_count": 10, "processed_symbol_count": 2,
               "symbol_count": 4, "failed_symbols": ["good", "bad"]}
    window = {"start_date": "2020-01-01", "end_date": "2026-01-01", "adjustment_type": "qfq", "daily_provider_name": "tencent"}
    result = module.recover_partial_a_share_ingestion(initial, window, "run:try:1", max_retries=2)
    assert [p["symbols"] for p in submitted] == [["good"], ["bad"], ["bad"]]
    assert len({p["idempotency_key"] for p in submitted}) == 3
    assert all(p["include_trading_calendar"] is False for p in submitted)
    assert result["object_paths"] == ["original", "recovered"]
    assert result["daily_bar_count"] == 13 and result["processed_symbol_count"] == 3
    assert result["failed_symbols"] == ["bad"] and initial["failed_symbols"] == ["good", "bad"]


def test_partial_backfill_does_not_treat_empty_recovery_as_success(monkeypatch) -> None:
    module = _load_helpers_module()
    monkeypatch.setattr(module, "_submit_a_share_ingestion", lambda payload: "job")
    monkeypatch.setattr(module, "_wait_for_a_share_ingestion", lambda _: {
        "object_path": "empty", "daily_bar_count": 0, "no_data_symbols": ["bad"]
    })
    result = module.recover_partial_a_share_ingestion(
        {"object_path": "original", "daily_bar_count": 1, "failed_symbols": ["bad"]},
        {"start_date": "2020-01-01", "end_date": "2026-01-01", "adjustment_type": "qfq", "daily_provider_name": "tencent"},
        "run", max_retries=1,
    )
    assert result["failed_symbols"] == ["bad"]
    assert result["object_paths"] == ["original"]


def test_tencent_source_is_preserved_in_both_plans():
    module = _load_helpers_module()
    plan = module.build_universe_plan({"status": "available", "provider": "tencent", "as_of_date": "2026-11-04", "symbols": ["600519.SH"]}, 20)
    assert plan["provider"] == "tencent"
    jobs = module.build_gap_ingest_jobs({"provider": "tencent", "adjustment_type": "qfq", "plans": [
        {"symbol": "600519.SH", "start_date": "2026-11-01", "end_date": "2026-11-04", "missing_date_count": 3}
    ]}, 20)
    assert jobs[0]["window"]["daily_provider_name"] == "tencent"


def test_split_batches_rejects_non_positive_size() -> None:
    module = _load_helpers_module()
    with pytest.raises(ValueError):
        module._split_batches(["a"], 0)


def test_plan_preserves_available_universe_date() -> None:
    module = _load_helpers_module()
    plan = module.build_universe_plan({"status": "available", "as_of_date": "2026-08-20", "symbols": ["000001.SZ", "600519.SH", "300001.SZ"]}, 2)
    assert plan == {"status": "available", "provider": "tencent", "universe_as_of_date": "2026-08-20", "symbol_count": 3, "historical_universe_complete": False, "batches": [["000001.SZ", "600519.SH"], ["300001.SZ"]]}


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
                "daily_provider_name": "tencent",
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
                "daily_provider_name": "tencent",
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
