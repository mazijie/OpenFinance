"""Replay existing annual-report Bronze objects into Silver and Gold."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from annual_report_ray_helpers import (
    assert_annual_report_run_succeeded,
    reconcile_annual_report_index,
    submit_and_wait_annual_report_ray,
)


def _post(path: str, payload: dict[str, object]) -> object:
    base = os.getenv("OPENFINANCE_INTERNAL_API_BASE_URL", "http://backend:8000").rstrip("/")
    token = os.getenv("OPENFINANCE_INTERNAL_API_TOKEN", "").strip()
    if not token:
        raise ValueError("OPENFINANCE_INTERNAL_API_TOKEN is not configured")
    request = Request(
        f"{base}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-OpenFinance-Token": token},
        method="POST",
    )
    with urlopen(request, timeout=int(os.getenv("PDF_PARSER_TIMEOUT_SECONDS", "14400"))) as response:
        return json.loads(response.read().decode("utf-8"))


@dag(
    dag_id="openfinance_annual_report_bronze_replay",
    description="Replay existing CNInfo Bronze annual reports without downloading them again.",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "openfinance", "retries": 2, "retry_delay": timedelta(minutes=10), "retry_exponential_backoff": True},
    tags=["openfinance", "annual-report", "bronze-replay", "rag"],
)
def annual_report_bronze_replay():
    @task
    def manifests() -> list[str]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        years = conf.get("years")
        stocks = conf.get("stocks") or []
        if not isinstance(years, list) or not years:
            raise ValueError("Bronze replay requires dag_run.conf.years, for example {\"years\": [2024]}")
        paths = _post("/internal/jobs/annual-reports-bronze-manifests", {"stocks": stocks, "years": years})
        if not isinstance(paths, list) or not paths:
            raise ValueError(f"No complete Bronze annual-report manifests found for years={years}, stocks={stocks}")
        return [str(path) for path in paths]

    @task(retries=0)
    def split_batches(manifest_paths: list[str]) -> list[list[str]]:
        """Create small, deterministic Ray Job payloads for Airflow task mapping.

        Each mapped task submits *one* Ray Job. The `batch_size` DAG-run
        parameter lets a smoke test use one report without changing code.
        """
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        size = int(conf.get("batch_size", os.getenv("RAY_REPORT_BATCH_SIZE", "3")))
        if not 1 <= size <= 20:
            raise ValueError("batch_size must be an integer from 1 to 20")

        unique_paths = sorted(set(manifest_paths))
        if not unique_paths:
            raise ValueError("No annual-report manifests available for Ray replay")
        batches = [unique_paths[i : i + size] for i in range(0, len(unique_paths), size)]
        print(f"Annual-report replay: {len(unique_paths)} manifests, {len(batches)} batches, size={size}")
        return batches

    @task(
        retries=0,  # Do not automatically submit another Ray Job if Airflow loses track of one.
        max_active_tis_per_dag=1,  # Start conservatively; Silver writes share a catalog.
        execution_timeout=timedelta(hours=5),  # Longer than default Ray wait (4 hours).
    )
    def ray_replay(manifest_paths: list[str]) -> dict[str, object]:
        result = submit_and_wait_annual_report_ray(manifest_paths, force=False)
        return {**result, "manifest_count": len(manifest_paths)}

    @task(retries=2, retry_exponential_backoff=True, trigger_rule="all_done")
    def chunk_index() -> dict[str, int]:
        # Reconcile successful Silver batches once even if other batches failed.
        # A separate terminal task below still marks the DAG run as failed.
        return reconcile_annual_report_index()

    @task(retries=0, trigger_rule="all_done")
    def verify_run() -> None:
        assert_annual_report_run_succeeded(get_current_context())

    paths = manifests()
    batches = split_batches(paths)
    replay = ray_replay.expand(manifest_paths=batches)
    indexed = chunk_index()
    verified = verify_run()
    replay >> indexed >> verified


annual_report_bronze_replay()
