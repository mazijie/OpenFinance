"""Airflow DAG for CNInfo annual-report ingestion and indexing."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from annual_report_ray_helpers import assert_annual_report_run_succeeded, collect_manifest_paths, reconcile_annual_report_index, submit_and_wait_annual_report_ray


def _post(path: str, payload: dict[str, object]) -> object:
    base = os.getenv("OPENFINANCE_INTERNAL_API_BASE_URL", "http://backend:8000").rstrip("/")
    token = os.getenv("OPENFINANCE_INTERNAL_API_TOKEN", "").strip()
    if not token:
        raise ValueError("OPENFINANCE_INTERNAL_API_TOKEN is not configured")
    request = Request(f"{base}{path}", data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json", "X-OpenFinance-Token": token}, method="POST")
    with urlopen(request, timeout=int(os.getenv("PDF_PARSER_TIMEOUT_SECONDS", "14400"))) as response:
        return json.loads(response.read().decode("utf-8"))


@dag(
    dag_id="openfinance_annual_report_pipeline",
    description="Collect CNInfo annual-report PDFs, extract pages, and build the annual-report vector index.",
    schedule=os.getenv("CNINFO_REPORT_AIRFLOW_SCHEDULE", "0 3 * * *"),
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "openfinance", "retries": 2, "retry_delay": timedelta(minutes=10), "retry_exponential_backoff": True},
    params={
        "years": Param(json.loads(os.getenv("CNINFO_REPORT_YEARS_JSON", "[]")), type="array",
                       items={"type": "integer", "minimum": 2000, "maximum": 2100},
                       title="年报年份", description="输入年份数组，例如 [2023, 2024]。"),
        "stocks": Param(json.loads(os.getenv("CNINFO_REPORT_STOCKS_JSON", "[]")), type="array",
                        items={"type": "string"}, title="证券代码",
                        description="每行输入一个证券代码，例如 600519.SH。"),
    },
    tags=["openfinance", "annual-report", "cninfo", "rag"],
)
def annual_report_pipeline():
    @task(retries=2, retry_exponential_backoff=True)
    def ingest() -> list[dict[str, object]]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        stocks = conf.get("stocks") or json.loads(os.getenv("CNINFO_REPORT_STOCKS_JSON", "[]"))
        years = conf.get("years") or json.loads(os.getenv("CNINFO_REPORT_YEARS_JSON", "[]"))
        if not stocks or not years:
            raise ValueError("Annual-report run requires non-empty stocks and years, either in dag_run.conf or environment")
        return _post("/internal/jobs/ingest-annual-reports", {"stocks": stocks, "years": years})

    @task
    def manifests(results: list[dict[str, object]]) -> list[str]:
        return collect_manifest_paths([results])

    @task(retries=2, retry_exponential_backoff=True, execution_timeout=timedelta(hours=4))
    def ray_replay(manifest_paths: list[str]) -> dict[str, object]:
        return submit_and_wait_annual_report_ray(manifest_paths, force=False)

    @task(trigger_rule="all_done", retries=2, retry_exponential_backoff=True)
    def chunk_index() -> dict[str, int]:
        # A retry still indexes when Silver contains no newly written chunks.
        return reconcile_annual_report_index()

    @task(trigger_rule="all_done")
    def final_status() -> None:
        assert_annual_report_run_succeeded(get_current_context())

    replay = ray_replay(manifests(ingest()))
    indexed = chunk_index()
    replay >> indexed
    indexed >> final_status()


annual_report_pipeline()
