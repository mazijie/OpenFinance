"""Airflow DAG for CNInfo annual-report ingestion and indexing."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


def _post(path: str, payload: dict[str, object]) -> object:
    base = os.getenv("OPENFINANCE_INTERNAL_API_BASE_URL", "http://backend:8000").rstrip("/")
    token = os.getenv("OPENFINANCE_INTERNAL_API_TOKEN", "").strip()
    if not token:
        raise ValueError("OPENFINANCE_INTERNAL_API_TOKEN is not configured")
    request = Request(f"{base}{path}", data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json", "X-OpenFinance-Token": token}, method="POST")
    with urlopen(request, timeout=300) as response:
        return json.loads(response.read().decode("utf-8"))


@dag(
    dag_id="openfinance_annual_report_pipeline",
    description="Collect CNInfo annual-report PDFs, extract pages, and build the annual-report vector index.",
    schedule=os.getenv("CNINFO_REPORT_AIRFLOW_SCHEDULE", "0 3 * * *"),
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "openfinance", "retries": 0, "retry_delay": timedelta(minutes=10)},
    tags=["openfinance", "annual-report", "cninfo", "rag"],
)
def annual_report_pipeline():
    @task
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
    def normalize(results: list[dict[str, object]]) -> dict[str, int]:
        paths: list[str] = []
        for result in results:
            if int(result.get("failed_count", 0)) > 0 or int(result.get("discovered_count", 0)) != int(result.get("written_count", 0)):
                raise ValueError(f"Annual-report ingestion was incomplete: {result}")
            paths.extend(str(path) for path in (result.get("manifest_paths") or []))
        return _post("/internal/jobs/annual-reports-bronze-to-silver", {"object_paths": paths})

    @task
    def index(stats: dict[str, int]) -> dict[str, int]:
        if not stats.get("written_reports"):
            return {"written_chunks": 0}
        return _post("/internal/jobs/annual-reports-rag-index", {"rebuild_all": False})

    index(normalize(ingest()))


annual_report_pipeline()
