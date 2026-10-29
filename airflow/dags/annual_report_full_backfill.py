"""Airflow DAG for controlled full-market CNInfo annual-report backfill."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from http.client import IncompleteRead
from time import sleep
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from annual_report_ray_helpers import assert_annual_report_run_succeeded, collect_manifest_paths, reconcile_annual_report_index, submit_and_wait_annual_report_ray


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


def _conf() -> dict[str, object]:
    context = get_current_context()
    dag_run = context.get("dag_run")
    return (dag_run.conf or {}) if dag_run else {}


@dag(
    dag_id="openfinance_annual_report_full_backfill",
    description="Download all current A-share annual-report PDFs from CNInfo in bounded batches.",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=int(os.getenv("CNINFO_FULL_BACKFILL_MAX_ACTIVE_TASKS", "4")),
    default_args={"owner": "openfinance", "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["openfinance", "annual-report", "cninfo", "backfill"],
)
def annual_report_full_backfill():
    @task
    def discover_batches() -> list[dict[str, object]]:
        conf = _conf()
        years = conf.get("years") or json.loads(os.getenv("CNINFO_REPORT_YEARS_JSON", "[2024]"))
        batch_size = int(conf.get("batch_size") or os.getenv("CNINFO_FULL_BACKFILL_BATCH_SIZE", "25"))
        if not years:
            raise ValueError("At least one report year is required")
        payload = None
        for attempt in range(2):
            request = Request(
                "https://www.cninfo.com.cn/new/data/szse_stock.json",
                headers={
                    "Accept-Encoding": "identity",
                    "User-Agent": os.getenv("CNINFO_USER_AGENT", "OpenFinance-Agent/0.1"),
                },
            )
            try:
                with urlopen(request, timeout=120) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except IncompleteRead:
                if attempt == 1:
                    raise
                sleep(2)
        if not isinstance(payload, dict):
            raise ValueError("CNInfo security master returned an invalid payload")
        rows = payload.get("stockList", [])
        stocks = []
        for row in rows:
            if row.get("category") != "A股":
                continue
            name = str(row.get("zwjc") or "").strip()
            if not name or name.endswith("退"):
                continue
            code = str(row.get("code") or "").strip()
            org_id = str(row.get("orgId") or "").strip()
            if not code or not org_id:
                continue
            exchange = "SH" if org_id.startswith("gssh") else "SZ" if org_id.startswith("gssz") else "BJ" if org_id.startswith("gfbj") else ""
            if exchange:
                stock = f"{code}.{exchange}"
                if exchange == "BJ":
                    stock = f"{stock}:{org_id}"
                stocks.append(stock)
        stocks = sorted(set(stocks))
        if not stocks:
            raise ValueError("CNInfo returned no active A-share securities")
        return [{"stocks": stocks[start : start + batch_size], "years": [int(year) for year in years]}
                for start in range(0, len(stocks), batch_size)]

    @task(
        retries=1,
        retry_delay=timedelta(minutes=10),
        max_active_tis_per_dag=int(os.getenv("CNINFO_FULL_BACKFILL_MAX_INGEST_TASKS", "1")),
    )
    def ingest_batch(batch: dict[str, object]) -> list[dict[str, object]]:
        return _post("/internal/jobs/ingest-annual-reports", batch)

    @task
    def manifests(batch_results: list[list[dict[str, object]]]) -> list[str]:
        return collect_manifest_paths(batch_results)

    @task(retries=2, retry_delay=timedelta(minutes=10), retry_exponential_backoff=True, execution_timeout=timedelta(hours=4))
    def ray_replay(manifest_paths: list[str]) -> dict[str, object]:
        return submit_and_wait_annual_report_ray(manifest_paths, force=False)

    @task(trigger_rule="all_done", retries=2, retry_delay=timedelta(minutes=10), retry_exponential_backoff=True)
    def chunk_index() -> dict[str, int]:
        return reconcile_annual_report_index()

    @task(trigger_rule="all_done")
    def final_status() -> None:
        assert_annual_report_run_succeeded(get_current_context())

    batches = discover_batches()
    ingestion = ingest_batch.expand(batch=batches)
    paths = manifests(ingestion)
    replay = ray_replay(paths)
    indexed = chunk_index()
    replay >> indexed
    indexed >> final_status()


annual_report_full_backfill()
