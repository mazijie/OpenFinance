"""Airflow DAG for controlled full-market CNInfo annual-report backfill."""

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
    request = Request(
        f"{base}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-OpenFinance-Token": token},
        method="POST",
    )
    with urlopen(request, timeout=900) as response:
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
    max_active_tasks=int(os.getenv("CNINFO_FULL_BACKFILL_MAX_ACTIVE_TASKS", "8")),
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
        request = Request(
            "https://www.cninfo.com.cn/new/data/szse_stock.json",
            headers={"User-Agent": os.getenv("CNINFO_USER_AGENT", "OpenFinance-Agent/0.1")},
        )
        with urlopen(request, timeout=120) as response:
            payload = json.loads(response.read().decode("utf-8"))
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
                stocks.append(f"{code}.{exchange}")
        stocks = sorted(set(stocks))
        if not stocks:
            raise ValueError("CNInfo returned no active A-share securities")
        return [{"stocks": stocks[start : start + batch_size], "years": [int(year) for year in years]}
                for start in range(0, len(stocks), batch_size)]

    @task(retries=1, retry_delay=timedelta(minutes=10))
    def ingest_batch(batch: dict[str, object]) -> list[dict[str, object]]:
        return _post("/internal/jobs/ingest-annual-reports", batch)

    @task(retries=1, retry_delay=timedelta(minutes=10))
    def normalize_batch(results: list[dict[str, object]]) -> dict[str, int]:
        paths: list[str] = []
        for result in results:
            failed = int(result.get("failed_count", 0))
            discovered = int(result.get("discovered_count", 0))
            written = int(result.get("written_count", 0))
            if failed or discovered != written:
                raise ValueError(f"Annual-report batch incomplete: {result}")
            paths.extend(str(path) for path in (result.get("manifest_paths") or []))
        return _post("/internal/jobs/annual-reports-bronze-to-silver", {"object_paths": paths})

    @task
    def quality_check(stats: list[dict[str, int]]) -> dict[str, int]:
        return {
            "batch_count": len(stats),
            "processed_objects": sum(int(item.get("processed_objects", 0)) for item in stats),
            "written_reports": sum(int(item.get("written_reports", 0)) for item in stats),
            "written_pages": sum(int(item.get("written_pages", 0)) for item in stats),
            "written_sections": sum(int(item.get("written_sections", 0)) for item in stats),
            "failed_count": sum(int(item.get("failed_count", 0)) for item in stats),
        }

    batches = discover_batches()
    ingestion = ingest_batch.expand(batch=batches)
    normalization = normalize_batch.expand(results=ingestion)
    quality_check(normalization)


annual_report_full_backfill()