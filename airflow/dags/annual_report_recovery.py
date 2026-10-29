"""Recover failed annual-report stages and reconcile the active index."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from annual_report_ray_helpers import assert_annual_report_run_succeeded, reconcile_annual_report_index


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
    dag_id="openfinance_annual_report_recovery",
    description="Resume failed annual-report stages and reconcile vector and lexical generations.",
    schedule=os.getenv("ANNUAL_REPORT_RECOVERY_SCHEDULE", "15 4 * * *"),
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "openfinance", "retries": 2, "retry_delay": timedelta(minutes=10), "retry_exponential_backoff": True},
    tags=["openfinance", "annual-report", "recovery", "reconciliation"],
)
def annual_report_recovery():
    @task(retries=2, retry_exponential_backoff=True)
    def pending_work() -> list[dict[str, object]]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        report_ids = conf.get("report_ids") or []
        return _post("/internal/jobs/annual-reports-recovery-plan", {"report_ids": report_ids})

    @task(
        max_active_tis_per_dag=1,
        retries=2,
        retry_exponential_backoff=True,
        execution_timeout=timedelta(hours=4),
    )
    def recover_one(work: dict[str, object]) -> dict[str, object]:
        return _post(
            "/internal/jobs/annual-reports-recover-one",
            {
                "manifest_path": work["manifest_path"],
                "report_id": work["report_id"],
                "pdf_sha256": work["pdf_sha256"],
                "pending_stages": work["pending_stages"],
            },
        )

    @task(trigger_rule="all_done", retries=2, retry_exponential_backoff=True)
    def reconcile_index() -> dict[str, int]:
        return reconcile_annual_report_index()

    @task(trigger_rule="all_done")
    def final_status() -> None:
        assert_annual_report_run_succeeded(get_current_context())

    recovered = recover_one.expand(work=pending_work())
    reconciled = reconcile_index()
    recovered >> reconciled
    reconciled >> final_status()


annual_report_recovery()
