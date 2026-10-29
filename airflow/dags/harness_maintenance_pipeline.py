"""Daily retention maintenance for archived research-agent checkpoints."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="openfinance_harness_maintenance_pipeline",
    description="Prune LangGraph checkpoints for old archived research threads.",
    schedule="15 4 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "openfinance", "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["openfinance", "agent-harness", "maintenance"],
)
def harness_maintenance_pipeline():
    @task
    def prune_checkpoints() -> dict[str, int]:
        base = os.getenv("OPENFINANCE_INTERNAL_API_BASE_URL", "http://backend:8000").rstrip("/")
        token = os.getenv("OPENFINANCE_INTERNAL_API_TOKEN", "").strip()
        request = Request(
            f"{base}/internal/jobs/prune-harness-checkpoints",
            data=b"{}",
            headers={"Content-Type": "application/json", "X-OpenFinance-Token": token},
            method="POST",
        )
        with urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))

    prune_checkpoints()


harness_maintenance_pipeline()

