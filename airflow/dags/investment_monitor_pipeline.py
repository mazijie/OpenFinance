"""After-close investment monitoring DAG backed by the durable API job queue."""

from __future__ import annotations

import json
import os
import time
from datetime import timedelta
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable


def _request(path: str, *, method: str = "GET", payload: dict[str, object] | None = None) -> dict[str, object]:
    base = os.getenv("OPENFINANCE_INTERNAL_API_BASE_URL", "http://backend:8000").rstrip("/")
    token = os.getenv("OPENFINANCE_INTERNAL_API_TOKEN", "").strip()
    if not token:
        raise ValueError("OPENFINANCE_INTERNAL_API_TOKEN is not configured")
    request = Request(
        f"{base}{path}",
        data=json.dumps(payload).encode("utf-8") if payload is not None else None,
        headers={"Content-Type": "application/json", "X-OpenFinance-Token": token},
        method=method,
    )
    with urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


@dag(
    dag_id="openfinance_investment_monitor_pipeline",
    description="Run the bounded research agent for the enabled A-share watchlist after close.",
    schedule=CronDataIntervalTimetable(
        os.getenv("INVESTMENT_MONITOR_AIRFLOW_SCHEDULE", "0 20 * * 1-5"),
        timezone="Asia/Shanghai",
    ),
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "openfinance", "retries": 0},
    tags=["openfinance", "investment-monitor", "research-agent"],
)
def investment_monitor_pipeline():
    @task
    def build_window() -> dict[str, str]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        if conf.get("trading_date"):
            trading_date = str(conf["trading_date"])
            cutoff = str(conf.get("cutoff_at") or f"{trading_date}T20:00:00+08:00")
        else:
            cutoff_local = context["data_interval_end"].in_timezone("Asia/Shanghai")
            trading_date = cutoff_local.date().isoformat()
            cutoff = cutoff_local.isoformat()
        return {"trading_date": trading_date, "cutoff_at": cutoff}

    @task
    def check_trading_day(window: dict[str, str]) -> dict[str, str]:
        query = urlencode({"trading_date": window["trading_date"]})
        result = _request(f"/internal/jobs/investment-monitor/trading-day?{query}")
        if result.get("is_open") is False:
            raise AirflowSkipException(f"{window['trading_date']} is not an exchange trading day")
        return window

    @task
    def submit(window: dict[str, str]) -> dict[str, object]:
        return _request(
            "/internal/jobs/investment-monitor/run",
            method="POST",
            payload={
                "trading_date": window["trading_date"],
                "cutoff_at": window["cutoff_at"],
            },
        )

    @task
    def wait_for_completion(accepted: dict[str, object]) -> dict[str, object]:
        run_id = str(accepted.get("run_id") or "").strip()
        if not run_id:
            raise ValueError("Monitor submission did not return a run id")
        timeout_seconds = int(os.getenv("INVESTMENT_MONITOR_JOB_TIMEOUT_SECONDS", "7200"))
        poll_seconds = max(1, int(os.getenv("INVESTMENT_MONITOR_JOB_POLL_SECONDS", "10")))
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            run = _request(f"/internal/jobs/investment-monitor/runs/{run_id}")
            status = str(run.get("status") or "")
            if status in {"completed", "degraded", "skipped", "failed"}:
                if status == "failed":
                    raise RuntimeError(f"Investment monitor run failed: {run.get('error')}")
                return run
            time.sleep(poll_seconds)
        raise TimeoutError(f"Investment monitor run {run_id} did not finish in time")

    window = check_trading_day(build_window())
    wait_for_completion(submit(window))


investment_monitor_pipeline()

