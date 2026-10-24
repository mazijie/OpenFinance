"""Airflow DAG for scheduled full-market A-share daily-bar synchronization."""

from __future__ import annotations

from datetime import timedelta
import os

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable

from a_share_pipeline_helpers import _submit_a_share_ingestion, _submit_finance_normalization, _wait_for_a_share_ingestion, _wait_for_finance_normalization, _post_internal_job, build_gap_ingest_jobs


@dag(
    dag_id="openfinance_a_share_daily_pipeline",
    description="Discover and repair missing A-share daily bars within the daily API budget.",
    schedule=CronDataIntervalTimetable(
        os.getenv("A_SHARE_DAILY_AIRFLOW_SCHEDULE", "15 18 * * 1-5"),
        timezone="Asia/Shanghai",
    ),
    start_date=pendulum.datetime(2026, 8, 3, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "openfinance", "retries": 0, "retry_delay": timedelta(minutes=10)},
    tags=["openfinance", "a-share", "market-data", "lakehouse", "gap-repair"],
)
def a_share_daily_pipeline():
    @task
    def build_window() -> dict[str, object]:
        adjustment_type = os.getenv("A_SHARE_DAILY_ADJUSTMENT_TYPE", "qfq").strip().lower()
        plan = _post_internal_job(
            "/internal/jobs/a-share-missing-bars-plan",
            {
                "adjustment_type": adjustment_type,
                "lookback_days": int(os.getenv("A_SHARE_GAP_LOOKBACK_DAYS", "60")),
            },
        )
        if plan.get("status") != "available":
            raise AirflowSkipException(
                f"No A-share gap batch scheduled: status={plan.get('status')}, "
                f"pending_symbols={plan.get('pending_symbol_count', 0)}, "
                f"remaining_budget={plan.get('remaining_budget')}"
            )
        return plan

    @task
    def build_ingest_jobs(plan: dict[str, object]) -> list[dict[str, object]]:
        return build_gap_ingest_jobs(
            plan,
            int(os.getenv("A_SHARE_INGEST_BATCH_SIZE", "100")),
        )

    @task(
        retries=1,
        retry_delay=timedelta(minutes=5),
        max_active_tis_per_dag=int(os.getenv("A_SHARE_MAX_ACTIVE_BATCHES", "8")),
    )
    def ingest_a_share_daily_bars(
        symbols: list[str],
        window: dict[str, str],
        missing_date_count: int,
        include_security_master: bool,
    ) -> dict[str, object]:
        context = get_current_context()
        idempotency_key = f"{context['run_id']}:{context['task_instance'].map_index}"
        job_id = _submit_a_share_ingestion({
            "symbols": symbols,
            "start_date": f"{window['start_date']}T00:00:00+00:00",
            "end_date": f"{window['end_date']}T00:00:00+00:00",
            "adjustment_type": window["adjustment_type"],
            "idempotency_key": idempotency_key,
            "include_security_master": include_security_master,
            "include_trading_calendar": False,
        })
        result = _wait_for_a_share_ingestion(job_id)
        if int(result.get("processed_symbol_count", 0)) == 0:
            raise ValueError(f"No daily bars returned for batch of {len(symbols)} symbols despite {missing_date_count} missing dates")
        return result

    @task(retries=0)
    def bronze_to_silver(ingest_result: dict[str, object]) -> dict[str, int]:
        return _wait_for_finance_normalization(_submit_finance_normalization({"object_paths": [str(ingest_result["object_path"])]}))  # type: ignore[return-value]

    @task
    def quality_check(plan: dict[str, object], ingest_results: list[dict[str, object]], silver_stats: list[dict[str, int]]) -> dict[str, int]:
        expected = int(plan["selected_symbol_count"])
        if len(ingest_results) != expected or len(silver_stats) != expected:
            raise ValueError("A-share gap batch results are incomplete")
        if sum(int(item.get("daily_bar_count", 0)) for item in ingest_results) <= 0:
            raise ValueError("No selected symbol returned daily bars")
        if any(int(item.get("failed_count", 0)) for item in silver_stats):
            raise ValueError("Silver normalization failed for at least one gap batch")
        failed_symbols = [
            symbol
            for item in ingest_results
            for symbol in item.get("failed_symbols", [])
        ]
        return {
            "processed_symbols": len(ingest_results),
            "daily_bar_count": sum(int(item.get("daily_bar_count", 0)) for item in ingest_results),
            "written_daily_bars": sum(int(item.get("written_daily_bars", 0)) for item in silver_stats),
            "failed_symbol_count": len(failed_symbols),
            "pending_symbol_count": int(plan["pending_symbol_count"]),
        }

    plan = build_window()
    ingest_results = ingest_a_share_daily_bars.expand_kwargs(build_ingest_jobs(plan))
    silver_stats = bronze_to_silver.expand(ingest_result=ingest_results)
    quality_check(plan, ingest_results, silver_stats)


a_share_daily_pipeline()
