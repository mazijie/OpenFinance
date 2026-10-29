"""Manual full-history A-share daily-bars backfill."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context

from a_share_pipeline_helpers import _get_a_share_symbols, _submit_a_share_ingestion, _submit_finance_normalization, _wait_for_a_share_ingestion, _wait_for_finance_normalization, _post_internal_job, build_universe_plan


@dag(dag_id="openfinance_a_share_backfill_pipeline", description="Manually backfill all current ordinary A-share daily bars.", schedule=None, start_date=pendulum.datetime(2026, 8, 3, tz="UTC"), catchup=False, max_active_runs=1, default_args={"owner": "openfinance", "retries": 0}, tags=["openfinance", "a-share", "market-data", "backfill"])
def a_share_backfill_pipeline():
    @task
    def build_window() -> dict[str, str]:
        return {"start_date": os.getenv("A_SHARE_BACKFILL_START_DATE", "1990-01-01"), "end_date": datetime.now(UTC).date().isoformat(), "adjustment_type": os.getenv("A_SHARE_DAILY_ADJUSTMENT_TYPE", "qfq").strip().lower()}

    @task
    def build_batches(window: dict[str, str]) -> dict[str, object]:
        plan = build_universe_plan(_get_a_share_symbols(window["adjustment_type"]), int(os.getenv("A_SHARE_BATCH_SIZE", "100")))
        if plan["status"] == "already_covered":
            raise AirflowSkipException(f"Universe {plan['universe_as_of_date']} is already fully covered")
        return plan

    @task
    def build_ingest_jobs(plan: dict[str, object], window: dict[str, str]) -> list[dict[str, object]]:
        return [{"batch": batch, "window": window} for batch in plan["batches"]]

    @task(retries=2, retry_delay=timedelta(minutes=5), max_active_tis_per_dag=int(os.getenv("A_SHARE_MAX_ACTIVE_BATCHES", "4")))
    def ingest(batch: list[str], window: dict[str, str]) -> dict[str, object]:
        context = get_current_context()
        idempotency_key = f"{context['dag'].dag_id}:{context['run_id']}:{context['task_instance'].map_index}"
        job_id = _submit_a_share_ingestion({
            "symbols": batch,
            "start_date": f"{window['start_date']}T00:00:00+00:00",
            "end_date": f"{window['end_date']}T00:00:00+00:00",
            "adjustment_type": window["adjustment_type"],
            "idempotency_key": idempotency_key,
        })
        result = _wait_for_a_share_ingestion(job_id)
        if int(result.get("daily_bar_count", 0)) == 0:
            print(f"WARNING: no historical daily bars returned for batch of {len(batch)} symbols")
        return result

    @task
    def normalize(result: dict[str, object]) -> dict[str, int]:
        return _wait_for_finance_normalization(_submit_finance_normalization({"object_paths": [str(result["object_path"])]}))  # type: ignore[return-value]

    @task
    def quality_check(plan: dict[str, object], window: dict[str, str], ingest_results: list[dict[str, object]], silver_stats: list[dict[str, int]]) -> dict[str, int]:
        failed_symbols = [symbol for result in ingest_results for symbol in result.get("failed_symbols", [])]
        if failed_symbols:
            raise ValueError(f"A-share ingestion failed for {len(failed_symbols)} symbols; coverage will not be recorded")
        expected = len(plan["batches"])
        if len(ingest_results) != expected or len(silver_stats) != expected:
            raise ValueError("A-share batch results are incomplete; coverage will not be recorded")
        if any(int(item.get("processed_objects", 0)) != 1 or int(item.get("failed_count", 0)) for item in silver_stats):
            raise ValueError("A-share normalization did not process every Bronze object")
        summary = {"symbol_count": sum(int(item.get("symbol_count", 0)) for item in ingest_results), "daily_bar_count": sum(int(item.get("daily_bar_count", 0)) for item in ingest_results), "processed_batches": len(ingest_results), "failed_batches": 0, "written_daily_bars": sum(int(item.get("written_daily_bars", 0)) for item in silver_stats), "filtered_count": sum(int(item.get("filtered_count", 0)) for item in silver_stats), "duplicate_count": sum(int(item.get("duplicate_count", 0)) for item in silver_stats)}
        if not summary["processed_batches"] or not summary["daily_bar_count"] or not (summary["written_daily_bars"] + summary["duplicate_count"]):
            raise ValueError("No valid A-share daily bars were written; coverage will not be recorded")
        context = get_current_context()
        _post_internal_job("/internal/jobs/a-share-universe-coverage", {"provider": "baostock", "adjustment_type": window["adjustment_type"], "universe_as_of_date": plan["universe_as_of_date"], "universe_symbol_count": plan["symbol_count"], "processed_batches": summary["processed_batches"], "written_daily_bars": summary["written_daily_bars"], "dag_id": context["dag"].dag_id, "dag_run_id": context["run_id"]})
        return summary

    window = build_window()
    plan = build_batches(window)
    results = ingest.expand_kwargs(build_ingest_jobs(plan, window))
    silver_stats = normalize.expand(result=results)
    quality_check(plan, window, results, silver_stats)


a_share_backfill_pipeline()
