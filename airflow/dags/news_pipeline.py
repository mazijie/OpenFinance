"""Airflow DAG for scheduled finance news ingestion and lakehouse normalization."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task


def _post_internal_job(path: str, payload: dict[str, object]) -> dict[str, object]:
    base_url = os.getenv("OPENFINANCE_INTERNAL_API_BASE_URL", "http://backend:8000").rstrip("/")
    token = os.getenv("OPENFINANCE_INTERNAL_API_TOKEN", "").strip()
    if not token:
        raise ValueError("OPENFINANCE_INTERNAL_API_TOKEN is not configured")

    request = Request(
        f"{base_url}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-OpenFinance-Token": token,
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=120) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:  # pragma: no cover - exercised in Airflow runtime
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Internal job request failed: {exc.code} {detail}") from exc
    except URLError as exc:  # pragma: no cover - exercised in Airflow runtime
        raise RuntimeError(f"Internal job request failed: {exc.reason}") from exc


@dag(
    dag_id="openfinance_news_pipeline",
    description="Collect Juhe finance news into Bronze and normalize it into Iceberg Silver.",
    schedule=os.getenv("NEWS_AIRFLOW_SCHEDULE", "*/30 * * * *"),
    start_date=pendulum.datetime(2026, 7, 14, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "openfinance",
        # A retry repeats quota-billed provider requests. Operators can rerun
        # deliberately after fixing infrastructure, but the DAG must fail closed.
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["openfinance", "finance-news", "lakehouse"],
)
def news_pipeline():
    """Define the scheduled finance news pipeline DAG."""

    @task
    def build_window() -> dict[str, str]:
        resolved_end = datetime.now(UTC)
        lookback_hours = int(os.getenv("NEWS_INGEST_LOOKBACK_HOURS", "6"))
        resolved_start = resolved_end - timedelta(hours=lookback_hours)
        return {
            "start_time": resolved_start.isoformat(),
            "end_time": resolved_end.isoformat(),
        }

    @task
    def ingest_finance_news(window: dict[str, str]) -> dict[str, object]:
        return _post_internal_job(
            "/internal/jobs/ingest-finance-news",
            {
                "start_time": window["start_time"],
                "end_time": window["end_time"],
                "limit": int(os.getenv("NEWS_INGEST_LIMIT", "20")),
            },
        )

    @task.short_circuit
    def bronze_object_path(ingest_result: dict[str, object]) -> str | None:
        return str(ingest_result.get("object_path") or "").strip() or None

    @task(retries=0)
    def bronze_to_silver(object_path: str) -> dict[str, int]:
        return _post_internal_job(
            "/internal/jobs/news-bronze-to-silver",
            {"object_paths": [object_path]},
        )

    @task
    def quality_check(stats: dict[str, int]) -> dict[str, int]:
        if int(stats.get("processed_objects", 0)) == 0:
            raise ValueError("No Bronze objects were processed in this DAG run.")
        return stats

    @task(retries=0)
    def rag_index(_: dict[str, int]) -> dict[str, object]:
        return _post_internal_job(
            "/internal/jobs/news-rag-index",
            {"rebuild_all": False},
        )

    @task
    def rag_quality_check(stats: dict[str, object]) -> dict[str, object]:
        if int(stats.get("scanned_articles", 0)) == 0:
            return stats
        return stats

    window = build_window()
    ingest_result = ingest_finance_news(window)
    object_path = bronze_object_path(ingest_result)
    silver_stats = quality_check(bronze_to_silver(object_path))
    rag_stats = rag_index(silver_stats)
    rag_quality_check(rag_stats)


news_pipeline()
