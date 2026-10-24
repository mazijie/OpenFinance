"""Scheduled incremental Gold-metric refresh through the local Spark cluster."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable


def _trading_day(trading_date: str) -> dict[str, object]:
    base = os.getenv("OPENFINANCE_INTERNAL_API_BASE_URL", "http://backend:8000").rstrip("/")
    token = os.getenv("OPENFINANCE_INTERNAL_API_TOKEN", "").strip()
    request = Request(
        f"{base}/internal/jobs/investment-monitor/trading-day?{urlencode({'trading_date': trading_date})}",
        headers={"X-OpenFinance-Token": token},
    )
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


@dag(
    dag_id="openfinance_gold_metrics_pipeline",
    description="Recompute the latest Gold feature window and merge it into Iceberg.",
    schedule=CronDataIntervalTimetable("30 19 * * 1-5", timezone="Asia/Shanghai"),
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "openfinance", "retries": 0, "retry_delay": timedelta(minutes=10)},
    tags=["openfinance", "spark", "gold", "investment-monitor"],
)
def gold_metrics_pipeline():
    @task
    def ensure_trading_day() -> str:
        context = get_current_context()
        trading_date = context["data_interval_end"].in_timezone("Asia/Shanghai").date().isoformat()
        if _trading_day(trading_date).get("is_open") is False:
            raise AirflowSkipException(f"{trading_date} is not an exchange trading day")
        return trading_date

    trading_day = ensure_trading_day()
    refresh = BashOperator(
        task_id="refresh_gold_incremental",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--jars /opt/airflow/extra-jars/iceberg-spark-runtime-4.1_2.13-1.12.0.jar,"
            "/opt/airflow/extra-jars/iceberg-aws-bundle-1.12.0.jar,"
            "/opt/airflow/extra-jars/postgresql-42.7.14.jar "
            "--properties-file /opt/openfinance/spark/conf/iceberg.properties "
            "/opt/openfinance/spark/jobs/build_gold_security_daily_metrics.py "
            "--mode incremental "
            "--lookback-days {{ var.value.get('gold_metrics_lookback_days', '45') }} "
            "--end-date {{ ti.xcom_pull(task_ids='ensure_trading_day') }}"
        ),
    )
    trading_day >> refresh


gold_metrics_pipeline()
