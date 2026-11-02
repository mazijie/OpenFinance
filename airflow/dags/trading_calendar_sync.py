"""Refresh the BaoStock exchange calendar once per China-local day."""
import os
import pendulum
from airflow.decorators import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from a_share_pipeline_helpers import _post_internal_job


@dag(dag_id="openfinance_trading_calendar_sync",
     schedule=CronDataIntervalTimetable(os.getenv("TRADING_CALENDAR_SYNC_SCHEDULE", "0 6 * * *"), timezone="Asia/Shanghai"),
     start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"), catchup=False,
     max_active_runs=1, default_args={"owner": "openfinance", "retries": 0},
     tags=["openfinance", "market", "calendar"])
def trading_calendar_sync():
    @task
    def sync():
        return _post_internal_job("/internal/jobs/trading-calendar-sync", {})
    sync()


trading_calendar_sync()
