"""Optional weekly Iceberg maintenance for recent market-history partitions."""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context


_SPARK = (
    "spark-submit --master spark://spark-master:7077 --deploy-mode client "
    "--jars /opt/airflow/extra-jars/iceberg-spark-runtime-4.1_2.13-1.12.0.jar,"
    "/opt/airflow/extra-jars/iceberg-aws-bundle-1.12.0.jar,"
    "/opt/airflow/extra-jars/postgresql-42.7.14.jar "
    "--properties-file /opt/openfinance/spark/conf/iceberg.properties "
)


@dag(dag_id="openfinance_market_history_compaction", schedule="0 2 * * 0",
     start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"), catchup=False,
     is_paused_upon_creation=True, max_active_runs=1,
     default_args={"owner": "openfinance", "retries": 1, "retry_delay": timedelta(minutes=15)},
     tags=["openfinance", "spark", "market-data", "maintenance"])
def market_history_compaction():
    @task
    def date_window() -> dict[str, str]:
        now = get_current_context()["data_interval_end"].in_timezone("Asia/Shanghai")
        start = now.start_of("month").subtract(months=1)
        end = now.start_of("month").add(months=1)
        return {"start": start.to_date_string(), "end": end.to_date_string()}

    window = date_window()
    compact = BashOperator(task_id="compact_recent_partitions", bash_command=(
        _SPARK + "--name openfinance-market-history-compaction "
        "/opt/openfinance/spark/jobs/compact_market_history.py "
        "--start-date {{ ti.xcom_pull(task_ids='date_window')['start'] }} "
        "--end-date {{ ti.xcom_pull(task_ids='date_window')['end'] }}"
    ))
    refresh_index = BashOperator(task_id="refresh_index", bash_command=(
        _SPARK + "--name openfinance-market-history-index "
        "/opt/openfinance/spark/jobs/build_market_history_index.py"
    ))
    window >> compact >> refresh_index


market_history_compaction()
