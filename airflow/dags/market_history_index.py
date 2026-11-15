"""Coalesce snapshot changes and refresh the market history index."""
from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator


@dag(dag_id="openfinance_market_history_index", schedule="*/15 * * * *",
     start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False,
     is_paused_upon_creation=True, max_active_runs=1, default_args={"owner": "openfinance", "retries": 1,
                                     "retry_delay": timedelta(minutes=5)},
     tags=["openfinance", "spark", "market-data"])
def market_history_index():
    BashOperator(task_id="refresh_index", bash_command=(
        "spark-submit --name openfinance-market-history-index "
        "--master spark://spark-master:7077 --deploy-mode client "
        "--jars /opt/airflow/extra-jars/iceberg-spark-runtime-4.1_2.13-1.12.0.jar,"
        "/opt/airflow/extra-jars/iceberg-aws-bundle-1.12.0.jar,"
        "/opt/airflow/extra-jars/postgresql-42.7.14.jar "
        "--properties-file /opt/openfinance/spark/conf/iceberg.properties "
        "/opt/openfinance/spark/jobs/build_market_history_index.py"
    ))


market_history_index()
