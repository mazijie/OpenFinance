"""Refresh the business-facing market cross section after market close."""
from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(dag_id='openfinance_market_cross_section',
     schedule=CronDataIntervalTimetable('30 20 * * 1-5', timezone='Asia/Shanghai'),
     start_date=pendulum.datetime(2026, 10, 1, tz='Asia/Shanghai'), catchup=False,
     max_active_runs=1, default_args={'owner': 'openfinance', 'retries': 1, 'retry_delay': timedelta(minutes=10)},
     tags=['openfinance', 'spark', 'gold'])
def market_cross_section():
    BashOperator(task_id='build_cross_section', bash_command=(
        'spark-submit --name openfinance-market-cross-section --master spark://spark-master:7077 --deploy-mode client '
        '--conf spark.cores.max=36 --conf spark.executor.memory=128g --conf spark.sql.shuffle.partitions=96 '
        '--jars /opt/airflow/extra-jars/iceberg-spark-runtime-4.1_2.13-1.12.0.jar,'
        '/opt/airflow/extra-jars/iceberg-aws-bundle-1.12.0.jar,'
        '/opt/airflow/extra-jars/postgresql-42.7.14.jar '
        '--properties-file /opt/openfinance/spark/conf/iceberg.properties '
        '/opt/openfinance/spark/jobs/build_market_cross_section.py'))


market_cross_section()
