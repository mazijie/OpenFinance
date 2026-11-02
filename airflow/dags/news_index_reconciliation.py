"""Reconcile existing Silver news only; this job never calls the news provider."""
import os
import pendulum
from airflow.decorators import dag, task
from a_share_pipeline_helpers import _post_internal_job


@dag(dag_id="openfinance_news_index_reconciliation",
     schedule=os.getenv("NEWS_INDEX_RECONCILE_SCHEDULE", "*/30 * * * *"),
     start_date=pendulum.datetime(2026, 7, 14, tz="UTC"), catchup=False,
     max_active_runs=1, default_args={"owner": "openfinance", "retries": 0},
     tags=["openfinance", "news", "reconciliation"])
def news_index_reconciliation():
    @task
    def reconcile():
        result = _post_internal_job("/internal/jobs/news-rag-index", {"rebuild_all": False})
        if result.get("errors") or result.get("embeddings_failed"):
            raise RuntimeError("News index reconciliation did not complete")
        return result
    reconcile()


news_index_reconciliation()
