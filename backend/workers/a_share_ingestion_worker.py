"""Worker process for durable ingestion, normalization, and monitor jobs."""

from __future__ import annotations

import time
from threading import Event, Thread
from datetime import datetime

from backend.ingestion.jobs.ingest_a_share_daily_bars import ingest_a_share_daily_bars
from backend.lakehouse.a_share_ingestion_jobs import AShareIngestionJobStore
from backend.lakehouse.finance_normalization_jobs import FinanceNormalizationJobStore
from backend.pipelines.finance_bronze_to_silver import FinanceBronzeToSilverPipeline
from backend.logging_utils import configure_logging, get_logger
from backend.monitoring.store import MonitorStore
from backend.services.investment_monitor_service import InvestmentMonitorService

logger = get_logger(__name__)


def run_worker() -> None:
    configure_logging()
    store = AShareIngestionJobStore()
    normalization_store = FinanceNormalizationJobStore()
    monitor_store = MonitorStore()
    store.ensure_schema()
    normalization_store.ensure_schema()
    monitor_store.ensure_schema()
    while True:
        try:
            ingestion_job = store.claim()
            if ingestion_job is not None:
                _run_ingestion_job(store, ingestion_job)
                continue
            normalization_job = normalization_store.claim()
            if normalization_job is not None:
                _run_normalization_job(normalization_store, normalization_job)
                continue
            monitor_run = monitor_store.claim_run()
            if monitor_run is None:
                time.sleep(2)
                continue
            _run_monitor_job(monitor_store, monitor_run)
        except Exception:
            logger.exception("Worker loop failed; retrying")
            time.sleep(5)


def _run_ingestion_job(store: AShareIngestionJobStore, job: dict) -> None:
    job_id = job["job_id"]
    try:
        payload = dict(job["payload"])
        for field in ("start_date", "end_date"):
            value = payload[field]
            payload[field] = datetime.fromisoformat(value).date() if isinstance(value, str) else value
        with _heartbeat(store, job_id):
            result = ingest_a_share_daily_bars(**payload, idempotency_key=job_id)
        store.complete(job_id, result.to_dict())
    except Exception as exc:
        store.fail(job_id, str(exc))
        logger.exception("A-share ingestion job failed", extra_fields={"job_id": job_id, "error": str(exc)})


def _run_normalization_job(store: FinanceNormalizationJobStore, job: dict) -> None:
    try:
        aggregate = {"read_count": 0, "filtered_count": 0, "duplicate_count": 0, "failed_count": 0, "written_security_master": 0, "written_trading_calendar": 0, "written_daily_bars": 0, "processed_objects": 0}
        with _heartbeat(store, job["job_id"]):
            for object_path in job["payload"].get("object_paths", []):
                for key, value in FinanceBronzeToSilverPipeline().process_object(object_path).to_dict().items():
                    aggregate[key] += int(value)
        store.complete(job["job_id"], aggregate)
    except Exception as exc:
        store.fail(job["job_id"], str(exc))
        logger.exception("Finance normalization job failed", extra_fields={"job_id": job["job_id"], "error": str(exc)})


def _run_monitor_job(store: MonitorStore, run) -> None:
    try:
        with _heartbeat(store, run.run_id, heartbeat_method="heartbeat_run"):
            InvestmentMonitorService(store=store).execute(run)
    except Exception as exc:
        store.finish_run(run.run_id, status="failed", stats={}, error=str(exc))
        logger.exception(
            "Investment monitor job failed",
            extra_fields={"run_id": run.run_id, "error": str(exc)},
        )


class _heartbeat:
    def __init__(self, store, job_id: str, interval: int = 30, heartbeat_method: str = "heartbeat"):
        self.store, self.job_id, self.interval = store, job_id, interval
        self.heartbeat_method = heartbeat_method
        self.stop = Event()
        self.thread = Thread(target=self._run, daemon=True)
    def __enter__(self):
        self.thread.start()
    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=2)
    def _run(self):
        while not self.stop.wait(self.interval):
            try: getattr(self.store, self.heartbeat_method)(self.job_id)
            except Exception: logger.exception("Job heartbeat failed", extra_fields={"job_id": self.job_id})


if __name__ == "__main__":
    run_worker()
