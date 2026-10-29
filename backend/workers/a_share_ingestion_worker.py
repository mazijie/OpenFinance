"""Worker process for durable A-share ingestion jobs."""

from __future__ import annotations

import time
from threading import Event, Thread
from datetime import datetime

from backend.ingestion.jobs.ingest_a_share_daily_bars import ingest_a_share_daily_bars
from backend.lakehouse.a_share_ingestion_jobs import AShareIngestionJobStore
from backend.logging_utils import configure_logging, get_logger

logger = get_logger(__name__)


def run_worker() -> None:
    configure_logging()
    store = AShareIngestionJobStore()
    store.ensure_schema()
    while True:
        try:
            ingestion_job = store.claim()
            if ingestion_job is not None:
                _run_ingestion_job(store, ingestion_job)
                continue
            time.sleep(2)
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
