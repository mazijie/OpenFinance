"""Single-writer worker for finance normalization jobs."""

from __future__ import annotations

from threading import Event, Thread
import time

from backend.job_store.finance_normalization_jobs import FinanceNormalizationJobStore
from backend.job_store.job_lease import JobLeaseLost
from backend.logging_utils import configure_logging, get_logger
from backend.pipelines.finance_bronze_to_silver import FinanceBronzeToSilverPipeline

logger = get_logger(__name__)


def run_worker() -> None:
    configure_logging()
    store = FinanceNormalizationJobStore()
    store.ensure_schema()
    while True:
        try:
            job = store.claim()
            if job is None:
                time.sleep(2)
                continue
            _run_normalization_job(store, job)
        except Exception:
            logger.exception("Normalization worker loop failed; retrying")
            time.sleep(5)


def _run_normalization_job(store: FinanceNormalizationJobStore, job: dict) -> None:
    job_id = job["job_id"]
    attempt = job["attempts"]
    try:
        with _heartbeat(store, job_id, attempt) as heartbeat:
            result = FinanceBronzeToSilverPipeline().process_objects(
                list(job["payload"].get("object_paths", [])), check_lease=heartbeat.check
            )
            heartbeat.check()
        store.complete(job_id, result.to_dict(), attempt)
    except JobLeaseLost:
        logger.warning("Normalization lease lost", extra_fields={"job_id": job_id, "attempt": attempt})
    except Exception as exc:
        try:
            store.fail(job_id, str(exc), attempt)
        except JobLeaseLost:
            logger.warning("Normalization failure ignored after lease loss", extra_fields={"job_id": job_id})
        logger.exception(
            "Finance normalization job failed",
            extra_fields={"job_id": job_id, "error": str(exc)},
        )


class _heartbeat:
    def __init__(self, store: FinanceNormalizationJobStore, job_id: str, attempt: int, interval: int = 30) -> None:
        self.store = store
        self.job_id = job_id
        self.attempt = attempt
        self.interval = interval
        self.stop = Event()
        self.lost = Event()
        self.thread = Thread(target=self._run, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop.set()
        self.thread.join(timeout=2)

    def _run(self) -> None:
        while not self.stop.wait(self.interval):
            try:
                self.store.heartbeat(self.job_id, self.attempt)
            except JobLeaseLost:
                self.lost.set()
                break
            except Exception:
                logger.exception("Normalization heartbeat failed", extra_fields={"job_id": self.job_id})

    def check(self):
        if self.lost.is_set(): raise JobLeaseLost(f"Job {self.job_id} lost its lease")
        self.store.heartbeat(self.job_id, self.attempt)


if __name__ == "__main__":
    run_worker()
