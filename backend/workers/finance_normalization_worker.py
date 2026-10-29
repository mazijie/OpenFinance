"""Single-writer worker for finance normalization jobs."""

from __future__ import annotations

from threading import Event, Thread
import time

from backend.lakehouse.finance_normalization_jobs import FinanceNormalizationJobStore
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
    try:
        with _heartbeat(store, job_id):
            result = FinanceBronzeToSilverPipeline().process_objects(
                list(job["payload"].get("object_paths", []))
            )
        store.complete(job_id, result.to_dict())
    except Exception as exc:
        store.fail(job_id, str(exc))
        logger.exception(
            "Finance normalization job failed",
            extra_fields={"job_id": job_id, "error": str(exc)},
        )


class _heartbeat:
    def __init__(self, store: FinanceNormalizationJobStore, job_id: str, interval: int = 30) -> None:
        self.store = store
        self.job_id = job_id
        self.interval = interval
        self.stop = Event()
        self.thread = Thread(target=self._run, daemon=True)

    def __enter__(self) -> None:
        self.thread.start()

    def __exit__(self, *_: object) -> None:
        self.stop.set()
        self.thread.join(timeout=2)

    def _run(self) -> None:
        while not self.stop.wait(self.interval):
            try:
                self.store.heartbeat(self.job_id)
            except Exception:
                logger.exception("Normalization heartbeat failed", extra_fields={"job_id": self.job_id})


if __name__ == "__main__":
    run_worker()
