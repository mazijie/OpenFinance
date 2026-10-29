"""Dedicated worker for durable investment-monitor runs."""

from __future__ import annotations

import time
from threading import Event, Thread

from backend.logging_utils import configure_logging, get_logger
from backend.monitoring.store import MonitorStore
from backend.services.investment_monitor_service import InvestmentMonitorService

logger = get_logger(__name__)


def run_monitor_worker() -> None:
    configure_logging()
    store = MonitorStore()
    store.ensure_schema()
    while True:
        try:
            run = store.claim_run()
            if run is None:
                time.sleep(2)
                continue
            try:
                with _heartbeat(store, run.run_id):
                    InvestmentMonitorService(store=store).execute(run)
            except Exception as exc:
                store.finish_run(run.run_id, status="failed", stats={}, error=str(exc))
                logger.exception(
                    "Investment monitor worker job failed",
                    extra_fields={"run_id": run.run_id, "error": str(exc)},
                )
        except Exception:
            logger.exception("Monitor worker loop failed; retrying")
            time.sleep(5)


class _heartbeat:
    def __init__(self, store: MonitorStore, run_id: str, interval: int = 30) -> None:
        self.store = store
        self.run_id = run_id
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
                self.store.heartbeat_run(self.run_id)
            except Exception:
                logger.exception(
                    "Monitor worker heartbeat failed",
                    extra_fields={"run_id": self.run_id},
                )


if __name__ == "__main__":
    run_monitor_worker()

