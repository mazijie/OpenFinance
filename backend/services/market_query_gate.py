"""Limit concurrent Iceberg history scans within this API worker."""
from contextlib import contextmanager
from threading import Condition


class MarketQueryGate:
    def __init__(self):
        self._condition = Condition()
        self._active = False
        self._waiting_pages = 0

    @contextmanager
    def acquire(self, *, page: bool):
        with self._condition:
            if page:
                self._waiting_pages += 1
            try:
                ready = self._condition.wait_for(
                    lambda: not self._active and (page or not self._waiting_pages), timeout=30 if page else 45
                )
                if not ready:
                    raise TimeoutError("Market history is busy; please retry")
                self._active = True
            finally:
                if page:
                    self._waiting_pages -= 1
                    self._condition.notify_all()
        try:
            yield
        finally:
            with self._condition:
                self._active = False
                self._condition.notify_all()


market_query_gate = MarketQueryGate()
