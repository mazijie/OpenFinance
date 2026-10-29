"""Cross-process coordination and retry helpers for Iceberg commits."""

from __future__ import annotations

from contextlib import contextmanager
import random
import threading
import time
from typing import Any, Callable, Iterator, TypeVar

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None

try:  # pragma: no cover - exercised in the container runtime
    from pyiceberg.exceptions import CommitFailedException
except ImportError:  # pragma: no cover - test fallback
    class CommitFailedException(Exception):
        """Fallback exception used when PyIceberg is not installed."""

from backend.config import settings
from backend.logging_utils import get_logger

logger = get_logger(__name__)
T = TypeVar("T")
_fallback_lock = threading.Lock()


@contextmanager
def iceberg_write_lock() -> Iterator[None]:
    """Hold one project-wide PostgreSQL advisory lock for an Iceberg commit."""

    if psycopg is None or not settings.DATABASE_URL:
        acquired = _fallback_lock.acquire(timeout=settings.ICEBERG_WRITE_LOCK_TIMEOUT_SECONDS)
        if not acquired:
            raise TimeoutError("Timed out waiting for local Iceberg write lock")
        try:
            yield
        finally:
            _fallback_lock.release()
        return

    conn = psycopg.connect(settings.DATABASE_URL, autocommit=True)
    acquired = False
    started = time.monotonic()
    try:
        with conn.cursor() as cur:
            deadline = started + settings.ICEBERG_WRITE_LOCK_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                cur.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", ("openfinance_iceberg_write",))
                acquired = bool(cur.fetchone()[0])
                if acquired:
                    break
                time.sleep(min(1.0, max(0.05, deadline - time.monotonic())))
        if not acquired:
            raise TimeoutError("Timed out waiting for PostgreSQL Iceberg write lock")
        logger.info("Iceberg write lock acquired", event="iceberg_write_lock_acquired", extra_fields={"wait_seconds": round(time.monotonic() - started, 3)})
        yield
    finally:
        if acquired:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", ("openfinance_iceberg_write",))
            logger.info("Iceberg write lock released", event="iceberg_write_lock_released")
        conn.close()


def append_with_retry(
    table_loader: Callable[[], Any],
    arrow_table: Any,
    *,
    table_name: str,
) -> T:
    """Serialize an append and reload the table after optimistic conflicts."""

    last_error: Exception | None = None
    max_attempts = max(1, settings.ICEBERG_WRITE_MAX_RETRIES + 1)
    for attempt in range(1, max_attempts + 1):
        try:
            with iceberg_write_lock():
                result = table_loader().append(arrow_table)
            logger.info("Iceberg append completed", event="iceberg_append_completed", extra_fields={"table": table_name, "attempt": attempt})
            return result
        except CommitFailedException as exc:
            last_error = exc
            if attempt >= max_attempts:
                logger.exception("Iceberg append retries exhausted", extra_fields={"table": table_name, "attempt": attempt})
                raise
            delay = min(settings.ICEBERG_WRITE_MAX_BACKOFF_SECONDS, settings.ICEBERG_WRITE_INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1)))
            delay = delay * (0.75 + random.random() * 0.5)
            logger.warning("Iceberg commit conflict; retrying", event="iceberg_append_retry", extra_fields={"table": table_name, "attempt": attempt, "max_attempts": max_attempts, "backoff_seconds": round(delay, 3), "error": str(exc)})
            time.sleep(delay)
            table_loader()
    raise last_error or RuntimeError("Iceberg append failed")


def upsert_with_retry(
    table_loader: Callable[[], Any],
    arrow_table: Any,
    *,
    table_name: str,
    join_cols: list[str],
) -> T:
    """Serialize an upsert and reload the table after optimistic conflicts."""

    last_error: Exception | None = None
    max_attempts = max(1, settings.ICEBERG_WRITE_MAX_RETRIES + 1)
    for attempt in range(1, max_attempts + 1):
        try:
            with iceberg_write_lock():
                result = table_loader().upsert(
                    arrow_table,
                    join_cols=join_cols,
                    snapshot_properties={"openfinance.write-mode": "upsert"},
                )
            logger.info(
                "Iceberg upsert completed",
                event="iceberg_upsert_completed",
                extra_fields={
                    "table": table_name,
                    "attempt": attempt,
                    "rows_inserted": result.rows_inserted,
                    "rows_updated": result.rows_updated,
                },
            )
            return result
        except CommitFailedException as exc:
            last_error = exc
            if attempt >= max_attempts:
                logger.exception(
                    "Iceberg upsert retries exhausted",
                    extra_fields={"table": table_name, "attempt": attempt},
                )
                raise
            delay = min(
                settings.ICEBERG_WRITE_MAX_BACKOFF_SECONDS,
                settings.ICEBERG_WRITE_INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1)),
            )
            delay = delay * (0.75 + random.random() * 0.5)
            logger.warning(
                "Iceberg commit conflict during upsert; retrying",
                event="iceberg_upsert_retry",
                extra_fields={
                    "table": table_name,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "backoff_seconds": round(delay, 3),
                    "error": str(exc),
                },
            )
            time.sleep(delay)
            table_loader()
    raise last_error or RuntimeError("Iceberg upsert failed")


def coordinated_write_with_retry(operation: Callable[[], T], *, table_name: str) -> T:
    """Retry an entire read/modify/write operation under the shared lock."""
    max_attempts = max(1, settings.ICEBERG_WRITE_MAX_RETRIES + 1)
    for attempt in range(1, max_attempts + 1):
        try:
            with iceberg_write_lock():
                return operation()
        except CommitFailedException:
            if attempt == max_attempts:
                raise
            delay = min(settings.ICEBERG_WRITE_MAX_BACKOFF_SECONDS, settings.ICEBERG_WRITE_INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1)))
            time.sleep(delay * (0.75 + random.random() * 0.5))
    raise RuntimeError(f"Iceberg write failed: {table_name}")
