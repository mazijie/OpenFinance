"""Iceberg catalog bootstrap helpers."""

from __future__ import annotations

from typing import Any
import atexit
import os
import threading

try:  # pragma: no cover - exercised in real environment
    from pyiceberg.catalog import Catalog, load_catalog
except ImportError:  # pragma: no cover - test fallback
    Catalog = Any  # type: ignore[assignment]
    load_catalog = None

from backend.config import settings


_catalog = None
_lock = threading.Lock()


def _after_fork() -> None:
    global _catalog, _lock
    # Inherited database connections must never be reused by a child process.
    if _catalog is not None and getattr(_catalog, "engine", None) is not None:
        _catalog.engine.dispose(close=False)
    _catalog = None
    _lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


def close_catalog() -> None:
    global _catalog
    with _lock:
        catalog, _catalog = _catalog, None
    engine = getattr(catalog, "engine", None)
    if engine is not None:
        engine.dispose()


atexit.register(close_catalog)


def get_catalog() -> Catalog:
    """Return one SQL catalog and connection pool per process."""
    global _catalog
    with _lock:
        if _catalog is None:
            _catalog = _load_catalog()
        return _catalog


def _load_catalog() -> Catalog:

    if load_catalog is None:
        raise RuntimeError("pyiceberg dependency is not installed")

    properties = {
        "type": "sql",
        "uri": settings.ICEBERG_CATALOG_URI,
        "warehouse": settings.ICEBERG_WAREHOUSE,
        "s3.endpoint": f"{settings.minio_http_scheme}://{settings.MINIO_ENDPOINT}",
        "s3.access-key-id": settings.MINIO_ACCESS_KEY,
        "s3.secret-access-key": settings.MINIO_SECRET_KEY,
        "s3.region": settings.MINIO_REGION or "us-east-1",
        "s3.path-style-access": str(settings.ICEBERG_S3_PATH_STYLE_ACCESS).lower(),
    }
    return load_catalog(settings.ICEBERG_CATALOG_NAME, **properties)
