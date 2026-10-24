"""Iceberg catalog bootstrap helpers."""

from __future__ import annotations

from typing import Any

try:  # pragma: no cover - exercised in real environment
    from pyiceberg.catalog import Catalog, load_catalog
except ImportError:  # pragma: no cover - test fallback
    Catalog = Any  # type: ignore[assignment]
    load_catalog = None

from backend.config import settings


def get_catalog() -> Catalog:
    """Load Iceberg SQL catalog backed by PostgreSQL."""

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
