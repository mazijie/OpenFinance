"""MinIO writer for parser-derived Document IR artifacts."""

from __future__ import annotations

import io
import json
from typing import Any

try:  # pragma: no cover - exercised in the runtime image
    from minio import Minio
    from minio.error import MinioException, S3Error
except ImportError:  # pragma: no cover
    Minio = Any  # type: ignore[assignment]
    MinioException = Exception
    S3Error = Exception

from backend.config import settings


class DerivedWriterError(Exception):
    """Raised when a derived artifact cannot be persisted."""


class DerivedObjectNotFound(DerivedWriterError):
    """The requested derived object does not exist yet."""


class DerivedWriter:
    """Persist JSON and binary parser artifacts outside the raw Bronze bucket."""

    def __init__(self, client: Minio | None = None) -> None:
        if client is not None:
            self.client = client
        else:
            if Minio is Any:
                raise DerivedWriterError("minio dependency is not installed")
            self.client = Minio(
                settings.MINIO_ENDPOINT,
                access_key=settings.MINIO_ACCESS_KEY,
                secret_key=settings.MINIO_SECRET_KEY,
                secure=settings.MINIO_SECURE,
                region=settings.MINIO_REGION,
            )
        self.bucket_name = settings.MINIO_DERIVED_BUCKET

    def ensure_bucket(self) -> None:
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
        except (S3Error, MinioException) as exc:
            raise DerivedWriterError(f"Failed to ensure derived bucket: {exc}") from exc

    def write_json(self, object_path: str, payload: dict[str, Any]) -> str:
        data = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        return self.write_bytes(object_path, data, content_type="application/json")

    def write_bytes(self, object_path: str, data: bytes, *, content_type: str) -> str:
        self.ensure_bucket()
        try:
            self.client.put_object(
                self.bucket_name,
                object_path,
                io.BytesIO(data),
                length=len(data),
                content_type=content_type,
            )
        except (S3Error, MinioException) as exc:
            raise DerivedWriterError(f"Failed to write derived object: {exc}") from exc
        return object_path

    def read_json(self, object_path: str) -> dict[str, Any]:
        try:
            response = self.client.get_object(self.bucket_name, object_path)
            try:
                return json.loads(response.read())
            finally:
                response.close()
                response.release_conn()
        except S3Error as exc:
            if getattr(exc, "code", None) in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                raise DerivedObjectNotFound(object_path) from exc
            raise DerivedWriterError(f"Failed to read derived object: {exc}") from exc
        except (MinioException, json.JSONDecodeError) as exc:
            raise DerivedWriterError(f"Failed to read derived object: {exc}") from exc

    def object_exists(self, object_path: str) -> bool:
        try:
            self.client.stat_object(self.bucket_name, object_path)
            return True
        except S3Error as exc:
            if getattr(exc, "code", None) in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                return False
            raise DerivedWriterError(f"Failed to stat derived object: {exc}") from exc
        except MinioException as exc:
            raise DerivedWriterError(f"Failed to stat derived object: {exc}") from exc

