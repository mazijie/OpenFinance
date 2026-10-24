"""Bronze object storage writer."""

from __future__ import annotations

import io
import json
import hashlib
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

try:  # pragma: no cover - exercised in real environment
    from minio import Minio
    from minio.error import MinioException, S3Error
except ImportError:  # pragma: no cover - test fallback
    Minio = Any  # type: ignore[assignment]
    MinioException = Exception
    S3Error = Exception

from backend.config import settings
from backend.logging_utils import get_logger
from backend.schemas.finance import FinanceBronzeObject
from backend.schemas.annual_report import AnnualReportAnnouncement, AnnualReportBronzeObject
from backend.schemas.news import BronzeNewsObject

logger = get_logger(__name__)


class BronzeWriterError(Exception):
    """Raised when Bronze persistence fails."""


class BronzeWriter:
    """Persist raw ingestion JSON files into MinIO Bronze."""

    def __init__(self, client: Minio | None = None) -> None:
        if client is not None:
            self.client = client
        else:
            if Minio is Any:
                raise BronzeWriterError("minio dependency is not installed")
            self.client = Minio(
                settings.MINIO_ENDPOINT,
                access_key=settings.MINIO_ACCESS_KEY,
                secret_key=settings.MINIO_SECRET_KEY,
                secure=settings.MINIO_SECURE,
                region=settings.MINIO_REGION,
            )
        self.bucket_name = settings.MINIO_BRONZE_BUCKET

    def ensure_bucket(self) -> None:
        """Create Bronze bucket when missing."""

        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
        except (S3Error, MinioException) as exc:
            raise BronzeWriterError(f"Failed to ensure bronze bucket: {exc}") from exc

    def write_news_raw(
        self,
        *,
        provider: str,
        query: str,
        raw_response: dict,
        start_time: datetime | None,
        end_time: datetime | None,
        limit: int,
        ingested_at: datetime | None = None,
    ) -> str:
        """Write one raw news response file and return object path."""

        ingested_at = (ingested_at or datetime.now(UTC)).astimezone(UTC)
        bronze_object = BronzeNewsObject(
            provider=provider,
            ingested_at=ingested_at,
            query=query,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            raw_response=raw_response,
        )
        return self._write_object(
            domain="news",
            provider=provider,
            ingested_at=ingested_at,
            payload=bronze_object.model_dump(),
            event="bronze_news_written",
        )

    def write_finance_raw(
        self,
        *,
        provider: str,
        raw_response: dict,
        object_key: str | None = None,
        request_params: dict[str, Any] | None = None,
        ingested_at: datetime | None = None,
        source_record_id: str | None = None,
        source_version: str | None = None,
        payload_hash: str | None = None,
    ) -> str:
        """Write one raw finance response file and return object path."""

        ingested_at = (ingested_at or datetime.now(UTC)).astimezone(UTC)
        bronze_object = FinanceBronzeObject(
            provider=provider,
            ingested_at=ingested_at,
            source_record_id=source_record_id,
            source_version=source_version,
            payload_hash=payload_hash,
            request_params=request_params or {},
            raw_response=raw_response,
        )
        return self._write_object(
            domain="finance",
            provider=provider,
            ingested_at=ingested_at,
            payload=bronze_object.model_dump(),
            object_key=object_key,
            event="bronze_finance_written",
        )

    def read_json(self, object_path: str) -> dict:
        """Read one Bronze JSON object."""

        try:
            response = self.client.get_object(self.bucket_name, object_path)
            try:
                return json.loads(response.read())
            finally:
                response.close()
                response.release_conn()
        except (S3Error, MinioException, json.JSONDecodeError) as exc:
            raise BronzeWriterError(f"Failed to read bronze object: {exc}") from exc

    def read_bytes(self, object_path: str) -> bytes:
        """Read one binary Bronze object."""

        try:
            response = self.client.get_object(self.bucket_name, object_path)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()
        except (S3Error, MinioException) as exc:
            raise BronzeWriterError(f"Failed to read bronze binary object: {exc}") from exc

    def presigned_get_url(self, object_path: str, expires_seconds: int = 900) -> str:
        """Return a short-lived URL for user-facing binary access."""

        from datetime import timedelta

        try:
            return self.client.presigned_get_object(self.bucket_name, object_path, expires=timedelta(seconds=expires_seconds))
        except (S3Error, MinioException) as exc:
            raise BronzeWriterError(f"Failed to create presigned Bronze URL: {exc}") from exc

    def write_annual_report(
        self,
        *,
        announcement: AnnualReportAnnouncement,
        pdf_bytes: bytes,
        query_params: dict[str, Any] | None = None,
        ingested_at: datetime | None = None,
    ) -> str:
        """Write a deterministic PDF and JSON manifest for one annual report."""

        ingested_at = (ingested_at or datetime.now(UTC)).astimezone(UTC)
        pdf_sha256 = hashlib.sha256(pdf_bytes).hexdigest()
        prefix = (
            f"reports/provider=cninfo/stock={announcement.stock_code}/"
            f"report_year={announcement.report_year}/announcement_id={announcement.announcement_id}"
        )
        pdf_object_path = f"{prefix}/report-{pdf_sha256[:16]}.pdf"
        manifest_path = f"{prefix}/metadata.json"
        self.ensure_bucket()
        try:
            self.client.put_object(
                self.bucket_name,
                pdf_object_path,
                io.BytesIO(pdf_bytes),
                length=len(pdf_bytes),
                content_type="application/pdf",
            )
            manifest = AnnualReportBronzeObject(
                ingested_at=ingested_at,
                announcement=announcement,
                query_params=query_params or {},
                pdf_sha256=pdf_sha256,
                pdf_size=len(pdf_bytes),
                pdf_object_path=pdf_object_path,
                metadata_object_path=manifest_path,
            )
            payload_bytes = json.dumps(manifest.model_dump(), ensure_ascii=False, default=str).encode("utf-8")
            self.client.put_object(
                self.bucket_name,
                manifest_path,
                io.BytesIO(payload_bytes),
                length=len(payload_bytes),
                content_type="application/json",
            )
        except (S3Error, MinioException) as exc:
            raise BronzeWriterError(f"Failed to write annual report Bronze objects: {exc}") from exc
        logger.info("Annual report PDF written to Bronze", extra_fields={"pdf_object_path": pdf_object_path, "manifest_path": manifest_path})
        return manifest_path

    def list_objects(self, prefix: str = "news/provider=juhe/") -> list[str]:
        """List Bronze object paths."""

        try:
            return [obj.object_name for obj in self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)]
        except (S3Error, MinioException) as exc:
            raise BronzeWriterError(f"Failed to list bronze objects: {exc}") from exc

    def all_external_ids(self, prefix: str = "news/provider=juhe/") -> set[str]:
        """Return provider ids already captured in Bronze, without API calls."""

        external_ids: set[str] = set()
        for object_path in self.list_objects(prefix=prefix):
            try:
                payload = self.read_json(object_path)
                raw_response = payload.get("raw_response", {})
                for article in raw_response.get("articles", []):
                    if isinstance(article, dict):
                        external_id = str(article.get("uniquekey") or article.get("external_news_id") or "").strip()
                        if external_id:
                            external_ids.add(external_id)
            except BronzeWriterError as exc:
                logger.warning(
                    "读取 Bronze 去重索引失败",
                    event="bronze_external_id_scan_failed",
                    extra_fields={"object_path": object_path, "error": str(exc)},
                )
        return external_ids

    def _write_object(
        self,
        *,
        domain: str,
        provider: str,
        ingested_at: datetime,
        payload: dict[str, Any],
        object_key: str | None = None,
        event: str,
    ) -> str:
        self.ensure_bucket()
        if object_key:
            object_name = hashlib.sha256(object_key.encode("utf-8")).hexdigest()
            object_path = f"{domain}/provider={provider}/requests/{object_name}.json"
        else:
            object_path = f"{domain}/provider={provider}/ingest_date={ingested_at:%Y-%m-%d}/hour={ingested_at:%H}/{uuid4()}.json"
        payload_bytes = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        buffer = io.BytesIO(payload_bytes)

        try:
            self.client.put_object(
                self.bucket_name,
                object_path,
                buffer,
                length=len(payload_bytes),
                content_type="application/json",
            )
        except Exception as exc:
            raise BronzeWriterError(f"Failed to write bronze object: {exc}") from exc

        logger.info(
            "原始数据已写入 Bronze",
            event=event,
            extra_fields={"bucket": self.bucket_name, "object_path": object_path},
        )
        return object_path
