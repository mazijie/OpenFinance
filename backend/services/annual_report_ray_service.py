"""Submit and inspect annual-report Ray Jobs."""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any

from backend.config import PROJECT_ROOT, settings


class AnnualReportRayError(RuntimeError):
    """Raised when the Ray Jobs API cannot submit or inspect a job."""


class AnnualReportRayService:
    _client_env_lock = threading.Lock()

    @staticmethod
    def _remote_storage_env() -> dict[str, str]:
        """Build worker storage settings without leaking Compose-local hosts.

        Ray workers run outside the backend container in the normal
        deployment. Falling back to ``DATABASE_URL`` or ``MINIO_ENDPOINT``
        here would silently send a job to localhost on the worker, so the
        network-facing values are required explicitly through ``RAY_*``.
        """

        required = {
            "RAY_DATABASE_URL": settings.RAY_DATABASE_URL.strip(),
            "RAY_MINIO_ENDPOINT": settings.RAY_MINIO_ENDPOINT.strip(),
            "RAY_ICEBERG_CATALOG_URI": settings.RAY_ICEBERG_CATALOG_URI.strip(),
            "RAY_ICEBERG_WAREHOUSE": settings.RAY_ICEBERG_WAREHOUSE.strip(),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise AnnualReportRayError(
                "Ray worker storage is not configured; set " + ", ".join(missing)
            )

        secure = settings.RAY_MINIO_SECURE
        if secure is None:
            secure = settings.MINIO_SECURE
        region = settings.RAY_MINIO_REGION
        if region is None:
            region = settings.MINIO_REGION
        return {
            "DATABASE_URL": required["RAY_DATABASE_URL"],
            "MINIO_ENDPOINT": required["RAY_MINIO_ENDPOINT"],
            "MINIO_ACCESS_KEY": settings.RAY_MINIO_ACCESS_KEY.strip() or settings.MINIO_ACCESS_KEY,
            "MINIO_SECRET_KEY": settings.RAY_MINIO_SECRET_KEY.strip() or settings.MINIO_SECRET_KEY,
            "MINIO_SECURE": str(bool(secure)).lower(),
            "MINIO_REGION": "" if region is None else str(region),
            "ICEBERG_CATALOG_URI": required["RAY_ICEBERG_CATALOG_URI"],
            "ICEBERG_WAREHOUSE": required["RAY_ICEBERG_WAREHOUSE"],
        }

    @staticmethod
    def _runtime_pip_packages() -> list[str]:
        raw = settings.RAY_RUNTIME_PIP_PACKAGES_JSON.strip() or "[]"
        try:
            packages = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise AnnualReportRayError("RAY_RUNTIME_PIP_PACKAGES_JSON must be valid JSON") from exc
        if not isinstance(packages, list) or not all(isinstance(item, str) and item.strip() for item in packages):
            raise AnnualReportRayError("RAY_RUNTIME_PIP_PACKAGES_JSON must be a JSON array of package strings")
        return [item.strip() for item in packages]

    def _client(self) -> Any:
        try:
            from ray.job_submission import JobSubmissionClient
        except ImportError as exc:  # pragma: no cover - depends on deployment image
            raise AnnualReportRayError("ray[default] is required in the backend image") from exc
        # Ray 2.59 lets the process-level RAY_ADDRESS override the explicit
        # client argument. The backend is not a Ray worker, so temporarily
        # remove that worker setting while constructing the Jobs client.
        with self._client_env_lock:
            previous = os.environ.pop("RAY_ADDRESS", None)
            try:
                return JobSubmissionClient(settings.RAY_DASHBOARD_URL)
            finally:
                if previous is not None:
                    os.environ["RAY_ADDRESS"] = previous

    def _runtime_env(self, manifest_paths: list[str], *, force: bool) -> dict[str, Any]:
        remote_storage = self._remote_storage_env()
        env_keys = (
            "MINIO_BRONZE_BUCKET",
            "MINIO_DERIVED_BUCKET",
            "MINIO_WAREHOUSE_BUCKET",
            "ICEBERG_CATALOG_NAME",
            "ICEBERG_NAMESPACE",
            "ICEBERG_ANNUAL_REPORT_TABLE",
            "ICEBERG_ANNUAL_REPORT_PAGE_TABLE",
            "ICEBERG_ANNUAL_REPORT_SECTION_TABLE",
            "ICEBERG_ANNUAL_REPORT_PARSE_RUN_TABLE",
            "ICEBERG_ANNUAL_REPORT_REGION_TABLE",
            "ICEBERG_ANNUAL_REPORT_CHUNK_TABLE",
            "ICEBERG_S3_PATH_STYLE_ACCESS",
            "PDF_PARSER_VERSION",
            "PDF_LAYOUT_ENGINE",
            "PDF_MINERU_TIER",
            "PDF_RENDER_DPI",
            "PDF_OCR_CONFIDENCE_THRESHOLD",
            "ANNUAL_REPORT_MAX_STAGE_ATTEMPTS",
            "RAY_ADDRESS",
            "RAY_CPU_TASK_CPUS",
            "RAY_GPU_TASK_CPUS",
            "RAY_MAX_IN_FLIGHT_REPORTS",
            "RAY_CPU_RESOURCE_LABEL",
            "RAY_GPU_RESOURCE_LABEL",
        )
        env_vars = {
            key: str(getattr(settings, key))
            for key in env_keys
            if getattr(settings, key, None) is not None
        }
        env_vars.update(remote_storage)
        if settings.RAY_RUNTIME_PIP_INDEX_URL.strip():
            env_vars["PIP_INDEX_URL"] = settings.RAY_RUNTIME_PIP_INDEX_URL.strip()
        if settings.RAY_RUNTIME_PIP_EXTRA_INDEX_URL.strip():
            env_vars["PIP_EXTRA_INDEX_URL"] = settings.RAY_RUNTIME_PIP_EXTRA_INDEX_URL.strip()
        env_vars["ANNUAL_REPORT_RAY_MANIFESTS"] = json.dumps(manifest_paths, ensure_ascii=False)
        env_vars["ANNUAL_REPORT_RAY_FORCE"] = "true" if force else "false"
        working_dir = settings.RAY_RUNTIME_WORKING_DIR.strip() or str(PROJECT_ROOT)
        runtime_env: dict[str, Any] = {"working_dir": working_dir, "env_vars": env_vars}
        packages = self._runtime_pip_packages()
        if packages:
            runtime_env["pip"] = {"packages": packages, "pip_check": False}
        return runtime_env

    def submit(self, manifest_paths: list[str], *, force: bool = False) -> dict[str, str]:
        if not manifest_paths:
            raise AnnualReportRayError("manifest_paths must not be empty")
        # Validate all worker-facing settings before creating a remote job. A
        # rejected request is easier to diagnose than a job that fails on every
        # node after reserving CPU/GPU resources.
        self._runtime_env(manifest_paths, force=force)
        client = self._client()
        try:
            job_id = client.submit_job(
                entrypoint="python -m backend.ray_annual_reports",
                runtime_env=self._runtime_env(manifest_paths, force=force),
                metadata={"application": "openfinance-annual-reports"},
            )
        except Exception as exc:
            raise AnnualReportRayError(f"Ray job submission failed: {exc}") from exc
        return {"job_id": str(job_id), "status": "submitted"}

    def status(self, job_id: str) -> dict[str, str]:
        client = self._client()
        try:
            result = client.get_job_status(job_id)
        except Exception as exc:
            raise AnnualReportRayError(f"Ray job status lookup failed: {exc}") from exc
        value = getattr(result, "value", result)
        return {"job_id": job_id, "status": str(value).lower()}

    def stop(self, job_id: str) -> dict[str, str]:
        client = self._client()
        try:
            client.stop_job(job_id)
        except Exception as exc:
            raise AnnualReportRayError(f"Ray job cancellation failed: {exc}") from exc
        return {"job_id": job_id, "status": "stopped"}

    def submit_and_wait(self, manifest_paths: list[str], *, force: bool = False) -> dict[str, str]:
        """Submit a job and wait for its terminal status for recovery callers."""

        submitted = self.submit(manifest_paths, force=force)
        job_id = submitted["job_id"]
        deadline = time.monotonic() + float(settings.RAY_JOB_TIMEOUT_SECONDS)
        while time.monotonic() < deadline:
            current = self.status(job_id)
            value = current["status"]
            if value == "succeeded":
                return current
            if value in {"failed", "stopped", "stopping"}:
                raise AnnualReportRayError(f"Ray annual-report job {job_id} ended with status={value}")
            time.sleep(max(1.0, float(settings.RAY_JOB_POLL_SECONDS)))
        raise AnnualReportRayError(
            f"Ray annual-report job {job_id} exceeded RAY_JOB_TIMEOUT_SECONDS={settings.RAY_JOB_TIMEOUT_SECONDS}"
        )
