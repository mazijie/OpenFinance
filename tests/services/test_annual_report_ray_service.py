from __future__ import annotations

import sys
import types


def test_runtime_env_carries_manifest_batch_and_storage_settings(monkeypatch) -> None:
    from backend.config import settings
    from backend.services.annual_report_ray_service import AnnualReportRayService

    monkeypatch.setattr(settings, "RAY_RUNTIME_WORKING_DIR", "/workspace/openfinance")
    monkeypatch.setattr(settings, "RAY_DATABASE_URL", "postgresql://ray-db:5432/test")
    monkeypatch.setattr(settings, "RAY_MINIO_ENDPOINT", "ray-minio:9000")
    monkeypatch.setattr(settings, "RAY_ICEBERG_CATALOG_URI", "postgresql+psycopg://ray-db:5432/test")
    monkeypatch.setattr(settings, "RAY_ICEBERG_WAREHOUSE", "s3://ray-warehouse/")

    runtime = AnnualReportRayService()._runtime_env(["reports/a/metadata.json"], force=True)

    assert runtime["working_dir"] == "/workspace/openfinance"
    assert runtime["env_vars"]["ANNUAL_REPORT_RAY_MANIFESTS"] == '["reports/a/metadata.json"]'
    assert runtime["env_vars"]["ANNUAL_REPORT_RAY_FORCE"] == "true"
    assert runtime["env_vars"]["DATABASE_URL"] == "postgresql://ray-db:5432/test"
    assert runtime["env_vars"]["MINIO_ENDPOINT"] == "ray-minio:9000"
    assert runtime["env_vars"]["ICEBERG_CATALOG_URI"] == "postgresql+psycopg://ray-db:5432/test"
    assert runtime["env_vars"]["ICEBERG_WAREHOUSE"] == "s3://ray-warehouse/"
    assert runtime["env_vars"]["ICEBERG_ANNUAL_REPORT_TABLE"] == settings.ICEBERG_ANNUAL_REPORT_TABLE
    assert runtime["env_vars"]["ICEBERG_ANNUAL_REPORT_SECTION_TABLE"] == settings.ICEBERG_ANNUAL_REPORT_SECTION_TABLE
    assert "pip" not in runtime


def test_submit_rejects_missing_remote_storage_before_ray_job(monkeypatch) -> None:
    from backend.config import settings
    from backend.services.annual_report_ray_service import AnnualReportRayError, AnnualReportRayService

    monkeypatch.setattr(settings, "RAY_DATABASE_URL", "")
    monkeypatch.setattr(settings, "RAY_MINIO_ENDPOINT", "")
    monkeypatch.setattr(settings, "RAY_ICEBERG_CATALOG_URI", "")
    monkeypatch.setattr(settings, "RAY_ICEBERG_WAREHOUSE", "")

    try:
        AnnualReportRayService().submit(["reports/a/metadata.json"])
    except AnnualReportRayError as exc:
        assert "RAY_DATABASE_URL" in str(exc)
        assert "RAY_MINIO_ENDPOINT" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("missing remote storage must reject submission")


def test_runtime_env_can_install_explicit_packages(monkeypatch) -> None:
    from backend.config import settings
    from backend.services.annual_report_ray_service import AnnualReportRayService

    monkeypatch.setattr(settings, "RAY_DATABASE_URL", "postgresql://ray-db:5432/test")
    monkeypatch.setattr(settings, "RAY_MINIO_ENDPOINT", "ray-minio:9000")
    monkeypatch.setattr(settings, "RAY_ICEBERG_CATALOG_URI", "postgresql+psycopg://ray-db:5432/test")
    monkeypatch.setattr(settings, "RAY_ICEBERG_WAREHOUSE", "s3://ray-warehouse/")
    monkeypatch.setattr(settings, "RAY_RUNTIME_PIP_PACKAGES_JSON", '["pydantic-settings==2.10.1", "minio"]')
    monkeypatch.setattr(settings, "RAY_RUNTIME_PIP_EXTRA_INDEX_URL", "https://example.invalid/paddle")

    runtime = AnnualReportRayService()._runtime_env(["reports/a/metadata.json"], force=False)

    assert runtime["pip"] == {"packages": ["pydantic-settings==2.10.1", "minio"], "pip_check": False}
    assert runtime["env_vars"]["PIP_EXTRA_INDEX_URL"] == "https://example.invalid/paddle"


def test_submit_and_status_use_ray_jobs_client(monkeypatch) -> None:
    from backend.config import settings
    from backend.services.annual_report_ray_service import AnnualReportRayService

    class FakeClient:
        last_submission: dict[str, object] = {}

        def __init__(self, dashboard_url: str) -> None:
            assert dashboard_url == "http://ray-head:8265"

        def submit_job(self, **kwargs):
            FakeClient.last_submission = kwargs
            return "raysubmit-1"

        def get_job_status(self, job_id: str):
            assert job_id == "raysubmit-1"
            return types.SimpleNamespace(value="SUCCEEDED")

    ray_module = types.ModuleType("ray")
    submission_module = types.ModuleType("ray.job_submission")
    submission_module.JobSubmissionClient = FakeClient
    ray_module.job_submission = submission_module
    monkeypatch.setitem(sys.modules, "ray", ray_module)
    monkeypatch.setitem(sys.modules, "ray.job_submission", submission_module)
    monkeypatch.setenv("RAY_ADDRESS", "auto")
    monkeypatch.setattr(settings, "RAY_DASHBOARD_URL", "http://ray-head:8265")
    monkeypatch.setattr(settings, "RAY_DATABASE_URL", "postgresql://ray-db:5432/test")
    monkeypatch.setattr(settings, "RAY_MINIO_ENDPOINT", "ray-minio:9000")
    monkeypatch.setattr(settings, "RAY_ICEBERG_CATALOG_URI", "postgresql+psycopg://ray-db:5432/test")
    monkeypatch.setattr(settings, "RAY_ICEBERG_WAREHOUSE", "s3://ray-warehouse/")

    service = AnnualReportRayService()
    submitted = service.submit(["reports/a/metadata.json"])
    status = service.status(submitted["job_id"])

    assert submitted == {"job_id": "raysubmit-1", "status": "submitted"}
    assert status == {"job_id": "raysubmit-1", "status": "succeeded"}
    assert FakeClient.last_submission["entrypoint"] == "python -m backend.ray_annual_reports"
