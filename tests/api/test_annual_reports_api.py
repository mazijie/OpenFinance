from __future__ import annotations

from fastapi.testclient import TestClient

from backend.api.annual_reports import get_annual_report_service
from backend.main import app


class FakeAnnualReportService:
    def read_pdf(self, report_id: str) -> bytes:
        if report_id == "missing":
            raise LookupError("Annual report not found")
        return b"%PDF-1.7 test pdf"


def test_download_report_serves_pdf_without_redirecting_to_object_storage() -> None:
    app.dependency_overrides[get_annual_report_service] = FakeAnnualReportService
    try:
        response = TestClient(app).get("/api/reports/report-1/download", follow_redirects=False)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/pdf"
    assert response.headers["content-disposition"] == 'inline; filename="annual-report.pdf"'
    assert "location" not in response.headers
    assert response.content == b"%PDF-1.7 test pdf"


def test_download_report_returns_404_for_unknown_report() -> None:
    app.dependency_overrides[get_annual_report_service] = FakeAnnualReportService
    try:
        response = TestClient(app).get("/api/reports/missing/download")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
