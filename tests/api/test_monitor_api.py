from __future__ import annotations

from datetime import UTC, date, datetime

from fastapi.testclient import TestClient

from backend.api.monitor import get_monitor_service, get_monitor_store
from backend.main import app
from backend.schemas.monitor import MonitorReport, MonitorReportPage, MonitorRunAccepted


class FakeStore:
    def __init__(self) -> None:
        now = datetime(2026, 10, 23, 12, tzinfo=UTC)
        self.report = MonitorReport(
            report_id="00000000-0000-0000-0000-000000000002",
            run_id="00000000-0000-0000-0000-000000000001",
            symbol="600519.SH",
            name="贵州茅台",
            trading_date=date(2026, 10, 23),
            data_cutoff_at=now,
            data_status="degraded",
            status="degraded",
            analysis="数据不足。",
            missing_data=["Gold stale"],
            generated_at=now,
        )

    def list_reports(self, **kwargs):
        return [self.report], 1

    def get_report(self, report_id):
        return self.report if report_id == self.report.report_id else None


class FakeService:
    def create_run(self, request):
        return MonitorRunAccepted(
            run_id="00000000-0000-0000-0000-000000000001",
            status="queued",
        )


def test_monitor_run_and_report_apis() -> None:
    app.dependency_overrides[get_monitor_store] = lambda: FakeStore()
    app.dependency_overrides[get_monitor_service] = lambda: FakeService()
    client = TestClient(app)
    try:
        accepted = client.post("/api/monitor/runs", json={})
        assert accepted.status_code == 202
        assert accepted.json()["status"] == "queued"
        reports = client.get("/api/monitor/reports")
        assert reports.status_code == 200
        assert reports.json()["items"][0]["status"] == "degraded"
        detail = client.get("/api/monitor/reports/00000000-0000-0000-0000-000000000002")
        assert detail.status_code == 200
        assert detail.json()["recommendation_5d"] is None
    finally:
        app.dependency_overrides.clear()
