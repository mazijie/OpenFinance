from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.ingestion.jobs import ingest_annual_reports
from backend.schemas.annual_report import AnnualReportAnnouncement


def test_bronze_repair_refuses_a_replacement_pdf_with_a_different_hash(monkeypatch):
    announcement = AnnualReportAnnouncement(
        announcement_id="announcement-1",
        stock_code="600519.SH",
        exchange="SH",
        report_year=2024,
        title="年报",
        announcement_time=datetime(2025, 4, 1, tzinfo=UTC),
        pdf_url="https://example.test/report.pdf",
    )

    class Writer:
        write_called = False

        def read_json(self, path):
            return {
                "ingested_at": datetime(2025, 4, 2, tzinfo=UTC).isoformat(),
                "announcement": announcement.model_dump(mode="json"),
                "pdf_sha256": "a" * 64,
                "pdf_size": 10,
                "pdf_object_path": "reports/report.pdf",
            }

        def object_exists(self, path):
            return False

        def write_annual_report(self, **kwargs):
            self.write_called = True
            raise AssertionError("mismatched PDF must not overwrite Bronze")

    class Collector:
        def list_annual_reports(self, **kwargs):
            return [announcement]

        def download_pdf(self, report):
            return b"replacement PDF"

        def close(self):
            pass

    writer = Writer()
    monkeypatch.setattr(ingest_annual_reports, "BronzeWriter", lambda: writer)
    monkeypatch.setattr(ingest_annual_reports, "CNInfoCollector", Collector)

    with pytest.raises(ValueError, match="expected SHA-256"):
        ingest_annual_reports.repair_annual_report_bronze(
            "reports/provider=cninfo/stock=600519.SH/report_year=2024/announcement_id=announcement-1/metadata.json",
            report_id="3d063848-a819-5e75-829b-22f0fe961a76",
            pdf_sha256="a" * 64,
        )

    assert writer.write_called is False
