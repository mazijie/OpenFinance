from __future__ import annotations

from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from backend.schemas.annual_report import AnnualReportAnnouncement, AnnualReportBronzeObject, AnnualReportRecord
from backend.services.annual_report_service import AnnualReportService


NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _manifest(*, announcement_id: str = "announcement-1") -> AnnualReportBronzeObject:
    return AnnualReportBronzeObject(
        ingested_at=NOW,
        announcement=AnnualReportAnnouncement(
            announcement_id=announcement_id,
            stock_code="600000",
            exchange="SH",
            company_name="测试公司",
            report_year=2024,
            title="测试公司2024年年度报告",
            announcement_time=NOW,
            pdf_url="https://example.test/report.pdf",
        ),
        pdf_sha256="a" * 64,
        pdf_size=10,
        pdf_object_path=f"reports/{announcement_id}.pdf",
    )


class FakeBronze:
    def __init__(self, manifest: AnnualReportBronzeObject) -> None:
        self.manifest = manifest

    def list_objects(self, *, prefix: str) -> list[str]:
        return ["reports/provider=cninfo/stock=600000/report_year=2024/announcement_id=announcement-1/metadata.json"]

    def read_json(self, path: str) -> dict[str, object]:
        return self.manifest.model_dump(mode="json")

    def object_exists(self, path: str) -> bool:
        return True

    def read_bytes(self, path: str) -> bytes:
        return b"%PDF-test"


class FakeWriter:
    def __init__(self, rows: list[AnnualReportRecord] | None = None) -> None:
        self.rows = rows or []

    def scan_reports(self) -> list[AnnualReportRecord]:
        return self.rows

    def scan_parse_runs(self, report_id: str | None = None):
        return []


def test_catalog_lists_bronze_pdf_before_silver_parse() -> None:
    service = AnnualReportService(writer=FakeWriter(), bronze_writer=FakeBronze(_manifest()))

    result = service.list_reports(stock_code=None, report_year=None, page=1, page_size=20)

    assert result.total == 1
    assert result.items[0].extraction_status == "pending_parse"
    assert result.items[0].page_count == 0
    assert result.items[0].pdf_url.endswith("/download")
    assert service.read_pdf(result.items[0].report_id) == b"%PDF-test"


def test_catalog_prefers_silver_extraction_status_when_available() -> None:
    manifest = _manifest()
    silver = AnnualReportRecord(
        report_id=uuid5(NAMESPACE_URL, f"cninfo:{manifest.announcement.announcement_id}:{manifest.pdf_sha256}").__str__(),
        stock_code="600000",
        exchange="SH",
        company_name="测试公司",
        report_year=2024,
        announcement_id="announcement-1",
        title="测试公司2024年年度报告",
        announcement_time=NOW,
        pdf_sha256=manifest.pdf_sha256,
        page_count=100,
        extraction_status="needs_review",
        pdf_object_path=manifest.pdf_object_path,
        parse_run_id="parse-run-1",
        ingested_at=NOW,
    )
    service = AnnualReportService(writer=FakeWriter([silver]), bronze_writer=FakeBronze(manifest))

    result = service.list_reports(stock_code=None, report_year=None, page=1, page_size=20)

    assert result.items[0].extraction_status == "needs_review"
    assert result.items[0].page_count == 100
